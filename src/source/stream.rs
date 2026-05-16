use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use richat_client::grpc::ConfigGrpcClient;
use richat_client::stream::SubscribeStream;
use richat_proto::geyser::{
    subscribe_update::UpdateOneof, SlotStatus as ProtoSlotStatus, SubscribeUpdate,
};
use richat_proto::richat::{GrpcSubscribeRequest, RichatFilter};
use serde_json::Value;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use tokio::sync::mpsc;
use tracing::{error, info, warn};
use yellowstone_grpc_proto::geyser::{
    SubscribeRequest, SubscribeRequestFilterAccounts, SubscribeRequestFilterBlocksMeta,
    SubscribeRequestFilterSlots, SubscribeRequestFilterTransactions,
};

use crate::config::{GrpcMode, SourceConfig};
use crate::metrics;
use crate::types::*;

use super::commitment::CommitmentTracker;

// Shared placeholder; the real agave-shape JSON is built in `into_block`. Sharing the
// Arc skips a per-tx "null" RawValue alloc on arrival.
static PLACEHOLDER_RAW: std::sync::LazyLock<Arc<Box<serde_json::value::RawValue>>> =
    std::sync::LazyLock::new(|| {
        Arc::new(serde_json::value::RawValue::from_string("null".into()).unwrap())
    });

/// Accumulates partial data for an in-progress slot from the gRPC stream.
struct SlotAccumulator {
    parent_slot: Slot,
    block_time: Option<UnixTimestamp>,
    block_height: Option<u64>,
    blockhash: Option<String>,
    transactions: HashMap<Signature, TransactionEntry>,
    address_signatures: HashMap<Pubkey, Vec<SignatureEntry>>,
    encoded_block: Option<bytes::Bytes>,
    fees: Vec<u64>,
    expected_tx_count: Option<usize>,
    sealed: bool,
}

impl SlotAccumulator {
    fn new(parent_slot: Slot) -> Self {
        Self {
            parent_slot,
            block_time: None,
            block_height: None,
            blockhash: None,
            transactions: HashMap::new(),
            address_signatures: HashMap::new(),
            encoded_block: None,
            fees: Vec::new(),
            expected_tx_count: None,
            sealed: false,
        }
    }

    fn is_complete(&self) -> bool {
        if self.sealed {
            return false;
        }
        match self.expected_tx_count {
            Some(expected) => self.transactions.len() >= expected && self.blockhash.is_some(),
            None => false,
        }
    }

    fn into_block(self, slot: Slot) -> BlockWithData {
        use rayon::prelude::*;
        use yellowstone_grpc_proto::prelude::SubscribeUpdateTransactionInfo;
        use yellowstone_grpc_proto::prost::Message;

        // Parallel per-tx JSON build cuts heavy-block seal latency from ~100ms to a few ms.
        let mut txs: Vec<TransactionEntry> = self.transactions.into_values().collect();
        txs.par_iter_mut().for_each(|tx| {
            if let Ok(info) = SubscribeUpdateTransactionInfo::decode(tx.payload.as_ref()) {
                // Single proto decode → prebuilt RawValue + the strings the CH writer stores.
                let mut comp =
                    crate::rpc::tx_format::build_tx_components(0, None, tx.err.clone(), info);
                if let Value::Object(ref mut map) = comp.full {
                    map.remove("slot");
                    map.remove("blockTime");
                }
                if let Ok(raw) = serde_json::value::to_raw_value(&comp.full) {
                    tx.prebuilt = Arc::new(raw);
                }
                tx.message = Arc::from(comp.message);
                tx.meta = Arc::from(comp.meta);
                tx.compute_units = comp.compute_units;
                tx.log_messages = Arc::from(comp.log_messages.into_boxed_slice());
            }
        });

        BlockWithData {
            info: BlockInfo {
                slot,
                parent_slot: self.parent_slot,
                block_time: self.block_time,
                block_height: self.block_height,
                blockhash: self.blockhash.unwrap_or_default(),
            },
            encoded_block: self.encoded_block.unwrap_or_default(),
            transactions: txs,
            address_signatures: self.address_signatures,
            fees: self.fees,
        }
    }
}

pub struct StreamSource {
    config: SourceConfig,
    sink: mpsc::Sender<SourceMessage>,
}

impl StreamSource {
    pub fn new(config: SourceConfig, sink: mpsc::Sender<SourceMessage>) -> Self {
        Self { config, sink }
    }

    /// Run with multi-source failover. Tries primary, then each fallback
    /// in order with backoff between cycles.
    pub async fn run(self) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);

        // Build endpoint list. QUIC mode is single-endpoint (no fallback rotation);
        // gRPC mode rotates primary + fallbacks before snapshot recovery kicks in.
        let endpoints: Vec<String> = if let Some(qc) = &self.config.quic {
            vec![qc.endpoint.clone()]
        } else {
            let mut v = vec![self
                .config
                .endpoint
                .clone()
                .expect("config validated: endpoint or quic must be set")];
            v.extend(self.config.fallback_endpoints.iter().cloned());
            v
        };
        let transport = if self.config.quic.is_some() {
            "quic"
        } else {
            "grpc"
        };
        let mut current_idx = 0;

        loop {
            let endpoint = &endpoints[current_idx % endpoints.len()];
            info!(
                endpoint,
                transport,
                idx = current_idx % endpoints.len(),
                "connecting to source"
            );

            // Stream errors rotate endpoints + backoff. Don't trigger snapshot recovery here:
            // it's a 30+ min destructive op, while gaps usually clear on reconnect.
            match self.run_stream_endpoint(endpoint).await {
                Ok(()) => {
                    info!("stream ended cleanly, reconnecting");
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    error!(error = %e, endpoint, "stream error, reconnecting");
                    current_idx += 1;
                    if current_idx % endpoints.len() == 0 {
                        tokio::time::sleep(backoff).await;
                        backoff = (backoff * 2).min(max_backoff);
                    }
                }
            }
        }
    }

    async fn run_stream_endpoint(&self, endpoint: &str) -> Result<()> {
        let stream: SubscribeStream = if let Some(qc) = &self.config.quic {
            // QUIC firehose. The leaf-shaped config carries its own endpoint;
            // the `endpoint` arg is just for logging in the caller.
            let _ = endpoint;
            let client = tokio::time::timeout(
                Duration::from_secs(self.config.connect_timeout_secs),
                qc.clone().connect(),
            )
            .await
            .map_err(|_| anyhow::anyhow!("QUIC connect timed out"))?
            .context("failed to connect to QUIC source")?;

            tokio::time::timeout(
                Duration::from_secs(self.config.request_timeout_secs),
                client.subscribe(
                    None,
                    Some(RichatFilter {
                        disable_accounts: false,
                        disable_transactions: false,
                        disable_entries: true,
                    }),
                ),
            )
            .await
            .map_err(|_| anyhow::anyhow!("QUIC subscribe timed out"))?
            .context("failed to subscribe to QUIC source")?
            .into_parsed()
        } else {
            let grpc_config = ConfigGrpcClient {
                endpoint: endpoint.to_string(),
                x_token: self.config.x_token.as_ref().map(|t| t.as_bytes().to_vec()),
                max_decoding_message_size: self.config.max_message_size,
                connect_timeout: Some(Duration::from_secs(self.config.connect_timeout_secs)),
                timeout: Some(Duration::from_secs(self.config.request_timeout_secs)),
                tcp_nodelay: true,
                tcp_keepalive: Some(Duration::from_secs(15)),
                keep_alive_while_idle: true,
                ..Default::default()
            };

            let mut client = tokio::time::timeout(
                Duration::from_secs(self.config.connect_timeout_secs),
                grpc_config.connect(),
            )
            .await
            .map_err(|_| anyhow::anyhow!("gRPC connect timed out"))?
            .context("failed to connect to gRPC source")?;

            match self.config.grpc_mode {
                GrpcMode::Yellowstone => {
                    // Dragon's Mouth: empty filter values = "all". Skip entries (unused) and
                    // full blocks (we reconstruct from txs + accounts + blocks_meta).
                    let mut accounts = std::collections::HashMap::new();
                    accounts.insert("all".to_string(), SubscribeRequestFilterAccounts::default());
                    let mut transactions = std::collections::HashMap::new();
                    transactions.insert(
                        "all".to_string(),
                        SubscribeRequestFilterTransactions::default(),
                    );
                    let mut slots = std::collections::HashMap::new();
                    slots.insert("all".to_string(), SubscribeRequestFilterSlots::default());
                    let mut blocks_meta = std::collections::HashMap::new();
                    blocks_meta.insert("all".to_string(), SubscribeRequestFilterBlocksMeta {});

                    let req = SubscribeRequest {
                        accounts,
                        slots,
                        transactions,
                        blocks_meta,
                        ..Default::default()
                    };

                    tokio::time::timeout(
                        Duration::from_secs(self.config.request_timeout_secs),
                        client.subscribe_dragons_mouth_once(req),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("gRPC subscribe timed out"))?
                    .context("failed to subscribe (yellowstone)")?
                    .into_parsed()
                }
                GrpcMode::Richat => {
                    // Richat-native only (richat-plugin-agave, richat-relay). Sesame leaves'
                    // richat path has backpressure quirks; prefer Yellowstone there.
                    tokio::time::timeout(
                        Duration::from_secs(self.config.request_timeout_secs),
                        client.subscribe_richat(GrpcSubscribeRequest {
                            replay_from_slot: None,
                            filter: Some(RichatFilter {
                                disable_accounts: false,
                                disable_transactions: false,
                                disable_entries: true,
                            }),
                        }),
                    )
                    .await
                    .map_err(|_| anyhow::anyhow!("gRPC subscribe timed out"))?
                    .context("failed to subscribe (richat)")?
                    .into_parsed()
                }
            }
        };

        tokio::pin!(stream);
        let mut tracker = CommitmentTracker::new();
        let mut accumulators: BTreeMap<Slot, SlotAccumulator> = BTreeMap::new();
        let mut blocks_count: u64 = 0;
        let mut accounts_count: u64 = 0;
        let mut txs_count: u64 = 0;
        let mut last_block_time = std::time::Instant::now();
        let mut last_stats_time = std::time::Instant::now();
        let gap_threshold = Duration::from_secs(self.config.gap_threshold_secs);
        let mut msg_count: u64 = 0;

        loop {
            // Inline every 10s; a select branch gets starved by the stream.
            if last_stats_time.elapsed() >= Duration::from_secs(10) {
                let gap_secs = last_block_time.elapsed().as_secs();
                info!(
                    blocks = blocks_count,
                    accounts = accounts_count,
                    transactions = txs_count,
                    processed = tracker.processed_slot(),
                    confirmed = tracker.confirmed_slot(),
                    finalized = tracker.finalized_slot(),
                    accumulators = accumulators.len(),
                    gap_secs,
                    msg_count,
                    sink_capacity = self.sink.capacity(),
                    sink_max = self.sink.max_capacity(),
                    "stream stats"
                );
                last_stats_time = std::time::Instant::now();

                if last_block_time.elapsed() > gap_threshold {
                    warn!(gap_secs, "no blocks for {gap_secs}s, reconnecting source");
                    return Err(anyhow::anyhow!("source gap exceeded {gap_secs}s threshold"));
                }
            }

            // Timeout guarantees the loop yields for stats/gap checks under continuous load.
            let msg = tokio::time::timeout(Duration::from_secs(5), stream.next()).await;
            let update: SubscribeUpdate = match msg {
                Ok(Some(Ok(update))) => {
                    msg_count += 1;
                    update
                }
                Ok(Some(Err(e))) => return Err(e.into()),
                Ok(None) => return Ok(()),
                Err(_timeout) => continue, // loops back to inline stats check
            };

            {
                match update.update_oneof {
                    Some(UpdateOneof::Account(account_update)) => {
                        if let Some(account) = account_update.account {
                            let (Ok(pubkey), Ok(owner)) = (
                                Pubkey::try_from(account.pubkey.as_slice()),
                                Pubkey::try_from(account.owner.as_slice()),
                            ) else {
                                tracing::warn!(
                                    slot = account_update.slot,
                                    "malformed account pubkey/owner, skipping"
                                );
                                metrics::SOURCE_MALFORMED.inc();
                                continue;
                            };

                            let update = AccountUpdate {
                                pubkey,
                                slot: account_update.slot,
                                owner,
                                lamports: account.lamports,
                                data: account.data,
                                executable: account.executable,
                                rent_epoch: account.rent_epoch,
                                write_version: account.write_version,
                            };

                            accounts_count += 1;
                            metrics::INGESTED_ACCOUNTS.inc();

                            if self
                                .sink
                                .send(SourceMessage::AccountUpdate(update))
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                        }
                    }

                    Some(UpdateOneof::Transaction(tx_update)) => {
                        if let Some(tx_info) = tx_update.transaction {
                            let slot = tx_update.slot;
                            let Ok(sig) = Signature::try_from(tx_info.signature.as_slice()) else {
                                tracing::warn!(slot, "malformed transaction signature, skipping");
                                metrics::SOURCE_MALFORMED.inc();
                                continue;
                            };

                            let acc = accumulators
                                .entry(slot)
                                .or_insert_with(|| SlotAccumulator::new(0));

                            let err_msg = tx_info
                                .meta
                                .as_ref()
                                .and_then(|m| m.err.as_ref())
                                .map(|e| format!("{:?}", e));
                            let fee = tx_info.meta.as_ref().map(|m| m.fee).unwrap_or(0);
                            let tx_idx = tx_info.index as u32;

                            // Harvest address keys before the proto is consumed by encode().
                            let mut account_pks: Vec<Pubkey> = Vec::new();
                            if let Some(tx_msg) = &tx_info.transaction {
                                if let Some(msg) = &tx_msg.message {
                                    account_pks.reserve(msg.account_keys.len());
                                    for key in &msg.account_keys {
                                        if let Ok(pk) = Pubkey::try_from(key.as_slice()) {
                                            account_pks.push(pk);
                                        }
                                    }
                                }
                            }

                            // agave-shape JSON prebuild is deferred to block-seal (parallel).
                            let payload = {
                                use yellowstone_grpc_proto::prost::Message;
                                let mut buf = Vec::with_capacity(tx_info.encoded_len());
                                let _ = tx_info.encode(&mut buf);
                                bytes::Bytes::from(buf)
                            };

                            acc.transactions.insert(
                                sig,
                                TransactionEntry {
                                    signature: sig,
                                    tx_index: tx_idx,
                                    err: err_msg.clone(),
                                    payload,
                                    prebuilt: PLACEHOLDER_RAW.clone(),
                                    message: Arc::from(""),
                                    meta: Arc::from(""),
                                    compute_units: 0,
                                    log_messages: Arc::from(
                                        Vec::<String>::new().into_boxed_slice(),
                                    ),
                                },
                            );

                            for pk in &account_pks {
                                acc.address_signatures.entry(*pk).or_default().push(
                                    SignatureEntry {
                                        signature: sig,
                                        err: err_msg.clone(),
                                        memo: None,
                                    },
                                );
                            }

                            acc.fees.push(fee);

                            txs_count += 1;
                            metrics::INGESTED_TRANSACTIONS.inc();
                        }
                    }

                    Some(UpdateOneof::BlockMeta(block_meta)) => {
                        let slot = block_meta.slot;
                        let acc = accumulators
                            .entry(slot)
                            .or_insert_with(|| SlotAccumulator::new(block_meta.parent_slot));

                        acc.parent_slot = block_meta.parent_slot;
                        acc.block_time = block_meta.block_time.map(|t| t.timestamp);
                        acc.block_height = block_meta.block_height.map(|h| h.block_height);
                        acc.blockhash = Some(block_meta.blockhash.clone());
                        acc.expected_tx_count =
                            Some(block_meta.executed_transaction_count as usize);

                        tracker.set_processed(slot);
                        metrics::LATEST_SLOT
                            .with_label_values(&["processed"])
                            .set(tracker.processed_slot() as i64);

                        if acc.is_complete() {
                            acc.sealed = true;
                            // Seal is CPU-heavy (~10-50ms via rayon); offload so the worker
                            // keeps reading the stream — otherwise upstream backs up and times out.
                            let accum = accumulators.remove(&slot).unwrap();
                            let block_data =
                                tokio::task::spawn_blocking(move || accum.into_block(slot))
                                    .await
                                    .expect("block seal task");
                            let block = Arc::new(block_data);

                            blocks_count += 1;
                            last_block_time = std::time::Instant::now();
                            metrics::INGESTED_BLOCKS.inc();

                            if self
                                .sink
                                .send(SourceMessage::Block { slot, block })
                                .await
                                .is_err()
                            {
                                return Ok(());
                            }
                        }

                        let _ = self
                            .sink
                            .send(SourceMessage::SlotStatus {
                                slot,
                                parent_slot: Some(block_meta.parent_slot),
                                status: SlotStatus::ProcessedOrSkipped,
                            })
                            .await;
                    }

                    Some(UpdateOneof::Slot(slot_update)) => {
                        let slot = slot_update.slot;
                        let status = match ProtoSlotStatus::try_from(slot_update.status) {
                            Ok(ProtoSlotStatus::SlotConfirmed) => {
                                tracker.set_confirmed(slot);
                                metrics::LATEST_SLOT
                                    .with_label_values(&["confirmed"])
                                    .set(tracker.confirmed_slot() as i64);
                                SlotStatus::Confirmed
                            }
                            Ok(ProtoSlotStatus::SlotFinalized) => {
                                tracker.set_finalized(slot);
                                metrics::LATEST_SLOT
                                    .with_label_values(&["finalized"])
                                    .set(tracker.finalized_slot() as i64);

                                let finalized = tracker.finalized_slot();
                                accumulators =
                                    accumulators.split_off(&finalized.saturating_sub(32));

                                SlotStatus::Finalized
                            }
                            _ => SlotStatus::ProcessedOrSkipped,
                        };

                        if self
                            .sink
                            .send(SourceMessage::SlotStatus {
                                slot,
                                parent_slot: Some(slot_update.parent.unwrap_or(0)),
                                status,
                            })
                            .await
                            .is_err()
                        {
                            return Ok(());
                        }
                    }

                    Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => {}

                    _ => {}
                }
            }
        }
    }
}
