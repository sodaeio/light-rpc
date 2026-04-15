use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use futures::StreamExt;
use richat_client::grpc::ConfigGrpcClient;
use richat_proto::geyser::{
    subscribe_update::UpdateOneof, SlotStatus as ProtoSlotStatus, SubscribeUpdate,
};
use richat_proto::richat::GrpcSubscribeRequest;
use solana_pubkey::Pubkey;
use solana_signature::Signature;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::config::SourceConfig;
use crate::metrics;
use crate::types::*;

use super::commitment::CommitmentTracker;

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
        BlockWithData {
            info: BlockInfo {
                slot,
                parent_slot: self.parent_slot,
                block_time: self.block_time,
                block_height: self.block_height,
                blockhash: self.blockhash.unwrap_or_default(),
            },
            encoded_block: self.encoded_block.unwrap_or_default(),
            transactions: self.transactions.into_values().collect(),
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

    /// Run the stream source with automatic reconnection.
    pub async fn run(self) -> Result<()> {
        let mut backoff = Duration::from_secs(1);
        let max_backoff = Duration::from_secs(60);

        loop {
            info!(endpoint = %self.config.endpoint, "connecting to gRPC source");

            match self.run_stream().await {
                Ok(()) => {
                    info!("stream ended cleanly, reconnecting");
                    backoff = Duration::from_secs(1);
                }
                Err(e) => {
                    error!(error = %e, "stream error, reconnecting in {:?}", backoff);
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(max_backoff);
                }
            }
        }
    }

    async fn run_stream(&self) -> Result<()> {
        let grpc_config = ConfigGrpcClient {
            endpoint: self.config.endpoint.clone(),
            x_token: self.config.x_token.as_ref().map(|t| t.as_bytes().to_vec()),
            max_decoding_message_size: self.config.max_message_size,
            connect_timeout: Some(Duration::from_secs(self.config.connect_timeout_secs)),
            timeout: Some(Duration::from_secs(self.config.request_timeout_secs)),
            tcp_nodelay: true,
            tcp_keepalive: Some(Duration::from_secs(15)),
            keep_alive_while_idle: true,
            ..Default::default()
        };

        let mut client = grpc_config
            .connect()
            .await
            .context("failed to connect to gRPC source")?;

        let stream = client
            .subscribe_richat(GrpcSubscribeRequest {
                replay_from_slot: None,
                filter: None,
            })
            .await
            .context("failed to subscribe")?
            .into_parsed();

        tokio::pin!(stream);
        let mut tracker = CommitmentTracker::new();
        let mut accumulators: BTreeMap<Slot, SlotAccumulator> = BTreeMap::new();
        let mut stats_interval = tokio::time::interval(Duration::from_secs(10));
        let mut blocks_count: u64 = 0;
        let mut accounts_count: u64 = 0;
        let mut txs_count: u64 = 0;

        loop {
            tokio::select! {
                msg = stream.next() => {
                    let update: SubscribeUpdate = match msg {
                        Some(Ok(update)) => update,
                        Some(Err(e)) => return Err(e.into()),
                        None => return Ok(()),
                    };

                    match update.update_oneof {
                        Some(UpdateOneof::Account(account_update)) => {
                            if let Some(account) = account_update.account {
                                let pubkey = Pubkey::try_from(account.pubkey.as_slice())
                                    .unwrap_or_default();
                                let owner = Pubkey::try_from(account.owner.as_slice())
                                    .unwrap_or_default();

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

                                if self.sink.send(SourceMessage::AccountUpdate(update)).await.is_err() {
                                    return Ok(());
                                }
                            }
                        }

                        Some(UpdateOneof::Transaction(tx_update)) => {
                            if let Some(tx_info) = tx_update.transaction {
                                let slot = tx_update.slot;
                                let sig = Signature::try_from(tx_info.signature.as_slice())
                                    .unwrap_or_default();

                                let acc = accumulators
                                    .entry(slot)
                                    .or_insert_with(|| SlotAccumulator::new(0));

                                let err_msg = tx_info.meta.as_ref()
                                    .and_then(|m| m.err.as_ref())
                                    .map(|e| format!("{:?}", e));

                                // Use yellowstone's prost export (ours is a different version).
                                let payload = {
                                    use yellowstone_grpc_proto::prost::Message;
                                    let mut buf = Vec::with_capacity(tx_info.encoded_len());
                                    let _ = tx_info.encode(&mut buf);
                                    bytes::Bytes::from(buf)
                                };
                                let tx_idx = tx_info.index as u32;

                                acc.transactions.insert(sig, TransactionEntry {
                                    signature: sig,
                                    tx_index: tx_idx,
                                    err: err_msg.clone(),
                                    payload,
                                });

                                // Index address → signature for getSignaturesForAddress
                                if let Some(tx_msg) = &tx_info.transaction {
                                    if let Some(msg) = &tx_msg.message {
                                        for key in &msg.account_keys {
                                            if let Ok(pk) = Pubkey::try_from(key.as_slice()) {
                                                acc.address_signatures
                                                    .entry(pk)
                                                    .or_default()
                                                    .push(SignatureEntry {
                                                        signature: sig,
                                                        err: err_msg.clone(),
                                                        memo: None,
                                                    });
                                            }
                                        }
                                    }
                                }

                                if let Some(meta) = &tx_info.meta {
                                    acc.fees.push(meta.fee);
                                }

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
                            acc.expected_tx_count = Some(block_meta.executed_transaction_count as usize);

                            tracker.set_processed(slot);
                            metrics::LATEST_SLOT
                                .with_label_values(&["processed"])
                                .set(tracker.processed_slot() as i64);

                            // Check if this block is now complete
                            if acc.is_complete() {
                                acc.sealed = true;
                                let block_data = accumulators.remove(&slot)
                                    .unwrap()
                                    .into_block(slot);
                                let block = Arc::new(block_data);

                                blocks_count += 1;
                                metrics::INGESTED_BLOCKS.inc();

                                if self.sink.send(SourceMessage::Block { slot, block }).await.is_err() {
                                    return Ok(());
                                }
                            }

                            // Send slot status
                            let _ = self.sink.send(SourceMessage::SlotStatus {
                                slot,
                                parent_slot: Some(block_meta.parent_slot),
                                status: SlotStatus::ProcessedOrSkipped,
                            }).await;
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

                                    // GC old accumulators below finalized
                                    let finalized = tracker.finalized_slot();
                                    accumulators = accumulators.split_off(&finalized.saturating_sub(32));

                                    SlotStatus::Finalized
                                }
                                _ => SlotStatus::ProcessedOrSkipped,
                            };

                            if self.sink.send(SourceMessage::SlotStatus {
                                slot,
                                parent_slot: Some(slot_update.parent.unwrap_or(0)),
                                status,
                            }).await.is_err() {
                                return Ok(());
                            }
                        }

                        Some(UpdateOneof::Ping(_)) | Some(UpdateOneof::Pong(_)) => {}

                        _ => {}
                    }
                }

                _ = stats_interval.tick() => {
                    info!(
                        blocks = blocks_count,
                        accounts = accounts_count,
                        transactions = txs_count,
                        processed = tracker.processed_slot(),
                        confirmed = tracker.confirmed_slot(),
                        finalized = tracker.finalized_slot(),
                        accumulators = accumulators.len(),
                        "stream stats"
                    );
                }
            }
        }
    }
}
