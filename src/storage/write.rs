use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::Result;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};

use crate::metrics;
use crate::types::*;

use super::accounts::AccountProcessor;
use super::files::BlockFileStorage;
use super::postgres::PgStorage;
use super::rocks::UnifiedRocksDb;

/// Throttle corruption-error logs to ~once per 30s. RocksDB MANIFEST may
/// reference a quarantined SST until a future compaction reorganizes the
/// affected level; until then every write retries the same dangling
/// reference, which would otherwise flood the log.
static LAST_CORRUPTION_LOG: AtomicU64 = AtomicU64::new(0);
fn should_log_corruption() -> bool {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let last = LAST_CORRUPTION_LOG.load(Ordering::Relaxed);
    if now.saturating_sub(last) < 30 {
        return false;
    }
    LAST_CORRUPTION_LOG
        .compare_exchange(last, now, Ordering::Relaxed, Ordering::Relaxed)
        .is_ok()
}

/// Source →[mpsc]→ StorageWriter →[broadcast]→ StorageReaders.
pub struct StorageWriter {
    rocks: UnifiedRocksDb,
    files: BlockFileStorage,
    pg_tx: mpsc::Sender<PgWriteJob>,
    broadcast_tx: broadcast::Sender<WriteToReadMessage>,
    ch_tx: Option<mpsc::Sender<super::clickhouse::ClickHouseWriteJob>>,
}

pub enum PgWriteJob {
    TokenMints(Vec<AccountUpdate>),
    TokenAccounts(Vec<AccountUpdate>),
    AddressTransactions(Vec<AddressTxEntry>),
    SlotUpdate(Slot),
}

pub struct AddressTxEntry {
    pub address: solana_pubkey::Pubkey,
    pub slot: Slot,
    pub signature: solana_signature::Signature,
    pub block_time: Option<UnixTimestamp>,
    pub err: Option<String>,
}

impl StorageWriter {
    pub fn new(
        rocks: UnifiedRocksDb,
        files: BlockFileStorage,
        pg_tx: mpsc::Sender<PgWriteJob>,
        broadcast_tx: broadcast::Sender<WriteToReadMessage>,
    ) -> Self {
        Self {
            rocks,
            files,
            pg_tx,
            broadcast_tx,
            ch_tx: None,
        }
    }

    pub fn with_clickhouse(
        mut self,
        ch_tx: mpsc::Sender<super::clickhouse::ClickHouseWriteJob>,
    ) -> Self {
        self.ch_tx = Some(ch_tx);
        self
    }

    pub async fn run(self, mut source_rx: mpsc::Receiver<SourceMessage>) -> Result<()> {
        info!("storage writer started");

        let mut account_buffer: Vec<AccountUpdate> = Vec::with_capacity(1024);
        let mut last_account_flush = Instant::now();
        let flush_interval = Duration::from_millis(200);
        let max_batch_size = 1000;

        loop {
            tokio::select! {
                msg = source_rx.recv() => {
                    match msg {
                        Some(SourceMessage::Block { slot, block }) => {
                            self.handle_block(slot, &block).await;
                        }

                        Some(SourceMessage::SlotStatus { slot, parent_slot, status }) => {
                            self.handle_slot_status(slot, parent_slot, status).await;
                        }

                        Some(SourceMessage::AccountUpdate(update)) => {
                            account_buffer.push(update);

                            if account_buffer.len() >= max_batch_size
                                || last_account_flush.elapsed() >= flush_interval
                            {
                                self.flush_accounts(&mut account_buffer).await;
                                last_account_flush = Instant::now();
                            }
                        }

                        None => {
                            info!("source channel closed, writer shutting down");
                            if !account_buffer.is_empty() {
                                self.flush_accounts(&mut account_buffer).await;
                            }
                            return Ok(());
                        }
                    }
                }

                _ = tokio::time::sleep(flush_interval) => {
                    if !account_buffer.is_empty()
                        && last_account_flush.elapsed() >= flush_interval
                    {
                        self.flush_accounts(&mut account_buffer).await;
                        last_account_flush = Instant::now();
                    }
                }
            }
        }
    }

    async fn handle_block(&self, slot: Slot, block: &Arc<BlockWithData>) {
        let start = Instant::now();

        if !block.encoded_block.is_empty() {
            if let Err(e) = self.files.write_block(slot, &block.encoded_block) {
                error!(slot, error = %e, "failed to write block file");
            }
        }

        let slot_data = serde_json::to_vec(&serde_json::json!({
            "block_time": block.info.block_time,
            "block_height": block.info.block_height,
            "blockhash": block.info.blockhash,
            "parent_slot": block.info.parent_slot,
        }))
        .unwrap_or_default();

        if let Err(e) = self.rocks.put_slot_index(slot, &slot_data) {
            let s = e.to_string();
            if !self.rocks.try_quarantine_corrupt_sst(&s, "slot_index")
                && should_log_corruption()
            {
                error!(slot, error = %s, "failed to write slot index");
            }
        }

        // tx_index value layout: see rpc::tx_format.
        let block_time = block.info.block_time.unwrap_or(0);
        let mut tx_entries: Vec<([u8; 64], Vec<u8>)> = Vec::with_capacity(block.transactions.len());
        for tx in &block.transactions {
            let err_bytes = tx.err.as_deref().map(|s| s.as_bytes()).unwrap_or(&[]);
            let err_len = err_bytes.len().min(u8::MAX as usize) as u8;
            let mut value = Vec::with_capacity(21 + err_len as usize + tx.payload.len());
            value.extend_from_slice(&slot.to_le_bytes());
            value.extend_from_slice(&tx.tx_index.to_le_bytes());
            value.extend_from_slice(&block_time.to_le_bytes());
            value.push(err_len);
            value.extend_from_slice(&err_bytes[..err_len as usize]);
            value.extend_from_slice(&tx.payload);

            let sig_bytes: [u8; 64] = tx.signature.as_ref().try_into().unwrap_or([0u8; 64]);
            tx_entries.push((sig_bytes, value));
        }
        if let Err(e) = self.rocks.put_tx_index_batch(&tx_entries) {
            let s = e.to_string();
            if !self.rocks.try_quarantine_corrupt_sst(&s, "tx_index")
                && should_log_corruption()
            {
                error!(slot, error = %s, "failed to write tx index batch");
            }
        }

        // bincode here: ~5× faster to deserialize on the gSFA/gTFA read path.
        let mut sfa_entries = Vec::new();
        for (address, sigs) in &block.address_signatures {
            let data = bincode::serialize(sigs).unwrap_or_default();
            sfa_entries.push((*address, slot, data));
        }
        if !sfa_entries.is_empty() {
            if let Err(e) = self.rocks.put_sfa_batch(&sfa_entries) {
                let s = e.to_string();
                if !self.rocks.try_quarantine_corrupt_sst(&s, "sfa_index")
                    && should_log_corruption()
                {
                    error!(slot, error = %s, "failed to write sfa index");
                }
            }
        }

        self.send_to_clickhouse(slot, block);

        let _ = self.broadcast_tx.send(WriteToReadMessage::NewBlock {
            slot,
            block: Arc::clone(block),
        });

        let elapsed = start.elapsed();
        metrics::STORAGE_WRITE_LATENCY.observe(elapsed.as_secs_f64());
        debug!(slot, elapsed_ms = elapsed.as_millis(), "block persisted");
    }

    fn send_to_clickhouse(&self, slot: Slot, block: &Arc<BlockWithData>) {
        use super::clickhouse::{
            ClickHouseBlockBatch, ClickHouseWriteJob, Pubkey32, Sig64, TransactionRow,
        };

        let Some(ch_tx) = &self.ch_tx else {
            return;
        };

        let mut sig_addrs: std::collections::HashMap<[u8; 64], Vec<[u8; 32]>> =
            std::collections::HashMap::with_capacity(block.transactions.len());
        for (address, sigs) in &block.address_signatures {
            let addr_bytes: [u8; 32] = address.to_bytes();
            for sig in sigs {
                let sig_bytes: [u8; 64] = sig.signature.as_ref().try_into().unwrap_or([0u8; 64]);
                sig_addrs.entry(sig_bytes).or_default().push(addr_bytes);
            }
        }

        let block_time = block.info.block_time.unwrap_or(0);
        let mut rows: Vec<TransactionRow> = Vec::with_capacity(block.transactions.len());
        for (idx, tx) in block.transactions.iter().enumerate() {
            let sig_bytes: [u8; 64] = tx.signature.as_ref().try_into().unwrap_or([0u8; 64]);
            let addresses = sig_addrs
                .remove(&sig_bytes)
                .unwrap_or_default()
                .into_iter()
                .map(Pubkey32)
                .collect::<Vec<_>>();
            let addr_count = addresses.len();
            let fee = block.fees.get(idx).copied().unwrap_or(0);
            rows.push(TransactionRow {
                slot,
                tx_index: idx as u32,
                signature: Sig64(sig_bytes),
                block_time,
                err: tx.err.is_some(),
                fee,
                // TODO: populate from decoded tx payload
                compute_units: 0,
                addresses,
                writable_mask: vec![0u8; addr_count],
                signer_mask: vec![0u8; addr_count],
                log_messages: Vec::new(),
                message: String::new(),
                meta: String::new(),
            });
        }

        let batch = ClickHouseBlockBatch {
            transactions: rows,
            // TODO: populate from decoded tx payload
            token_owner_sigs: Vec::new(),
            program_invocations: Vec::new(),
        };

        if ch_tx.try_send(ClickHouseWriteJob::Block(batch)).is_err() {
            metrics::CLICKHOUSE_DROP.inc();
            tracing::warn!(slot, "clickhouse channel full, dropping batch");
        }
    }

    async fn handle_slot_status(&self, slot: Slot, _parent_slot: Option<Slot>, status: SlotStatus) {
        let msg = match status {
            SlotStatus::Confirmed => {
                if self.pg_tx.try_send(PgWriteJob::SlotUpdate(slot)).is_err() {
                    metrics::PG_DROP.with_label_values(&["slot"]).inc();
                }
                WriteToReadMessage::BlockConfirmed { slot }
            }
            SlotStatus::Finalized => WriteToReadMessage::SlotFinalized { slot },
            SlotStatus::Dead => WriteToReadMessage::BlockDead { slot },
            SlotStatus::ProcessedOrSkipped => return,
        };

        let _ = self.broadcast_tx.send(msg);
    }

    async fn flush_accounts(&self, buffer: &mut Vec<AccountUpdate>) {
        if buffer.is_empty() {
            return;
        }

        // Dedup by pubkey, keep newest (slot, write_version). Same-slot
        // updates need write_version DESC — without it dedup_by_key keeps
        // the earliest intra-slot state (stream order), losing the final
        // value for accounts written multiple times per slot (DEX vaults,
        // fee payers).
        buffer.sort_by(|a, b| {
            a.pubkey
                .cmp(&b.pubkey)
                .then(b.slot.cmp(&a.slot))
                .then(b.write_version.cmp(&a.write_version))
        });
        buffer.dedup_by_key(|u| u.pubkey);

        let mut mint_updates = Vec::new();
        let mut ta_updates = Vec::new();

        let updates = std::mem::take(buffer);
        let mut all_refs: Vec<&AccountUpdate> = Vec::with_capacity(updates.len());
        for update in &updates {
            all_refs.push(update);
            match update.classify() {
                AccountKind::TokenMint => mint_updates.push(update),
                AccountKind::TokenAccount => ta_updates.push(update),
                AccountKind::ProgramAccount => {}
            }
        }

        // Canonical write: every update lands in CF_ACCOUNTS so reads see
        // the stream's latest state. Aggregator paths below are additive.
        if !all_refs.is_empty() {
            match AccountProcessor::write_accounts(&self.rocks, &all_refs) {
                Ok(count) => debug!(count, "wrote accounts to rocksdb"),
                Err(e) => {
                    let s = e.to_string();
                    if !self.rocks.try_quarantine_corrupt_sst(&s, "accounts")
                        && should_log_corruption()
                    {
                        error!(error = %s, "failed to write accounts");
                    }
                }
            }
            let pubkeys: Vec<[u8; 32]> = all_refs.iter().map(|u| u.pubkey.to_bytes()).collect();
            let _ = self
                .broadcast_tx
                .send(WriteToReadMessage::AccountsUpdated { pubkeys });
        }

        if !mint_updates.is_empty() {
            for m in &mint_updates {
                if m.data.len() >= 45 {
                    let _ = self.broadcast_tx.send(WriteToReadMessage::MintUpdated {
                        mint: m.pubkey.to_bytes(),
                        decimals: m.data[44] as i32,
                        slot: m.slot,
                    });
                }
            }
            let owned: Vec<AccountUpdate> = mint_updates.into_iter().cloned().collect();
            if self.pg_tx.try_send(PgWriteJob::TokenMints(owned)).is_err() {
                metrics::PG_DROP.with_label_values(&["mints"]).inc();
            }
        }

        if !ta_updates.is_empty() {
            // SPL Token Account layout: owner at [32..64], amount LE at [64..72].
            let mut atas: Vec<([u8; 32], [u8; 32])> = Vec::with_capacity(ta_updates.len());
            let mut by_mint: std::collections::HashMap<[u8; 32], Vec<(u64, [u8; 32])>> =
                std::collections::HashMap::new();
            for u in &ta_updates {
                if u.data.len() < 72 {
                    continue;
                }
                let mut mint = [0u8; 32];
                mint.copy_from_slice(&u.data[0..32]);
                let mut owner = [0u8; 32];
                owner.copy_from_slice(&u.data[32..64]);
                let amount = u64::from_le_bytes(u.data[64..72].try_into().unwrap());
                atas.push((owner, u.pubkey.to_bytes()));
                by_mint
                    .entry(mint)
                    .or_default()
                    .push((amount, u.pubkey.to_bytes()));
            }
            if !atas.is_empty() {
                if let Err(e) = self.rocks.put_owner_atas_batch(&atas) {
                    let s = e.to_string();
                    if !self.rocks.try_quarantine_corrupt_sst(&s, "owner_atas")
                        && should_log_corruption()
                    {
                        error!(error = %s, "failed to write owner_atas");
                    }
                }
            }
            for (mint, updates) in by_mint {
                if let Err(e) = self.rocks.update_mint_top_holders(&mint, &updates) {
                    let s = e.to_string();
                    if !self
                        .rocks
                        .try_quarantine_corrupt_sst(&s, "mint_top_holders")
                        && should_log_corruption()
                    {
                        error!(error = %s, "failed to update mint_top_holders");
                    }
                }
            }
            let pubkeys: Vec<[u8; 32]> = ta_updates.iter().map(|u| u.pubkey.to_bytes()).collect();
            let _ = self
                .broadcast_tx
                .send(WriteToReadMessage::AccountsUpdated { pubkeys });
            let owned: Vec<AccountUpdate> = ta_updates.into_iter().cloned().collect();
            if self
                .pg_tx
                .try_send(PgWriteJob::TokenAccounts(owned))
                .is_err()
            {
                metrics::PG_DROP.with_label_values(&["token_accounts"]).inc();
            }
        }
    }
}

/// PG writer drain loop. Bounded channel; writers drop on overflow since
/// account writes are last-write-wins.
pub async fn pg_writer_loop(pg: PgStorage, mut rx: mpsc::Receiver<PgWriteJob>) {
    info!("pg writer started");

    while let Some(job) = rx.recv().await {
        let result = match job {
            PgWriteJob::TokenMints(mints) => pg.upsert_token_mints(&mints).await,
            PgWriteJob::TokenAccounts(accounts) => pg.upsert_token_accounts(&accounts).await,
            PgWriteJob::AddressTransactions(entries) => {
                let tuples: Vec<_> = entries
                    .iter()
                    .map(|e| (e.address, e.slot, e.signature, e.block_time, e.err.clone()))
                    .collect();
                pg.insert_address_transactions(&tuples).await
            }
            PgWriteJob::SlotUpdate(slot) => pg.upsert_slot(slot).await,
        };

        if let Err(e) = result {
            error!(error = %e, "pg write failed");
        }
    }

    info!("pg writer shutting down");
}
