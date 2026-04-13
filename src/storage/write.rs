use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use tokio::sync::{broadcast, mpsc};
use tracing::{debug, error, info};

use crate::metrics;
use crate::types::*;

use super::accounts::AccountProcessor;
use super::files::BlockFileStorage;
use super::postgres::PgStorage;
use super::rocks::UnifiedRocksDb;

/// The write worker consumes from the source pipeline and persists data
/// to RocksDB (blocks + accounts), file storage (block data), and PostgreSQL
/// (tokens). It then broadcasts updates to the read workers.
///
/// Pipeline: Source →[mpsc]→ **StorageWriter** →[broadcast]→ StorageReaders
pub struct StorageWriter {
    rocks: UnifiedRocksDb,
    files: BlockFileStorage,
    pg_tx: mpsc::Sender<PgWriteJob>,
    broadcast_tx: broadcast::Sender<WriteToReadMessage>,
}

/// Jobs sent to the isolated PostgreSQL writer task.
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
        }
    }

    /// Main write loop. Drains messages from the source and persists them.
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

                // Periodic flush for partial account batches
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

        // 1. Write encoded block to file storage
        if !block.encoded_block.is_empty() {
            if let Err(e) = self.files.write_block(slot, &block.encoded_block) {
                error!(slot, error = %e, "failed to write block file");
            }
        }

        // 2. Index slot metadata in RocksDB
        let slot_data = serde_json::to_vec(&serde_json::json!({
            "block_time": block.info.block_time,
            "block_height": block.info.block_height,
            "blockhash": block.info.blockhash,
            "parent_slot": block.info.parent_slot,
        }))
        .unwrap_or_default();

        if let Err(e) = self.rocks.put_slot_index(slot, &slot_data) {
            error!(slot, error = %e, "failed to write slot index");
        }

        // 3. Index transactions
        for tx in &block.transactions {
            let tx_data = serde_json::to_vec(&serde_json::json!({
                "slot": slot,
                "offset": tx.offset,
                "length": tx.length,
                "err": tx.err,
            }))
            .unwrap_or_default();

            let sig_bytes: &[u8; 64] = tx.signature.as_ref().try_into().unwrap_or(&[0u8; 64]);
            if let Err(e) = self.rocks.put_tx_index(sig_bytes, &tx_data) {
                error!(slot, error = %e, "failed to write tx index");
            }
        }

        // 4. Index address → signature mappings in RocksDB
        let mut sfa_entries = Vec::new();
        for (address, sigs) in &block.address_signatures {
            let data = serde_json::to_vec(&sigs).unwrap_or_default();
            sfa_entries.push((*address, slot, data));
        }
        if !sfa_entries.is_empty() {
            if let Err(e) = self.rocks.put_sfa_batch(&sfa_entries) {
                error!(slot, error = %e, "failed to write sfa index");
            }
        }

        // 5. Send address-transaction entries to PG writer (non-blocking)
        let mut addr_tx_entries = Vec::new();
        for (address, sigs) in &block.address_signatures {
            for sig in sigs {
                addr_tx_entries.push(AddressTxEntry {
                    address: *address,
                    slot,
                    signature: sig.signature,
                    block_time: block.info.block_time,
                    err: sig.err.clone(),
                });
            }
        }
        if !addr_tx_entries.is_empty() {
            let _ = self
                .pg_tx
                .try_send(PgWriteJob::AddressTransactions(addr_tx_entries));
        }

        // 6. Broadcast to read workers
        let _ = self.broadcast_tx.send(WriteToReadMessage::NewBlock {
            slot,
            block: Arc::clone(block),
        });

        let elapsed = start.elapsed();
        metrics::STORAGE_WRITE_LATENCY.observe(elapsed.as_secs_f64());
        debug!(slot, elapsed_ms = elapsed.as_millis(), "block persisted");
    }

    async fn handle_slot_status(&self, slot: Slot, _parent_slot: Option<Slot>, status: SlotStatus) {
        let msg = match status {
            SlotStatus::Confirmed => {
                let _ = self.pg_tx.try_send(PgWriteJob::SlotUpdate(slot));
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

        // Deduplicate by pubkey, keeping the highest slot
        buffer.sort_by(|a, b| a.pubkey.cmp(&b.pubkey).then(b.slot.cmp(&a.slot)));
        buffer.dedup_by_key(|u| u.pubkey);

        // Drain and split by type — no cloning, moves ownership
        let mut mint_updates = Vec::new();
        let mut ta_updates = Vec::new();
        let mut prog_refs = Vec::new();

        // Classify: mints and token accounts get moved into PG jobs,
        // program accounts stay as references for RocksDB batch write
        let updates = std::mem::take(buffer);
        for update in &updates {
            match update.classify() {
                AccountKind::TokenMint => mint_updates.push(update),
                AccountKind::TokenAccount => ta_updates.push(update),
                AccountKind::ProgramAccount => prog_refs.push(update),
            }
        }

        // Program accounts → RocksDB (fast, local, synchronous)
        if !prog_refs.is_empty() {
            match AccountProcessor::write_program_accounts(&self.rocks, &prog_refs) {
                Ok(count) => debug!(count, "wrote program accounts to rocksdb"),
                Err(e) => error!(error = %e, "failed to write program accounts"),
            }
        }

        // Token mints → PG (via bounded channel, non-blocking)
        if !mint_updates.is_empty() {
            let owned: Vec<AccountUpdate> = mint_updates.into_iter().cloned().collect();
            let _ = self.pg_tx.try_send(PgWriteJob::TokenMints(owned));
        }

        // Token accounts → PG (via bounded channel, non-blocking)
        if !ta_updates.is_empty() {
            let owned: Vec<AccountUpdate> = ta_updates.into_iter().cloned().collect();
            let _ = self.pg_tx.try_send(PgWriteJob::TokenAccounts(owned));
        }

        drop(updates);
    }
}

/// Isolated PostgreSQL writer task. Runs on its own, drains PgWriteJob
/// messages from the bounded channel. If PG is slow, the channel fills
/// and the write worker drops jobs (accounts are last-write-wins anyway).
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
