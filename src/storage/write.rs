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

/// Source →[mpsc]→ StorageWriter →[broadcast]→ StorageReaders.
pub struct StorageWriter {
    rocks: UnifiedRocksDb,
    files: BlockFileStorage,
    pg_tx: mpsc::Sender<PgWriteJob>,
    broadcast_tx: broadcast::Sender<WriteToReadMessage>,
    #[cfg(feature = "clickhouse")]
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
            #[cfg(feature = "clickhouse")]
            ch_tx: None,
        }
    }

    #[cfg(feature = "clickhouse")]
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

        // tx_index value: see rpc::tx_format for the layout.
        let block_time = block.info.block_time.unwrap_or(0);
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

            let sig_bytes: &[u8; 64] = tx.signature.as_ref().try_into().unwrap_or(&[0u8; 64]);
            if let Err(e) = self.rocks.put_tx_index(sig_bytes, &value) {
                error!(slot, error = %e, "failed to write tx index");
            }
        }

        // address → signatures (RocksDB). bincode is ~5× faster than serde_json
        // to deserialize on read; the value is hot in gSFA / gTFA.
        let mut sfa_entries = Vec::new();
        for (address, sigs) in &block.address_signatures {
            let data = bincode::serialize(sigs).unwrap_or_default();
            sfa_entries.push((*address, slot, data));
        }
        if !sfa_entries.is_empty() {
            if let Err(e) = self.rocks.put_sfa_batch(&sfa_entries) {
                error!(slot, error = %e, "failed to write sfa index");
            }
        }

        // gSFA/gTFA are served from sfa_index + owner_atas; PG write dropped.

        #[cfg(feature = "clickhouse")]
        self.send_to_clickhouse(slot, block);

        let _ = self.broadcast_tx.send(WriteToReadMessage::NewBlock {
            slot,
            block: Arc::clone(block),
        });

        let elapsed = start.elapsed();
        metrics::STORAGE_WRITE_LATENCY.observe(elapsed.as_secs_f64());
        debug!(slot, elapsed_ms = elapsed.as_millis(), "block persisted");
    }

    #[cfg(feature = "clickhouse")]
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
                let sig_bytes: [u8; 64] = sig
                    .signature
                    .as_ref()
                    .try_into()
                    .unwrap_or([0u8; 64]);
                sig_addrs.entry(sig_bytes).or_default().push(addr_bytes);
            }
        }

        let block_time = block.info.block_time.unwrap_or(0);
        let mut rows: Vec<TransactionRow> = Vec::with_capacity(block.transactions.len());
        for (idx, tx) in block.transactions.iter().enumerate() {
            let sig_bytes: [u8; 64] = tx
                .signature
                .as_ref()
                .try_into()
                .unwrap_or([0u8; 64]);
            let addresses = sig_addrs
                .remove(&sig_bytes)
                .unwrap_or_default()
                .into_iter()
                .map(|a| Pubkey32(a.to_vec()))
                .collect::<Vec<_>>();
            let addr_count = addresses.len();
            let fee = block.fees.get(idx).copied().unwrap_or(0);
            rows.push(TransactionRow {
                slot,
                tx_index: idx as u32,
                signature: Sig64(sig_bytes.to_vec()),
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
            tracing::warn!(slot, "clickhouse channel full, dropping batch");
        }
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

        // Dedup by pubkey, keep highest slot.
        buffer.sort_by(|a, b| a.pubkey.cmp(&b.pubkey).then(b.slot.cmp(&a.slot)));
        buffer.dedup_by_key(|u| u.pubkey);

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
            let pubkeys: Vec<[u8; 32]> = prog_refs.iter().map(|u| u.pubkey.to_bytes()).collect();
            let _ = self
                .broadcast_tx
                .send(WriteToReadMessage::AccountsUpdated { pubkeys });
        }

        // Token mints → PG (via bounded channel, non-blocking)
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
            let _ = self.pg_tx.try_send(PgWriteJob::TokenMints(owned));
        }

        // Token accounts → PG (via bounded channel, non-blocking)
        if !ta_updates.is_empty() {
            // Index owner → token_account for gTFA. SPL Token Account layout
            // puts the owner wallet at bytes [32..64] of account data and the
            // amount at [64..72] LE.
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
                    error!(error = %e, "failed to write owner_atas");
                }
            }
            for (mint, updates) in by_mint {
                if let Err(e) = self.rocks.update_mint_top_holders(&mint, &updates) {
                    error!(error = %e, "failed to update mint_top_holders");
                }
            }
            let pubkeys: Vec<[u8; 32]> =
                ta_updates.iter().map(|u| u.pubkey.to_bytes()).collect();
            let _ = self
                .broadcast_tx
                .send(WriteToReadMessage::AccountsUpdated { pubkeys });
            let owned: Vec<AccountUpdate> = ta_updates.into_iter().cloned().collect();
            let _ = self.pg_tx.try_send(PgWriteJob::TokenAccounts(owned));
        }
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
