use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use parking_lot::RwLock;
use tokio::sync::broadcast;
use tracing::{info, warn};

use crate::metrics;
use crate::types::*;

use super::accounts::StoredAccount;
use super::files::BlockFileStorage;
use super::postgres::PgStorage;
use super::rocks::UnifiedRocksDb;

pub struct MemoryCache {
    blocks: RwLock<BTreeMap<Slot, Arc<BlockWithData>>>,
    recent_blockhashes: RwLock<BTreeMap<Slot, String>>,
    processed_slot: AtomicU64,
    confirmed_slot: AtomicU64,
    finalized_slot: AtomicU64,
}

impl MemoryCache {
    pub fn new() -> Self {
        Self {
            blocks: RwLock::new(BTreeMap::new()),
            recent_blockhashes: RwLock::new(BTreeMap::new()),
            processed_slot: AtomicU64::new(0),
            confirmed_slot: AtomicU64::new(0),
            finalized_slot: AtomicU64::new(0),
        }
    }

    pub fn processed_slot(&self) -> Slot {
        self.processed_slot.load(Ordering::Relaxed)
    }

    pub fn confirmed_slot(&self) -> Slot {
        self.confirmed_slot.load(Ordering::Relaxed)
    }

    pub fn finalized_slot(&self) -> Slot {
        self.finalized_slot.load(Ordering::Relaxed)
    }

    pub fn insert_block(&self, slot: Slot, block: Arc<BlockWithData>) {
        let blockhash = block.info.blockhash.clone();
        self.blocks.write().insert(slot, block);
        self.recent_blockhashes.write().insert(slot, blockhash);

        let current = self.processed_slot.load(Ordering::Relaxed);
        if slot > current {
            self.processed_slot.store(slot, Ordering::Relaxed);
        }
        metrics::MEMORY_CACHED_BLOCKS.set(self.blocks.read().len() as i64);
    }

    pub fn get_block(&self, slot: Slot) -> Option<Arc<BlockWithData>> {
        self.blocks.read().get(&slot).cloned()
    }

    pub fn get_blockhash(&self, slot: Slot) -> Option<String> {
        self.recent_blockhashes.read().get(&slot).cloned()
    }

    pub fn is_blockhash_valid(&self, blockhash: &str) -> bool {
        self.recent_blockhashes.read().values().any(|h| h == blockhash)
    }

    pub fn set_confirmed(&self, slot: Slot) {
        let current = self.confirmed_slot.load(Ordering::Relaxed);
        if slot > current {
            self.confirmed_slot.store(slot, Ordering::Relaxed);
        }
    }

    pub fn set_finalized(&self, slot: Slot) {
        let current = self.finalized_slot.load(Ordering::Relaxed);
        if slot > current {
            self.finalized_slot.store(slot, Ordering::Relaxed);
        }
        self.gc(slot);
    }

    fn gc(&self, finalized: Slot) {
        let cutoff = finalized.saturating_sub(64);
        self.blocks.write().retain(|s, _| *s >= cutoff);

        let mut hashes = self.recent_blockhashes.write();
        hashes.retain(|s, _| *s >= cutoff);
        while hashes.len() > 512 {
            hashes.pop_first();
        }
        drop(hashes);

        metrics::MEMORY_CACHED_BLOCKS.set(self.blocks.read().len() as i64);
    }
}

impl Default for MemoryCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Tiered storage reader. Methods are called directly from RPC handlers.
///
/// Lookup order:
///   1. In-memory cache (sub-ms, zero I/O)
///   2. RocksDB indexes (single disk read)
///   3. Block file storage (LZ4 decompress)
///   4. PostgreSQL (relational token/asset queries)
pub struct StorageReader {
    cache: Arc<MemoryCache>,
    rocks: UnifiedRocksDb,
    files: Arc<BlockFileStorage>,
    pg: PgStorage,
}

impl StorageReader {
    pub fn new(
        cache: Arc<MemoryCache>,
        rocks: UnifiedRocksDb,
        files: Arc<BlockFileStorage>,
        pg: PgStorage,
    ) -> Self {
        Self { cache, rocks, files, pg }
    }

    pub fn cache(&self) -> &Arc<MemoryCache> {
        &self.cache
    }

    pub fn pg(&self) -> &PgStorage {
        &self.pg
    }

    /// Broadcast listener that keeps the memory cache in sync with the writer.
    pub async fn run_cache_updater(
        cache: Arc<MemoryCache>,
        mut rx: broadcast::Receiver<WriteToReadMessage>,
    ) {
        info!("cache updater started");
        loop {
            match rx.recv().await {
                Ok(WriteToReadMessage::NewBlock { slot, block }) => {
                    cache.insert_block(slot, block);
                }
                Ok(WriteToReadMessage::BlockConfirmed { slot }) => {
                    cache.set_confirmed(slot);
                }
                Ok(WriteToReadMessage::SlotFinalized { slot }) => {
                    cache.set_finalized(slot);
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "cache updater lagged");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!("broadcast closed, cache updater exiting");
                    return;
                }
            }
        }
    }

    // --- Block / history methods ---

    pub fn get_slot(&self, commitment: Commitment) -> Slot {
        match commitment {
            Commitment::Processed => self.cache.processed_slot(),
            Commitment::Confirmed => self.cache.confirmed_slot(),
            Commitment::Finalized => self.cache.finalized_slot(),
        }
    }

    pub fn get_block(&self, slot: Slot) -> Result<Option<Arc<BlockWithData>>> {
        if let Some(block) = self.cache.get_block(slot) {
            return Ok(Some(block));
        }

        if let Some(slot_data) = self.rocks.get_slot_index(slot)? {
            if self.files.has_block(slot) {
                let raw = self.files.read_block(slot)?;
                let info: serde_json::Value = serde_json::from_slice(&slot_data)?;
                let block = BlockWithData {
                    info: BlockInfo {
                        slot,
                        parent_slot: info["parent_slot"].as_u64().unwrap_or(0),
                        block_time: info["block_time"].as_i64(),
                        block_height: info["block_height"].as_u64(),
                        blockhash: info["blockhash"].as_str().unwrap_or("").to_string(),
                    },
                    encoded_block: raw.into(),
                    transactions: Vec::new(),
                    address_signatures: Default::default(),
                    fees: Vec::new(),
                };
                return Ok(Some(Arc::new(block)));
            }
        }
        Ok(None)
    }

    pub fn get_block_height(&self, commitment: Commitment) -> Result<Option<u64>> {
        let slot = self.get_slot(commitment);
        if let Some(block) = self.cache.get_block(slot) {
            return Ok(block.info.block_height);
        }
        if let Some(data) = self.rocks.get_slot_index(slot)? {
            let info: serde_json::Value = serde_json::from_slice(&data)?;
            return Ok(info["block_height"].as_u64());
        }
        Ok(None)
    }

    pub fn get_latest_blockhash(&self, commitment: Commitment) -> Option<(String, Slot)> {
        let slot = self.get_slot(commitment);
        self.cache.get_blockhash(slot).map(|h| (h, slot))
    }

    pub fn is_blockhash_valid(&self, blockhash: &str) -> bool {
        self.cache.is_blockhash_valid(blockhash)
    }

    pub fn get_transaction(&self, signature: &[u8; 64]) -> Result<Option<serde_json::Value>> {
        if let Some(data) = self.rocks.get_tx_index(signature)? {
            let info: serde_json::Value = serde_json::from_slice(&data)?;
            return Ok(Some(info));
        }
        Ok(None)
    }

    pub fn get_signatures_for_address(
        &self,
        address: &solana_pubkey::Pubkey,
        before_slot: Option<Slot>,
        limit: usize,
    ) -> Result<Vec<TransactionInfo>> {
        let entries = self.rocks.iter_sfa(address, before_slot, limit)?;
        let mut results = Vec::with_capacity(entries.len());
        for (slot, data) in entries {
            let sigs: Vec<SignatureEntry> = serde_json::from_slice(&data)?;
            for sig in sigs {
                results.push(TransactionInfo {
                    signature: sig.signature,
                    slot,
                    block_time: None,
                    err: sig.err,
                    memo: sig.memo,
                });
            }
        }
        Ok(results)
    }

    // --- Account state methods ---

    pub fn get_account_info(&self, pubkey: &[u8; 32]) -> Result<Option<serde_json::Value>> {
        if let Some(data) = self.rocks.get_account(pubkey)? {
            if let Some(account) = StoredAccount::deserialize(&data) {
                return Ok(Some(serde_json::json!({
                    "lamports": account.lamports,
                    "owner": bs58::encode(&account.owner).into_string(),
                    "data": [bs58::encode(&account.data).into_string(), "base58"],
                    "executable": account.executable,
                    "rentEpoch": account.rent_epoch,
                    "space": account.data.len(),
                })));
            }
        }
        Ok(None)
    }

    pub fn get_program_accounts(&self, program_id: &[u8; 32]) -> Result<Vec<serde_json::Value>> {
        let accounts = self.rocks.get_program_accounts(program_id)?;
        let mut results = Vec::with_capacity(accounts.len());
        for (pubkey, data) in &accounts {
            if let Some(account) = StoredAccount::deserialize(data) {
                results.push(serde_json::json!({
                    "pubkey": bs58::encode(pubkey).into_string(),
                    "account": {
                        "lamports": account.lamports,
                        "owner": bs58::encode(&account.owner).into_string(),
                        "data": [bs58::encode(&account.data).into_string(), "base58"],
                        "executable": account.executable,
                        "rentEpoch": account.rent_epoch,
                        "space": account.data.len(),
                    }
                }));
            }
        }
        Ok(results)
    }

    // --- Token methods (PostgreSQL) ---

    pub async fn get_token_accounts_by_owner(&self, owner: &[u8]) -> Result<Vec<serde_json::Value>> {
        let rows = self.pg.get_token_accounts_by_owner(owner).await?;
        Ok(rows.iter().map(|(pubkey, raw_data)| {
            serde_json::json!({
                "pubkey": bs58::encode(pubkey).into_string(),
                "account": {
                    "data": [bs58::encode(raw_data).into_string(), "base58"],
                    "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                }
            })
        }).collect())
    }

    pub async fn health_check(&self) -> bool {
        self.pg.health_check().await.is_ok() && self.cache.processed_slot() > 0
    }
}
