use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use anyhow::Result;
use parking_lot::RwLock;
use tokio::sync::{broadcast, oneshot};
use tracing::{info, warn};

use light_indexer_core::metrics;
use light_indexer_core::types::*;

use crate::files::BlockFileStorage;
use crate::postgres::PgStorage;
use crate::rocks::UnifiedRocksDb;

/// In-memory cache of recently processed blocks and commitment state.
/// Updated via the broadcast channel from the write worker.
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
        self.recent_blockhashes
            .read()
            .values()
            .any(|h| h == blockhash)
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

    /// Remove cached blocks and blockhashes below the finalization watermark.
    /// Keep a buffer of ~64 slots for recent queries.
    fn gc(&self, finalized: Slot) {
        let cutoff = finalized.saturating_sub(64);
        self.blocks.write().retain(|s, _| *s >= cutoff);
        self.recent_blockhashes.write().retain(|s, _| *s >= cutoff);

        // Trim to max 512 recent blockhashes
        let mut hashes = self.recent_blockhashes.write();
        while hashes.len() > 512 {
            hashes.pop_first();
        }

        metrics::MEMORY_CACHED_BLOCKS.set(self.blocks.read().len() as i64);
    }
}

impl Default for MemoryCache {
    fn default() -> Self {
        Self::new()
    }
}

/// Read queries that can be sent from the RPC layer to the storage reader.
pub enum ReadRequest {
    GetBlock {
        slot: Slot,
        tx: oneshot::Sender<Result<Option<Arc<BlockWithData>>>>,
    },
    GetTransaction {
        signature: [u8; 64],
        tx: oneshot::Sender<Result<Option<serde_json::Value>>>,
    },
    GetSignaturesForAddress {
        address: solana_pubkey::Pubkey,
        before_slot: Option<Slot>,
        limit: usize,
        tx: oneshot::Sender<Result<Vec<TransactionInfo>>>,
    },
    GetSlot {
        commitment: Commitment,
        tx: oneshot::Sender<Slot>,
    },
    GetBlockHeight {
        commitment: Commitment,
        tx: oneshot::Sender<Result<Option<u64>>>,
    },
    GetLatestBlockhash {
        commitment: Commitment,
        tx: oneshot::Sender<Result<Option<(String, u64)>>>,
    },
    IsBlockhashValid {
        blockhash: String,
        tx: oneshot::Sender<bool>,
    },
    GetAccountInfo {
        pubkey: [u8; 32],
        tx: oneshot::Sender<Result<Option<serde_json::Value>>>,
    },
    GetProgramAccounts {
        program_id: [u8; 32],
        tx: oneshot::Sender<Result<Vec<serde_json::Value>>>,
    },
    GetTokenAccountsByOwner {
        owner: Vec<u8>,
        tx: oneshot::Sender<Result<Vec<serde_json::Value>>>,
    },
    HealthCheck {
        tx: oneshot::Sender<bool>,
    },
}

/// The storage reader processes read queries using a tiered lookup:
///   1. In-memory cache (zero I/O, sub-ms)
///   2. RocksDB indexes (single disk read)
///   3. File storage for full block data
///   4. PostgreSQL for relational token/asset queries
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
        Self {
            cache,
            rocks,
            files,
            pg,
        }
    }

    pub fn cache(&self) -> &Arc<MemoryCache> {
        &self.cache
    }

    /// Run the broadcast listener that keeps the memory cache in sync.
    pub async fn run_cache_updater(
        cache: Arc<MemoryCache>,
        mut broadcast_rx: broadcast::Receiver<WriteToReadMessage>,
    ) {
        info!("cache updater started");
        loop {
            match broadcast_rx.recv().await {
                Ok(WriteToReadMessage::NewBlock { slot, block }) => {
                    cache.insert_block(slot, block);
                }
                Ok(WriteToReadMessage::BlockConfirmed { slot }) => {
                    cache.set_confirmed(slot);
                    metrics::LATEST_SLOT
                        .with_label_values(&["confirmed"])
                        .set(slot as i64);
                }
                Ok(WriteToReadMessage::SlotFinalized { slot }) => {
                    cache.set_finalized(slot);
                    metrics::LATEST_SLOT
                        .with_label_values(&["finalized"])
                        .set(slot as i64);
                }
                Ok(WriteToReadMessage::BlockDead { .. }) => {}
                Ok(WriteToReadMessage::Init { .. }) => {}
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "cache updater lagged behind broadcast");
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!("broadcast closed, cache updater shutting down");
                    return;
                }
            }
        }
    }

    /// Process a read request using tiered storage.
    pub async fn handle_request(&self, request: ReadRequest) {
        match request {
            ReadRequest::GetBlock { slot, tx } => {
                let result = self.get_block(slot).await;
                let _ = tx.send(result);
            }
            ReadRequest::GetTransaction { signature, tx } => {
                let result = self.get_transaction(&signature).await;
                let _ = tx.send(result);
            }
            ReadRequest::GetSignaturesForAddress {
                address,
                before_slot,
                limit,
                tx,
            } => {
                let result = self.get_signatures_for_address(&address, before_slot, limit).await;
                let _ = tx.send(result);
            }
            ReadRequest::GetSlot { commitment, tx } => {
                let slot = match commitment {
                    Commitment::Processed => self.cache.processed_slot(),
                    Commitment::Confirmed => self.cache.confirmed_slot(),
                    Commitment::Finalized => self.cache.finalized_slot(),
                };
                let _ = tx.send(slot);
            }
            ReadRequest::GetBlockHeight { commitment, tx } => {
                let slot = match commitment {
                    Commitment::Processed => self.cache.processed_slot(),
                    Commitment::Confirmed => self.cache.confirmed_slot(),
                    Commitment::Finalized => self.cache.finalized_slot(),
                };
                let result = self.get_block_height(slot).await;
                let _ = tx.send(result);
            }
            ReadRequest::GetLatestBlockhash { commitment, tx } => {
                let slot = match commitment {
                    Commitment::Processed => self.cache.processed_slot(),
                    Commitment::Confirmed => self.cache.confirmed_slot(),
                    Commitment::Finalized => self.cache.finalized_slot(),
                };
                let hash = self.cache.get_blockhash(slot);
                let _ = tx.send(Ok(hash.map(|h| (h, slot))));
            }
            ReadRequest::IsBlockhashValid { blockhash, tx } => {
                let valid = self.cache.is_blockhash_valid(&blockhash);
                let _ = tx.send(valid);
            }
            ReadRequest::GetAccountInfo { pubkey, tx } => {
                let result = self.get_account_info(&pubkey).await;
                let _ = tx.send(result);
            }
            ReadRequest::GetProgramAccounts { program_id, tx } => {
                let result = self.get_program_accounts(&program_id).await;
                let _ = tx.send(result);
            }
            ReadRequest::GetTokenAccountsByOwner { owner, tx } => {
                let result = self.get_token_accounts_by_owner(&owner).await;
                let _ = tx.send(result);
            }
            ReadRequest::HealthCheck { tx } => {
                let healthy = self.pg.health_check().await.is_ok()
                    && self.cache.processed_slot() > 0;
                let _ = tx.send(healthy);
            }
        }
    }

    // --- Tiered lookup methods ---

    async fn get_block(&self, slot: Slot) -> Result<Option<Arc<BlockWithData>>> {
        // Tier 1: memory cache
        if let Some(block) = self.cache.get_block(slot) {
            return Ok(Some(block));
        }

        // Tier 2: RocksDB slot index → file storage
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

    async fn get_transaction(&self, signature: &[u8; 64]) -> Result<Option<serde_json::Value>> {
        if let Some(data) = self.rocks.get_tx_index(signature)? {
            let info: serde_json::Value = serde_json::from_slice(&data)?;
            return Ok(Some(info));
        }
        Ok(None)
    }

    async fn get_signatures_for_address(
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
                    block_time: None, // could be looked up from slot index
                    err: sig.err,
                    memo: sig.memo,
                });
            }
        }

        Ok(results)
    }

    async fn get_block_height(&self, slot: Slot) -> Result<Option<u64>> {
        if let Some(block) = self.cache.get_block(slot) {
            return Ok(block.info.block_height);
        }
        if let Some(data) = self.rocks.get_slot_index(slot)? {
            let info: serde_json::Value = serde_json::from_slice(&data)?;
            return Ok(info["block_height"].as_u64());
        }
        Ok(None)
    }

    async fn get_account_info(&self, pubkey: &[u8; 32]) -> Result<Option<serde_json::Value>> {
        use crate::accounts::StoredAccount;

        if let Some(data) = self.rocks.get_account(pubkey)? {
            if let Some(account) = StoredAccount::deserialize(&data) {
                let value = serde_json::json!({
                    "lamports": account.lamports,
                    "owner": bs58::encode(&account.owner).into_string(),
                    "data": [bs58::encode(&account.data).into_string(), "base58"],
                    "executable": account.executable,
                    "rentEpoch": account.rent_epoch,
                    "space": account.data.len(),
                });
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    async fn get_program_accounts(&self, program_id: &[u8; 32]) -> Result<Vec<serde_json::Value>> {
        use crate::accounts::StoredAccount;

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

    async fn get_token_accounts_by_owner(
        &self,
        owner: &[u8],
    ) -> Result<Vec<serde_json::Value>> {
        let rows = self.pg.get_token_accounts_by_owner(owner).await?;
        let mut results = Vec::with_capacity(rows.len());

        for (pubkey, raw_data) in &rows {
            results.push(serde_json::json!({
                "pubkey": bs58::encode(pubkey).into_string(),
                "account": {
                    "data": [bs58::encode(raw_data).into_string(), "base58"],
                    "owner": "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
                }
            }));
        }

        Ok(results)
    }
}
