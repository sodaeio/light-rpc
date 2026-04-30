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

const LRU_SHARDS: usize = 32;

#[inline(always)]
fn shard_of(key_byte: u8) -> usize {
    (key_byte as usize) & (LRU_SHARDS - 1)
}

pub struct MemoryCache {
    blocks: RwLock<BTreeMap<Slot, Arc<BlockWithData>>>,
    /// RCU pattern: writers clone, mutate, swap; reads are wait-free.
    recent_blockhashes: arc_swap::ArcSwap<BTreeMap<Slot, Arc<str>>>,
    /// sig → (slot, tx index). Routes getTransaction to the cached block's
    /// pre-built RawValue without touching RocksDB.
    sig_index: dashmap::DashMap<[u8; 64], (Slot, u32)>,
    processed_slot: AtomicU64,
    confirmed_slot: AtomicU64,
    finalized_slot: AtomicU64,
    /// Unix-seconds timestamp of the last finalized-slot update.
    finalized_slot_updated_at: AtomicU64,
}

impl MemoryCache {
    pub fn new() -> Self {
        Self {
            blocks: RwLock::new(BTreeMap::new()),
            recent_blockhashes: arc_swap::ArcSwap::from_pointee(BTreeMap::new()),
            sig_index: dashmap::DashMap::with_capacity(128 * 4096),
            processed_slot: AtomicU64::new(0),
            confirmed_slot: AtomicU64::new(0),
            finalized_slot: AtomicU64::new(0),
            finalized_slot_updated_at: AtomicU64::new(0),
        }
    }

    pub fn lookup_sig(&self, sig: &[u8; 64]) -> Option<(Slot, u32)> {
        self.sig_index.get(sig).map(|e| *e.value())
    }

    pub fn finalized_slot_age_secs(&self) -> u64 {
        let last = self.finalized_slot_updated_at.load(Ordering::Relaxed);
        if last == 0 {
            return u64::MAX;
        }
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        now.saturating_sub(last)
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
        let blockhash: Arc<str> = Arc::from(block.info.blockhash.as_str());
        for (idx, tx) in block.transactions.iter().enumerate() {
            let sig_bytes: [u8; 64] = match tx.signature.as_ref().try_into() {
                Ok(b) => b,
                Err(_) => continue,
            };
            self.sig_index.insert(sig_bytes, (slot, idx as u32));
        }
        self.blocks.write().insert(slot, block);
        self.recent_blockhashes.rcu(|m| {
            let mut next = (**m).clone();
            next.insert(slot, Arc::clone(&blockhash));
            next
        });
        let current = self.processed_slot.load(Ordering::Relaxed);
        if slot > current {
            self.processed_slot.store(slot, Ordering::Relaxed);
        }
        metrics::MEMORY_CACHED_BLOCKS.set(self.blocks.read().len() as i64);
    }

    pub fn get_block(&self, slot: Slot) -> Option<Arc<BlockWithData>> {
        self.blocks.read().get(&slot).cloned()
    }

    pub fn get_blockhash(&self, slot: Slot) -> Option<Arc<str>> {
        self.recent_blockhashes.load().get(&slot).cloned()
    }

    /// Most recent blockhash at or below `slot`. Handles skipped slots in gLBH.
    pub fn get_blockhash_at_or_below(&self, slot: Slot) -> Option<(Arc<str>, Slot)> {
        self.recent_blockhashes
            .load()
            .range(..=slot)
            .next_back()
            .map(|(s, h)| (Arc::clone(h), *s))
    }

    pub fn is_blockhash_valid(&self, blockhash: &str) -> bool {
        self.recent_blockhashes
            .load()
            .values()
            .any(|h| h.as_ref() == blockhash)
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
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.finalized_slot_updated_at.store(now, Ordering::Relaxed);
        self.gc(slot);
    }

    fn gc(&self, finalized: Slot) {
        let cutoff = finalized.saturating_sub(64);
        let evicted: Vec<Arc<BlockWithData>> = {
            let mut blocks = self.blocks.write();
            let mut drop_keys: Vec<Slot> = Vec::new();
            for k in blocks.keys() {
                if *k < cutoff {
                    drop_keys.push(*k);
                }
            }
            drop_keys
                .into_iter()
                .filter_map(|k| blocks.remove(&k))
                .collect()
        };
        for block in evicted {
            for tx in &block.transactions {
                if let Ok(sig_bytes) = <&[u8; 64]>::try_from(tx.signature.as_ref()) {
                    self.sig_index.remove(sig_bytes);
                }
            }
        }
        self.recent_blockhashes.rcu(|m| {
            let mut next = (**m).clone();
            next.retain(|s, _| *s >= cutoff);
            while next.len() > 512 {
                next.pop_first();
            }
            next
        });
        metrics::MEMORY_CACHED_BLOCKS.set(self.blocks.read().len() as i64);
    }
}

impl Default for MemoryCache {
    fn default() -> Self {
        Self::new()
    }
}

type EncodedAccountShard =
    parking_lot::Mutex<lru::LruCache<([u8; 32], u8), Arc<Box<serde_json::value::RawValue>>>>;

/// Routes each query through the appropriate backend (memory cache, RocksDB,
/// PostgreSQL, block files, ClickHouse).
pub struct StorageReader {
    cache: Arc<MemoryCache>,
    rocks: UnifiedRocksDb,
    files: Arc<BlockFileStorage>,
    pg: PgStorage,
    tx_cache: Vec<parking_lot::Mutex<lru::LruCache<[u8; 64], Arc<serde_json::Value>>>>,
    mint_meta_cache: dashmap::DashMap<[u8; 32], (i32, u64)>,
    account_lru:
        Vec<parking_lot::Mutex<lru::LruCache<[u8; 32], Arc<super::accounts::StoredAccount>>>>,
    /// Bounded LRU; an unbounded map OOMs under random-key load.
    encoded_account_lru: Vec<EncodedAccountShard>,
    ch_client: Option<clickhouse::Client>,
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
            tx_cache: (0..LRU_SHARDS)
                .map(|_| {
                    parking_lot::Mutex::new(lru::LruCache::new(
                        std::num::NonZeroUsize::new(500_000 / LRU_SHARDS).unwrap(),
                    ))
                })
                .collect(),
            mint_meta_cache: dashmap::DashMap::new(),
            account_lru: (0..LRU_SHARDS)
                .map(|_| {
                    parking_lot::Mutex::new(lru::LruCache::new(
                        std::num::NonZeroUsize::new(1_000_000 / LRU_SHARDS).unwrap(),
                    ))
                })
                .collect(),
            encoded_account_lru: (0..LRU_SHARDS)
                .map(|_| {
                    parking_lot::Mutex::new(lru::LruCache::new(
                        std::num::NonZeroUsize::new(500_000 / LRU_SHARDS).unwrap(),
                    ))
                })
                .collect(),
            ch_client: None,
        }
    }

    pub fn with_clickhouse(mut self, client: clickhouse::Client) -> Self {
        self.ch_client = Some(client);
        self
    }

    fn enc_idx(encoding: &str) -> u8 {
        match encoding {
            "base58" => 0,
            "base64+zstd" => 2,
            "jsonParsed" => 3,
            _ => 1,
        }
    }

    pub fn cache(&self) -> &Arc<MemoryCache> {
        &self.cache
    }

    pub fn pg(&self) -> &PgStorage {
        &self.pg
    }

    pub async fn run_cache_updater(
        cache: Arc<MemoryCache>,
        mut rx: broadcast::Receiver<WriteToReadMessage>,
    ) {
        info!("cache updater started");
        loop {
            match rx.recv().await {
                Ok(WriteToReadMessage::NewBlock { slot, block }) => cache.insert_block(slot, block),
                Ok(WriteToReadMessage::BlockConfirmed { slot }) => cache.set_confirmed(slot),
                Ok(WriteToReadMessage::SlotFinalized { slot }) => cache.set_finalized(slot),
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "cache updater lagged")
                }
                Err(broadcast::error::RecvError::Closed) => {
                    info!("broadcast closed, cache updater exiting");
                    return;
                }
            }
        }
    }

    pub async fn run_reader_invalidator(
        self: Arc<Self>,
        mut rx: broadcast::Receiver<WriteToReadMessage>,
    ) {
        info!("reader invalidator started");
        loop {
            match rx.recv().await {
                Ok(WriteToReadMessage::AccountsUpdated { pubkeys }) => {
                    for pk in &pubkeys {
                        let s = shard_of(pk[0]);
                        self.account_lru[s].lock().pop(pk);
                        let mut enc_shard = self.encoded_account_lru[s].lock();
                        for enc in 0u8..4 {
                            enc_shard.pop(&(*pk, enc));
                        }
                    }
                }
                Ok(WriteToReadMessage::MintUpdated {
                    mint,
                    decimals,
                    slot,
                }) => {
                    self.mint_meta_insert(mint, decimals, slot);
                }
                Ok(_) => {}
                Err(broadcast::error::RecvError::Lagged(n)) => {
                    warn!(skipped = n, "reader invalidator lagged")
                }
                Err(broadcast::error::RecvError::Closed) => return,
            }
        }
    }

    fn rocks_get_account(&self, pubkey: &[u8; 32]) -> Option<StoredAccount> {
        let s = shard_of(pubkey[0]);
        if let Some(arc) = self.account_lru[s].lock().get(pubkey).cloned() {
            let stored = (*arc).clone();
            return (!stored.is_closed()).then_some(stored);
        }
        let stored = self
            .rocks
            .get_account(pubkey)
            .ok()
            .flatten()
            .and_then(|data| StoredAccount::deserialize(&data))?;
        self.account_lru[s]
            .lock()
            .put(*pubkey, Arc::new(stored.clone()));
        (!stored.is_closed()).then_some(stored)
    }

    fn encode_account_data(data: &[u8], encoding: &str) -> serde_json::Value {
        match encoding {
            "base58" => serde_json::json!([bs58::encode(data).into_string(), "base58"]),
            "base64+zstd" => {
                // Below 5 KiB zstd overhead exceeds the size win.
                if data.len() < 5000 {
                    serde_json::json!([base64_simd::STANDARD.encode_to_string(data), "base64"])
                } else {
                    let compressed = zstd::encode_all(data, 0).unwrap_or_default();
                    serde_json::json!([
                        base64_simd::STANDARD.encode_to_string(&compressed),
                        "base64+zstd"
                    ])
                }
            }
            _ => serde_json::json!([base64_simd::STANDARD.encode_to_string(data), "base64"]),
        }
    }

    fn account_to_json(account: &StoredAccount, encoding: &str) -> serde_json::Value {
        let owner_str = bs58::encode(&account.owner).into_string();

        if encoding == "jsonParsed" {
            if let Some(parsed) = Self::try_parse_account(account, &owner_str) {
                return serde_json::json!({
                    "lamports": account.lamports,
                    "owner": owner_str,
                    "data": parsed,
                    "executable": account.executable,
                    "rentEpoch": account.rent_epoch,
                    "space": account.data.len(),
                });
            }
            // Fall through to base64.
        }

        serde_json::json!({
            "lamports": account.lamports,
            "owner": owner_str,
            "data": Self::encode_account_data(&account.data, encoding),
            "executable": account.executable,
            "rentEpoch": account.rent_epoch,
            "space": account.data.len(),
        })
    }

    /// jsonParsed support for SPL Token mints and accounts.
    fn try_parse_account(account: &StoredAccount, owner: &str) -> Option<serde_json::Value> {
        let is_token_program = owner == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"
            || owner == "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb";

        if !is_token_program {
            return None;
        }

        let data = &account.data;
        let program = if owner == "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA" {
            "spl-token"
        } else {
            "spl-token-2022"
        };

        if data.len() == 165 {
            let mint = bs58::encode(&data[0..32]).into_string();
            let acct_owner = bs58::encode(&data[32..64]).into_string();
            let amount = u64::from_le_bytes(data[64..72].try_into().ok()?);
            let has_delegate = u32::from_le_bytes(data[72..76].try_into().ok()?) == 1;
            let delegate = if has_delegate {
                Some(bs58::encode(&data[76..108]).into_string())
            } else {
                None
            };
            let state = match data[108] {
                0 => "uninitialized",
                1 => "initialized",
                2 => "frozen",
                _ => "initialized",
            };

            return Some(serde_json::json!({
                "program": program,
                "parsed": {
                    "type": "account",
                    "info": {
                        "mint": mint,
                        "owner": acct_owner,
                        "tokenAmount": {
                            "amount": amount.to_string(),
                            "decimals": 0,
                            "uiAmount": null,
                            "uiAmountString": amount.to_string(),
                        },
                        "delegate": delegate,
                        "state": state,
                        "isNative": false,
                    }
                },
                "space": 165
            }));
        }

        if data.len() == 82 {
            let has_mint_authority = data[0] == 1;
            let mint_authority = if has_mint_authority {
                Some(bs58::encode(&data[4..36]).into_string())
            } else {
                None
            };
            let supply = u64::from_le_bytes(data[36..44].try_into().ok()?);
            let decimals = data[44];
            let is_initialized = data[45] == 1;
            let has_freeze = data[46] == 1;
            let freeze_authority = if has_freeze {
                Some(bs58::encode(&data[50..82]).into_string())
            } else {
                None
            };

            return Some(serde_json::json!({
                "program": program,
                "parsed": {
                    "type": "mint",
                    "info": {
                        "mintAuthority": mint_authority,
                        "supply": supply.to_string(),
                        "decimals": decimals,
                        "isInitialized": is_initialized,
                        "freezeAuthority": freeze_authority,
                    }
                },
                "space": 82
            }));
        }

        None
    }

    /// Rebuild the 165-byte SPL token-account binary from parsed PG columns.
    fn reconstruct_token_account_data(row: &super::postgres::TokenAccountRow) -> Vec<u8> {
        let mut data = vec![0u8; 165];
        let mint_len = row.mint.len().min(32);
        data[..mint_len].copy_from_slice(&row.mint[..mint_len]);
        let owner_len = row.owner.len().min(32);
        data[32..32 + owner_len].copy_from_slice(&row.owner[..owner_len]);
        data[64..72].copy_from_slice(&(row.amount as u64).to_le_bytes());
        if let Some(ref delegate) = row.delegate {
            data[72..76].copy_from_slice(&1u32.to_le_bytes());
            let d_len = delegate.len().min(32);
            data[76..76 + d_len].copy_from_slice(&delegate[..d_len]);
        }
        data[108] = if row.frozen { 2 } else { 1 };
        data
    }

    fn token_account_to_json(
        row: &super::postgres::TokenAccountRow,
        encoding: &str,
    ) -> serde_json::Value {
        let owner_str = bs58::encode(&row.token_program).into_string();

        let data_field = if encoding == "jsonParsed" {
            let state = if row.frozen { "frozen" } else { "initialized" };
            serde_json::json!({
                "program": if owner_str.starts_with("Token") { "spl-token" } else { "spl-token-2022" },
                "parsed": {
                    "type": "account",
                    "info": {
                        "mint": bs58::encode(&row.mint).into_string(),
                        "owner": bs58::encode(&row.owner).into_string(),
                        "tokenAmount": {
                            "amount": row.amount.to_string(),
                            "decimals": 0,
                            "uiAmount": null,
                            "uiAmountString": row.amount.to_string(),
                        },
                        "delegate": row.delegate.as_ref().map(|d| bs58::encode(d).into_string()),
                        "state": state,
                        "isNative": false,
                    }
                },
                "space": 165
            })
        } else {
            let raw = Self::reconstruct_token_account_data(row);
            Self::encode_account_data(&raw, encoding)
        };

        serde_json::json!({
            "pubkey": bs58::encode(&row.pubkey).into_string(),
            "account": {
                "lamports": row.lamports.max(0) as u64,
                "data": data_field,
                "owner": owner_str,
                "executable": false,
                "rentEpoch": u64::MAX,
                "space": 165
            }
        })
    }

    fn asset_to_json(a: &super::postgres::AssetRow) -> serde_json::Value {
        let name = a.raw_name.as_ref().map(|n| {
            String::from_utf8_lossy(n)
                .trim_end_matches('\0')
                .to_string()
        });
        let symbol = a.raw_symbol.as_ref().map(|s| {
            String::from_utf8_lossy(s)
                .trim_end_matches('\0')
                .to_string()
        });
        serde_json::json!({
            "id": bs58::encode(&a.id).into_string(),
            "interface": a.specification_asset_class.as_deref().unwrap_or("V1_NFT"),
            "content": {
                "json_uri": a.metadata_url.as_deref().unwrap_or(""),
                "metadata": a.metadata,
                "chain_data": a.chain_data,
            },
            "ownership": {
                "owner": a.owner.as_ref().map(|o| bs58::encode(o).into_string()),
                "delegate": a.delegate.as_ref().map(|d| bs58::encode(d).into_string()),
                "frozen": a.frozen,
            },
            "compression": {
                "compressed": a.compressed,
                "tree": a.tree_id.as_ref().map(|t| bs58::encode(t).into_string()),
                "leaf": a.leaf.as_ref().map(|l| bs58::encode(l).into_string()),
                "seq": a.seq,
            },
            "royalty": { "basis_points": a.royalty_amount },
            "supply": { "print_current_supply": a.supply.to_string() },
            "burnt": a.burnt,
            "name": name,
            "symbol": symbol,
        })
    }

    fn asset_list_response(
        total: i64,
        page: i64,
        limit: i64,
        assets: &[super::postgres::AssetRow],
    ) -> serde_json::Value {
        serde_json::json!({
            "total": total,
            "limit": limit,
            "page": page,
            "items": assets.iter().map(Self::asset_to_json).collect::<Vec<_>>()
        })
    }

    pub fn get_slot(&self, commitment: Commitment) -> Slot {
        match commitment {
            Commitment::Processed => self.cache.processed_slot(),
            Commitment::Confirmed => self.cache.confirmed_slot(),
            Commitment::Finalized => self.cache.finalized_slot(),
        }
    }

    pub fn get_first_available_slot(&self) -> Slot {
        self.rocks.first_slot_index().unwrap_or(0)
    }

    /// Falls through to ClickHouse when the local slot_index window misses.
    pub async fn get_blocks_in_range(&self, start: Slot, end: Slot) -> Result<Vec<Slot>> {
        let slots = self.rocks.scan_slot_index_range(start, end)?;
        if slots.is_empty() {
            if let Some(ref ch) = self.ch_client {
                return super::clickhouse_read::get_blocks_in_range(ch, start, end).await;
            }
        }
        Ok(slots)
    }

    pub async fn get_block_time(&self, slot: Slot) -> Result<Option<UnixTimestamp>> {
        if let Some(block) = self.cache.get_block(slot) {
            return Ok(block.info.block_time);
        }
        if let Some(data) = self.rocks.get_slot_index(slot)? {
            let info: serde_json::Value = serde_json::from_slice(&data)?;
            if let Some(bt) = info.get("block_time").and_then(|v| v.as_i64()) {
                return Ok(Some(bt));
            }
        }
        if let Some(ref ch) = self.ch_client {
            return super::clickhouse_read::get_block_time(ch, slot).await;
        }
        Ok(None)
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

    pub fn get_latest_blockhash(&self, commitment: Commitment) -> Option<(Arc<str>, Slot)> {
        let slot = self.get_slot(commitment);
        if let Some(h) = self.cache.get_blockhash(slot) {
            return Some((h, slot));
        }
        // Skipped commitment slot: fall back to most recent blockhash ≤ slot.
        self.cache.get_blockhash_at_or_below(slot)
    }

    pub fn is_blockhash_valid(&self, blockhash: &str) -> bool {
        self.cache.is_blockhash_valid(blockhash)
    }

    /// tx_cache → tx_index → ClickHouse. Hot path stays sync; the cold path
    /// goes async only on rocks miss with CH configured.
    pub async fn get_transaction(&self, signature: &[u8; 64]) -> Result<Option<serde_json::Value>> {
        let s = shard_of(signature[0]);
        if let Some(cached) = self.tx_cache[s].lock().get(signature).cloned() {
            return Ok(Some((*cached).clone()));
        }
        if let Some(data) = self.rocks.get_tx_index(signature)? {
            let decoded = crate::rpc::tx_format::decode_tx_index(&data)?;
            self.tx_cache[s]
                .lock()
                .put(*signature, Arc::new(decoded.clone()));
            return Ok(Some(decoded));
        }
        if let Some(ref ch) = self.ch_client {
            return super::clickhouse_read::get_transaction(ch, signature).await;
        }
        Ok(None)
    }

    /// Hot-path getTransaction. Splices slot + blockTime into the cached
    /// prebuilt bytes; no re-decode, no Value allocation.
    pub fn get_transaction_prebuilt(
        &self,
        signature: &[u8; 64],
    ) -> Option<Box<serde_json::value::RawValue>> {
        let (slot, idx) = self.cache.lookup_sig(signature)?;
        let block = self.cache.get_block(slot)?;
        let tx = block.transactions.get(idx as usize)?;
        let prebuilt_bytes = tx.prebuilt.get();
        if !prebuilt_bytes.starts_with('{') {
            return None;
        }
        let bt = block
            .info
            .block_time
            .map(|t| t.to_string())
            .unwrap_or_else(|| "null".to_string());
        let mut out = String::with_capacity(prebuilt_bytes.len() + 48);
        out.push_str(r#"{"slot":"#);
        out.push_str(&slot.to_string());
        out.push_str(r#","blockTime":"#);
        out.push_str(&bt);
        out.push(',');
        out.push_str(&prebuilt_bytes[1..]);
        serde_json::value::RawValue::from_string(out).ok()
    }

    pub fn get_transactions_batch(
        &self,
        signatures: &[[u8; 64]],
    ) -> Result<Vec<Option<serde_json::Value>>> {
        let mut results: Vec<Option<serde_json::Value>> = Vec::with_capacity(signatures.len());
        let mut missing: Vec<(usize, [u8; 64])> = Vec::new();
        for (i, sig) in signatures.iter().enumerate() {
            let s = shard_of(sig[0]);
            if let Some(v) = self.tx_cache[s].lock().get(sig).cloned() {
                results.push(Some((*v).clone()));
            } else {
                results.push(None);
                missing.push((i, *sig));
            }
        }
        if missing.is_empty() {
            return Ok(results);
        }
        let miss_keys: Vec<[u8; 64]> = missing.iter().map(|(_, s)| *s).collect();
        let raw = self.rocks.get_tx_index_batch(&miss_keys)?;
        for ((idx, sig), data) in missing.into_iter().zip(raw) {
            if let Some(bytes) = data {
                let decoded = crate::rpc::tx_format::decode_tx_index(&bytes)?;
                let s = shard_of(sig[0]);
                self.tx_cache[s].lock().put(sig, Arc::new(decoded.clone()));
                results[idx] = Some(decoded);
            }
        }
        Ok(results)
    }

    /// sfa_index prefix scan; ClickHouse fills the tail when local results
    /// fall short of `limit`.
    pub async fn get_signatures_for_address(
        &self,
        address: &solana_pubkey::Pubkey,
        before_slot: Option<Slot>,
        limit: usize,
    ) -> Result<Vec<TransactionInfo>> {
        let entries = self.rocks.iter_sfa_limited(address, before_slot, limit)?;
        let mut results: Vec<TransactionInfo> = Vec::with_capacity(limit);
        for (slot, sigs) in entries {
            for sig in sigs {
                results.push(TransactionInfo {
                    signature: sig.signature,
                    slot,
                    block_time: None,
                    err: sig.err,
                    memo: sig.memo,
                });
                if results.len() >= limit {
                    return Ok(results);
                }
            }
        }

        if results.len() < limit {
            if let Some(ref ch) = self.ch_client {
                let remaining = limit - results.len();
                let ch_before = results.last().map(|r| r.slot).or(before_slot);
                let addr_bytes: [u8; 32] = address.to_bytes();
                let ch_entries = super::clickhouse_read::get_signatures_for_address(
                    ch,
                    &addr_bytes,
                    ch_before,
                    remaining,
                )
                .await?;
                for e in ch_entries {
                    results.push(TransactionInfo {
                        signature: solana_signature::Signature::default(),
                        slot: e.slot,
                        block_time: Some(e.block_time),
                        err: if e.err {
                            Some("true".to_string())
                        } else {
                            None
                        },
                        memo: None,
                    });
                }
            }
        }

        Ok(results)
    }

    /// Bound on token accounts considered per gTFA call.
    const GTFA_ATA_CAP: usize = 2048;

    pub async fn get_transactions_for_address(
        &self,
        owner: &[u8],
        before_slot: Option<Slot>,
        limit: usize,
    ) -> Result<Vec<serde_json::Value>> {
        let owner_key: [u8; 32] = match owner.try_into() {
            Ok(k) => k,
            Err(_) => return Ok(Vec::new()),
        };

        let atas = self.rocks.get_owner_atas(&owner_key, Self::GTFA_ATA_CAP)?;
        let addresses: Vec<[u8; 32]> = std::iter::once(owner_key).chain(atas).collect();

        // Fan out per-address iter_sfa onto the blocking pool.
        let mut handles = Vec::with_capacity(addresses.len());
        for addr in addresses.iter().copied() {
            let rocks = self.rocks.clone();
            let before = before_slot;
            let lim = limit;
            handles.push(tokio::task::spawn_blocking(move || {
                let pk = solana_pubkey::Pubkey::new_from_array(addr);
                rocks.iter_sfa_limited(&pk, before, lim)
            }));
        }

        let mut all: Vec<(Slot, SignatureEntry)> =
            Vec::with_capacity(addresses.len() * limit.min(100));
        for h in handles {
            if let Ok(Ok(entries)) = h.await {
                for (slot, sigs) in entries {
                    for sig in sigs {
                        all.push((slot, sig));
                    }
                }
            }
        }

        all.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
        all.truncate(limit);

        let finalized = self.cache.finalized_slot();
        let confirmed = self.cache.confirmed_slot();

        // One multi_get_cf round-trip; results zip back in slot order.
        let sig_bytes_vec: Vec<[u8; 64]> = all
            .iter()
            .filter_map(|(_, sig)| sig.signature.as_ref().try_into().ok())
            .collect();
        let hydrated = self.get_transactions_batch(&sig_bytes_vec)?;

        let mut results = Vec::with_capacity(all.len());
        let mut idx = 0;
        for (slot, sig) in all {
            if <&[u8; 64]>::try_from(sig.signature.as_ref()).is_err() {
                continue;
            }
            let tx = hydrated
                .get(idx)
                .and_then(|opt| opt.clone())
                .unwrap_or(serde_json::Value::Null);
            idx += 1;
            let status = if slot <= finalized {
                "finalized"
            } else if slot <= confirmed {
                "confirmed"
            } else {
                "processed"
            };
            results.push(serde_json::json!({
                "signature": sig.signature.to_string(),
                "slot": slot,
                "err": sig.err,
                "confirmationStatus": status,
                "transaction": tx,
            }));
        }
        Ok(results)
    }

    pub async fn get_account_info(
        &self,
        pubkey: &[u8; 32],
        encoding: &str,
    ) -> Result<Option<serde_json::Value>> {
        if let Some(stored) = super::native::lookup(pubkey) {
            return Ok(Some(Self::account_to_json(&stored, encoding)));
        }

        if let Some(stored) = self.rocks_get_account(pubkey) {
            return Ok(Some(Self::account_to_json(&stored, encoding)));
        }

        if let Some(row) = self.pg.get_program_account_by_pubkey(pubkey).await? {
            let stored = StoredAccount {
                owner: row.owner.try_into().unwrap_or([0u8; 32]),
                lamports: row.lamports as u64,
                data: row.data,
                executable: row.executable,
                rent_epoch: row.rent_epoch as u64,
                slot: row.slot_updated as u64,
            };
            if !stored.is_closed() {
                return Ok(Some(Self::account_to_json(&stored, encoding)));
            }
        }

        // SPL mints live in `tokens`, not `program_accounts`.
        if let Some(mint_row) = self.pg.get_token_mint(pubkey).await? {
            let supply = mint_row.supply.to_string().parse::<u64>().unwrap_or(0);
            let mut data = vec![0u8; 82];
            if let Some(ref auth) = mint_row.mint_authority {
                data[0] = 1;
                if auth.len() == 32 {
                    data[4..36].copy_from_slice(auth);
                }
            }
            data[36..44].copy_from_slice(&supply.to_le_bytes());
            data[44] = mint_row.decimals as u8;
            data[45] = 1;
            if let Some(ref freeze) = mint_row.freeze_authority {
                data[46] = 1;
                if freeze.len() == 32 {
                    data[50..82].copy_from_slice(freeze);
                }
            }

            let stored = StoredAccount {
                owner: mint_row.token_program.try_into().unwrap_or([0u8; 32]),
                lamports: 496630637030,
                data,
                executable: false,
                rent_epoch: u64::MAX,
                slot: mint_row.slot_updated as u64,
            };
            return Ok(Some(Self::account_to_json(&stored, encoding)));
        }

        Ok(None)
    }

    /// Pre-serialized variant; jsonrpsee emits the RawValue verbatim.
    pub async fn get_account_info_raw(
        &self,
        pubkey: &[u8; 32],
        encoding: &str,
    ) -> Result<Option<Arc<Box<serde_json::value::RawValue>>>> {
        let idx = Self::enc_idx(encoding);
        let s = shard_of(pubkey[0]);
        if let Some(arc) = self.encoded_account_lru[s]
            .lock()
            .get(&(*pubkey, idx))
            .cloned()
        {
            return Ok(Some(arc));
        }

        let value = match self.get_account_info(pubkey, encoding).await? {
            Some(v) => v,
            None => return Ok(None),
        };
        let raw = serde_json::value::to_raw_value(&value)
            .map_err(|e| anyhow::anyhow!("serialize account: {e}"))?;
        let arc = Arc::new(raw);
        self.encoded_account_lru[s]
            .lock()
            .put((*pubkey, idx), Arc::clone(&arc));
        Ok(Some(arc))
    }

    pub async fn get_multiple_accounts(
        &self,
        pubkeys: &[[u8; 32]],
        encoding: &str,
    ) -> Vec<Option<serde_json::Value>> {
        let mut results: Vec<Option<serde_json::Value>> = vec![None; pubkeys.len()];
        let mut rocks_idx: Vec<usize> = Vec::with_capacity(pubkeys.len());

        for (i, pk) in pubkeys.iter().enumerate() {
            if let Some(stored) = super::native::lookup(pk) {
                results[i] = Some(Self::account_to_json(&stored, encoding));
            } else {
                rocks_idx.push(i);
            }
        }

        if !rocks_idx.is_empty() {
            let batch_keys: Vec<[u8; 32]> = rocks_idx.iter().map(|&i| pubkeys[i]).collect();
            let batch_vals = self.rocks.get_accounts_batch(&batch_keys);
            let mut missing: Vec<usize> = Vec::new();
            for (j, val) in batch_vals.into_iter().enumerate() {
                let idx = rocks_idx[j];
                match val.and_then(|b| super::accounts::StoredAccount::deserialize(&b)) {
                    Some(stored) if !stored.is_closed() => {
                        results[idx] = Some(Self::account_to_json(&stored, encoding));
                    }
                    _ => missing.push(idx),
                }
            }
            if !missing.is_empty() {
                let miss_keys: Vec<Vec<u8>> =
                    missing.iter().map(|&i| pubkeys[i].to_vec()).collect();
                if let Ok(rows) = self.pg.get_program_accounts_by_pubkeys(&miss_keys).await {
                    let mut by_pk: std::collections::HashMap<Vec<u8>, _> =
                        rows.into_iter().map(|r| (r.pubkey.clone(), r)).collect();
                    for idx in missing {
                        if let Some(row) = by_pk.remove(pubkeys[idx].as_slice()) {
                            let stored = super::accounts::StoredAccount {
                                owner: row.owner.try_into().unwrap_or([0u8; 32]),
                                lamports: row.lamports as u64,
                                data: row.data,
                                executable: row.executable,
                                rent_epoch: row.rent_epoch as u64,
                                slot: row.slot_updated as u64,
                            };
                            if !stored.is_closed() {
                                results[idx] = Some(Self::account_to_json(&stored, encoding));
                            }
                        }
                    }
                }
            }
        }

        results
    }

    pub async fn get_program_accounts(
        &self,
        program_id: &[u8; 32],
        encoding: &str,
    ) -> Result<Vec<serde_json::Value>> {
        let rocks_result = self.rocks.get_program_accounts(program_id)?;
        Ok(rocks_result
            .iter()
            .filter_map(|(pubkey, data)| {
                StoredAccount::deserialize(data).map(|account| {
                    serde_json::json!({
                        "pubkey": bs58::encode(pubkey).into_string(),
                        "account": Self::account_to_json(&account, encoding)
                    })
                })
            })
            .collect())
    }

    pub async fn get_token_accounts_by_owner(
        &self,
        owner: &[u8],
        encoding: &str,
    ) -> Result<Vec<serde_json::Value>> {
        let owner_key: [u8; 32] = match owner.try_into() {
            Ok(k) => k,
            Err(_) => return Ok(Vec::new()),
        };

        // RocksDB-first: prefix-scan owner_atas, batch-fetch accounts,
        // parse the SPL layout inline.
        let atas = self.rocks.get_owner_atas(&owner_key, 10_000)?;
        if !atas.is_empty() {
            let bytes = self.rocks.get_accounts_batch(&atas);
            let mut out = Vec::with_capacity(atas.len());
            for (pubkey, data) in atas.iter().zip(bytes) {
                let Some(raw) = data else { continue };
                let Some(stored) = super::accounts::StoredAccount::deserialize(&raw) else {
                    continue;
                };
                if stored.data.len() < 72 {
                    continue;
                }
                if let Some(v) = Self::spl_token_account_json(pubkey, &stored, encoding) {
                    out.push(v);
                }
            }
            if !out.is_empty() {
                return Ok(out);
            }
        }

        // Fallback for fresh deployments with owner_atas unpopulated.
        let rows = self.pg.get_token_accounts_by_owner(owner).await?;
        Ok(rows
            .iter()
            .map(|r| Self::token_account_to_json(r, encoding))
            .collect())
    }

    fn spl_token_account_json(
        pubkey: &[u8; 32],
        stored: &super::accounts::StoredAccount,
        encoding: &str,
    ) -> Option<serde_json::Value> {
        let data = &stored.data;
        if data.len() < 72 {
            return None;
        }
        let mint = &data[0..32];
        let owner = &data[32..64];
        let amount = u64::from_le_bytes(data[64..72].try_into().ok()?);
        let frozen = data.len() > 108 && data[108] == 2;
        let owner_program = bs58::encode(&stored.owner).into_string();

        let data_field = if encoding == "jsonParsed" {
            let state = if frozen { "frozen" } else { "initialized" };
            serde_json::json!({
                "program": if owner_program.starts_with("Token") { "spl-token" } else { "spl-token-2022" },
                "parsed": {
                    "type": "account",
                    "info": {
                        "mint": bs58::encode(mint).into_string(),
                        "owner": bs58::encode(owner).into_string(),
                        "tokenAmount": {
                            "amount": amount.to_string(),
                            "decimals": 0,
                            "uiAmount": null,
                            "uiAmountString": amount.to_string(),
                        },
                        "state": state,
                        "isNative": false,
                    }
                },
                "space": data.len(),
            })
        } else {
            Self::encode_account_data(data, encoding)
        };

        Some(serde_json::json!({
            "pubkey": bs58::encode(pubkey).into_string(),
            "account": {
                "lamports": stored.lamports,
                "owner": owner_program,
                "data": data_field,
                "executable": stored.executable,
                "rentEpoch": stored.rent_epoch,
                "space": data.len(),
            }
        }))
    }

    pub async fn get_token_accounts_by_delegate(
        &self,
        delegate: &[u8],
        encoding: &str,
    ) -> Result<Vec<serde_json::Value>> {
        let rows = self.pg.get_token_accounts_by_delegate(delegate).await?;
        Ok(rows
            .iter()
            .map(|r| Self::token_account_to_json(r, encoding))
            .collect())
    }

    pub async fn get_token_supply(&self, mint_pubkey: &[u8]) -> Result<Option<serde_json::Value>> {
        if let Some(row) = self.pg.get_token_mint(mint_pubkey).await? {
            let supply_str = row.supply.to_string();
            let decimals = row.decimals;
            let ui_amount = if decimals > 0 {
                supply_str.parse::<f64>().unwrap_or(0.0) / 10f64.powi(decimals)
            } else {
                supply_str.parse::<f64>().unwrap_or(0.0)
            };
            return Ok(Some(serde_json::json!({
                "amount": supply_str,
                "decimals": decimals,
                "uiAmount": ui_amount,
                "uiAmountString": format!("{ui_amount}"),
            })));
        }
        Ok(None)
    }

    pub async fn get_token_largest_accounts(
        &self,
        mint: &[u8],
        limit: i64,
    ) -> Result<Vec<serde_json::Value>> {
        let mint_key: [u8; 32] = match mint.try_into() {
            Ok(k) => k,
            Err(_) => return Ok(Vec::new()),
        };
        let decimals = self.mint_decimals_cached(&mint_key, mint).await;
        let mut holders = self.rocks.get_mint_top_holders(&mint_key)?;
        // Empty result = mint hasn't seen a TA write yet; fall back to PG.
        if holders.is_empty() {
            let rows = self.pg.get_token_largest_accounts(mint, limit).await?;
            return Ok(rows
                .iter()
                .map(|row| Self::token_holder_json(&row.pubkey, row.amount as u64, decimals))
                .collect());
        }
        holders.truncate(limit as usize);
        Ok(holders
            .iter()
            .map(|(amount, pk)| Self::token_holder_json(pk, *amount, decimals))
            .collect())
    }

    async fn mint_decimals_cached(&self, mint_key: &[u8; 32], mint: &[u8]) -> i32 {
        if let Some(entry) = self.mint_meta_cache.get(mint_key) {
            return entry.0;
        }
        let decimals = self
            .pg
            .get_token_mint(mint)
            .await
            .ok()
            .flatten()
            .map(|r| r.decimals)
            .unwrap_or(0);
        self.mint_meta_put(*mint_key, decimals, self.cache.processed_slot());
        decimals
    }

    pub fn mint_meta_insert(&self, mint: [u8; 32], decimals: i32, slot: u64) {
        self.mint_meta_put(mint, decimals, slot);
    }

    fn mint_meta_put(&self, mint: [u8; 32], decimals: i32, slot: u64) {
        // Cap at 200k entries; halve on overflow.
        if self.mint_meta_cache.len() >= 200_000 {
            let victims: Vec<[u8; 32]> = self
                .mint_meta_cache
                .iter()
                .take(100_000)
                .map(|e| *e.key())
                .collect();
            for v in victims {
                self.mint_meta_cache.remove(&v);
            }
        }
        self.mint_meta_cache.insert(mint, (decimals, slot));
    }

    fn token_holder_json(pubkey: &[u8], amount: u64, decimals: i32) -> serde_json::Value {
        let ui_amount = if decimals > 0 {
            amount as f64 / 10f64.powi(decimals)
        } else {
            amount as f64
        };
        serde_json::json!({
            "address": bs58::encode(pubkey).into_string(),
            "amount": amount.to_string(),
            "decimals": decimals,
            "uiAmount": ui_amount,
            "uiAmountString": format!("{ui_amount}"),
        })
    }

    pub async fn get_asset(&self, id: &[u8]) -> Result<Option<serde_json::Value>> {
        let asset = match self.pg.get_asset(id).await? {
            Some(a) => a,
            None => return Ok(None),
        };
        let creators = self.pg.get_asset_creators(id).await?;
        let authority = self.pg.get_asset_authority(id).await?;
        let grouping = self.pg.get_asset_grouping(id).await?;

        let mut json = Self::asset_to_json(&asset);
        let obj = json.as_object_mut().unwrap();

        obj.insert(
            "authorities".to_string(),
            serde_json::json!(authority
                .map(|a| vec![serde_json::json!({
                    "address": bs58::encode(&a.authority).into_string(),
                    "scopes": a.scopes,
                })])
                .unwrap_or_default()),
        );
        obj.insert(
            "creators".to_string(),
            serde_json::json!(creators
                .iter()
                .map(|c| serde_json::json!({
                    "address": bs58::encode(&c.creator).into_string(),
                    "share": c.share,
                    "verified": c.verified,
                }))
                .collect::<Vec<_>>()),
        );
        obj.insert(
            "grouping".to_string(),
            serde_json::json!(grouping
                .iter()
                .map(|g| serde_json::json!({
                    "group_key": g.group_key,
                    "group_value": g.group_value,
                    "verified": g.verified,
                }))
                .collect::<Vec<_>>()),
        );

        Ok(Some(json))
    }

    pub async fn get_assets_by_owner(
        &self,
        owner: &[u8],
        page: i64,
        limit: i64,
    ) -> Result<serde_json::Value> {
        let (total, rows) = self.pg.get_assets_by_owner(owner, page, limit).await?;
        Ok(Self::asset_list_response(total, page, limit, &rows))
    }

    pub async fn get_assets_by_creator(
        &self,
        creator: &[u8],
        page: i64,
        limit: i64,
    ) -> Result<serde_json::Value> {
        let (total, rows) = self.pg.get_assets_by_creator(creator, page, limit).await?;
        Ok(Self::asset_list_response(total, page, limit, &rows))
    }

    pub async fn get_assets_by_group(
        &self,
        group_key: &str,
        group_value: &str,
        page: i64,
        limit: i64,
    ) -> Result<serde_json::Value> {
        let (total, rows) = self
            .pg
            .get_assets_by_group(group_key, group_value, page, limit)
            .await?;
        Ok(Self::asset_list_response(total, page, limit, &rows))
    }

    pub async fn get_assets_by_authority(
        &self,
        authority: &[u8],
        page: i64,
        limit: i64,
    ) -> Result<serde_json::Value> {
        let (total, rows) = self
            .pg
            .get_assets_by_authority(authority, page, limit)
            .await?;
        Ok(Self::asset_list_response(total, page, limit, &rows))
    }

    pub async fn health_check(&self) -> bool {
        self.pg.health_check().await.is_ok() && self.cache.processed_slot() > 0
    }
}
