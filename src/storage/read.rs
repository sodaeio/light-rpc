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

/// Unified storage reader. Each method picks the optimal storage path internally:
///   - Memory cache for recent blocks/slots
///   - RocksDB for account point lookups and block indexes
///   - PostgreSQL for token queries, program accounts, and DAS assets
///   - Block files for historical block data
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
                Err(broadcast::error::RecvError::Lagged(n)) => warn!(skipped = n, "cache updater lagged"),
                Err(broadcast::error::RecvError::Closed) => {
                    info!("broadcast closed, cache updater exiting");
                    return;
                }
            }
        }
    }

    // -- Private helpers (RocksDB point lookups) --

    fn rocks_get_account(&self, pubkey: &[u8; 32]) -> Option<StoredAccount> {
        self.rocks.get_account(pubkey).ok().flatten()
            .and_then(|data| StoredAccount::deserialize(&data))
    }

    fn account_to_json(account: &StoredAccount, encoding: &str) -> serde_json::Value {
        let data_field = match encoding {
            "base58" => serde_json::json!([bs58::encode(&account.data).into_string(), "base58"]),
            _ => {
                use base64::Engine;
                serde_json::json!([
                    base64::engine::general_purpose::STANDARD.encode(&account.data),
                    "base64"
                ])
            }
        };
        serde_json::json!({
            "lamports": account.lamports,
            "owner": bs58::encode(&account.owner).into_string(),
            "data": data_field,
            "executable": account.executable,
            "rentEpoch": account.rent_epoch,
            "space": account.data.len(),
        })
    }

    fn token_account_to_json(row: &super::postgres::TokenAccountRow) -> serde_json::Value {
        serde_json::json!({
            "pubkey": bs58::encode(&row.pubkey).into_string(),
            "account": {
                "mint": bs58::encode(&row.mint).into_string(),
                "owner": bs58::encode(&row.owner).into_string(),
                "amount": row.amount.to_string(),
                "delegate": row.delegate.as_ref().map(|d| bs58::encode(d).into_string()),
                "delegatedAmount": row.delegated_amount.to_string(),
                "frozen": row.frozen,
                "tokenProgram": bs58::encode(&row.token_program).into_string(),
            }
        })
    }

    fn asset_to_json(a: &super::postgres::AssetRow) -> serde_json::Value {
        let name = a.raw_name.as_ref()
            .map(|n| String::from_utf8_lossy(n).trim_end_matches('\0').to_string());
        let symbol = a.raw_symbol.as_ref()
            .map(|s| String::from_utf8_lossy(s).trim_end_matches('\0').to_string());
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

    fn asset_list_response(total: i64, page: i64, limit: i64, assets: &[super::postgres::AssetRow]) -> serde_json::Value {
        serde_json::json!({
            "total": total,
            "limit": limit,
            "page": page,
            "items": assets.iter().map(Self::asset_to_json).collect::<Vec<_>>()
        })
    }

    // -- Public API: Block / History --

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
            return Ok(Some(serde_json::from_slice(&data)?));
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

    // -- Public API: Account State --
    // RocksDB first for point lookups, PG fallback for program scans

    pub fn get_account_info(&self, pubkey: &[u8; 32], encoding: &str) -> Result<Option<serde_json::Value>> {
        if let Some(stored) = self.rocks_get_account(pubkey) {
            return Ok(Some(Self::account_to_json(&stored, encoding)));
        }
        // PG program_accounts fallback
        // (sync context — use block_on or accept no PG fallback for point lookups)
        Ok(None)
    }

    pub async fn get_multiple_accounts(&self, pubkeys: &[[u8; 32]], encoding: &str) -> Vec<Option<serde_json::Value>> {
        pubkeys.iter().map(|pk| {
            self.rocks_get_account(pk).map(|stored| Self::account_to_json(&stored, encoding))
        }).collect()
    }

    pub async fn get_program_accounts(&self, program_id: &[u8; 32], encoding: &str) -> Result<Vec<serde_json::Value>> {
        // RocksDB prefix scan
        let rocks_result = self.rocks.get_program_accounts(program_id)?;
        if !rocks_result.is_empty() {
            return Ok(rocks_result.iter().filter_map(|(pubkey, data)| {
                StoredAccount::deserialize(data).map(|account| {
                    serde_json::json!({
                        "pubkey": bs58::encode(pubkey).into_string(),
                        "account": Self::account_to_json(&account, encoding)
                    })
                })
            }).collect());
        }

        // PostgreSQL program_accounts table
        let rows = self.pg.get_program_accounts_pg(program_id).await?;
        Ok(rows.iter().map(|row| {
            serde_json::json!({
                "pubkey": bs58::encode(&row.pubkey).into_string(),
                "account": {
                    "lamports": row.lamports,
                    "owner": bs58::encode(&row.owner).into_string(),
                    "data": [bs58::encode(&row.data).into_string(), "base58"],
                    "executable": row.executable,
                    "rentEpoch": row.rent_epoch,
                    "space": row.data.len(),
                }
            })
        }).collect())
    }

    // -- Public API: Tokens (PostgreSQL) --

    pub async fn get_token_accounts_by_owner(&self, owner: &[u8]) -> Result<Vec<serde_json::Value>> {
        let rows = self.pg.get_token_accounts_by_owner(owner).await?;
        Ok(rows.iter().map(Self::token_account_to_json).collect())
    }

    pub async fn get_token_accounts_by_delegate(&self, delegate: &[u8]) -> Result<Vec<serde_json::Value>> {
        let rows = self.pg.get_token_accounts_by_delegate(delegate).await?;
        Ok(rows.iter().map(Self::token_account_to_json).collect())
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

    pub async fn get_token_largest_accounts(&self, mint: &[u8], limit: i64) -> Result<Vec<serde_json::Value>> {
        let rows = self.pg.get_token_largest_accounts(mint, limit).await?;
        Ok(rows.iter().map(|row| {
            serde_json::json!({
                "address": bs58::encode(&row.pubkey).into_string(),
                "amount": row.amount.to_string(),
            })
        }).collect())
    }

    // -- Public API: DAS Assets (PostgreSQL) --

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

        obj.insert("authorities".to_string(), serde_json::json!(
            authority.map(|a| vec![serde_json::json!({
                "address": bs58::encode(&a.authority).into_string(),
                "scopes": a.scopes,
            })]).unwrap_or_default()
        ));
        obj.insert("creators".to_string(), serde_json::json!(
            creators.iter().map(|c| serde_json::json!({
                "address": bs58::encode(&c.creator).into_string(),
                "share": c.share,
                "verified": c.verified,
            })).collect::<Vec<_>>()
        ));
        obj.insert("grouping".to_string(), serde_json::json!(
            grouping.iter().map(|g| serde_json::json!({
                "group_key": g.group_key,
                "group_value": g.group_value,
                "verified": g.verified,
            })).collect::<Vec<_>>()
        ));

        Ok(Some(json))
    }

    pub async fn get_assets_by_owner(&self, owner: &[u8], page: i64, limit: i64) -> Result<serde_json::Value> {
        let (total, rows) = self.pg.get_assets_by_owner(owner, page, limit).await?;
        Ok(Self::asset_list_response(total, page, limit, &rows))
    }

    pub async fn get_assets_by_creator(&self, creator: &[u8], page: i64, limit: i64) -> Result<serde_json::Value> {
        let (total, rows) = self.pg.get_assets_by_creator(creator, page, limit).await?;
        Ok(Self::asset_list_response(total, page, limit, &rows))
    }

    pub async fn get_assets_by_group(&self, group_key: &str, group_value: &str, page: i64, limit: i64) -> Result<serde_json::Value> {
        let (total, rows) = self.pg.get_assets_by_group(group_key, group_value, page, limit).await?;
        Ok(Self::asset_list_response(total, page, limit, &rows))
    }

    pub async fn get_assets_by_authority(&self, authority: &[u8], page: i64, limit: i64) -> Result<serde_json::Value> {
        let (total, rows) = self.pg.get_assets_by_authority(authority, page, limit).await?;
        Ok(Self::asset_list_response(total, page, limit, &rows))
    }

    pub async fn health_check(&self) -> bool {
        self.pg.health_check().await.is_ok() && self.cache.processed_slot() > 0
    }
}
