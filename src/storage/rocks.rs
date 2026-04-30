use anyhow::{Context, Result};
use rocksdb::{
    BlockBasedIndexType, BlockBasedOptions, BoundColumnFamily, Cache, ColumnFamilyDescriptor,
    DBCompressionType, DBWithThreadMode, MultiThreaded, Options, SliceTransform, WriteBatch,
    WriteOptions,
};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::config::RocksDbConfig;
use crate::metrics;
use crate::source::snapshot::SnapshotAccount;
use crate::storage::accounts::StoredAccount;
use crate::types::*;

static SPL_TOKEN: std::sync::LazyLock<[u8; 32]> = std::sync::LazyLock::new(|| {
    bs58::decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
        .into_vec()
        .unwrap()
        .try_into()
        .unwrap()
});
static SPL_TOKEN_2022: std::sync::LazyLock<[u8; 32]> = std::sync::LazyLock::new(|| {
    bs58::decode("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
        .into_vec()
        .unwrap()
        .try_into()
        .unwrap()
});

const CF_SLOT_INDEX: &str = "slot_index";
const CF_TX_INDEX: &str = "tx_index";
const CF_SFA_INDEX: &str = "sfa_index";
const CF_ACCOUNTS: &str = "accounts";
const CF_PROGRAM_INDEX: &str = "program_index";
const CF_OWNER_ATAS: &str = "owner_atas";
const CF_MINT_TOP_HOLDERS: &str = "mint_top_holders";

const ALL_CFS: &[&str] = &[
    CF_SLOT_INDEX,
    CF_TX_INDEX,
    CF_SFA_INDEX,
    CF_ACCOUNTS,
    CF_PROGRAM_INDEX,
    CF_OWNER_ATAS,
    CF_MINT_TOP_HOLDERS,
];

pub const MINT_TOP_HOLDERS_K: usize = 100;

fn apply_cf_compaction_tuning(opts: &mut Options) {
    apply_cf_compaction_tuning_with(opts, 2);
}

fn apply_cf_compaction_tuning_with(opts: &mut Options, l0_trigger: i32) {
    opts.set_level_zero_file_num_compaction_trigger(l0_trigger);
    // Generous L0 triggers — a cold-start L0 stall would block sync put_cf and hang the runtime.
    opts.set_level_zero_slowdown_writes_trigger(2000);
    opts.set_level_zero_stop_writes_trigger(8000);
    opts.set_target_file_size_base(128 * 1024 * 1024);
    opts.set_target_file_size_multiplier(2);
    opts.set_max_bytes_for_level_base(1024 * 1024 * 1024);
    opts.set_max_bytes_for_level_multiplier(10.0);
    opts.set_level_compaction_dynamic_level_bytes(true);
    opts.set_bottommost_compression_type(DBCompressionType::Zstd);
    opts.set_bottommost_zstd_max_train_bytes(0, true);
}

fn build_block_opts(
    cache: &Cache,
    block_size: usize,
    with_bloom: bool,
    prefix_bloom: bool,
) -> BlockBasedOptions {
    let mut bo = BlockBasedOptions::default();
    bo.set_block_cache(cache);
    bo.set_block_size(block_size);
    bo.set_format_version(5);
    bo.set_index_type(BlockBasedIndexType::TwoLevelIndexSearch);
    bo.set_partition_filters(true);
    bo.set_metadata_block_size(4096);
    bo.set_cache_index_and_filter_blocks(true);
    bo.set_pin_top_level_index_and_filter(true);
    bo.set_pin_l0_filter_and_index_blocks_in_cache(true);
    if with_bloom {
        // 15 bits/key: FP ~0.04% (vs 0.9% at 10).
        bo.set_bloom_filter(15.0, false);
        if prefix_bloom {
            bo.set_whole_key_filtering(false);
        }
    }
    bo
}

type DB = DBWithThreadMode<MultiThreaded>;

pub struct UnifiedRocksDb {
    db: Arc<DB>,
    /// Used by the self-heal path to move corrupt SSTs to `<root>/quarantine/`.
    root: std::path::PathBuf,
}

impl UnifiedRocksDb {
    pub fn open(config: &RocksDbConfig) -> Result<Self> {
        let path = Path::new(&config.path);
        std::fs::create_dir_all(path).context("creating rocksdb directory")?;

        // Restore quarantined SSTs before open: MANIFEST still references them, so
        // a missing file crashloops startup. Still-bad files re-quarantine at read time.
        let quarantine = path.join("quarantine");
        if quarantine.exists() {
            if let Ok(entries) = std::fs::read_dir(&quarantine) {
                for entry in entries.flatten() {
                    let from = entry.path();
                    let Some(name) = from.file_name() else {
                        continue;
                    };
                    let to = path.join(name);
                    match std::fs::rename(&from, &to) {
                        Ok(()) => {
                            tracing::warn!(file = %from.display(), "restored quarantined SST for DB open")
                        }
                        Err(e) => {
                            tracing::error!(file = %from.display(), error = %e, "failed to restore quarantined SST")
                        }
                    }
                }
            }
        }

        // 64 GiB shared block cache: sized for cold tx_index lookups without crowding memtables.
        let cache = Cache::new_lru_cache(64 * 1024 * 1024 * 1024);

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        // 512 MB × 6: fewer L0 flushes during cold-start at the cost of slower gSFA scans
        // over unflushed memtables — cold-start dominates ingest cost on this deployment.
        db_opts.set_write_buffer_size(512 * 1024 * 1024);
        db_opts.set_max_write_buffer_number(6);
        db_opts.set_max_open_files(config.max_open_files);
        db_opts.set_allow_concurrent_memtable_write(true);
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get() as i32)
            .unwrap_or(4);
        db_opts.increase_parallelism(parallelism);
        // Must exceed subcompactions × active_cfs, otherwise manual compaction runs single-threaded.
        db_opts.set_max_background_jobs(parallelism.max(8));
        // DB-level setting; the CFOptions one of the same name never reaches the compactor.
        db_opts.set_max_subcompactions(parallelism as u32);
        db_opts.set_periodic_compaction_seconds(3600);

        // 2 GB/s: NVMe sustains this; lower caps throttle the compactor faster than L0 drains.
        db_opts.set_ratelimiter(2 * 1024 * 1024 * 1024, 100_000, 10);

        // Direct I/O on flush+compaction prevents torn kernel writes surfacing as half-written SSTs.
        db_opts.set_use_direct_io_for_flush_and_compaction(true);
        db_opts.set_bytes_per_sync(1024 * 1024);
        db_opts.set_wal_bytes_per_sync(1024 * 1024);
        db_opts.set_paranoid_checks(true);

        db_opts.enable_statistics();
        db_opts.set_stats_dump_period_sec(600);
        db_opts.set_report_bg_io_stats(true);
        db_opts.set_wal_recovery_mode(rocksdb::DBRecoveryMode::PointInTime);

        if config.enable_pipelined_writes {
            db_opts.set_enable_pipelined_write(true);
        }

        let cfs = vec![
            Self::slot_index_cf_opts(&cache),
            Self::tx_index_cf_opts(&cache),
            Self::sfa_index_cf_opts(&cache),
            Self::accounts_cf_opts(&cache),
            Self::program_index_cf_opts(&cache),
            Self::owner_atas_cf_opts(&cache),
            Self::mint_top_holders_cf_opts(&cache),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cfs)
            .context("opening rocksdb with column families")?;

        let quarantine = path.join("quarantine");
        if !quarantine.exists() {
            let _ = std::fs::create_dir_all(&quarantine);
        }

        Ok(Self {
            db: Arc::new(db),
            root: path.to_path_buf(),
        })
    }

    /// Quarantine a corrupt SST surfaced by `Corruption: ... in /path/N.sst`,
    /// then force-compact the CF so RocksDB drops its reference. Lost keys
    /// refill from the stream. Returns true iff a file was quarantined.
    pub fn try_quarantine_corrupt_sst(&self, err: &str, cf_hint: &str) -> bool {
        if !err.contains("Corruption") || !err.contains(".sst") {
            return false;
        }
        let Some(idx) = err.find(" in ") else {
            return false;
        };
        let tail = &err[idx + 4..];
        let Some(end) = tail.find(".sst") else {
            return false;
        };
        let sst_path = std::path::PathBuf::from(&tail[..end + 4]);
        let Some(file_name) = sst_path.file_name() else {
            return false;
        };

        let dest = self.root.join("quarantine").join(file_name);
        // Concurrent callers race; NotFound means another thread already quarantined this SST.
        let moved_now = match std::fs::rename(&sst_path, &dest) {
            Ok(()) => true,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return true,
            Err(e) => {
                tracing::error!(file = %sst_path.display(), error = %e,
                    "self-heal: failed to quarantine corrupt SST");
                return false;
            }
        };

        tracing::warn!(
            file = %sst_path.display(),
            cf = cf_hint,
            "self-heal: quarantined corrupt SST, forcing compaction"
        );
        crate::metrics::ROCKSDB_QUARANTINED.inc();

        if let Some(cf) = self.db.cf_handle(cf_hint) {
            self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        moved_now
    }

    fn slot_index_cf_opts(cache: &Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        apply_cf_compaction_tuning(&mut opts);
        let block_opts = build_block_opts(cache, 4 * 1024, false, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_SLOT_INDEX, opts)
    }

    fn tx_index_cf_opts(cache: &Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        // Thin L0: write-heavy CF served via point lookups.
        apply_cf_compaction_tuning_with(&mut opts, 1);
        let block_opts = build_block_opts(cache, 16 * 1024, true, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_TX_INDEX, opts)
    }

    fn sfa_index_cf_opts(cache: &Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
        opts.set_memtable_prefix_bloom_ratio(0.1);
        apply_cf_compaction_tuning_with(&mut opts, 1);
        let block_opts = build_block_opts(cache, 16 * 1024, true, true);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_SFA_INDEX, opts)
    }

    fn accounts_cf_opts(cache: &Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        apply_cf_compaction_tuning(&mut opts);
        let block_opts = build_block_opts(cache, 8 * 1024, true, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_ACCOUNTS, opts)
    }

    fn program_index_cf_opts(cache: &Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
        opts.set_memtable_prefix_bloom_ratio(0.1);
        apply_cf_compaction_tuning(&mut opts);
        let block_opts = build_block_opts(cache, 4 * 1024, true, true);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_PROGRAM_INDEX, opts)
    }

    fn owner_atas_cf_opts(cache: &Cache) -> ColumnFamilyDescriptor {
        // Key: [owner(32) | token_account(32)] → empty. Prefix scan by owner.
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
        opts.set_memtable_prefix_bloom_ratio(0.1);
        apply_cf_compaction_tuning_with(&mut opts, 1);
        let block_opts = build_block_opts(cache, 4 * 1024, true, true);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_OWNER_ATAS, opts)
    }

    fn mint_top_holders_cf_opts(cache: &Cache) -> ColumnFamilyDescriptor {
        // Key: mint(32) → bincode Vec<(amount u64, pubkey [u8;32])>, sorted
        // DESC, capped at MINT_TOP_HOLDERS_K.
        let mut opts = Options::default();
        opts.set_compression_type(DBCompressionType::Lz4);
        apply_cf_compaction_tuning_with(&mut opts, 1);
        let block_opts = build_block_opts(cache, 4 * 1024, true, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_MINT_TOP_HOLDERS, opts)
    }

    fn cf(&self, name: &str) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(name).expect("column family must exist")
    }

    pub fn put_slot_index(&self, slot: Slot, data: &[u8]) -> Result<()> {
        self.db
            .put_cf(&self.cf(CF_SLOT_INDEX), slot.to_be_bytes(), data)?;
        Ok(())
    }

    pub fn get_slot_index(&self, slot: Slot) -> Result<Option<Vec<u8>>> {
        Ok(self
            .db
            .get_cf(&self.cf(CF_SLOT_INDEX), slot.to_be_bytes())?)
    }

    pub fn first_slot_index(&self) -> Option<Slot> {
        let cf = self.cf(CF_SLOT_INDEX);
        let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        iter.next()
            .and_then(|r| r.ok())
            .and_then(|(k, _)| k.as_ref().try_into().ok().map(Slot::from_be_bytes))
    }

    pub fn scan_slot_index_range(&self, start: Slot, end: Slot) -> Result<Vec<Slot>> {
        let cf = self.cf(CF_SLOT_INDEX);
        let mut iter = self.db.iterator_cf(
            &cf,
            rocksdb::IteratorMode::From(&start.to_be_bytes(), rocksdb::Direction::Forward),
        );
        let mut slots = Vec::with_capacity(((end - start) / 3).min(50_000) as usize);
        while let Some(Ok((key, _))) = iter.next() {
            if let Ok(bytes) = <[u8; 8]>::try_from(key.as_ref()) {
                let slot = Slot::from_be_bytes(bytes);
                if slot > end {
                    break;
                }
                slots.push(slot);
            }
        }
        Ok(slots)
    }

    pub fn put_tx_index(&self, signature: &[u8; 64], data: &[u8]) -> Result<()> {
        self.db.put_cf(&self.cf(CF_TX_INDEX), signature, data)?;
        Ok(())
    }

    /// Single WriteBatch — replaces 5000 per-tx FFI calls per block on the hot path.
    pub fn put_tx_index_batch(&self, entries: &[([u8; 64], Vec<u8>)]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let cf = self.cf(CF_TX_INDEX);
        let mut batch = WriteBatch::default();
        for (sig, data) in entries {
            batch.put_cf(&cf, sig, data);
        }
        self.db.write(batch)?;
        Ok(())
    }

    pub fn get_tx_index(&self, signature: &[u8; 64]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf(&self.cf(CF_TX_INDEX), signature)?)
    }

    pub fn get_tx_index_batch(&self, signatures: &[[u8; 64]]) -> Result<Vec<Option<Vec<u8>>>> {
        let cf = self.cf(CF_TX_INDEX);
        let keys: Vec<(&Arc<BoundColumnFamily<'_>>, &[u8; 64])> =
            signatures.iter().map(|s| (&cf, s)).collect();
        Ok(self
            .db
            .multi_get_cf(keys)
            .into_iter()
            .map(|r| r.ok().flatten())
            .collect())
    }

    /// Key: [address(32) | slot(8 BE)] → bincode Vec<SignatureEntry>.
    pub fn put_sfa_batch(&self, entries: &[(solana_pubkey::Pubkey, Slot, Vec<u8>)]) -> Result<()> {
        let cf = self.cf(CF_SFA_INDEX);
        let mut batch = WriteBatch::default();
        for (address, slot, data) in entries {
            let mut key = Vec::with_capacity(40);
            key.extend_from_slice(address.as_ref());
            key.extend_from_slice(&slot.to_be_bytes());
            batch.put_cf(&cf, &key, data);
        }
        self.db.write(batch)?;
        Ok(())
    }

    /// Reverse slot scan with sig-counted limit. Hot addresses pack hundreds of sigs per slot,
    /// so `iter_sfa`'s slot-counted limit overshoots the requested page size.
    pub fn iter_sfa_limited(
        &self,
        address: &solana_pubkey::Pubkey,
        before_slot: Option<Slot>,
        sig_limit: usize,
    ) -> Result<Vec<(Slot, Vec<super::super::types::SignatureEntry>)>> {
        let cf = self.cf(CF_SFA_INDEX);
        let prefix = address.as_ref();

        let start_slot = before_slot.unwrap_or(u64::MAX);
        let mut seek_key = Vec::with_capacity(40);
        seek_key.extend_from_slice(prefix);
        seek_key.extend_from_slice(&start_slot.to_be_bytes());

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_readahead_size(256 * 1024);
        read_opts.set_prefix_same_as_start(true);
        let mut iter = self.db.raw_iterator_cf_opt(&cf, read_opts);
        iter.seek_for_prev(&seek_key);

        let mut results: Vec<(Slot, Vec<super::super::types::SignatureEntry>)> = Vec::new();
        let mut total = 0usize;
        while iter.valid() && total < sig_limit {
            let (Some(key), Some(value)) = (iter.key(), iter.value()) else {
                break;
            };
            if key.len() < 40 || &key[..32] != prefix {
                break;
            }
            let slot = u64::from_be_bytes(key[32..40].try_into().unwrap());
            let sigs: Vec<super::super::types::SignatureEntry> =
                bincode::deserialize(value).or_else(|_| serde_json::from_slice(value))?;
            let remaining = sig_limit - total;
            let take = remaining.min(sigs.len());
            total += take;
            if take == sigs.len() {
                results.push((slot, sigs));
            } else {
                results.push((slot, sigs.into_iter().take(take).collect()));
            }
            iter.prev();
        }
        Ok(results)
    }

    /// Returns (slot, raw_bytes) pairs. Prefer `iter_sfa_limited`.
    pub fn iter_sfa(
        &self,
        address: &solana_pubkey::Pubkey,
        before_slot: Option<Slot>,
        limit: usize,
    ) -> Result<Vec<(Slot, Vec<u8>)>> {
        let cf = self.cf(CF_SFA_INDEX);
        let prefix = address.as_ref();

        let start_slot = before_slot.unwrap_or(u64::MAX);
        let mut seek_key = Vec::with_capacity(40);
        seek_key.extend_from_slice(prefix);
        seek_key.extend_from_slice(&start_slot.to_be_bytes());

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_readahead_size(256 * 1024);
        read_opts.set_prefix_same_as_start(true);
        let mut iter = self.db.raw_iterator_cf_opt(&cf, read_opts);
        iter.seek_for_prev(&seek_key);

        let mut results = Vec::with_capacity(limit);
        while iter.valid() && results.len() < limit {
            if let (Some(key), Some(value)) = (iter.key(), iter.value()) {
                if key.len() < 40 || &key[..32] != prefix {
                    break;
                }
                let slot = u64::from_be_bytes(key[32..40].try_into().unwrap());
                results.push((slot, value.to_vec()));
            }
            iter.prev();
        }
        Ok(results)
    }

    pub fn put_account(&self, pubkey: &[u8; 32], data: &[u8]) -> Result<()> {
        self.db.put_cf(&self.cf(CF_ACCOUNTS), pubkey, data)?;
        Ok(())
    }

    pub fn get_account(&self, pubkey: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf(&self.cf(CF_ACCOUNTS), pubkey)?)
    }

    pub fn get_accounts_batch(&self, pubkeys: &[[u8; 32]]) -> Vec<Option<Vec<u8>>> {
        let cf = self.cf(CF_ACCOUNTS);
        let keys: Vec<(&Arc<BoundColumnFamily<'_>>, &[u8; 32])> =
            pubkeys.iter().map(|k| (&cf, k)).collect();
        self.db
            .multi_get_cf(keys)
            .into_iter()
            .map(|r| r.ok().flatten())
            .collect()
    }

    /// Key: [program_id(32) | pubkey(32)] → empty.
    pub fn put_program_index(&self, program_id: &[u8; 32], pubkey: &[u8; 32]) -> Result<()> {
        let mut key = Vec::with_capacity(64);
        key.extend_from_slice(program_id);
        key.extend_from_slice(pubkey);
        self.db.put_cf(&self.cf(CF_PROGRAM_INDEX), &key, [])?;
        Ok(())
    }

    pub fn put_owner_ata(&self, owner: &[u8; 32], pubkey: &[u8; 32]) -> Result<()> {
        let mut key = Vec::with_capacity(64);
        key.extend_from_slice(owner);
        key.extend_from_slice(pubkey);
        self.db.put_cf(&self.cf(CF_OWNER_ATAS), &key, [])?;
        Ok(())
    }

    pub fn put_owner_atas_batch(&self, entries: &[([u8; 32], [u8; 32])]) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }
        let cf = self.cf(CF_OWNER_ATAS);
        let mut batch = WriteBatch::default();
        for (owner, pubkey) in entries {
            let mut key = [0u8; 64];
            key[..32].copy_from_slice(owner);
            key[32..].copy_from_slice(pubkey);
            batch.put_cf(&cf, key, []);
        }
        self.db.write(batch)?;
        Ok(())
    }

    pub fn get_mint_top_holders(&self, mint: &[u8; 32]) -> Result<Vec<(u64, [u8; 32])>> {
        match self.db.get_cf(&self.cf(CF_MINT_TOP_HOLDERS), mint)? {
            Some(bytes) => Ok(bincode::deserialize(&bytes).unwrap_or_default()),
            None => Ok(Vec::new()),
        }
    }

    /// Merge `(amount, token_account)` updates into the per-mint top-K.
    pub fn update_mint_top_holders(
        &self,
        mint: &[u8; 32],
        updates: &[(u64, [u8; 32])],
    ) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let cf = self.cf(CF_MINT_TOP_HOLDERS);
        let mut current: Vec<(u64, [u8; 32])> = match self.db.get_cf(&cf, mint)? {
            Some(bytes) => bincode::deserialize(&bytes).unwrap_or_default(),
            None => Vec::new(),
        };
        for (amount, pk) in updates {
            current.retain(|(_, existing)| existing != pk);
            if *amount > 0 {
                current.push((*amount, *pk));
            }
        }
        current.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
        current.truncate(MINT_TOP_HOLDERS_K);
        let encoded = bincode::serialize(&current)?;
        self.db.put_cf(&cf, mint, encoded)?;
        Ok(())
    }

    /// Snapshot batch apply (WAL off): accounts, program_index, owner_atas in one WriteBatch.
    /// Mint-top-holders RMW is deferred to `mint_agg` — collapses ~150M RMWs to ~num_mints writes.
    pub fn apply_snapshot_batch(
        &self,
        accounts: &[SnapshotAccount],
        slot: Slot,
        mint_agg: &mut HashMap<[u8; 32], Vec<(u64, [u8; 32])>>,
    ) -> Result<()> {
        let cf_acc = self.cf(CF_ACCOUNTS);
        let cf_prog = self.cf(CF_PROGRAM_INDEX);
        let cf_atas = self.cf(CF_OWNER_ATAS);
        let mut batch = WriteBatch::default();
        let mut prog_key = [0u8; 64];
        let mut ata_key = [0u8; 64];

        for acc in accounts {
            let stored = StoredAccount {
                owner: acc.owner,
                lamports: acc.lamports,
                data: acc.data.clone(),
                executable: acc.executable,
                rent_epoch: acc.rent_epoch,
                slot,
            }
            .serialize();
            batch.put_cf(&cf_acc, acc.pubkey, stored);

            prog_key[..32].copy_from_slice(&acc.owner);
            prog_key[32..].copy_from_slice(&acc.pubkey);
            batch.put_cf(&cf_prog, prog_key, []);

            // SPL Token / Token-2022 account: 165-byte payload, owner is the token program.
            if acc.data.len() == 165 && (acc.owner == *SPL_TOKEN || acc.owner == *SPL_TOKEN_2022) {
                let mut owner_pk = [0u8; 32];
                owner_pk.copy_from_slice(&acc.data[32..64]);
                ata_key[..32].copy_from_slice(&owner_pk);
                ata_key[32..].copy_from_slice(&acc.pubkey);
                batch.put_cf(&cf_atas, ata_key, []);

                let amount = u64::from_le_bytes(acc.data[64..72].try_into().unwrap_or([0; 8]));
                if amount > 0 {
                    let mut mint = [0u8; 32];
                    mint.copy_from_slice(&acc.data[0..32]);
                    let entry = mint_agg.entry(mint).or_default();
                    entry.push((amount, acc.pubkey));
                    // Cap growth at 2K to bound RAM on hot mints; trimmed to top-K below.
                    if entry.len() >= MINT_TOP_HOLDERS_K * 2 {
                        entry.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
                        entry.truncate(MINT_TOP_HOLDERS_K);
                    }
                }
            }
        }

        let mut wo = WriteOptions::default();
        wo.disable_wal(true);
        self.db.write_opt(batch, &wo)?;
        Ok(())
    }

    /// Cold-start probe. Checking slot_index instead would re-trigger reapply every restart,
    /// because snapshots populate accounts but not slot_index.
    pub fn accounts_empty(&self) -> bool {
        let cf = self.cf(CF_ACCOUNTS);
        let mut iter = self.db.iterator_cf(&cf, rocksdb::IteratorMode::Start);
        iter.next().is_none()
    }

    /// One-shot batch flush. Pair with `apply_snapshot_batch` at end of cold-start.
    pub fn flush_mint_top_holders(
        &self,
        mut agg: HashMap<[u8; 32], Vec<(u64, [u8; 32])>>,
    ) -> Result<usize> {
        let cf = self.cf(CF_MINT_TOP_HOLDERS);
        let mut batch = WriteBatch::default();
        let count = agg.len();
        for (mint, holders) in agg.iter_mut() {
            holders.sort_unstable_by_key(|b| std::cmp::Reverse(b.0));
            holders.truncate(MINT_TOP_HOLDERS_K);
            let encoded = bincode::serialize(holders)?;
            batch.put_cf(&cf, mint, encoded);
        }
        let mut wo = WriteOptions::default();
        wo.disable_wal(true);
        self.db.write_opt(batch, &wo)?;
        Ok(count)
    }

    pub fn get_owner_atas(&self, owner: &[u8; 32], cap: usize) -> Result<Vec<[u8; 32]>> {
        let cf = self.cf(CF_OWNER_ATAS);
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek(owner);
        let mut out = Vec::new();
        while iter.valid() && out.len() < cap {
            if let Some(key) = iter.key() {
                if key.len() != 64 || &key[..32] != owner {
                    break;
                }
                out.push(key[32..64].try_into().unwrap());
            }
            iter.next();
        }
        Ok(out)
    }

    pub fn get_program_accounts(&self, program_id: &[u8; 32]) -> Result<Vec<([u8; 32], Vec<u8>)>> {
        let cf_prog = self.cf(CF_PROGRAM_INDEX);
        let cf_acct = self.cf(CF_ACCOUNTS);

        let mut read_opts = rocksdb::ReadOptions::default();
        read_opts.set_readahead_size(2 * 1024 * 1024);
        let mut iter = self.db.raw_iterator_cf_opt(&cf_prog, read_opts);
        iter.seek(program_id);

        let mut pubkeys: Vec<[u8; 32]> = Vec::new();
        while iter.valid() {
            if let Some(key) = iter.key() {
                if key.len() != 64 || &key[..32] != program_id {
                    break;
                }
                pubkeys.push(key[32..64].try_into().unwrap());
            }
            iter.next();
        }

        if pubkeys.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<(&Arc<BoundColumnFamily<'_>>, &[u8; 32])> =
            pubkeys.iter().map(|k| (&cf_acct, k)).collect();
        let values = self.db.multi_get_cf(keys);

        let mut results = Vec::with_capacity(pubkeys.len());
        for (pubkey, val) in pubkeys.into_iter().zip(values) {
            if let Ok(Some(data)) = val {
                results.push((pubkey, data));
            }
        }
        Ok(results)
    }

    pub fn write_account_batch(&self, accounts: &[StoredAccountEntry]) -> Result<()> {
        let mut batch = WriteBatch::default();
        let cf_acct = self.cf(CF_ACCOUNTS);
        let cf_prog = self.cf(CF_PROGRAM_INDEX);

        for entry in accounts {
            batch.put_cf(&cf_acct, entry.pubkey, &entry.data);

            let mut prog_key = Vec::with_capacity(64);
            prog_key.extend_from_slice(&entry.owner);
            prog_key.extend_from_slice(&entry.pubkey);
            batch.put_cf(&cf_prog, &prog_key, []);
        }

        self.db.write(batch)?;
        Ok(())
    }

    pub fn inner(&self) -> &Arc<DB> {
        &self.db
    }

    pub fn compact_all(&self) -> Result<()> {
        for name in ALL_CFS {
            let cf = self.cf(name);
            self.db.compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
    }

    /// One DeleteRange per distinct address; walks the CF once.
    pub fn prune_sfa_before(&self, cutoff_slot: Slot) -> Result<u64> {
        let cf = self.cf(CF_SFA_INDEX);
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek_to_first();

        let mut dropped = 0u64;
        let mut last_address: Option<[u8; 32]> = None;

        while iter.valid() {
            let Some(key) = iter.key() else { break };
            if key.len() < 40 {
                iter.next();
                continue;
            }
            let address: [u8; 32] = key[0..32].try_into().unwrap();

            if Some(address) == last_address {
                iter.next();
                continue;
            }
            last_address = Some(address);

            let mut start = Vec::with_capacity(40);
            start.extend_from_slice(&address);
            start.extend_from_slice(&0u64.to_be_bytes());
            let mut end = Vec::with_capacity(40);
            end.extend_from_slice(&address);
            end.extend_from_slice(&cutoff_slot.to_be_bytes());

            self.db.delete_range_cf(&cf, &start, &end)?;
            dropped += 1;

            let mut next_prefix = address;
            for i in (0..32).rev() {
                if next_prefix[i] < u8::MAX {
                    next_prefix[i] += 1;
                    next_prefix[i + 1..32].fill(0);
                    break;
                }
            }
            iter.seek(next_prefix);
        }

        Ok(dropped)
    }

    pub fn prune_slot_index_before(&self, cutoff_slot: Slot) -> Result<u64> {
        let cf = self.cf(CF_SLOT_INDEX);
        let start = 0u64.to_be_bytes();
        let end = cutoff_slot.to_be_bytes();
        self.db.delete_range_cf(&cf, start, end)?;
        Ok(cutoff_slot)
    }

    /// Full scan; keys are signatures, not slot-ordered.
    pub fn prune_tx_index_before(&self, cutoff_slot: Slot) -> Result<u64> {
        let cf = self.cf(CF_TX_INDEX);
        let mut iter = self.db.raw_iterator_cf(&cf);
        iter.seek_to_first();

        let mut batch = rocksdb::WriteBatch::default();
        let mut dropped = 0u64;

        while iter.valid() {
            let Some(key) = iter.key() else { break };
            let Some(value) = iter.value() else {
                iter.next();
                continue;
            };

            // Disambiguate JSON backfill (`{"`) from prost; prost slot LSB collides with `{` 1/256.
            let slot = if value.starts_with(b"{\"") {
                serde_json::from_slice::<serde_json::Value>(value)
                    .ok()
                    .and_then(|v| v.get("slot").and_then(|s| s.as_u64()))
                    .unwrap_or(u64::MAX)
            } else if value.len() >= 8 {
                u64::from_le_bytes(value[0..8].try_into().unwrap_or([0; 8]))
            } else {
                u64::MAX
            };

            if slot < cutoff_slot {
                batch.delete_cf(&cf, key);
                dropped += 1;
                if dropped.is_multiple_of(100_000) {
                    self.db.write(batch)?;
                    batch = rocksdb::WriteBatch::default();
                }
            }

            iter.next();
        }

        if !batch.is_empty() {
            self.db.write(batch)?;
        }
        Ok(dropped)
    }

    pub fn estimated_size_bytes(&self) -> u64 {
        let mut total = 0u64;
        for name in ALL_CFS {
            let cf = self.cf(name);
            if let Ok(Some(size)) = self
                .db
                .property_int_value_cf(&cf, "rocksdb.estimate-live-data-size")
            {
                total += size;
            }
        }
        total
    }

    pub fn update_metrics(&self) {
        for name in ALL_CFS {
            let cf = self.cf(name);
            let sst = self
                .db
                .property_int_value_cf(&cf, "rocksdb.num-files-at-level0")
                .ok()
                .flatten()
                .unwrap_or(0) as i64;
            metrics::ROCKSDB_L0_FILES
                .with_label_values(&[name])
                .set(sst);

            if let Ok(Some(total)) = self
                .db
                .property_int_value_cf(&cf, "rocksdb.total-sst-files-size")
            {
                metrics::ROCKSDB_LIVE_DATA_SIZE
                    .with_label_values(&[name])
                    .set(total as i64);
            }

            let mut total_sst = 0i64;
            for level in 0..7 {
                let prop = format!("rocksdb.num-files-at-level{level}");
                if let Ok(Some(n)) = self.db.property_int_value_cf(&cf, &prop) {
                    total_sst += n as i64;
                }
            }
            metrics::ROCKSDB_SST_COUNT
                .with_label_values(&[name])
                .set(total_sst);
        }
    }
}

impl Clone for UnifiedRocksDb {
    fn clone(&self) -> Self {
        Self {
            db: Arc::clone(&self.db),
            root: self.root.clone(),
        }
    }
}

pub struct StoredAccountEntry {
    pub pubkey: [u8; 32],
    pub owner: [u8; 32],
    pub data: Vec<u8>,
    pub slot: Slot,
}
