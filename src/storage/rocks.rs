use anyhow::{Context, Result};
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded,
    Options, SliceTransform, WriteBatch,
};
use std::path::Path;
use std::sync::Arc;

use crate::config::RocksDbConfig;
use crate::metrics;
use crate::types::*;

/// Column family names for all indexed data.
///
/// Block pipeline CFs (history queries):
const CF_SLOT_INDEX: &str = "slot_index";
const CF_TX_INDEX: &str = "tx_index";
const CF_SFA_INDEX: &str = "sfa_index";

/// Account pipeline CFs (state queries):
const CF_ACCOUNTS: &str = "accounts";
const CF_PROGRAM_INDEX: &str = "program_index";

const ALL_CFS: &[&str] = &[
    CF_SLOT_INDEX,
    CF_TX_INDEX,
    CF_SFA_INDEX,
    CF_ACCOUNTS,
    CF_PROGRAM_INDEX,
];

/// Apply shared compaction tuning to a CF's options.
/// Without this, RocksDB uses defaults (L0 trigger=4, unbounded level sizes)
/// which falls behind under high ingest and piles up thousands of L0 SSTs.
fn apply_cf_compaction_tuning(opts: &mut Options) {
    opts.set_level_zero_file_num_compaction_trigger(2);
    opts.set_level_zero_slowdown_writes_trigger(20);
    opts.set_level_zero_stop_writes_trigger(36);
    opts.set_target_file_size_base(64 * 1024 * 1024);
    opts.set_max_bytes_for_level_base(512 * 1024 * 1024);
    opts.set_max_bytes_for_level_multiplier(10.0);
}

type DB = DBWithThreadMode<MultiThreaded>;

pub struct UnifiedRocksDb {
    db: Arc<DB>,
}

impl UnifiedRocksDb {
    pub fn open(config: &RocksDbConfig) -> Result<Self> {
        let path = Path::new(&config.path);
        std::fs::create_dir_all(path).context("creating rocksdb directory")?;

        // Shared block cache across all CFs — bounds total memory usage.
        // Without this, each CF allocates its own unbounded cache.
        let cache = rocksdb::Cache::new_lru_cache(512 * 1024 * 1024); // 512MB shared

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        // Per-CF write buffer. 5 CFs × 64MB × 2 (active + flushing) = 640MB max memtables.
        db_opts.set_write_buffer_size(64 * 1024 * 1024);
        db_opts.set_max_write_buffer_number(2);
        db_opts.set_max_open_files(config.max_open_files);
        db_opts.set_allow_concurrent_memtable_write(true);
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get() as i32)
            .unwrap_or(4);
        db_opts.increase_parallelism(parallelism);
        db_opts.set_max_background_jobs(parallelism.max(4));
        // Force L0 to drain even during low-write periods; prevents silent accumulation.
        db_opts.set_periodic_compaction_seconds(3600);

        if config.enable_pipelined_writes {
            db_opts.set_enable_pipelined_write(true);
        }

        let cfs = vec![
            Self::slot_index_cf_opts(&cache),
            Self::tx_index_cf_opts(&cache),
            Self::sfa_index_cf_opts(&cache),
            Self::accounts_cf_opts(&cache),
            Self::program_index_cf_opts(&cache),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cfs)
            .context("opening rocksdb with column families")?;

        Ok(Self { db: Arc::new(db) })
    }

    fn slot_index_cf_opts(cache: &rocksdb::Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        apply_cf_compaction_tuning(&mut opts);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(cache);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_SLOT_INDEX, opts)
    }

    fn tx_index_cf_opts(cache: &rocksdb::Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        apply_cf_compaction_tuning(&mut opts);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(cache);
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_TX_INDEX, opts)
    }

    fn sfa_index_cf_opts(cache: &rocksdb::Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
        apply_cf_compaction_tuning(&mut opts);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(cache);
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_SFA_INDEX, opts)
    }

    fn accounts_cf_opts(cache: &rocksdb::Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        apply_cf_compaction_tuning(&mut opts);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(cache);
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_ACCOUNTS, opts)
    }

    fn program_index_cf_opts(cache: &rocksdb::Cache) -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
        apply_cf_compaction_tuning(&mut opts);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(cache);
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_PROGRAM_INDEX, opts)
    }

    fn cf(&self, name: &str) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(name).expect("column family must exist")
    }

    // --- Block pipeline operations ---

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

    pub fn put_tx_index(&self, signature: &[u8; 64], data: &[u8]) -> Result<()> {
        self.db.put_cf(&self.cf(CF_TX_INDEX), signature, data)?;
        Ok(())
    }

    pub fn get_tx_index(&self, signature: &[u8; 64]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf(&self.cf(CF_TX_INDEX), signature)?)
    }

    /// Write address → signature entries for a slot.
    /// Key format: [address(32) | slot(8 BE)]
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

    /// Iterate signatures for an address in reverse slot order.
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

        let mut iter = self.db.raw_iterator_cf(&cf);
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

    // --- Account pipeline operations ---

    pub fn put_account(&self, pubkey: &[u8; 32], data: &[u8]) -> Result<()> {
        self.db.put_cf(&self.cf(CF_ACCOUNTS), pubkey, data)?;
        Ok(())
    }

    pub fn get_account(&self, pubkey: &[u8; 32]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf(&self.cf(CF_ACCOUNTS), pubkey)?)
    }

    /// Store program index entry. Key: [program_id(32) | pubkey(32)] → empty
    pub fn put_program_index(&self, program_id: &[u8; 32], pubkey: &[u8; 32]) -> Result<()> {
        let mut key = Vec::with_capacity(64);
        key.extend_from_slice(program_id);
        key.extend_from_slice(pubkey);
        self.db.put_cf(&self.cf(CF_PROGRAM_INDEX), &key, [])?;
        Ok(())
    }

    /// Get all accounts owned by a program via prefix scan.
    pub fn get_program_accounts(&self, program_id: &[u8; 32]) -> Result<Vec<([u8; 32], Vec<u8>)>> {
        let cf_prog = self.cf(CF_PROGRAM_INDEX);
        let cf_acct = self.cf(CF_ACCOUNTS);

        let mut iter = self.db.raw_iterator_cf(&cf_prog);
        iter.seek(program_id);

        let mut results = Vec::new();
        while iter.valid() {
            if let Some(key) = iter.key() {
                if key.len() != 64 || &key[..32] != program_id {
                    break;
                }
                let pubkey: [u8; 32] = key[32..64].try_into().unwrap();
                if let Ok(Some(data)) = self.db.get_cf(&cf_acct, pubkey) {
                    results.push((pubkey, data));
                }
            }
            iter.next();
        }
        Ok(results)
    }

    /// Batch write accounts and their program indexes atomically.
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

    /// Force a full-range compaction on every CF. Blocking and expensive —
    /// use once after recovery or on a scheduled maintenance window to drain
    /// an accumulated L0 backlog. Safe to call while serving reads.
    pub fn compact_all(&self) -> Result<()> {
        for name in ALL_CFS {
            let cf = self.cf(name);
            self.db
                .compact_range_cf(&cf, None::<&[u8]>, None::<&[u8]>);
        }
        Ok(())
    }

    /// Delete all sfa_index entries older than `cutoff_slot`.
    /// Keys are (address[32] | slot[8 BE]); we delete per-address ranges.
    /// Since we don't have a global "list of addresses" cheaply, we scan
    /// the CF once, skip-seek per prefix. For a prune-retention workload
    /// running daily, this is acceptable.
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

            // Range: [address | 0] .. [address | cutoff]
            let mut start = Vec::with_capacity(40);
            start.extend_from_slice(&address);
            start.extend_from_slice(&0u64.to_be_bytes());
            let mut end = Vec::with_capacity(40);
            end.extend_from_slice(&address);
            end.extend_from_slice(&cutoff_slot.to_be_bytes());

            self.db.delete_range_cf(&cf, &start, &end)?;
            dropped += 1;

            // Seek past this address to the next one
            let mut next_prefix = address;
            for i in (0..32).rev() {
                if next_prefix[i] < u8::MAX {
                    next_prefix[i] += 1;
                    for j in i + 1..32 {
                        next_prefix[j] = 0;
                    }
                    break;
                }
            }
            iter.seek(next_prefix);
        }

        Ok(dropped)
    }

    /// Refresh Prometheus gauges for per-CF SST counts, L0 file counts, and
    /// estimated live data size. Call on a timer (e.g. every 60s) from main.
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

            // num-live-sst-files is tracked via total across all levels.
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
        }
    }
}

pub struct StoredAccountEntry {
    pub pubkey: [u8; 32],
    pub owner: [u8; 32],
    pub data: Vec<u8>,
    pub slot: Slot,
}
