use anyhow::{Context, Result};
use rocksdb::{
    BlockBasedOptions, BoundColumnFamily, ColumnFamilyDescriptor, DBWithThreadMode, MultiThreaded,
    Options, SliceTransform, WriteBatch,
};
use std::path::Path;
use std::sync::Arc;

use light_indexer_core::config::RocksDbConfig;
use light_indexer_core::types::*;

/// Column family names for all indexed data.
///
/// Block pipeline CFs (history queries):
const CF_SLOT_INDEX: &str = "slot_index";
const CF_TX_INDEX: &str = "tx_index";
const CF_SFA_INDEX: &str = "sfa_index";

/// Account pipeline CFs (state queries):
const CF_ACCOUNTS: &str = "accounts";
const CF_PROGRAM_INDEX: &str = "program_index";

type DB = DBWithThreadMode<MultiThreaded>;

pub struct UnifiedRocksDb {
    db: Arc<DB>,
}

impl UnifiedRocksDb {
    pub fn open(config: &RocksDbConfig) -> Result<Self> {
        let path = Path::new(&config.path);
        std::fs::create_dir_all(path).context("creating rocksdb directory")?;

        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        db_opts.set_write_buffer_size(config.write_buffer_size);
        db_opts.set_max_open_files(config.max_open_files);
        db_opts.set_allow_concurrent_memtable_write(true);
        let parallelism = std::thread::available_parallelism()
            .map(|n| n.get() as i32)
            .unwrap_or(4);
        db_opts.increase_parallelism(parallelism);
        db_opts.set_max_background_jobs(4);

        if config.enable_pipelined_writes {
            db_opts.set_enable_pipelined_write(true);
        }

        let cfs = vec![
            // Block pipeline: slot metadata
            Self::slot_index_cf_opts(),
            // Block pipeline: transaction signature → location
            Self::tx_index_cf_opts(),
            // Block pipeline: address → signatures
            Self::sfa_index_cf_opts(),
            // Account pipeline: pubkey → account data
            Self::accounts_cf_opts(),
            // Account pipeline: program_id + pubkey → empty (prefix scan)
            Self::program_index_cf_opts(),
        ];

        let db = DB::open_cf_descriptors(&db_opts, path, cfs)
            .context("opening rocksdb with column families")?;

        Ok(Self { db: Arc::new(db) })
    }

    fn slot_index_cf_opts() -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        ColumnFamilyDescriptor::new(CF_SLOT_INDEX, opts)
    }

    fn tx_index_cf_opts() -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_TX_INDEX, opts)
    }

    fn sfa_index_cf_opts() -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        // Prefix extractor: first 32 bytes = address
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_SFA_INDEX, opts)
    }

    fn accounts_cf_opts() -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        opts.set_bottommost_compression_type(rocksdb::DBCompressionType::Zstd);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_bloom_filter(10.0, false);
        opts.set_block_based_table_factory(&block_opts);
        ColumnFamilyDescriptor::new(CF_ACCOUNTS, opts)
    }

    fn program_index_cf_opts() -> ColumnFamilyDescriptor {
        let mut opts = Options::default();
        opts.set_compression_type(rocksdb::DBCompressionType::Lz4);
        // Prefix extractor: first 32 bytes = program_id for prefix scans
        opts.set_prefix_extractor(SliceTransform::create_fixed_prefix(32));
        let mut block_opts = BlockBasedOptions::default();
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
        Ok(self.db.get_cf(&self.cf(CF_SLOT_INDEX), slot.to_be_bytes())?)
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
    pub fn put_sfa_batch(
        &self,
        entries: &[(solana_pubkey::Pubkey, Slot, Vec<u8>)],
    ) -> Result<()> {
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
    pub fn put_program_index(
        &self,
        program_id: &[u8; 32],
        pubkey: &[u8; 32],
    ) -> Result<()> {
        let mut key = Vec::with_capacity(64);
        key.extend_from_slice(program_id);
        key.extend_from_slice(pubkey);
        self.db.put_cf(&self.cf(CF_PROGRAM_INDEX), &key, &[])?;
        Ok(())
    }

    /// Get all accounts owned by a program via prefix scan.
    pub fn get_program_accounts(
        &self,
        program_id: &[u8; 32],
    ) -> Result<Vec<([u8; 32], Vec<u8>)>> {
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
                if let Ok(Some(data)) = self.db.get_cf(&cf_acct, &pubkey) {
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
            batch.put_cf(&cf_acct, &entry.pubkey, &entry.data);

            let mut prog_key = Vec::with_capacity(64);
            prog_key.extend_from_slice(&entry.owner);
            prog_key.extend_from_slice(&entry.pubkey);
            batch.put_cf(&cf_prog, &prog_key, &[]);
        }

        self.db.write(batch)?;
        Ok(())
    }

    pub fn inner(&self) -> &Arc<DB> {
        &self.db
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
