//! ClickHouse historical store: base `transactions` table, MVs for
//! gSFA / gSFA_hot / sig status, and a direct token-owner table.

use anyhow::{Context, Result};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use serde_big_array::BigArray;
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use crate::types::Slot;

pub enum ClickHouseWriteJob {
    Block(ClickHouseBlockBatch),
}

pub struct ClickHouseBlockBatch {
    pub transactions: Vec<TransactionRow>,
    pub token_owner_sigs: Vec<TokenOwnerSigRow>,
    pub program_invocations: Vec<ProgramInvocationRow>,
}

pub async fn clickhouse_writer_loop(
    store: std::sync::Arc<ClickHouseStore>,
    mut rx: mpsc::Receiver<ClickHouseWriteJob>,
) {
    use std::time::{Duration, Instant};

    info!("clickhouse writer started");

    const FLUSH_ROWS: usize = 50_000;
    const FLUSH_INTERVAL: Duration = Duration::from_secs(2);

    let mut tx_buf: Vec<TransactionRow> = Vec::with_capacity(FLUSH_ROWS);
    let mut tok_buf: Vec<TokenOwnerSigRow> = Vec::with_capacity(FLUSH_ROWS);
    let mut prog_buf: Vec<ProgramInvocationRow> = Vec::with_capacity(FLUSH_ROWS);
    let mut last_flush = Instant::now();

    loop {
        let timeout = FLUSH_INTERVAL.saturating_sub(last_flush.elapsed());
        let recv_result = tokio::time::timeout(timeout, rx.recv()).await;

        match recv_result {
            Ok(Some(ClickHouseWriteJob::Block(batch))) => {
                tx_buf.extend(batch.transactions);
                tok_buf.extend(batch.token_owner_sigs);
                prog_buf.extend(batch.program_invocations);
            }
            Ok(None) => {
                warn!("clickhouse channel closed, flushing and exiting");
                flush(&store, &mut tx_buf, &mut tok_buf, &mut prog_buf).await;
                return;
            }
            Err(_) => {}
        }

        let should_flush = tx_buf.len() >= FLUSH_ROWS
            || tok_buf.len() >= FLUSH_ROWS
            || prog_buf.len() >= FLUSH_ROWS
            || last_flush.elapsed() >= FLUSH_INTERVAL;

        if should_flush && !(tx_buf.is_empty() && tok_buf.is_empty() && prog_buf.is_empty()) {
            flush(&store, &mut tx_buf, &mut tok_buf, &mut prog_buf).await;
            last_flush = Instant::now();
        }
    }
}

async fn flush(
    store: &ClickHouseStore,
    tx_buf: &mut Vec<TransactionRow>,
    tok_buf: &mut Vec<TokenOwnerSigRow>,
    prog_buf: &mut Vec<ProgramInvocationRow>,
) {
    if !tx_buf.is_empty() {
        if let Err(e) = store.insert_transactions(tx_buf).await {
            error!(error = %e, rows = tx_buf.len(), "clickhouse transactions insert failed");
        }
        tx_buf.clear();
    }
    if !tok_buf.is_empty() {
        if let Err(e) = store.insert_token_owner_sigs(tok_buf).await {
            error!(error = %e, rows = tok_buf.len(), "clickhouse token_owner_sigs insert failed");
        }
        tok_buf.clear();
    }
    if !prog_buf.is_empty() {
        if let Err(e) = store.insert_program_invocations(prog_buf).await {
            error!(error = %e, rows = prog_buf.len(), "clickhouse program_invocations insert failed");
        }
        prog_buf.clear();
    }
}

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct ClickHouseConfig {
    pub url: String,
    #[serde(default = "default_db")]
    pub database: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// Route historical reads to ClickHouse. Writes are always dual.
    #[serde(default)]
    pub read_enabled: bool,
}

fn default_db() -> String {
    "light_rpc".to_string()
}

pub struct ClickHouseStore {
    client: Client,
    config: ClickHouseConfig,
}

impl ClickHouseStore {
    pub async fn connect(config: &ClickHouseConfig) -> Result<Self> {
        let mut client = Client::default()
            .with_url(&config.url)
            .with_setting("async_insert", "1")
            .with_setting("wait_for_async_insert", "0")
            .with_setting("async_insert_max_data_size", "10485760")
            .with_setting("async_insert_busy_timeout_ms", "1000")
            .with_setting("async_insert_deduplicate", "1")
            .with_setting("network_compression_method", "lz4");
        if let Some(u) = &config.username {
            client = client.with_user(u);
        }
        if let Some(p) = &config.password {
            client = client.with_password(p);
        }
        client = client.with_database(&config.database);

        let store = Self {
            client,
            config: config.clone(),
        };
        store.migrate().await.context("clickhouse migrations")?;
        info!(url = %config.url, db = %config.database, "clickhouse connected");
        Ok(store)
    }

    pub fn read_enabled(&self) -> bool {
        self.config.read_enabled
    }

    pub fn client(&self) -> &Client {
        &self.client
    }

    async fn migrate(&self) -> Result<()> {
        self.client
            .query(&format!(
                "CREATE DATABASE IF NOT EXISTS {}",
                self.config.database
            ))
            .execute()
            .await?;

        // 365d row TTL, 60d column TTL on wide payloads.
        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS transactions (
                    slot           UInt64    CODEC(Delta, LZ4),
                    tx_index       UInt32    CODEC(T64, LZ4),
                    signature      FixedString(64),
                    block_time     Int64     CODEC(DoubleDelta, LZ4),
                    err            Bool,
                    fee            UInt64    CODEC(T64, LZ4),
                    compute_units  UInt32    CODEC(T64, LZ4),
                    addresses      Array(FixedString(32)),
                    writable_mask  Array(UInt8)  CODEC(ZSTD(1)),
                    signer_mask    Array(UInt8)  CODEC(ZSTD(1)),
                    log_messages   Array(String) CODEC(ZSTD(3)) TTL toDateTime(block_time) + INTERVAL 60 DAY,
                    message        String        CODEC(ZSTD(3)) TTL toDateTime(block_time) + INTERVAL 60 DAY,
                    meta           String        CODEC(ZSTD(3)) TTL toDateTime(block_time) + INTERVAL 60 DAY,

                    INDEX idx_signature signature TYPE bloom_filter(0.01) GRANULARITY 4,
                    INDEX idx_fee fee TYPE minmax GRANULARITY 4,
                    INDEX idx_cu compute_units TYPE minmax GRANULARITY 4
                ) ENGINE = MergeTree
                PARTITION BY toYYYYMMDD(toDateTime(block_time))
                ORDER BY (slot, tx_index)
                TTL toDateTime(block_time) + INTERVAL 365 DAY
                SETTINGS index_granularity = 8192,
                         min_bytes_for_wide_part = 10485760
                "#,
            )
            .execute()
            .await?;

        // gSFA cold MV. index_granularity=1024 trades primary-index size for
        // random point-lookup latency.
        self.client
            .query(
                r#"
                CREATE MATERIALIZED VIEW IF NOT EXISTS gsfa_mv
                ENGINE = MergeTree
                PARTITION BY toYYYYMM(toDateTime(block_time))
                ORDER BY (address, slot, tx_index)
                TTL toDateTime(block_time) + INTERVAL 365 DAY
                SETTINGS index_granularity = 1024
                AS SELECT
                    arrayJoin(addresses) AS address,
                    slot,
                    tx_index,
                    signature,
                    block_time,
                    err
                FROM transactions
                "#,
            )
            .execute()
            .await?;

        self.client
            .query(
                r#"
                CREATE MATERIALIZED VIEW IF NOT EXISTS gsfa_hot_mv
                ENGINE = MergeTree
                PARTITION BY toYYYYMMDD(toDateTime(block_time))
                ORDER BY (address, slot, tx_index)
                TTL toDateTime(block_time) + INTERVAL 14 DAY
                SETTINGS index_granularity = 1024
                AS SELECT
                    arrayJoin(addresses) AS address,
                    slot,
                    tx_index,
                    signature,
                    block_time,
                    err
                FROM transactions
                "#,
            )
            .execute()
            .await?;

        self.client
            .query(
                r#"
                CREATE MATERIALIZED VIEW IF NOT EXISTS sig_status_mv
                ENGINE = ReplacingMergeTree(slot)
                ORDER BY signature
                TTL toDateTime(block_time) + INTERVAL 365 DAY
                SETTINGS index_granularity = 8192
                AS SELECT
                    signature,
                    slot,
                    block_time,
                    err
                FROM transactions
                "#,
            )
            .execute()
            .await?;

        // Token-owner → signatures. Written directly; the MV path can't see
        // the owner without joining token-account state.
        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS token_owner_signatures (
                    owner      FixedString(32),
                    mint       FixedString(32),
                    slot       UInt64   CODEC(Delta, LZ4),
                    tx_index   UInt32   CODEC(T64, LZ4),
                    signature  FixedString(64),
                    block_time Int64    CODEC(DoubleDelta, LZ4),
                    INDEX idx_mint mint TYPE bloom_filter(0.01) GRANULARITY 4
                ) ENGINE = MergeTree
                PARTITION BY toYYYYMM(toDateTime(block_time))
                ORDER BY (owner, slot, tx_index)
                TTL toDateTime(block_time) + INTERVAL 365 DAY
                SETTINGS index_granularity = 1024
                "#,
            )
            .execute()
            .await?;

        // Program → signatures: one row per (program, tx) touch.
        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS program_invocations (
                    program_id  FixedString(32),
                    slot        UInt64   CODEC(Delta, LZ4),
                    tx_index    UInt32   CODEC(T64, LZ4),
                    signature   FixedString(64),
                    block_time  Int64    CODEC(DoubleDelta, LZ4),
                    err         Bool
                ) ENGINE = MergeTree
                PARTITION BY toYYYYMM(toDateTime(block_time))
                ORDER BY (program_id, slot, tx_index)
                TTL toDateTime(block_time) + INTERVAL 365 DAY
                SETTINGS index_granularity = 1024
                "#,
            )
            .execute()
            .await?;

        // DAS asset tables. Reads currently land on Postgres; these migrate
        // them to ClickHouse once the write path is wired up.
        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS asset (
                    id              FixedString(32),
                    asset_class     LowCardinality(String),
                    owner           FixedString(32),
                    delegate        Nullable(FixedString(32)),
                    frozen          Bool,
                    supply          UInt64,
                    compressed      Bool,
                    tree_id         Nullable(FixedString(32)),
                    leaf            Nullable(FixedString(32)),
                    nonce           Nullable(UInt64),
                    royalty_amount  UInt32,
                    burnt           Bool,
                    slot_updated    UInt64   CODEC(Delta, LZ4),
                    seq             UInt64
                ) ENGINE = ReplacingMergeTree(slot_updated)
                PARTITION BY toYYYYMM(toDateTime(slot_updated * 400 / 1000))
                ORDER BY (owner, id)
                SETTINGS index_granularity = 1024
                "#,
            )
            .execute()
            .await?;

        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS asset_data (
                    asset_id      FixedString(32),
                    chain_data    String CODEC(ZSTD(3)),
                    metadata_url  String,
                    metadata      String CODEC(ZSTD(3)),
                    raw_name      String,
                    raw_symbol    String
                ) ENGINE = ReplacingMergeTree
                ORDER BY asset_id
                SETTINGS index_granularity = 1024
                "#,
            )
            .execute()
            .await?;

        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS asset_creators (
                    asset_id  FixedString(32),
                    creator   FixedString(32),
                    share     UInt8,
                    verified  Bool,
                    position  UInt8
                ) ENGINE = MergeTree
                ORDER BY (creator, asset_id)
                SETTINGS index_granularity = 1024
                "#,
            )
            .execute()
            .await?;

        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS asset_grouping (
                    asset_id     FixedString(32),
                    group_key    LowCardinality(String),
                    group_value  Nullable(String),
                    verified     Bool
                ) ENGINE = MergeTree
                ORDER BY (group_key, group_value, asset_id)
                SETTINGS index_granularity = 1024, allow_nullable_key = 1
                "#,
            )
            .execute()
            .await?;

        self.client
            .query(
                r#"
                CREATE TABLE IF NOT EXISTS asset_authority (
                    asset_id   FixedString(32),
                    authority  FixedString(32),
                    scopes     Array(LowCardinality(String))
                ) ENGINE = MergeTree
                ORDER BY (authority, asset_id)
                SETTINGS index_granularity = 1024
                "#,
            )
            .execute()
            .await?;

        self.client
            .query(
                r#"
                CREATE MATERIALIZED VIEW IF NOT EXISTS program_daily_stats_mv
                ENGINE = SummingMergeTree
                PARTITION BY toYYYYMM(day)
                ORDER BY (day, program_id)
                AS SELECT
                    toDate(toDateTime(block_time)) AS day,
                    program_id,
                    count() AS tx_count,
                    countIf(err) AS err_count
                FROM program_invocations
                GROUP BY day, program_id
                "#,
            )
            .execute()
            .await?;

        info!("clickhouse migrations complete");
        Ok(())
    }

    pub async fn insert_transactions(&self, rows: &[TransactionRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self.client.insert::<TransactionRow>("transactions").await?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_token_owner_sigs(&self, rows: &[TokenOwnerSigRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<TokenOwnerSigRow>("token_owner_signatures")
            .await?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn insert_program_invocations(&self, rows: &[ProgramInvocationRow]) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let mut insert = self
            .client
            .insert::<ProgramInvocationRow>("program_invocations")
            .await?;
        for row in rows {
            insert.write(row).await?;
        }
        insert.end().await?;
        Ok(())
    }

    pub async fn get_signatures_for_program(
        &self,
        program_id: &[u8; 32],
        before_slot: Option<Slot>,
        limit: u32,
    ) -> Result<Vec<SignatureRow>> {
        let before = before_slot.unwrap_or(u64::MAX);
        let rows = self
            .client
            .query(
                "SELECT signature, slot, tx_index, block_time, err
                 FROM program_invocations
                 WHERE program_id = ? AND slot < ?
                 ORDER BY slot DESC, tx_index DESC
                 LIMIT ?",
            )
            .bind(program_id.as_slice())
            .bind(before)
            .bind(limit)
            .fetch_all::<SignatureRow>()
            .await?;
        Ok(rows)
    }

    pub async fn get_signatures_for_address(
        &self,
        address: &[u8; 32],
        before_slot: Option<Slot>,
        limit: u32,
    ) -> Result<Vec<SignatureRow>> {
        let before = before_slot.unwrap_or(u64::MAX);
        let rows = self
            .client
            .query(
                "SELECT signature, slot, tx_index, block_time, err
                 FROM gsfa_mv
                 WHERE address = ? AND slot < ?
                 ORDER BY slot DESC, tx_index DESC
                 LIMIT ?",
            )
            .bind(address.as_slice())
            .bind(before)
            .bind(limit)
            .fetch_all::<SignatureRow>()
            .await?;
        Ok(rows)
    }

    pub async fn get_signatures_for_token_owner(
        &self,
        owner: &[u8; 32],
        before_slot: Option<Slot>,
        limit: u32,
    ) -> Result<Vec<SignatureRow>> {
        let before = before_slot.unwrap_or(u64::MAX);
        let rows = self
            .client
            .query(
                "SELECT signature, slot, tx_index, block_time, err
                 FROM token_owner_signatures
                 WHERE owner = ? AND slot < ?
                 ORDER BY slot DESC, tx_index DESC
                 LIMIT ?",
            )
            .bind(owner.as_slice())
            .bind(before)
            .bind(limit)
            .fetch_all::<SignatureRow>()
            .await?;
        Ok(rows)
    }
}

// Sig64 needs serde-big-array because serde does not derive for arrays > 32.
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent)]
pub struct Pubkey32(pub [u8; 32]);

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent)]
pub struct Sig64(#[serde(with = "BigArray")] pub [u8; 64]);

#[derive(clickhouse::Row, Serialize)]
pub struct TransactionRow {
    pub slot: u64,
    pub tx_index: u32,
    pub signature: Sig64,
    pub block_time: i64,
    pub err: bool,
    pub fee: u64,
    pub compute_units: u32,
    pub addresses: Vec<Pubkey32>,
    pub writable_mask: Vec<u8>,
    pub signer_mask: Vec<u8>,
    pub log_messages: Vec<String>,
    pub message: String,
    pub meta: String,
}

#[derive(clickhouse::Row, Serialize)]
pub struct TokenOwnerSigRow {
    pub owner: Pubkey32,
    pub mint: Pubkey32,
    pub slot: u64,
    pub tx_index: u32,
    pub signature: Sig64,
    pub block_time: i64,
}

#[derive(clickhouse::Row, Serialize)]
pub struct ProgramInvocationRow {
    pub program_id: Pubkey32,
    pub slot: u64,
    pub tx_index: u32,
    pub signature: Sig64,
    pub block_time: i64,
    pub err: bool,
}

#[derive(clickhouse::Row, Deserialize, Debug)]
pub struct SignatureRow {
    pub signature: Sig64,
    pub slot: u64,
    pub tx_index: u32,
    pub block_time: i64,
    pub err: bool,
}
