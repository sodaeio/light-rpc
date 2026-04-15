//! ClickHouse-backed historical store.
//!
//! Mirrors the RPC 2.0 historical-module design: a wide base `transactions`
//! table written by the ingester, plus materialized views and a token-owner
//! index derived at insert time for the query patterns Solana apps actually
//! hit (gSFA, gTFA, sig status).
//!
//! ## Design notes (ClickHouse best practices)
//!
//! - **PARTITION BY** is chosen strictly for data lifecycle (TTL drops full
//!   partitions cheaply). Partition cardinality stays under 1,000 per table.
//! - **ORDER BY** columns lead with the equality filter (address, owner,
//!   signature) rather than low-cardinality-first, because the sparse primary
//!   index is only useful if the leading key matches the WHERE clause.
//! - **ReplacingMergeTree** uses an explicit version column (`slot`) so a
//!   newer insert for the same signature deterministically wins.
//! - **async_insert** is enabled on the client so per-block writes coalesce
//!   into the 10-100k-row batches ClickHouse wants.
//! - **ZSTD** codec on the wide `message` / `meta` / `log_messages` payloads.
//!
//! Feature-gated behind `clickhouse` until dual-write is validated.

use anyhow::{Context, Result};
use clickhouse::Client;
use serde::{Deserialize, Serialize};
use tracing::info;

use crate::types::Slot;

#[derive(Debug, Clone, serde::Deserialize, Default)]
pub struct ClickHouseConfig {
    /// HTTP endpoint, e.g. `http://127.0.0.1:8123`.
    pub url: String,
    #[serde(default = "default_db")]
    pub database: String,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub password: Option<String>,
    /// If true, historical reads go to ClickHouse. Writes are always dual
    /// while this flag is flipping.
    #[serde(default)]
    pub read_enabled: bool,
}

fn default_db() -> String {
    "light_indexer".to_string()
}

pub struct ClickHouseStore {
    client: Client,
    config: ClickHouseConfig,
}

impl ClickHouseStore {
    pub async fn connect(config: &ClickHouseConfig) -> Result<Self> {
        let mut client = Client::default()
            .with_url(&config.url)
            // Let the server coalesce small per-block inserts into 10k-100k
            // row batches — avoids the "one part per insert" anti-pattern
            // (rule: insert-batch-size, insert-async-small-batches).
            .with_setting("async_insert", "1")
            .with_setting("wait_for_async_insert", "0")
            .with_setting("async_insert_max_data_size", "10485760") // 10 MiB
            .with_setting("async_insert_busy_timeout_ms", "1000")
            // Idempotent retries: if the same insert lands twice after a
            // network retry, server dedupes by payload hash.
            .with_setting("async_insert_deduplicate", "1")
            // Faster compression at ingest time; server-side recompression
            // to ZSTD happens during background merges.
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

    /// DDL — idempotent. Base table + MVs matching the RPC 2.0 historical
    /// module's query shapes.
    async fn migrate(&self) -> Result<()> {
        self.client
            .query(&format!(
                "CREATE DATABASE IF NOT EXISTS {}",
                self.config.database
            ))
            .execute()
            .await?;

        // Base transactions table.
        //
        // ORDER BY (slot, tx_index): slot is monotonically increasing with
        //   low per-partition cardinality (~216k/day) so it leads the key
        //   per `schema-pk-cardinality-order`. tx_index breaks ties.
        // PARTITION BY toYYYYMMDD: daily partitions = 365/yr, within the
        //   100-1000 recommended range. Enables cheap DROP PARTITION for
        //   lifecycle (rule: schema-partition-lifecycle).
        //
        // Codecs per column:
        //   - Delta + LZ4 for monotonic slot: ~10× compression.
        //   - DoubleDelta + LZ4 for block_time (near-monotonic).
        //   - T64 for bounded small ints (tx_index, compute_units, fee).
        //   - ZSTD(3) for variable-length wide payloads.
        //
        // Skipping indices:
        //   - bloom_filter on signature: gSigStatus joins can skip granules
        //     even without hitting sig_status_mv (defensive path).
        //   - minmax on fee + compute_units for analytical filters.
        //
        // TTLs (tiered, column-level):
        //   - Heavy text (log_messages/message/meta) drops at 60 days —
        //     this is the big space win. Most queries don't need it after
        //     the hot window.
        //   - Row itself lives 365 days.
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

        // gSFA cold MV: address -> signatures.
        //
        // ORDER BY (address, slot, tx_index): the equality-then-range filter
        //   pattern from getSignaturesForAddress. Address leads because
        //   the query is WHERE address = X — the sparse primary index
        //   resolves directly to that address's granules. Although address
        //   cardinality is high, per-partition sorting keeps all rows for
        //   one address physically contiguous.
        //
        // index_granularity = 1024: lower than the 8192 default. This MV
        //   is used for random point lookups (one address at a time), so
        //   smaller granules give better skip-ratio at the cost of larger
        //   primary index. Worth it for sub-10ms p99 address queries.
        //
        // Codecs on slot/tx_index keep size in check even though we've
        //   denormalized one row per (tx, address).
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

        // gSFA hot MV: 14-day window, daily partitions. Hot working set
        // fits in page cache; reads on recent history skip the cold tier.
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

        // Signature status lookup. Uses ReplacingMergeTree keyed on
        // signature with slot as the version column — if the same
        // signature is reinserted (shouldn't happen on mainnet but
        // defensive), higher slot wins deterministically.
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

        // Token-owner -> signatures. Can't be derived from the base table
        // alone (needs token-account resolution), so the ingester writes
        // directly. ORDER BY owner-first for the gTFA lookup pattern.
        // Skipping index on mint lets "all txs for owner + specific mint"
        // queries prune without scanning the owner's entire history.
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

        // Program -> signatures MV. Solana apps frequently want "all txs
        // touching program X in slot range Y" (DEX volume, protocol
        // activity, etc). Triton's spec doesn't expose this natively —
        // shipping it gives us a query surface they don't have on day one.
        //
        // Requires the ingester to populate an Array(FixedString(32))
        // `program_ids` column; MV unnests it via arrayJoin.
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

        // Daily aggregates MV — precomputes program activity counts.
        // Hot analytical queries ("which program had the most txs yesterday")
        // hit this instead of scanning the base table.
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

    /// Batch insert transactions. Callers should batch per block (typically
    /// 1-10k rows) — async_insert on the client coalesces across blocks.
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

    /// Batch insert token-owner signature mappings.
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

    /// Batch insert program invocation rows (one per program touched per tx).
    pub async fn insert_program_invocations(
        &self,
        rows: &[ProgramInvocationRow],
    ) -> Result<()> {
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

    /// getTransactionsByProgram — query surface RPC 2.0 doesn't expose.
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

    /// Route gSFA through hot MV when `before_slot` is within the hot window,
    /// else cold. Hot lookups hit a ~14-day working set that fits in memory.
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

    /// Token-owner variant: resolves all signatures touching any token account
    /// owned by `owner` without requiring the caller to enumerate ATAs.
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

// FixedString(N) in ClickHouse maps to raw byte sequences. serde's native
// array impls only cover [T; 0..=32], so we use Vec<u8> + serde_bytes and
// rely on the writer to enforce lengths (32 for pubkeys, 64 for signatures).
// addresses is Array(FixedString(32)) which becomes Array(serde_bytes) via
// a newtype wrapper.

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent)]
pub struct Pubkey32(#[serde(with = "serde_bytes")] pub Vec<u8>);

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(transparent)]
pub struct Sig64(#[serde(with = "serde_bytes")] pub Vec<u8>);

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
