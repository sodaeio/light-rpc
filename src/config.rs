use figment::{
    providers::{Format, Yaml},
    Figment,
};
use serde::Deserialize;
use std::path::Path;

use crate::Commitment;

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub source: SourceConfig,
    pub storage: StorageConfig,
    pub rpc: RpcConfig,
    #[serde(default)]
    pub metrics: MetricsConfig,
    #[serde(default)]
    pub threads: ThreadConfig,
}

#[derive(Debug, Clone, Deserialize)]
pub struct SourceConfig {
    /// Primary gRPC endpoint. Required unless `quic` is set.
    #[serde(default)]
    pub endpoint: Option<String>,
    /// Fallback gRPC endpoints. Tried in order when primary fails.
    /// gRPC only — ignored when `quic` is set.
    #[serde(default)]
    pub fallback_endpoints: Vec<String>,
    /// QUIC firehose source. When set, gRPC `endpoint`/`fallback_endpoints`
    /// are ignored. Field shape mirrors sesame leaf's `client:` block.
    #[serde(default)]
    pub quic: Option<richat_client::quic::ConfigQuicClient>,
    /// gRPC subscribe protocol. "yellowstone" = Dragon's Mouth (default,
    /// works with sesame leaves). "richat" = native richat protocol (use
    /// only when connecting to a richat-native server, e.g. richat-plugin-agave).
    #[serde(default)]
    pub grpc_mode: GrpcMode,
    pub x_token: Option<String>,
    #[serde(default)]
    pub commitment: Commitment,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
    #[serde(default = "default_gap_threshold")]
    pub gap_threshold_secs: u64,
    /// Local directory containing Solana snapshot files for account state recovery.
    #[serde(default = "default_snapshot_dir")]
    pub snapshot_dir: String,
}

#[derive(Debug, Clone, Copy, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum GrpcMode {
    #[default]
    Yellowstone,
    Richat,
}

fn default_gap_threshold() -> u64 {
    60
}
fn default_snapshot_dir() -> String {
    "/tmp/light-rpc-snapshots".into()
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub rocksdb: RocksDbConfig,
    pub blocks: BlockStorageConfig,
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    #[serde(default)]
    pub retention: RetentionConfig,
    #[serde(default)]
    pub clickhouse: Option<crate::storage::clickhouse::ClickHouseConfig>,
}

/// History retention. Pruning runs every 10 min.
/// Account-state CFs (accounts, owner_atas, program_index, mint_top_holders)
/// are never pruned.
#[derive(Debug, Clone, Deserialize)]
pub struct RetentionConfig {
    /// Max days to keep slot_index entries. 0 = keep forever.
    #[serde(default = "default_retention_days")]
    pub slot_index_days: u64,
    /// Max days to keep tx_index entries. 0 = keep forever.
    #[serde(default = "default_retention_days")]
    pub tx_index_days: u64,
    /// Max days to keep sfa_index entries. 0 = keep forever.
    #[serde(default = "default_sfa_retention_days")]
    pub sfa_index_days: u64,
    /// Max total disk usage for RocksDB in GB. When exceeded, oldest
    /// history is pruned until below this limit. 0 = no limit.
    #[serde(default)]
    pub max_disk_gb: u64,
    /// Prune interval in seconds.
    #[serde(default = "default_prune_interval")]
    pub prune_interval_secs: u64,
}

impl Default for RetentionConfig {
    fn default() -> Self {
        Self {
            slot_index_days: default_retention_days(),
            tx_index_days: default_retention_days(),
            sfa_index_days: default_sfa_retention_days(),
            max_disk_gb: 0,
            prune_interval_secs: default_prune_interval(),
        }
    }
}

fn default_retention_days() -> u64 {
    2
}
fn default_sfa_retention_days() -> u64 {
    7
}
fn default_prune_interval() -> u64 {
    600
}

#[derive(Debug, Clone, Deserialize)]
pub struct RocksDbConfig {
    pub path: String,
    #[serde(default = "default_write_buffer")]
    pub write_buffer_size: usize,
    #[serde(default = "default_max_open_files")]
    pub max_open_files: i32,
    #[serde(default = "default_true")]
    pub enable_pipelined_writes: bool,
    #[serde(default = "default_true")]
    pub enable_wal: bool,
    /// Shared block-cache size across all CFs, in GiB. Right-size for the host:
    /// big nodes can afford 32+, shared hosts should stay at 4-8 to leave room
    /// for memtables and other tenants.
    #[serde(default = "default_block_cache_gb")]
    pub block_cache_gb: usize,
    /// Max immutable memtables per CF. Each CF can hold up to this many
    /// `write_buffer_size`-sized buffers, so total memtable budget is
    /// roughly `7 (CFs) * write_buffer_size * max_write_buffer_number`.
    #[serde(default = "default_max_write_buffer_number")]
    pub max_write_buffer_number: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct BlockStorageConfig {
    pub path: String,
    #[serde(default = "default_compression")]
    pub compression: String,
    #[serde(default = "default_max_stored_blocks")]
    pub max_stored_blocks: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PostgresConfig {
    pub url: String,
    #[serde(default = "default_max_pg_connections")]
    pub max_connections: u32,
    #[serde(default = "default_min_pg_connections")]
    pub min_connections: u32,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_idle_timeout")]
    pub idle_timeout_secs: u64,
    /// Retention window in slots (~30 days at 2.5 slots/sec).
    #[serde(default = "default_address_retention_slots")]
    pub address_retention_slots: u64,
}

fn default_address_retention_slots() -> u64 {
    6_480_000
}

#[derive(Debug, Clone, Deserialize)]
pub struct PipelineConfig {
    #[serde(default = "default_source_to_write")]
    pub source_to_write: usize,
    #[serde(default = "default_write_to_read")]
    pub write_to_read: usize,
    #[serde(default = "default_read_to_rpc")]
    pub read_to_rpc: usize,
    #[serde(default = "default_pg_write_buffer")]
    pub pg_write_buffer: usize,
}

impl Default for PipelineConfig {
    fn default() -> Self {
        Self {
            source_to_write: default_source_to_write(),
            write_to_read: default_write_to_read(),
            read_to_rpc: default_read_to_rpc(),
            pg_write_buffer: default_pg_write_buffer(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct RpcConfig {
    #[serde(default = "default_rpc_endpoint")]
    pub endpoint: String,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    #[serde(default)]
    pub workers: usize,
    pub upstream: Option<String>,
    #[serde(default)]
    pub forwarded_methods: Vec<String>,
    /// Empty → use DEFAULT_GPA_BLOCKED; set to `[]` to disable blocking.
    #[serde(default)]
    pub gpa_blocked_programs: Option<Vec<String>>,
    #[serde(default = "default_gpa_max_accounts")]
    pub gpa_max_accounts: usize,
}

fn default_gpa_max_accounts() -> usize {
    100_000
}

pub const DEFAULT_GPA_BLOCKED: &[&str] = &[
    "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA",
    "TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb",
    "ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL",
    "11111111111111111111111111111111",
    "Vote111111111111111111111111111111111111111",
    "Stake11111111111111111111111111111111111111",
    "AddressLookupTab1e1111111111111111111111111",
    "ComputeBudget111111111111111111111111111111",
];

#[derive(Debug, Clone, Deserialize)]
pub struct MetricsConfig {
    #[serde(default = "default_metrics_endpoint")]
    pub endpoint: String,
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self {
            endpoint: default_metrics_endpoint(),
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
pub struct ThreadConfig {
    #[serde(default = "default_one")]
    pub source: usize,
    #[serde(default = "default_one")]
    pub storage_write: usize,
    #[serde(default)]
    pub storage_read: usize,
    #[serde(default)]
    pub rpc: usize,
}

impl Default for ThreadConfig {
    fn default() -> Self {
        Self {
            source: 1,
            storage_write: 1,
            storage_read: 0,
            rpc: 0,
        }
    }
}

impl ThreadConfig {
    pub fn storage_read_count(&self) -> usize {
        if self.storage_read == 0 {
            (num_cpus::get() / 2).max(1)
        } else {
            self.storage_read
        }
    }

    pub fn rpc_count(&self) -> usize {
        if self.rpc == 0 {
            num_cpus::get()
        } else {
            self.rpc
        }
    }
}

impl Config {
    pub fn load(path: &Path) -> anyhow::Result<Self> {
        let config: Config = Figment::new().merge(Yaml::file(path)).extract()?;
        match (&config.source.endpoint, &config.source.quic) {
            (None, None) => anyhow::bail!("source: must set either `endpoint` (gRPC) or `quic`"),
            (Some(_), Some(_)) => {
                anyhow::bail!("source: set only one of `endpoint` (gRPC) or `quic`, not both")
            }
            _ => {}
        }
        Ok(config)
    }
}

fn default_connect_timeout() -> u64 {
    10
}
fn default_request_timeout() -> u64 {
    60
}
fn default_max_message_size() -> usize {
    64 * 1024 * 1024
}
fn default_write_buffer() -> usize {
    256 * 1024 * 1024
}
fn default_max_open_files() -> i32 {
    512
}
fn default_block_cache_gb() -> usize {
    16
}
fn default_max_write_buffer_number() -> i32 {
    3
}
fn default_true() -> bool {
    true
}
fn default_compression() -> String {
    "lz4".into()
}
fn default_max_stored_blocks() -> usize {
    500_000
}
fn default_max_pg_connections() -> u32 {
    50
}
fn default_min_pg_connections() -> u32 {
    5
}
fn default_idle_timeout() -> u64 {
    300
}
fn default_source_to_write() -> usize {
    2048
}
fn default_write_to_read() -> usize {
    1024
}
fn default_read_to_rpc() -> usize {
    4096
}
fn default_pg_write_buffer() -> usize {
    10_000
}
fn default_rpc_endpoint() -> String {
    "0.0.0.0:8876".into()
}
fn default_max_connections() -> usize {
    1024
}
fn default_metrics_endpoint() -> String {
    "0.0.0.0:9090".into()
}
fn default_one() -> usize {
    1
}
