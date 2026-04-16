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
    pub endpoint: String,
    pub x_token: Option<String>,
    #[serde(default)]
    pub commitment: Commitment,
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
    #[serde(default = "default_request_timeout")]
    pub request_timeout_secs: u64,
    #[serde(default = "default_max_message_size")]
    pub max_message_size: usize,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StorageConfig {
    pub rocksdb: RocksDbConfig,
    pub blocks: BlockStorageConfig,
    pub postgres: PostgresConfig,
    #[serde(default)]
    pub pipeline: PipelineConfig,
    #[cfg(feature = "clickhouse")]
    #[serde(default)]
    pub clickhouse: Option<crate::storage::clickhouse::ClickHouseConfig>,
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
