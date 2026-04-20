use prometheus::{
    Histogram, HistogramOpts, HistogramVec, IntCounter, IntCounterVec, IntGauge, IntGaugeVec, Opts,
    Registry,
};
use std::sync::LazyLock;

pub static REGISTRY: LazyLock<Registry> = LazyLock::new(Registry::new);

pub static INGESTED_BLOCKS: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new("li_ingested_blocks_total", "Total blocks ingested").unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static INGESTED_ACCOUNTS: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "li_ingested_accounts_total",
        "Total account updates ingested",
    )
    .unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static INGESTED_TRANSACTIONS: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new("li_ingested_txs_total", "Total transactions ingested").unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static ROCKSDB_QUARANTINED: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "li_rocksdb_quarantined_sst_total",
        "Corrupt SST files moved to quarantine/ by the self-heal layer",
    )
    .unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static SOURCE_MALFORMED: LazyLock<IntCounter> = LazyLock::new(|| {
    let counter = IntCounter::new(
        "li_source_malformed_total",
        "gRPC updates skipped due to malformed pubkey/signature",
    )
    .unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static LATEST_SLOT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new("li_latest_slot", "Latest slot by commitment level"),
        &["commitment"],
    )
    .unwrap();
    REGISTRY.register(Box::new(gauge.clone())).unwrap();
    gauge
});

pub static ROCKSDB_SST_COUNT: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new("li_rocksdb_sst_count", "Live SST file count per column family"),
        &["cf"],
    )
    .unwrap();
    REGISTRY.register(Box::new(gauge.clone())).unwrap();
    gauge
});

pub static ROCKSDB_LIVE_DATA_SIZE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new(
            "li_rocksdb_live_data_bytes",
            "Estimated live data bytes per column family",
        ),
        &["cf"],
    )
    .unwrap();
    REGISTRY.register(Box::new(gauge.clone())).unwrap();
    gauge
});

pub static ROCKSDB_L0_FILES: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new("li_rocksdb_l0_files", "Number of files at L0 per column family"),
        &["cf"],
    )
    .unwrap();
    REGISTRY.register(Box::new(gauge.clone())).unwrap();
    gauge
});

pub static PIPELINE_CHANNEL_SIZE: LazyLock<IntGaugeVec> = LazyLock::new(|| {
    let gauge = IntGaugeVec::new(
        Opts::new("li_pipeline_channel_size", "Pipeline channel utilization"),
        &["channel"],
    )
    .unwrap();
    REGISTRY.register(Box::new(gauge.clone())).unwrap();
    gauge
});

pub static RPC_REQUESTS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new("li_rpc_requests_total", "Total RPC requests by method"),
        &["method"],
    )
    .unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static RPC_ERRORS: LazyLock<IntCounterVec> = LazyLock::new(|| {
    let counter = IntCounterVec::new(
        Opts::new("li_rpc_errors_total", "Total RPC errors by method"),
        &["method"],
    )
    .unwrap();
    REGISTRY.register(Box::new(counter.clone())).unwrap();
    counter
});

pub static RPC_LATENCY: LazyLock<HistogramVec> = LazyLock::new(|| {
    let histogram = HistogramVec::new(
        HistogramOpts::new("li_rpc_latency_seconds", "RPC method latency").buckets(vec![
            0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0,
        ]),
        &["method"],
    )
    .unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static STORAGE_WRITE_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new("li_storage_write_seconds", "Storage write latency")
            .buckets(vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]),
    )
    .unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static PG_WRITE_LATENCY: LazyLock<Histogram> = LazyLock::new(|| {
    let histogram = Histogram::with_opts(
        HistogramOpts::new("li_pg_write_seconds", "PostgreSQL batch write latency").buckets(vec![
            0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5,
        ]),
    )
    .unwrap();
    REGISTRY.register(Box::new(histogram.clone())).unwrap();
    histogram
});

pub static MEMORY_CACHED_BLOCKS: LazyLock<IntGauge> = LazyLock::new(|| {
    let gauge = IntGauge::new("li_memory_cached_blocks", "Blocks cached in memory").unwrap();
    REGISTRY.register(Box::new(gauge.clone())).unwrap();
    gauge
});
