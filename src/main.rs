use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{error, info, warn};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use light_rpc::config::Config;
use light_rpc::metrics::REGISTRY;
use light_rpc::rpc::server::RpcServer;
use light_rpc::rpc::upstream::UpstreamForwarder;
use light_rpc::source::StreamSource;
use light_rpc::storage::files::BlockFileStorage;
use light_rpc::storage::postgres::PgStorage;
use light_rpc::storage::read::{MemoryCache, StorageReader};
use light_rpc::storage::rocks::UnifiedRocksDb;
use light_rpc::storage::write::{pg_writer_loop, StorageWriter};

#[derive(Parser)]
#[command(name = "light-rpc", about = "Unified Solana indexer and RPC server")]
struct Cli {
    #[arg(short, long, default_value = "config.yml")]
    config: PathBuf,

    #[arg(long)]
    check: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = Config::load(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;

    if cli.check {
        println!("Configuration is valid.");
        return Ok(());
    }

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,light_rpc=debug".into()),
        )
        .with_target(true)
        .init();

    info!(version = env!("CARGO_PKG_VERSION"), "starting light-rpc");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.threads.rpc_count())
        .max_blocking_threads(512)
        .thread_name("li-main")
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    rt.block_on(run(config))
}

async fn run(config: Config) -> Result<()> {
    let rocks = UnifiedRocksDb::open(&config.storage.rocksdb).context("opening rocksdb")?;
    info!(path = %config.storage.rocksdb.path, "rocksdb opened");

    let files =
        Arc::new(BlockFileStorage::open(&config.storage.blocks).context("opening block storage")?);
    info!(path = %config.storage.blocks.path, "block storage opened");

    let pg = PgStorage::connect(&config.storage.postgres)
        .await
        .context("connecting to postgres")?;
    pg.migrate().await.context("running migrations")?;

    let memory_cache = Arc::new(MemoryCache::new());

    let (source_tx, source_rx) =
        tokio::sync::mpsc::channel(config.storage.pipeline.source_to_write);
    let (broadcast_tx, _) = tokio::sync::broadcast::channel(config.storage.pipeline.write_to_read);
    let (pg_tx, pg_rx) = tokio::sync::mpsc::channel(config.storage.pipeline.pg_write_buffer);

    let metrics_endpoint = config.metrics.endpoint.clone();
    let health_cache = Arc::clone(&memory_cache);
    tokio::spawn(async move {
        if let Err(e) = run_metrics_server(&metrics_endpoint, health_cache).await {
            error!(error = %e, "metrics server failed");
        }
    });

    let rocks_for_metrics = rocks.clone();
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(std::time::Duration::from_secs(60));
        loop {
            ticker.tick().await;
            rocks_for_metrics.update_metrics();
        }
    });

    let pg_retention = pg.clone();
    let rocks_retention = rocks.clone();
    let cache_retention = Arc::clone(&memory_cache);
    let retention_config = config.storage.retention.clone();
    let retention_slots = config.storage.postgres.address_retention_slots;
    tokio::spawn(async move {
        run_retention_loop(
            pg_retention,
            rocks_retention,
            cache_retention,
            retention_slots,
            retention_config,
        )
        .await;
    });

    let pg_writer = pg.clone();
    tokio::spawn(async move { pg_writer_loop(pg_writer, pg_rx).await });

    let cache_rx = broadcast_tx.subscribe();
    let invalidator_rx = broadcast_tx.subscribe();
    let cache_ref = Arc::clone(&memory_cache);
    tokio::spawn(async move { StorageReader::run_cache_updater(cache_ref, cache_rx).await });

    #[allow(unused_mut)]
    let mut writer = StorageWriter::new(
        rocks.clone(),
        files.as_ref().clone_for_writer(),
        pg_tx,
        broadcast_tx,
    );

    {
        use light_rpc::storage::clickhouse::{clickhouse_writer_loop, ClickHouseStore};
        if let Some(ch_cfg) = &config.storage.clickhouse {
            let store = ClickHouseStore::connect(ch_cfg)
                .await
                .context("connecting to clickhouse")?;
            let store = Arc::new(store);
            let (ch_tx, ch_rx) = tokio::sync::mpsc::channel(4096);
            let store_for_task = Arc::clone(&store);
            tokio::spawn(async move {
                clickhouse_writer_loop(store_for_task, ch_rx).await;
            });
            writer = writer.with_clickhouse(ch_tx);
            info!("clickhouse dual-write enabled");
        }
    }

    // Writer runs on its own OS thread + single-thread runtime. Its put_cf
    // calls are synchronous and block when RocksDB throttles writes; running
    // on a dedicated thread keeps that blocking off the main worker pool, so
    // the source/RPC tasks aren't starved while compaction is heavy.
    std::thread::Builder::new()
        .name("li-writer".into())
        .spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("writer runtime");
            rt.block_on(async move {
                if let Err(e) = writer.run(source_rx).await {
                    error!(error = %e, "storage writer failed");
                }
            });
        })
        .expect("spawn writer thread");

    if rocks.accounts_empty() {
        let snap_dir = &config.source.snapshot_dir;
        info!("empty database, attempting cold-start snapshot load from {snap_dir}");
        match cold_start_snapshot(snap_dir, &rocks).await {
            Ok((slot, count)) => info!(slot, accounts = count, "cold-start snapshot loaded"),
            Err(e) => {
                warn!(error = %e, "cold-start snapshot failed, starting from stream")
            }
        }
    }

    let source = StreamSource::new(config.source, source_tx);
    tokio::spawn(async move {
        if let Err(e) = source.run().await {
            error!(error = %e, "stream source failed");
        }
    });

    let reader = Arc::new(StorageReader::new(memory_cache, rocks, files, pg));
    let invalidator_reader = Arc::clone(&reader);
    tokio::spawn(async move {
        invalidator_reader
            .run_reader_invalidator(invalidator_rx)
            .await;
    });
    let upstream = config.rpc.upstream.as_ref().map(|endpoint| {
        UpstreamForwarder::new(endpoint.clone(), config.rpc.forwarded_methods.clone())
    });

    let server = RpcServer::new(config.rpc, reader, upstream);
    server.run().await.context("rpc server failed")
}

/// Stream-parses AppendVec accounts from the latest local snapshot set
/// (full + optional matching incremental) into every RocksDB CF.
/// Auto-compactions run concurrent with the apply — RocksDB is tuned
/// (large memtables, generous L0 stop_trigger) so writes never stall.
async fn cold_start_snapshot(snapshot_dir: &str, rocks: &UnifiedRocksDb) -> Result<(u64, usize)> {
    use light_rpc::source::snapshot;

    let dir = std::path::Path::new(snapshot_dir);
    let set = snapshot::find_latest_snapshot(dir)?;
    let rocks = rocks.clone();
    tokio::task::spawn_blocking(move || snapshot::apply_snapshot_set(&rocks, &set)).await?
}

async fn run_retention_loop(
    pg: PgStorage,
    rocks: UnifiedRocksDb,
    cache: Arc<MemoryCache>,
    retention_slots: u64,
    retention: light_rpc::config::RetentionConfig,
) {
    use std::time::Duration;

    loop {
        if cache.finalized_slot() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    let interval_secs = retention.prune_interval_secs.max(60);
    let mut ticker = tokio::time::interval(Duration::from_secs(interval_secs));
    let slots_per_day: u64 = 216_000;

    loop {
        ticker.tick().await;
        let current = cache.finalized_slot();
        if current == 0 {
            continue;
        }

        match pg.ensure_address_partitions(current, 4).await {
            Ok(_) => {}
            Err(e) => error!(error = %e, "failed to ensure partitions"),
        }
        let pg_cutoff = current.saturating_sub(retention_slots);
        if pg_cutoff > 0 {
            match pg.drop_address_partitions_before(pg_cutoff).await {
                Ok(dropped) if !dropped.is_empty() => {
                    info!(cutoff = pg_cutoff, dropped = ?dropped, "pruned PG partitions")
                }
                _ => {}
            }
        }

        if retention.sfa_index_days > 0 {
            let cutoff = current.saturating_sub(retention.sfa_index_days * slots_per_day);
            if cutoff > 0 {
                let r = rocks.clone();
                let dropped = tokio::task::spawn_blocking(move || r.prune_sfa_before(cutoff))
                    .await
                    .ok()
                    .and_then(|r| r.ok())
                    .unwrap_or(0);
                if dropped > 0 {
                    info!(
                        cutoff,
                        addresses = dropped,
                        days = retention.sfa_index_days,
                        "pruned sfa_index"
                    );
                }
            }
        }

        if retention.slot_index_days > 0 {
            let cutoff = current.saturating_sub(retention.slot_index_days * slots_per_day);
            if cutoff > 0 {
                let r = rocks.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    match r.prune_slot_index_before(cutoff) {
                        Ok(_) => info!(
                            cutoff,
                            days = retention.slot_index_days,
                            "pruned slot_index"
                        ),
                        Err(e) => {
                            // Quarantine on corruption; suppress the noisy
                            // log on subsequent prunes hitting the same
                            // dangling MANIFEST reference.
                            let s = e.to_string();
                            if !r.try_quarantine_corrupt_sst(&s, "slot_index") {
                                error!(error = %s, "slot_index prune failed");
                            }
                        }
                    }
                })
                .await;
            }
        }

        if retention.tx_index_days > 0 {
            let cutoff = current.saturating_sub(retention.tx_index_days * slots_per_day);
            if cutoff > 0 {
                let r = rocks.clone();
                let _ =
                    tokio::task::spawn_blocking(move || match r.prune_tx_index_before(cutoff) {
                        Ok(n) => info!(
                            cutoff,
                            dropped = n,
                            days = retention.tx_index_days,
                            "pruned tx_index"
                        ),
                        Err(e) => {
                            let s = e.to_string();
                            if !r.try_quarantine_corrupt_sst(&s, "tx_index") {
                                error!(error = %s, "tx_index prune failed");
                            }
                        }
                    })
                    .await;
            }
        }

        if retention.max_disk_gb > 0 {
            let size_bytes = rocks.estimated_size_bytes();
            let size_gb = size_bytes / (1024 * 1024 * 1024);
            if size_gb > retention.max_disk_gb {
                warn!(
                    size_gb,
                    limit_gb = retention.max_disk_gb,
                    "RocksDB exceeds disk limit, aggressive pruning needed"
                );
                let emergency_days = retention.tx_index_days / 2;
                let cutoff = current.saturating_sub(emergency_days.max(1) * slots_per_day);
                let r = rocks.clone();
                let _ = tokio::task::spawn_blocking(move || {
                    let _ = r.prune_slot_index_before(cutoff);
                    let _ = r.prune_tx_index_before(cutoff);
                })
                .await;
            }
        }

        let size_gb = rocks.estimated_size_bytes() / (1024 * 1024 * 1024);
        info!(current, rocksdb_gb = size_gb, "retention check complete");
    }
}

const READY_MAX_LAG_SECS: u64 = 60;

async fn run_metrics_server(endpoint: &str, cache: Arc<MemoryCache>) -> Result<()> {
    use prometheus::Encoder;

    let addr: std::net::SocketAddr = endpoint.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "metrics server started");

    loop {
        let (stream, _) = listener.accept().await?;
        let cache = Arc::clone(&cache);
        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            let service = hyper::service::service_fn(move |req: hyper::Request<_>| {
                let cache = Arc::clone(&cache);
                async move {
                    let path = req.uri().path();
                    let (status, body): (u16, bytes::Bytes) = match path {
                        "/healthz" => (200, bytes::Bytes::from_static(b"ok\n")),
                        "/readyz" => {
                            let finalized = cache.finalized_slot();
                            let last_update_age = cache.finalized_slot_age_secs();
                            if finalized > 0 && last_update_age <= READY_MAX_LAG_SECS {
                                (
                                    200,
                                    bytes::Bytes::from(format!(
                                        "ready slot={finalized} lag_secs={last_update_age}\n"
                                    )),
                                )
                            } else {
                                (
                                    503,
                                    bytes::Bytes::from(format!(
                                        "not_ready slot={finalized} lag_secs={last_update_age}\n"
                                    )),
                                )
                            }
                        }
                        _ => {
                            use std::sync::Mutex;
                            static BUF: std::sync::OnceLock<Mutex<Vec<u8>>> =
                                std::sync::OnceLock::new();
                            let encoder = prometheus::TextEncoder::new();
                            let metric_families = REGISTRY.gather();
                            let mut buf = BUF
                                .get_or_init(|| Mutex::new(Vec::with_capacity(64 * 1024)))
                                .lock()
                                .unwrap();
                            buf.clear();
                            encoder.encode(&metric_families, &mut *buf).unwrap();
                            (200, bytes::Bytes::from(buf.clone()))
                        }
                    };
                    let resp = hyper::Response::builder()
                        .status(status)
                        .body(http_body_util::Full::new(body))
                        .unwrap();
                    Ok::<_, std::convert::Infallible>(resp)
                }
            });
            if let Err(e) = hyper::server::conn::http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                tracing::warn!(error = %e, "metrics connection error");
            }
        });
    }
}
