use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{error, info};

#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;

use light_indexer::config::Config;
use light_indexer::metrics::REGISTRY;
use light_indexer::rpc::server::RpcServer;
use light_indexer::rpc::upstream::UpstreamForwarder;
use light_indexer::source::StreamSource;
use light_indexer::storage::files::BlockFileStorage;
use light_indexer::storage::postgres::PgStorage;
use light_indexer::storage::read::{MemoryCache, StorageReader};
use light_indexer::storage::rocks::UnifiedRocksDb;
use light_indexer::storage::write::{pg_writer_loop, StorageWriter};

#[derive(Parser)]
#[command(
    name = "light-indexer",
    about = "Unified Solana indexer and RPC server"
)]
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
                .unwrap_or_else(|_| "info,light_indexer=debug".into()),
        )
        .with_target(true)
        .init();

    info!(
        version = env!("CARGO_PKG_VERSION"),
        "starting light-indexer"
    );

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
    let retention_slots = config.storage.postgres.address_retention_slots;
    tokio::spawn(async move {
        run_retention_loop(pg_retention, rocks_retention, cache_retention, retention_slots).await;
    });

    let pg_writer = pg.clone();
    tokio::spawn(async move { pg_writer_loop(pg_writer, pg_rx).await });

    let cache_rx = broadcast_tx.subscribe();
    let cache_ref = Arc::clone(&memory_cache);
    tokio::spawn(async move { StorageReader::run_cache_updater(cache_ref, cache_rx).await });

    #[allow(unused_mut)]
    let mut writer = StorageWriter::new(
        rocks.clone(),
        files.as_ref().clone_for_writer(),
        pg_tx,
        broadcast_tx,
    );

    #[cfg(feature = "clickhouse")]
    {
        use light_indexer::storage::clickhouse::{
            clickhouse_writer_loop, ClickHouseStore,
        };
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

    tokio::spawn(async move {
        if let Err(e) = writer.run(source_rx).await {
            error!(error = %e, "storage writer failed");
        }
    });

    let source = StreamSource::new(config.source, source_tx);
    tokio::spawn(async move {
        if let Err(e) = source.run().await {
            error!(error = %e, "stream source failed");
        }
    });

    let reader = Arc::new(StorageReader::new(memory_cache, rocks, files, pg));
    let upstream = config.rpc.upstream.as_ref().map(|endpoint| {
        UpstreamForwarder::new(endpoint.clone(), config.rpc.forwarded_methods.clone())
    });

    let server = RpcServer::new(config.rpc, reader, upstream);
    server.run().await.context("rpc server failed")
}

async fn run_retention_loop(
    pg: PgStorage,
    rocks: UnifiedRocksDb,
    cache: Arc<MemoryCache>,
    retention_slots: u64,
) {
    use std::time::Duration;

    loop {
        if cache.finalized_slot() > 0 {
            break;
        }
        tokio::time::sleep(Duration::from_secs(10)).await;
    }

    let mut ticker = tokio::time::interval(Duration::from_secs(3600));
    loop {
        ticker.tick().await;
        let current = cache.finalized_slot();
        if current == 0 {
            continue;
        }

        match pg.ensure_address_partitions(current, 4).await {
            Ok(_) => info!(current_slot = current, "ensured address_transactions partitions"),
            Err(e) => error!(error = %e, "failed to ensure address_transactions partitions"),
        }

        let cutoff = current.saturating_sub(retention_slots);
        if cutoff == 0 {
            continue;
        }

        match pg.drop_address_partitions_before(cutoff).await {
            Ok(dropped) if !dropped.is_empty() => {
                info!(cutoff, dropped = ?dropped, "dropped old address_transactions partitions")
            }
            Ok(_) => {}
            Err(e) => error!(error = %e, "failed to drop old partitions"),
        }

        let rocks_task = rocks.clone();
        let dropped = tokio::task::spawn_blocking(move || rocks_task.prune_sfa_before(cutoff))
            .await
            .ok()
            .and_then(|r| r.ok())
            .unwrap_or(0);
        if dropped > 0 {
            info!(cutoff, addresses = dropped, "pruned sfa_index before cutoff");
        }
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
