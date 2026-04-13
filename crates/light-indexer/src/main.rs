use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result};
use clap::Parser;
use tracing::{error, info};

use light_indexer_core::config::Config;
use light_indexer_core::metrics::REGISTRY;
use light_indexer_rpc::server::RpcServer;
use light_indexer_rpc::upstream::UpstreamForwarder;
use light_indexer_source::StreamSource;
use light_indexer_storage::files::BlockFileStorage;
use light_indexer_storage::postgres::PgStorage;
use light_indexer_storage::read::{MemoryCache, StorageReader};
use light_indexer_storage::rocks::UnifiedRocksDb;
use light_indexer_storage::write::{pg_writer_loop, StorageWriter};

#[derive(Parser)]
#[command(name = "light-indexer", about = "Unified Solana indexer and RPC server")]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "config.yml")]
    config: PathBuf,

    /// Validate config and exit
    #[arg(long)]
    check: bool,
}

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Load configuration
    let config = Config::load(&cli.config)
        .with_context(|| format!("loading config from {}", cli.config.display()))?;

    if cli.check {
        println!("Configuration is valid.");
        return Ok(());
    }

    // Initialize tracing
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

    // Build the multi-runtime pipeline
    //
    // Architecture:
    //
    //   [Source Runtime]                    [Metrics Runtime]
    //        │                                   │
    //        │ mpsc(source_to_write)              │ HTTP /metrics
    //        ▼                                   │
    //   [Write Runtime]                          │
    //        │                                   │
    //        ├── RocksDB writes (blocks + accounts)
    //        ├── File writes (block data)
    //        ├──→ mpsc(pg_write_buffer) → [PG Writer Task]
    //        │
    //        │ broadcast(write_to_read)
    //        ▼
    //   [Read/RPC Runtime]
    //        ├── Cache updater (broadcast subscriber)
    //        ├── StorageReader (tiered: cache → rocks → files → pg)
    //        └── Axum HTTP server (JSON-RPC)

    let source_config = config.source.clone();
    let storage_config = config.storage.clone();
    let rpc_config = config.rpc.clone();
    let metrics_config = config.metrics.clone();
    let thread_config = config.threads.clone();

    // --- Storage layer initialization (shared across runtimes) ---

    let rocks = UnifiedRocksDb::open(&storage_config.rocksdb)
        .context("opening rocksdb")?;
    info!("rocksdb opened at {}", storage_config.rocksdb.path);

    let files = Arc::new(
        BlockFileStorage::open(&storage_config.blocks)
            .context("opening block file storage")?,
    );
    info!("block storage opened at {}", storage_config.blocks.path);

    let memory_cache = Arc::new(MemoryCache::new());

    // --- Pipeline channels ---

    let (source_tx, source_rx) =
        tokio::sync::mpsc::channel(storage_config.pipeline.source_to_write);

    let (broadcast_tx, _) =
        tokio::sync::broadcast::channel(storage_config.pipeline.write_to_read);

    let (pg_tx, pg_rx) =
        tokio::sync::mpsc::channel(storage_config.pipeline.pg_write_buffer);

    // --- Build the main runtime ---

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(thread_config.rpc_count())
        .thread_name("li-main")
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    rt.block_on(async move {
        // Connect to PostgreSQL
        let pg = PgStorage::connect(&storage_config.postgres)
            .await
            .context("connecting to postgres")?;
        pg.migrate().await.context("running migrations")?;

        // --- Spawn metrics server ---
        let metrics_endpoint = metrics_config.endpoint.clone();
        tokio::spawn(async move {
            if let Err(e) = run_metrics_server(&metrics_endpoint).await {
                error!(error = %e, "metrics server failed");
            }
        });

        // --- Spawn PG writer task (isolated from main pipeline) ---
        let pg_writer = pg.clone();
        tokio::spawn(async move {
            pg_writer_loop(pg_writer, pg_rx).await;
        });

        // --- Spawn cache updater (broadcast subscriber) ---
        let cache_for_updater = Arc::clone(&memory_cache);
        let broadcast_rx = broadcast_tx.subscribe();
        tokio::spawn(async move {
            StorageReader::run_cache_updater(cache_for_updater, broadcast_rx).await;
        });

        // --- Spawn storage writer ---
        let writer = StorageWriter::new(
            rocks.clone(),
            files.as_ref().clone_for_writer(),
            pg_tx,
            broadcast_tx,
        );
        tokio::spawn(async move {
            if let Err(e) = writer.run(source_rx).await {
                error!(error = %e, "storage writer failed");
            }
        });

        // --- Spawn gRPC source ---
        let source = StreamSource::new(source_config, source_tx);
        tokio::spawn(async move {
            if let Err(e) = source.run().await {
                error!(error = %e, "stream source failed");
            }
        });

        // --- Build and run RPC server (blocks this task) ---
        let reader = Arc::new(StorageReader::new(
            memory_cache,
            rocks,
            files,
            pg,
        ));

        let upstream = rpc_config.upstream.as_ref().map(|endpoint| {
            UpstreamForwarder::new(
                endpoint.clone(),
                rpc_config.forwarded_methods.clone(),
            )
        });

        let server = RpcServer::new(rpc_config, reader, upstream);
        server.run().await.context("rpc server failed")?;

        Ok::<_, anyhow::Error>(())
    })?;

    Ok(())
}

async fn run_metrics_server(endpoint: &str) -> Result<()> {
    use prometheus::Encoder;

    let addr: std::net::SocketAddr = endpoint.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(%addr, "metrics server started");

    loop {
        let (stream, _) = listener.accept().await?;
        tokio::spawn(async move {
            let io = hyper_util::rt::TokioIo::new(stream);
            let service = hyper::service::service_fn(|_req| async {
                let encoder = prometheus::TextEncoder::new();
                let metric_families = REGISTRY.gather();
                let mut buffer = Vec::new();
                encoder.encode(&metric_families, &mut buffer).unwrap();
                Ok::<_, std::convert::Infallible>(hyper::Response::new(
                    http_body_util::Full::new(bytes::Bytes::from(buffer)),
                ))
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
