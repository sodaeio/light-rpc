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

    info!(version = env!("CARGO_PKG_VERSION"), "starting light-indexer");

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(config.threads.rpc_count())
        .thread_name("li-main")
        .enable_all()
        .build()
        .context("building tokio runtime")?;

    rt.block_on(run(config))
}

async fn run(config: Config) -> Result<()> {
    // Storage layer
    let rocks = UnifiedRocksDb::open(&config.storage.rocksdb).context("opening rocksdb")?;
    info!(path = %config.storage.rocksdb.path, "rocksdb opened");

    let files = Arc::new(
        BlockFileStorage::open(&config.storage.blocks).context("opening block storage")?,
    );
    info!(path = %config.storage.blocks.path, "block storage opened");

    let pg = PgStorage::connect(&config.storage.postgres).await.context("connecting to postgres")?;
    pg.migrate().await.context("running migrations")?;

    let memory_cache = Arc::new(MemoryCache::new());

    // Pipeline channels
    let (source_tx, source_rx) = tokio::sync::mpsc::channel(config.storage.pipeline.source_to_write);
    let (broadcast_tx, _) = tokio::sync::broadcast::channel(config.storage.pipeline.write_to_read);
    let (pg_tx, pg_rx) = tokio::sync::mpsc::channel(config.storage.pipeline.pg_write_buffer);

    // Metrics server
    let metrics_endpoint = config.metrics.endpoint.clone();
    tokio::spawn(async move {
        if let Err(e) = run_metrics_server(&metrics_endpoint).await {
            error!(error = %e, "metrics server failed");
        }
    });

    // PG writer (isolated task)
    let pg_writer = pg.clone();
    tokio::spawn(async move { pg_writer_loop(pg_writer, pg_rx).await });

    // Cache updater (broadcast subscriber)
    let cache_rx = broadcast_tx.subscribe();
    let cache_ref = Arc::clone(&memory_cache);
    tokio::spawn(async move { StorageReader::run_cache_updater(cache_ref, cache_rx).await });

    // Storage writer
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

    // gRPC source
    let source = StreamSource::new(config.source, source_tx);
    tokio::spawn(async move {
        if let Err(e) = source.run().await {
            error!(error = %e, "stream source failed");
        }
    });

    // RPC server (blocks)
    let reader = Arc::new(StorageReader::new(memory_cache, rocks, files, pg));
    let upstream = config.rpc.upstream.as_ref().map(|endpoint| {
        UpstreamForwarder::new(endpoint.clone(), config.rpc.forwarded_methods.clone())
    });

    let server = RpcServer::new(config.rpc, reader, upstream);
    server.run().await.context("rpc server failed")
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
