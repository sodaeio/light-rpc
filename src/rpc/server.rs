use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result};
use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use jsonrpsee::RpcModule;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tracing::{error, info};

use crate::config::RpcConfig;
use crate::metrics;
use crate::storage::read::StorageReader;

use super::methods;
use super::upstream::UpstreamForwarder;

pub struct RpcServer {
    config: RpcConfig,
    reader: Arc<StorageReader>,
    upstream: Option<UpstreamForwarder>,
}

pub struct RpcContext {
    pub reader: Arc<StorageReader>,
    pub upstream: Option<UpstreamForwarder>,
    pub gpa_blocked: std::collections::HashSet<[u8; 32]>,
    pub gpa_max_accounts: usize,
    pub block_cache: BlockResponseCache,
}

pub const BLOCK_CACHE_SHARDS: usize = 16;
pub type BlockCacheShard = parking_lot::Mutex<lru::LruCache<(u64, u64), Arc<serde_json::Value>>>;
pub type BlockResponseCache = Arc<[BlockCacheShard; BLOCK_CACHE_SHARDS]>;

#[inline]
pub fn block_cache_shard(slot: u64, cfg_hash: u64) -> usize {
    let mut h = slot.wrapping_mul(0x9E3779B97F4A7C15);
    h ^= cfg_hash;
    h.wrapping_mul(0xBF58476D1CE4E5B9) as usize % BLOCK_CACHE_SHARDS
}

type RpcState = Arc<RpcModule<RpcContext>>;

impl RpcServer {
    pub fn new(
        config: RpcConfig,
        reader: Arc<StorageReader>,
        upstream: Option<UpstreamForwarder>,
    ) -> Self {
        Self {
            config,
            reader,
            upstream,
        }
    }

    fn build_gpa_blocklist(config: &RpcConfig) -> std::collections::HashSet<[u8; 32]> {
        use crate::config::DEFAULT_GPA_BLOCKED;
        let entries: Vec<String> = match &config.gpa_blocked_programs {
            Some(list) => list.clone(),
            None => DEFAULT_GPA_BLOCKED.iter().map(|s| s.to_string()).collect(),
        };
        let mut set = std::collections::HashSet::with_capacity(entries.len());
        for s in entries {
            match bs58::decode(&s).into_vec() {
                Ok(v) if v.len() == 32 => {
                    set.insert(v.try_into().unwrap());
                }
                _ => tracing::warn!(pubkey = %s, "invalid gpa_blocked_programs entry, skipping"),
            }
        }
        tracing::info!(count = set.len(), "gpa blocklist loaded");
        set
    }

    pub async fn run(self) -> Result<()> {
        let gpa_blocked = Self::build_gpa_blocklist(&self.config);
        let cap = std::num::NonZeroUsize::new(64).unwrap();
        let block_cache: BlockResponseCache =
            Arc::new(std::array::from_fn(|_| parking_lot::Mutex::new(lru::LruCache::new(cap))));
        let context = RpcContext {
            reader: Arc::clone(&self.reader),
            upstream: self.upstream,
            gpa_blocked,
            gpa_max_accounts: self.config.gpa_max_accounts,
            block_cache,
        };

        let module = methods::build_rpc_module(context)?;
        let state: RpcState = Arc::new(module);

        let cors = CorsLayer::new()
            .allow_methods([axum::http::Method::POST, axum::http::Method::GET])
            .allow_headers([axum::http::header::CONTENT_TYPE])
            .allow_origin(Any);

        // Only compress responses big enough that gzip setup isn't net-loss.
        // Tiny getSlot responses stay uncompressed.
        let compression = CompressionLayer::new()
            .gzip(true)
            .br(true)
            .quality(tower_http::CompressionLevel::Fastest);

        let app = Router::new()
            .route("/", post(handle_jsonrpc))
            .route("/health", get(handle_health))
            .layer(cors)
            .layer(compression)
            .with_state(state);

        let addr: SocketAddr = self
            .config
            .endpoint
            .parse()
            .context("parsing rpc endpoint address")?;

        // One listener per worker via SO_REUSEPORT. Kernel hashes inbound
        // connections by 5-tuple so cross-worker accept-queue contention is
        // gone. Matches the scaling pattern used by nginx/envoy.
        let n_listeners = num_cpus::get().clamp(1, 32);
        info!(%addr, listeners = n_listeners, "rpc server starting");

        let mut handles = Vec::with_capacity(n_listeners);
        let shutdown = std::sync::Arc::new(tokio::sync::Notify::new());
        let sig_shutdown = std::sync::Arc::clone(&shutdown);
        tokio::spawn(async move {
            shutdown_signal().await;
            sig_shutdown.notify_waiters();
        });

        for i in 0..n_listeners {
            let listener = bind_reuseport(addr)?;
            let svc = app.clone();
            let shutdown = std::sync::Arc::clone(&shutdown);
            handles.push(tokio::spawn(async move {
                let _ = axum::serve(listener, svc)
                    .with_graceful_shutdown(async move { shutdown.notified().await })
                    .await;
                tracing::info!(listener = i, "rpc listener stopped");
            }));
        }

        for h in handles {
            let _ = h.await;
        }
        Ok(())
    }
}

fn bind_reuseport(addr: SocketAddr) -> Result<tokio::net::TcpListener> {
    let sock = socket2::Socket::new(
        socket2::Domain::IPV4,
        socket2::Type::STREAM,
        Some(socket2::Protocol::TCP),
    )?;
    sock.set_reuse_address(true)?;
    sock.set_reuse_port(true)?;
    sock.set_nonblocking(true)?;
    sock.set_nodelay(true)?;
    sock.bind(&addr.into())?;
    sock.listen(32768)?;
    Ok(tokio::net::TcpListener::from_std(sock.into())?)
}

async fn handle_jsonrpc(
    State(module): State<RpcState>,
    body: String,
) -> (StatusCode, [(&'static str, &'static str); 1], String) {
    let start = Instant::now();
    let method = extract_method(&body).unwrap_or("unknown").to_string();
    metrics::RPC_REQUESTS.with_label_values(&[&method]).inc();

    // Detect batch
    let trimmed = body.trim_start();
    if trimmed.starts_with('[') {
        let requests: Vec<serde_json::Value> = match serde_json::from_str(trimmed) {
            Ok(v) => v,
            Err(_) => {
                let err = r#"{"jsonrpc":"2.0","error":{"code":-32700,"message":"Parse error"},"id":null}"#;
                return (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    err.to_string(),
                );
            }
        };

        let mut responses = Vec::with_capacity(requests.len());
        for req in &requests {
            let req_str = req.to_string();
            let resp = module.raw_json_request(&req_str, 1).await;
            match resp {
                Ok((resp_str, _)) => responses.push(resp_str),
                Err(e) => {
                    let err_resp = serde_json::json!({
                        "jsonrpc": "2.0",
                        "error": {"code": -32603, "message": e.to_string()},
                        "id": req.get("id")
                    });
                    responses.push(err_resp.to_string());
                }
            }
        }

        let batch_response = format!("[{}]", responses.join(","));
        let elapsed = start.elapsed();
        metrics::RPC_LATENCY
            .with_label_values(&["batch"])
            .observe(elapsed.as_secs_f64());
        (
            StatusCode::OK,
            [("content-type", "application/json")],
            batch_response,
        )
    } else {
        match module.raw_json_request(&body, 1).await {
            Ok((response, _)) => {
                let elapsed = start.elapsed();
                metrics::RPC_LATENCY
                    .with_label_values(&[&method])
                    .observe(elapsed.as_secs_f64());
                (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    response,
                )
            }
            Err(e) => {
                metrics::RPC_ERRORS.with_label_values(&[&method]).inc();
                error!(method = %method, error = %e, "rpc error");
                let error_response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {"code": -32603, "message": e.to_string()},
                    "id": null
                });
                (
                    StatusCode::OK,
                    [("content-type", "application/json")],
                    error_response.to_string(),
                )
            }
        }
    }
}

async fn handle_health(State(_module): State<RpcState>) -> impl IntoResponse {
    (StatusCode::OK, Json(serde_json::json!({"status": "ok"})))
}

fn extract_method(body: &str) -> Option<&str> {
    let idx = body.find("\"method\"")?;
    let rest = &body[idx + 8..];
    let start = rest.find('"')? + 1;
    let end = start + rest[start..].find('"')?;
    Some(&rest[start..end])
}

async fn shutdown_signal() {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to listen for ctrl+c");
    info!("shutdown signal received");
}
