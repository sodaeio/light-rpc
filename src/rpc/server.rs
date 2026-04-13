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

    pub async fn run(self) -> Result<()> {
        let context = RpcContext {
            reader: Arc::clone(&self.reader),
            upstream: self.upstream,
        };

        let module = methods::build_rpc_module(context)?;
        let state: RpcState = Arc::new(module);

        let cors = CorsLayer::new()
            .allow_methods([axum::http::Method::POST, axum::http::Method::GET])
            .allow_headers([axum::http::header::CONTENT_TYPE])
            .allow_origin(Any);

        let compression = CompressionLayer::new().gzip(true).br(true);

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

        info!(%addr, "rpc server starting");

        let listener = tokio::net::TcpListener::bind(addr).await?;
        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown_signal())
            .await?;

        Ok(())
    }
}

async fn handle_jsonrpc(
    State(module): State<RpcState>,
    body: String,
) -> (StatusCode, [(& 'static str, &'static str); 1], String) {
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
                return (StatusCode::OK, [("content-type", "application/json")], err.to_string());
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
        metrics::RPC_LATENCY.with_label_values(&["batch"]).observe(elapsed.as_secs_f64());
        (StatusCode::OK, [("content-type", "application/json")], batch_response)
    } else {
        match module.raw_json_request(&body, 1).await {
            Ok((response, _)) => {
                let elapsed = start.elapsed();
                metrics::RPC_LATENCY.with_label_values(&[&method]).observe(elapsed.as_secs_f64());
                (StatusCode::OK, [("content-type", "application/json")], response)
            }
            Err(e) => {
                metrics::RPC_ERRORS.with_label_values(&[&method]).inc();
                error!(method = %method, error = %e, "rpc error");
                let error_response = serde_json::json!({
                    "jsonrpc": "2.0",
                    "error": {"code": -32603, "message": e.to_string()},
                    "id": null
                });
                (StatusCode::OK, [("content-type", "application/json")], error_response.to_string())
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
