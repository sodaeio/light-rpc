pub mod accounts;
pub mod assets;
pub mod blocks;
pub mod tokens;
pub mod transactions;

use anyhow::Result;
use jsonrpsee::RpcModule;

use crate::rpc::server::RpcContext;
use crate::types::{Commitment, Slot};

pub type RpcResult = Result<serde_json::Value, jsonrpsee::types::ErrorObjectOwned>;

const API_VERSION: &str = "2.2.4";

/// Read commitment from the options object at `params[opts_idx]`.
/// Default = Processed (storage is single-version; Processed gives the
/// freshest slot for context).
pub fn commitment_from(params: &[serde_json::Value], opts_idx: usize) -> Commitment {
    params
        .get(opts_idx)
        .and_then(|v| v.get("commitment"))
        .and_then(|v| v.as_str())
        .map(|s| match s {
            "finalized" => Commitment::Finalized,
            "confirmed" => Commitment::Confirmed,
            _ => Commitment::Processed,
        })
        .unwrap_or(Commitment::Processed)
}

pub fn slot_for(ctx: &RpcContext, c: Commitment) -> Slot {
    let cache = ctx.reader.cache();
    match c {
        Commitment::Finalized => cache.finalized_slot(),
        Commitment::Confirmed => cache.confirmed_slot(),
        Commitment::Processed => cache.processed_slot(),
    }
}

pub fn rpc_response(slot: u64, value: serde_json::Value) -> serde_json::Value {
    serde_json::json!({
        "context": { "slot": slot, "apiVersion": API_VERSION },
        "value": value
    })
}

#[derive(Clone, serde::Serialize)]
pub struct RpcResp<T: serde::Serialize + Clone> {
    pub context: RpcCtx,
    pub value: T,
}

#[derive(Clone, serde::Serialize)]
pub struct RpcCtx {
    pub slot: u64,
    #[serde(rename = "apiVersion")]
    pub api_version: &'static str,
}

pub fn rpc_resp<T: serde::Serialize + Clone>(slot: u64, value: T) -> RpcResp<T> {
    RpcResp {
        context: RpcCtx {
            slot,
            api_version: API_VERSION,
        },
        value,
    }
}

pub fn build_rpc_module(context: RpcContext) -> Result<RpcModule<RpcContext>> {
    let mut module = RpcModule::new(context);

    blocks::register(&mut module)?;
    transactions::register(&mut module)?;
    accounts::register(&mut module)?;
    tokens::register(&mut module)?;
    assets::register(&mut module)?;

    tracing::info!(
        methods = module.method_names().count(),
        "rpc module initialized"
    );

    Ok(module)
}
