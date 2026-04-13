use anyhow::Result;
use jsonrpsee::types::Params;
use jsonrpsee::RpcModule;

use crate::types::*;
use crate::rpc::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getBlock", |params, ctx, _| async move {
        let (slot,): (Slot,) = params.parse()?;
        match ctx.reader.get_block(slot) {
            Ok(Some(block)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "blockhash": block.info.blockhash,
                "parentSlot": block.info.parent_slot,
                "blockTime": block.info.block_time,
                "blockHeight": block.info.block_height,
                "transactions": block.transactions.iter().map(|t| serde_json::json!({
                    "signature": t.signature.to_string(),
                    "err": t.err,
                })).collect::<Vec<_>>(),
            })),
            Ok(None) => Err(err(-32009, "Slot not found")),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getBlockHeight", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        match ctx.reader.get_block_height(commitment) {
            Ok(Some(h)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(h)),
            Ok(None) => Ok(serde_json::json!(0)),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getSlot", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(ctx.reader.get_slot(commitment)))
    })?;

    module.register_async_method("getLatestBlockhash", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        match ctx.reader.get_latest_blockhash(commitment) {
            Some((blockhash, slot)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": { "blockhash": blockhash, "lastValidBlockHeight": slot + 150 },
                "context": { "slot": slot }
            })),
            None => Err(err(-32603, "No blockhash available")),
        }
    })?;

    module.register_async_method("isBlockhashValid", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let blockhash = p.first().and_then(|v| v.as_str()).unwrap_or("").to_string();
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "value": ctx.reader.is_blockhash_valid(&blockhash),
            "context": { "slot": ctx.reader.cache().processed_slot() }
        }))
    })?;

    module.register_async_method("getBlockTime", |params, ctx, _| async move {
        let (slot,): (Slot,) = params.parse()?;
        match ctx.reader.get_block(slot) {
            Ok(Some(block)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(block.info.block_time)),
            _ => Ok(serde_json::Value::Null),
        }
    })?;

    module.register_async_method("getVersion", |_, _, _| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "solana-core": "2.2.0",
            "feature-set": 0,
            "light-indexer": env!("CARGO_PKG_VERSION")
        }))
    })?;

    Ok(())
}

fn parse_commitment(params: &Params) -> Commitment {
    if let Ok(p) = params.parse::<Vec<serde_json::Value>>() {
        if let Some(obj) = p.first().and_then(|v| v.as_object()) {
            if let Some(c) = obj.get("commitment").and_then(|v| v.as_str()) {
                return match c {
                    "processed" => Commitment::Processed,
                    "confirmed" => Commitment::Confirmed,
                    _ => Commitment::Finalized,
                };
            }
        }
    }
    Commitment::Finalized
}
