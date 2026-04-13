use anyhow::Result;
use jsonrpsee::types::Params;
use jsonrpsee::RpcModule;
use tokio::sync::oneshot;

use light_indexer_core::types::*;
use light_indexer_storage::read::ReadRequest;

use crate::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getBlock", |params, ctx, _| async move {
        let (slot,): (Slot,) = params.parse()?;
        let (tx, rx) = oneshot::channel();
        ctx.reader.handle_request(ReadRequest::GetBlock { slot, tx }).await;

        match rx.await {
            Ok(Ok(Some(block))) => {
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                    "blockhash": block.info.blockhash,
                    "parentSlot": block.info.parent_slot,
                    "blockTime": block.info.block_time,
                    "blockHeight": block.info.block_height,
                    "transactions": block.transactions.iter().map(|t| {
                        serde_json::json!({
                            "signature": t.signature.to_string(),
                            "err": t.err,
                        })
                    }).collect::<Vec<_>>(),
                }))
            }
            Ok(Ok(None)) => Err(err(-32009, "Slot not found")),
            _ => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("getBlockHeight", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        let (tx, rx) = oneshot::channel();
        ctx.reader
            .handle_request(ReadRequest::GetBlockHeight { commitment, tx })
            .await;

        match rx.await {
            Ok(Ok(Some(height))) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(height)),
            Ok(Ok(None)) => Ok(serde_json::json!(0)),
            _ => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("getSlot", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        let (tx, rx) = oneshot::channel();
        ctx.reader
            .handle_request(ReadRequest::GetSlot { commitment, tx })
            .await;

        match rx.await {
            Ok(slot) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(slot)),
            Err(_) => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("getLatestBlockhash", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        let (tx, rx) = oneshot::channel();
        ctx.reader
            .handle_request(ReadRequest::GetLatestBlockhash { commitment, tx })
            .await;

        match rx.await {
            Ok(Ok(Some((blockhash, slot)))) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": {
                    "blockhash": blockhash,
                    "lastValidBlockHeight": slot + 150
                },
                "context": {"slot": slot}
            })),
            _ => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("isBlockhashValid", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let blockhash = p
            .first()
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        let (tx, rx) = oneshot::channel();
        ctx.reader
            .handle_request(ReadRequest::IsBlockhashValid { blockhash, tx })
            .await;

        match rx.await {
            Ok(valid) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": valid,
                "context": {"slot": ctx.reader.cache().processed_slot()}
            })),
            Err(_) => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("getBlockTime", |params, ctx, _| async move {
        let (slot,): (Slot,) = params.parse()?;
        let (tx, rx) = oneshot::channel();
        ctx.reader.handle_request(ReadRequest::GetBlock { slot, tx }).await;

        match rx.await {
            Ok(Ok(Some(block))) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(block.info.block_time)),
            Ok(Ok(None)) => Ok(serde_json::Value::Null),
            _ => Err(err(-32603, "Internal error")),
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
