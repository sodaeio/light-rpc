use anyhow::Result;
use jsonrpsee::types::Params;
use jsonrpsee::RpcModule;

use super::rpc_response;
use crate::rpc::server::RpcContext;
use crate::types::*;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getBlock", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let slot: Slot = p
            .first()
            .and_then(|v| v.as_u64())
            .ok_or_else(|| err(-32602, "Invalid slot"))?;
        let cfg = p.get(1).and_then(|v| v.as_object());
        let tx_details = cfg
            .and_then(|o| o.get("transactionDetails"))
            .and_then(|v| v.as_str())
            .unwrap_or("full");
        let include_rewards = cfg
            .and_then(|o| o.get("rewards"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        match ctx.reader.get_block(slot) {
            Ok(Some(block)) => {
                let transactions = match tx_details {
                    "none" => serde_json::Value::Array(Vec::new()),
                    "signatures" => serde_json::json!(block
                        .transactions
                        .iter()
                        .map(|t| t.signature.to_string())
                        .collect::<Vec<_>>()),
                    _ => serde_json::json!(block
                        .transactions
                        .iter()
                        .map(|t| serde_json::json!({
                            "transaction": [t.signature.to_string()],
                            "meta": {
                                "err": t.err.as_ref().map(|e| serde_json::json!(e)),
                                "fee": 0,
                                "preBalances": [],
                                "postBalances": [],
                            },
                            "version": "legacy",
                        }))
                        .collect::<Vec<_>>()),
                };
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                    "blockhash": block.info.blockhash,
                    "previousBlockhash": null,
                    "parentSlot": block.info.parent_slot,
                    "blockTime": block.info.block_time,
                    "blockHeight": block.info.block_height,
                    "rewards": if include_rewards { serde_json::json!([]) } else { serde_json::Value::Null },
                    "transactions": transactions,
                }))
            }
            Ok(None) => Err(err(
                -32009,
                "Slot was skipped, or missing in long-term storage",
            )),
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
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(ctx
            .reader
            .get_slot(commitment)))
    })?;

    module.register_async_method("getLatestBlockhash", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        let slot = ctx.reader.get_slot(commitment);
        match ctx.reader.get_latest_blockhash(commitment) {
            Some((blockhash, bh_slot)) => {
                let last_valid = match ctx.reader.get_block_height(commitment) {
                    Ok(Some(h)) => h + 150,
                    _ => bh_slot + 150,
                };
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(
                    slot,
                    serde_json::json!({
                        "blockhash": blockhash,
                        "lastValidBlockHeight": last_valid
                    }),
                ))
            }
            None => Err(err(-32603, "No blockhash available")),
        }
    })?;

    module.register_async_method("isBlockhashValid", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let blockhash = p.first().and_then(|v| v.as_str()).unwrap_or("").to_string();
        let slot = ctx.reader.cache().processed_slot();
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(
            slot,
            serde_json::json!(ctx.reader.is_blockhash_valid(&blockhash)),
        ))
    })?;

    module.register_async_method("getBlockTime", |params, ctx, _| async move {
        let (slot,): (Slot,) = params.parse()?;
        match ctx.reader.get_block(slot) {
            Ok(Some(block)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(
                block.info.block_time
            )),
            _ => Ok(serde_json::Value::Null),
        }
    })?;

    module.register_async_method("getVersion", |_, _, _| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "solana-core": "2.2.4",
            "feature-set": 0,
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
