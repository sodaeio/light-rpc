use anyhow::Result;
use jsonrpsee::RpcModule;

use super::rpc_response;
use crate::rpc::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getTransaction", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let sig_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid signature"))?;
        let sig_bytes: [u8; 64] = bs58::decode(sig_str).into_vec()
            .map_err(|_| err(-32602, "Invalid signature encoding"))?
            .try_into().map_err(|_| err(-32602, "Invalid signature length"))?;

        match ctx.reader.get_transaction(&sig_bytes) {
            Ok(Some(info)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(info),
            Ok(None) => Ok(serde_json::Value::Null),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getSignaturesForAddress", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let address_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid address"))?;
        let address_bytes = bs58::decode(address_str).into_vec().map_err(|_| err(-32602, "Invalid encoding"))?;
        let address = solana_pubkey::Pubkey::try_from(address_bytes.as_slice()).map_err(|_| err(-32602, "Invalid pubkey"))?;

        let opts = p.get(1).and_then(|v| v.as_object());
        let limit = opts.and_then(|o| o.get("limit")).and_then(|v| v.as_u64()).unwrap_or(1000) as usize;
        let before_slot = opts.and_then(|o| o.get("before")).and_then(|v| v.as_u64());

        match ctx.reader.get_signatures_for_address(&address, before_slot, limit) {
            Ok(sigs) => {
                let finalized = ctx.reader.cache().finalized_slot();
                let confirmed = ctx.reader.cache().confirmed_slot();
                let results: Vec<serde_json::Value> = sigs.iter().map(|s| {
                    let status = if s.slot <= finalized {
                        "finalized"
                    } else if s.slot <= confirmed {
                        "confirmed"
                    } else {
                        "processed"
                    };
                    serde_json::json!({
                        "signature": s.signature.to_string(),
                        "slot": s.slot,
                        "blockTime": s.block_time,
                        "err": s.err,
                        "memo": s.memo,
                        "confirmationStatus": status,
                    })
                }).collect();
                // getSignaturesForAddress returns array directly, no context wrapper
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(results))
            }
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getSignatureStatuses", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let sigs = p.first().and_then(|v| v.as_array()).cloned().unwrap_or_default();
        let slot = ctx.reader.cache().processed_slot();
        let finalized = ctx.reader.cache().finalized_slot();

        let statuses: Vec<serde_json::Value> = sigs.iter().map(|sig_val| {
            let sig_str = sig_val.as_str().unwrap_or("");
            let sig_bytes: Result<[u8; 64], _> = bs58::decode(sig_str).into_vec()
                .map_err(|_| ())
                .and_then(|v| v.try_into().map_err(|_| ()));

            match sig_bytes {
                Ok(bytes) => {
                    match ctx.reader.get_transaction(&bytes) {
                        Ok(Some(tx_info)) => {
                            let tx_slot = tx_info["slot"].as_u64().unwrap_or(0);
                            serde_json::json!({
                                "slot": tx_slot,
                                "confirmations": null,
                                "err": tx_info.get("err").cloned().unwrap_or(serde_json::Value::Null),
                                "confirmationStatus": if tx_slot <= finalized { "finalized" } else { "confirmed" },
                            })
                        }
                        _ => serde_json::Value::Null,
                    }
                }
                Err(_) => serde_json::Value::Null,
            }
        }).collect();

        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(slot, serde_json::json!(statuses)))
    })?;

    Ok(())
}
