use anyhow::Result;
use jsonrpsee::RpcModule;
use tokio::sync::oneshot;

use light_indexer_storage::read::ReadRequest;

use crate::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getTransaction", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let sig_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid signature"))?;

        let sig_bytes: [u8; 64] = bs58::decode(sig_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid signature encoding"))?
            .try_into()
            .map_err(|_| err(-32602, "Invalid signature length"))?;

        let (tx, rx) = oneshot::channel();
        ctx.reader
            .handle_request(ReadRequest::GetTransaction { signature: sig_bytes, tx })
            .await;

        match rx.await {
            Ok(Ok(Some(info))) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(info),
            Ok(Ok(None)) => Ok(serde_json::Value::Null),
            _ => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("getSignaturesForAddress", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let address_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid address"))?;

        let address_bytes = bs58::decode(address_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid address encoding"))?;

        let address = solana_pubkey::Pubkey::try_from(address_bytes.as_slice())
            .map_err(|_| err(-32602, "Invalid pubkey"))?;

        let opts = p.get(1).and_then(|v| v.as_object());
        let limit = opts.and_then(|o| o.get("limit")).and_then(|v| v.as_u64()).unwrap_or(1000) as usize;
        let before_slot = opts.and_then(|o| o.get("before")).and_then(|v| v.as_u64());

        let (tx, rx) = oneshot::channel();
        ctx.reader
            .handle_request(ReadRequest::GetSignaturesForAddress { address, before_slot, limit, tx })
            .await;

        match rx.await {
            Ok(Ok(sigs)) => {
                let results: Vec<serde_json::Value> = sigs.iter().map(|s| {
                    serde_json::json!({
                        "signature": s.signature.to_string(),
                        "slot": s.slot,
                        "blockTime": s.block_time,
                        "err": s.err,
                        "memo": s.memo,
                    })
                }).collect();
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(results))
            }
            _ => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("getSignatureStatuses", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let sigs = p.first().and_then(|v| v.as_array()).cloned().unwrap_or_default();

        let statuses: Vec<serde_json::Value> = sigs.iter().map(|_| {
            serde_json::json!({
                "slot": ctx.reader.cache().confirmed_slot(),
                "confirmations": null,
                "err": null,
                "confirmationStatus": "finalized"
            })
        }).collect();

        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "value": statuses,
            "context": {"slot": ctx.reader.cache().processed_slot()}
        }))
    })?;

    Ok(())
}
