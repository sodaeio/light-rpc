use anyhow::Result;
use jsonrpsee::RpcModule;
use tokio::sync::oneshot;

use light_indexer_storage::read::ReadRequest;

use crate::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getAccountInfo", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let pubkey_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid pubkey"))?;

        let pubkey_bytes: [u8; 32] = bs58::decode(pubkey_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into()
            .map_err(|_| err(-32602, "Invalid pubkey length"))?;

        let (tx, rx) = oneshot::channel();
        ctx.reader.handle_request(ReadRequest::GetAccountInfo { pubkey: pubkey_bytes, tx }).await;

        match rx.await {
            Ok(Ok(Some(account))) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": account,
                "context": {"slot": ctx.reader.cache().processed_slot()}
            })),
            Ok(Ok(None)) => Ok(serde_json::json!({
                "value": null,
                "context": {"slot": ctx.reader.cache().processed_slot()}
            })),
            _ => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("getMultipleAccounts", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let pubkeys = p.first().and_then(|v| v.as_array()).cloned().unwrap_or_default();

        let mut accounts = Vec::with_capacity(pubkeys.len());
        for pk_val in &pubkeys {
            let pk_str = pk_val.as_str().unwrap_or("");
            let pk_bytes: [u8; 32] = match bs58::decode(pk_str).into_vec() {
                Ok(v) if v.len() == 32 => v.try_into().unwrap(),
                _ => { accounts.push(serde_json::Value::Null); continue; }
            };

            let (tx, rx) = oneshot::channel();
            ctx.reader.handle_request(ReadRequest::GetAccountInfo { pubkey: pk_bytes, tx }).await;
            match rx.await {
                Ok(Ok(Some(account))) => accounts.push(account),
                _ => accounts.push(serde_json::Value::Null),
            }
        }

        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "value": accounts,
            "context": {"slot": ctx.reader.cache().processed_slot()}
        }))
    })?;

    module.register_async_method("getProgramAccounts", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let program_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid program id"))?;

        let program_bytes: [u8; 32] = bs58::decode(program_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into()
            .map_err(|_| err(-32602, "Invalid pubkey length"))?;

        let (tx, rx) = oneshot::channel();
        ctx.reader.handle_request(ReadRequest::GetProgramAccounts { program_id: program_bytes, tx }).await;

        match rx.await {
            Ok(Ok(accounts)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(accounts)),
            _ => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("getBalance", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let pubkey_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid pubkey"))?;

        let pubkey_bytes: [u8; 32] = bs58::decode(pubkey_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into()
            .map_err(|_| err(-32602, "Invalid pubkey length"))?;

        let (tx, rx) = oneshot::channel();
        ctx.reader.handle_request(ReadRequest::GetAccountInfo { pubkey: pubkey_bytes, tx }).await;

        match rx.await {
            Ok(Ok(Some(account))) => {
                let lamports = account["lamports"].as_u64().unwrap_or(0);
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                    "value": lamports,
                    "context": {"slot": ctx.reader.cache().processed_slot()}
                }))
            }
            _ => Ok(serde_json::json!({
                "value": 0,
                "context": {"slot": ctx.reader.cache().processed_slot()}
            })),
        }
    })?;

    Ok(())
}
