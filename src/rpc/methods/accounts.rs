use anyhow::Result;
use jsonrpsee::RpcModule;

use crate::rpc::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getAccountInfo", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let pubkey_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid pubkey"))?;
        let pubkey_bytes: [u8; 32] = bs58::decode(pubkey_str).into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into().map_err(|_| err(-32602, "Invalid pubkey length"))?;

        match ctx.reader.get_account_info(&pubkey_bytes) {
            Ok(Some(account)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": account,
                "context": { "slot": ctx.reader.cache().processed_slot() }
            })),
            Ok(None) => Ok(serde_json::json!({
                "value": null,
                "context": { "slot": ctx.reader.cache().processed_slot() }
            })),
            Err(e) => Err(err(-32603, &e.to_string())),
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
            match ctx.reader.get_account_info(&pk_bytes) {
                Ok(Some(a)) => accounts.push(a),
                _ => accounts.push(serde_json::Value::Null),
            }
        }

        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "value": accounts,
            "context": { "slot": ctx.reader.cache().processed_slot() }
        }))
    })?;

    module.register_async_method("getProgramAccounts", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let program_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid program id"))?;
        let program_bytes: [u8; 32] = bs58::decode(program_str).into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into().map_err(|_| err(-32602, "Invalid pubkey length"))?;

        match ctx.reader.get_program_accounts(&program_bytes) {
            Ok(accounts) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(accounts)),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getBalance", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let pubkey_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid pubkey"))?;
        let pubkey_bytes: [u8; 32] = bs58::decode(pubkey_str).into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into().map_err(|_| err(-32602, "Invalid pubkey length"))?;

        let lamports = match ctx.reader.get_account_info(&pubkey_bytes) {
            Ok(Some(a)) => a["lamports"].as_u64().unwrap_or(0),
            _ => 0,
        };
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "value": lamports,
            "context": { "slot": ctx.reader.cache().processed_slot() }
        }))
    })?;

    Ok(())
}
