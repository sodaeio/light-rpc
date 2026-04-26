use std::sync::Arc;

use anyhow::Result;
use jsonrpsee::RpcModule;

use super::{rpc_resp, rpc_response};
use crate::rpc::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getAccountInfo", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let pubkey_str = p
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Invalid pubkey"))?;
        let pubkey_bytes: [u8; 32] = bs58::decode(pubkey_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into()
            .map_err(|_| err(-32602, "Invalid pubkey length"))?;
        let encoding = p
            .get(1)
            .and_then(|v| v.get("encoding"))
            .and_then(|v| v.as_str())
            .unwrap_or("base64")
            .to_string();
        let slot = ctx.reader.cache().processed_slot();

        let raw = match ctx
            .reader
            .get_account_info_raw(&pubkey_bytes, &encoding)
            .await
        {
            Ok(Some(arc)) => arc,
            _ => Arc::new(serde_json::value::RawValue::from_string("null".into()).unwrap()),
        };
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_resp(slot, raw))
    })?;

    module.register_async_method("getMultipleAccounts", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let pubkey_strs = p
            .first()
            .and_then(|v| v.as_array())
            .cloned()
            .unwrap_or_default();
        let encoding = p
            .get(1)
            .and_then(|v| v.get("encoding"))
            .and_then(|v| v.as_str())
            .unwrap_or("base64");

        if pubkey_strs.len() > 100 {
            return Err(err(-32602, "Too many accounts requested (max 100)"));
        }

        let mut keys = Vec::with_capacity(pubkey_strs.len());
        for pk_val in &pubkey_strs {
            let pk_str = pk_val.as_str().unwrap_or("");
            match bs58::decode(pk_str).into_vec() {
                Ok(v) if v.len() == 32 => keys.push(v.try_into().unwrap()),
                _ => keys.push([0u8; 32]),
            }
        }

        let slot = ctx.reader.cache().processed_slot();
        let accounts = ctx.reader.get_multiple_accounts(&keys, encoding).await;
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(slot, serde_json::json!(accounts)))
    })?;

    module.register_async_method("getProgramAccounts", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let program_str = p
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Invalid program id"))?;
        let program_bytes: [u8; 32] = bs58::decode(program_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into()
            .map_err(|_| err(-32602, "Invalid pubkey length"))?;

        // GPA on Token/System/etc. would return GB-scale payloads.
        if ctx.gpa_blocked.contains(&program_bytes) {
            return Err(err(
                -32602,
                &format!(
                    "getProgramAccounts is not supported for {program_str}. \
                     Use getTokenAccountsByOwner / getTokenAccountsByDelegate \
                     for SPL Token programs, or getAccountInfo for a single \
                     account."
                ),
            ));
        }

        let opts = p.get(1).and_then(|v| v.as_object());
        let encoding = opts
            .and_then(|o| o.get("encoding"))
            .and_then(|v| v.as_str())
            .unwrap_or("base64");
        let with_context = opts
            .and_then(|o| o.get("withContext"))
            .and_then(|v| v.as_bool())
            .unwrap_or(false);
        // Cursor: client echoes the last pubkey received as `after`.
        let after: Option<[u8; 32]> = opts
            .and_then(|o| o.get("after"))
            .and_then(|v| v.as_str())
            .and_then(|s| bs58::decode(s).into_vec().ok())
            .and_then(|v| v.try_into().ok());
        let limit = opts
            .and_then(|o| o.get("limit"))
            .and_then(|v| v.as_u64())
            .map(|l| l.min(ctx.gpa_max_accounts as u64) as usize)
            .unwrap_or(ctx.gpa_max_accounts);

        match ctx
            .reader
            .get_program_accounts(&program_bytes, encoding)
            .await
        {
            Ok(mut accounts) => {
                if let Some(cursor) = after {
                    let cursor_str = bs58::encode(cursor).into_string();
                    if let Some(pos) = accounts
                        .iter()
                        .position(|a| a.get("pubkey").and_then(|v| v.as_str()) == Some(&cursor_str))
                    {
                        accounts = accounts.split_off(pos + 1);
                    }
                }
                accounts.truncate(limit);
                if with_context {
                    let slot = ctx.reader.cache().processed_slot();
                    Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(
                        slot,
                        serde_json::json!(accounts),
                    ))
                } else {
                    Ok(serde_json::json!(accounts))
                }
            }
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getBalance", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let pubkey_str = p
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Invalid pubkey"))?;
        let pubkey_bytes: [u8; 32] = bs58::decode(pubkey_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into()
            .map_err(|_| err(-32602, "Invalid pubkey length"))?;

        let slot = ctx.reader.cache().processed_slot();
        let lamports = match ctx.reader.get_account_info(&pubkey_bytes, "base64").await {
            Ok(Some(a)) => a["lamports"].as_u64().unwrap_or(0),
            _ => 0,
        };
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(slot, serde_json::json!(lamports)))
    })?;

    Ok(())
}
