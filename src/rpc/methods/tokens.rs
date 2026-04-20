use anyhow::Result;
use jsonrpsee::RpcModule;

use super::rpc_response;
use crate::rpc::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getTokenAccountsByOwner", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let owner_str = p
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Invalid owner"))?;
        let owner_bytes = bs58::decode(owner_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?;
        // Second param can be {mint, programId}, third is config with encoding
        let encoding = p
            .get(2)
            .and_then(|v| v.get("encoding"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                p.get(1)
                    .and_then(|v| v.get("encoding"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("base64");
        let slot = ctx.reader.cache().processed_slot();

        match ctx
            .reader
            .get_token_accounts_by_owner(&owner_bytes, encoding)
            .await
        {
            Ok(accounts) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(
                slot,
                serde_json::json!(accounts),
            )),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getTokenAccountsByDelegate", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let delegate_str = p
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Invalid delegate"))?;
        let delegate_bytes = bs58::decode(delegate_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?;
        let encoding = p
            .get(2)
            .and_then(|v| v.get("encoding"))
            .and_then(|v| v.as_str())
            .or_else(|| {
                p.get(1)
                    .and_then(|v| v.get("encoding"))
                    .and_then(|v| v.as_str())
            })
            .unwrap_or("base64");
        let slot = ctx.reader.cache().processed_slot();

        match ctx
            .reader
            .get_token_accounts_by_delegate(&delegate_bytes, encoding)
            .await
        {
            Ok(accounts) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(
                slot,
                serde_json::json!(accounts),
            )),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getTokenSupply", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let mint_str = p
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Invalid mint"))?;
        let mint_bytes = bs58::decode(mint_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?;
        let slot = ctx.reader.cache().processed_slot();

        match ctx.reader.get_token_supply(&mint_bytes).await {
            Ok(Some(supply)) => {
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(slot, supply))
            }
            Ok(None) => Err(err(-32602, "could not find mint")),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getTokenLargestAccounts", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let mint_str = p
            .first()
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Invalid mint"))?;
        let mint_bytes = bs58::decode(mint_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?;
        let mint_key: [u8; 32] = mint_bytes
            .as_slice()
            .try_into()
            .map_err(|_| err(-32602, "Invalid mint length"))?;
        let slot = ctx.reader.cache().processed_slot();

        let reader = std::sync::Arc::clone(&ctx.reader);
        let arc = ctx
            .gtla_coalescer
            .run(mint_key, move || async move {
                reader
                    .get_token_largest_accounts(&mint_bytes, 20)
                    .await
                    .unwrap_or_default()
            })
            .await;
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(slot, serde_json::json!(*arc)))
    })?;

    Ok(())
}
