use anyhow::Result;
use jsonrpsee::RpcModule;

use crate::rpc::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getTokenAccountsByOwner", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let owner_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid owner"))?;
        let owner_bytes = bs58::decode(owner_str).into_vec().map_err(|_| err(-32602, "Invalid encoding"))?;

        match ctx.reader.get_token_accounts_by_owner(&owner_bytes).await {
            Ok(accounts) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": accounts,
                "context": { "slot": ctx.reader.cache().processed_slot() }
            })),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getTokenAccountsByDelegate", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let delegate_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid delegate"))?;
        let delegate_bytes = bs58::decode(delegate_str).into_vec().map_err(|_| err(-32602, "Invalid encoding"))?;

        match ctx.reader.get_token_accounts_by_delegate(&delegate_bytes).await {
            Ok(accounts) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": accounts,
                "context": { "slot": ctx.reader.cache().processed_slot() }
            })),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getTokenSupply", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let mint_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid mint"))?;
        let mint_bytes = bs58::decode(mint_str).into_vec().map_err(|_| err(-32602, "Invalid encoding"))?;

        match ctx.reader.get_token_supply(&mint_bytes).await {
            Ok(Some(supply)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": supply,
                "context": { "slot": ctx.reader.cache().processed_slot() }
            })),
            Ok(None) => Err(err(-32602, "Mint not found")),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getTokenLargestAccounts", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let mint_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid mint"))?;
        let mint_bytes = bs58::decode(mint_str).into_vec().map_err(|_| err(-32602, "Invalid encoding"))?;

        match ctx.reader.get_token_largest_accounts(&mint_bytes, 20).await {
            Ok(accounts) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": accounts,
                "context": { "slot": ctx.reader.cache().processed_slot() }
            })),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    Ok(())
}
