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

    module.register_async_method("getTokenAccountsByDelegate", |_params, ctx, _| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "value": [],
            "context": { "slot": ctx.reader.cache().processed_slot() }
        }))
    })?;

    module.register_async_method("getTokenSupply", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let mint_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid mint"))?;
        let mint_bytes: [u8; 32] = bs58::decode(mint_str).into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into().map_err(|_| err(-32602, "Invalid mint length"))?;

        match ctx.reader.get_account_info(&mint_bytes) {
            Ok(Some(_)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": { "amount": "0", "decimals": 0, "uiAmount": 0.0, "uiAmountString": "0" },
                "context": { "slot": ctx.reader.cache().processed_slot() }
            })),
            _ => Err(err(-32602, "Mint not found")),
        }
    })?;

    module.register_async_method("getTokenLargestAccounts", |_params, ctx, _| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "value": [],
            "context": { "slot": ctx.reader.cache().processed_slot() }
        }))
    })?;

    Ok(())
}
