use anyhow::Result;
use jsonrpsee::RpcModule;
use tokio::sync::oneshot;

use light_indexer_storage::read::ReadRequest;

use crate::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getTokenAccountsByOwner", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let owner_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid owner"))?;

        let owner_bytes = bs58::decode(owner_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?;

        let (tx, rx) = oneshot::channel();
        ctx.reader.handle_request(ReadRequest::GetTokenAccountsByOwner { owner: owner_bytes, tx }).await;

        match rx.await {
            Ok(Ok(accounts)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                "value": accounts,
                "context": {"slot": ctx.reader.cache().processed_slot()}
            })),
            _ => Err(err(-32603, "Internal error")),
        }
    })?;

    module.register_async_method("getTokenAccountsByDelegate", |_params, ctx, _| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "value": [],
            "context": {"slot": ctx.reader.cache().processed_slot()}
        }))
    })?;

    module.register_async_method("getTokenSupply", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let mint_str = p.first().and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Invalid mint"))?;

        let mint_bytes: [u8; 32] = bs58::decode(mint_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?
            .try_into()
            .map_err(|_| err(-32602, "Invalid mint length"))?;

        let (tx, rx) = oneshot::channel();
        ctx.reader.handle_request(ReadRequest::GetAccountInfo { pubkey: mint_bytes, tx }).await;

        match rx.await {
            Ok(Ok(Some(_account))) => {
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                    "value": {
                        "amount": "0",
                        "decimals": 0,
                        "uiAmount": 0.0,
                        "uiAmountString": "0"
                    },
                    "context": {"slot": ctx.reader.cache().processed_slot()}
                }))
            }
            _ => Err(err(-32602, "Mint not found")),
        }
    })?;

    module.register_async_method("getTokenLargestAccounts", |_params, ctx, _| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "value": [],
            "context": {"slot": ctx.reader.cache().processed_slot()}
        }))
    })?;

    Ok(())
}
