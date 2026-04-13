use anyhow::Result;
use jsonrpsee::RpcModule;

use crate::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getAsset", |params, _ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let id = p.get("id").and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Missing 'id' parameter"))?;

        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "interface": "V1_NFT",
            "id": id,
            "content": null,
            "authorities": [],
            "compression": {"compressed": false},
            "grouping": [],
            "royalty": {},
            "creators": [],
            "ownership": {},
            "supply": null,
            "mutable": true,
        }))
    })?;

    module.register_async_method("getAssetsByOwner", |params, _ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let _owner = p.get("ownerAddress").and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Missing 'ownerAddress' parameter"))?;
        let page = p.get("page").and_then(|v| v.as_u64()).unwrap_or(1);
        let limit = p.get("limit").and_then(|v| v.as_u64()).unwrap_or(1000);

        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "total": 0, "limit": limit, "page": page, "items": []
        }))
    })?;

    module.register_async_method("getAssetsByCreator", |params, _ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let _creator = p.get("creatorAddress").and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Missing 'creatorAddress' parameter"))?;

        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "total": 0, "limit": 1000, "page": 1, "items": []
        }))
    })?;

    module.register_async_method("getAssetsByGroup", |params, _ctx, _| async move {
        let _p: serde_json::Value = params.parse()?;
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "total": 0, "limit": 1000, "page": 1, "items": []
        }))
    })?;

    module.register_async_method("getAssetsByAuthority", |params, _ctx, _| async move {
        let _p: serde_json::Value = params.parse()?;
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "total": 0, "limit": 1000, "page": 1, "items": []
        }))
    })?;

    module.register_async_method("searchAssets", |params, _ctx, _| async move {
        let _p: serde_json::Value = params.parse()?;
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "total": 0, "limit": 1000, "page": 1, "items": []
        }))
    })?;

    module.register_async_method("getAssetProof", |params, _ctx, _| async move {
        let _p: serde_json::Value = params.parse()?;
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "root": "", "proof": [], "node_index": 0, "leaf": "", "tree_id": ""
        }))
    })?;

    Ok(())
}
