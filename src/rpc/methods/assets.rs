use anyhow::Result;
use jsonrpsee::RpcModule;

use crate::rpc::server::RpcContext;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getAsset", |params, ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let id_str = p
            .get("id")
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Missing 'id'"))?;
        let id_bytes = bs58::decode(id_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid id encoding"))?;

        match ctx.reader.get_asset(&id_bytes).await {
            Ok(Some(asset)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(asset),
            Ok(None) => Err(err(-32602, "Asset not found")),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getAssetsByOwner", |params, ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let owner_str = p
            .get("ownerAddress")
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Missing 'ownerAddress'"))?;
        let owner_bytes = bs58::decode(owner_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?;
        let page = p.get("page").and_then(|v| v.as_i64()).unwrap_or(1);
        let limit = p.get("limit").and_then(|v| v.as_i64()).unwrap_or(1000);

        match ctx
            .reader
            .get_assets_by_owner(&owner_bytes, page, limit)
            .await
        {
            Ok(result) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(result),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getAssetsByCreator", |params, ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let creator_str = p
            .get("creatorAddress")
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Missing 'creatorAddress'"))?;
        let creator_bytes = bs58::decode(creator_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?;
        let page = p.get("page").and_then(|v| v.as_i64()).unwrap_or(1);
        let limit = p.get("limit").and_then(|v| v.as_i64()).unwrap_or(1000);

        match ctx
            .reader
            .get_assets_by_creator(&creator_bytes, page, limit)
            .await
        {
            Ok(result) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(result),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getAssetsByGroup", |params, ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let group_key = p
            .get("groupKey")
            .and_then(|v| v.as_str())
            .unwrap_or("collection");
        let group_value = p
            .get("groupValue")
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Missing 'groupValue'"))?;
        let page = p.get("page").and_then(|v| v.as_i64()).unwrap_or(1);
        let limit = p.get("limit").and_then(|v| v.as_i64()).unwrap_or(1000);

        match ctx
            .reader
            .get_assets_by_group(group_key, group_value, page, limit)
            .await
        {
            Ok(result) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(result),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getAssetsByAuthority", |params, ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let authority_str = p
            .get("authorityAddress")
            .and_then(|v| v.as_str())
            .ok_or_else(|| err(-32602, "Missing 'authorityAddress'"))?;
        let authority_bytes = bs58::decode(authority_str)
            .into_vec()
            .map_err(|_| err(-32602, "Invalid encoding"))?;
        let page = p.get("page").and_then(|v| v.as_i64()).unwrap_or(1);
        let limit = p.get("limit").and_then(|v| v.as_i64()).unwrap_or(1000);

        match ctx
            .reader
            .get_assets_by_authority(&authority_bytes, page, limit)
            .await
        {
            Ok(result) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(result),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("searchAssets", |params, ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let page = p.get("page").and_then(|v| v.as_i64()).unwrap_or(1);
        let limit = p.get("limit").and_then(|v| v.as_i64()).unwrap_or(1000);

        if let Some(owner) = p.get("ownerAddress").and_then(|v| v.as_str()) {
            let owner_bytes = bs58::decode(owner)
                .into_vec()
                .map_err(|_| err(-32602, "Invalid encoding"))?;
            match ctx
                .reader
                .get_assets_by_owner(&owner_bytes, page, limit)
                .await
            {
                Ok(result) => return Ok::<_, jsonrpsee::types::ErrorObjectOwned>(result),
                Err(e) => return Err(err(-32603, &e.to_string())),
            }
        }

        if let Some(creator) = p.get("creatorAddress").and_then(|v| v.as_str()) {
            let creator_bytes = bs58::decode(creator)
                .into_vec()
                .map_err(|_| err(-32602, "Invalid encoding"))?;
            match ctx
                .reader
                .get_assets_by_creator(&creator_bytes, page, limit)
                .await
            {
                Ok(result) => return Ok::<_, jsonrpsee::types::ErrorObjectOwned>(result),
                Err(e) => return Err(err(-32603, &e.to_string())),
            }
        }

        Ok(serde_json::json!({ "total": 0, "limit": limit, "page": page, "items": [] }))
    })?;

    module.register_async_method("getAssetProof", |params, ctx, _| async move {
        let p: serde_json::Value = params.parse()?;
        let id_str = p.get("id").and_then(|v| v.as_str()).ok_or_else(|| err(-32602, "Missing 'id'"))?;
        let id_bytes = bs58::decode(id_str).into_vec().map_err(|_| err(-32602, "Invalid id encoding"))?;

        let asset = ctx.reader.pg().get_asset(&id_bytes).await
            .map_err(|e| err(-32603, &e.to_string()))?;

        match asset {
            Some(a) if a.compressed => {
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
                    "root": "",
                    "proof": [],
                    "node_index": a.nonce.unwrap_or(0),
                    "leaf": a.leaf.as_ref().map(|l| bs58::encode(l).into_string()).unwrap_or_default(),
                    "tree_id": a.tree_id.as_ref().map(|t| bs58::encode(t).into_string()).unwrap_or_default(),
                }))
            }
            Some(_) => Err(err(-32602, "Asset is not compressed")),
            None => Err(err(-32602, "Asset not found")),
        }
    })?;

    Ok(())
}
