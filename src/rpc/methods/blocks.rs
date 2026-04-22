use std::sync::Arc;

use anyhow::Result;
use jsonrpsee::types::Params;
use jsonrpsee::RpcModule;

use super::rpc_response;
use crate::rpc::server::RpcContext;
use crate::types::*;

fn err(code: i32, msg: &str) -> jsonrpsee::types::ErrorObjectOwned {
    jsonrpsee::types::ErrorObject::owned(code, msg.to_string(), None::<()>)
}

/// Build the getBlock response as raw JSON bytes and insert into the shard
/// cache. Returns None when the slot is missing. Called exactly once per
/// (slot, cfg_hash) under the block coalescer.
fn build_block_raw(
    reader: &crate::storage::read::StorageReader,
    slot: Slot,
    cfg_hash: u64,
    tx_details: &str,
    include_rewards: bool,
    shard: usize,
    block_cache: &crate::rpc::server::BlockResponseCache,
) -> Option<crate::rpc::server::BlockCacheEntry> {
    let block = reader.get_block(slot).ok().flatten()?;

    let per_tx = if tx_details == "full" { 1536 } else { 96 };
    let mut body = String::with_capacity(32 + block.transactions.len() * per_tx);
    body.push_str(r#"{"blockhash":""#);
    body.push_str(&block.info.blockhash);
    body.push_str(r#"","previousBlockhash":null,"parentSlot":"#);
    body.push_str(&block.info.parent_slot.to_string());
    body.push_str(r#","blockTime":"#);
    match block.info.block_time {
        Some(t) => body.push_str(&t.to_string()),
        None => body.push_str("null"),
    }
    body.push_str(r#","blockHeight":"#);
    match block.info.block_height {
        Some(h) => body.push_str(&h.to_string()),
        None => body.push_str("null"),
    }
    body.push_str(r#","rewards":"#);
    body.push_str(if include_rewards { "[]" } else { "null" });
    match tx_details {
        "none" => {
            body.push_str(r#","transactions":[]"#);
        }
        "signatures" => {
            body.push_str(r#","signatures":["#);
            for (i, t) in block.transactions.iter().enumerate() {
                if i > 0 {
                    body.push(',');
                }
                body.push('"');
                body.push_str(&t.signature.to_string());
                body.push('"');
            }
            body.push(']');
        }
        _ => {
            body.push_str(r#","transactions":["#);
            for (i, t) in block.transactions.iter().enumerate() {
                if i > 0 {
                    body.push(',');
                }
                body.push_str(t.prebuilt.get());
            }
            body.push(']');
        }
    }
    body.push('}');

    let raw: Box<serde_json::value::RawValue> =
        serde_json::value::RawValue::from_string(body).ok()?;
    let entry: crate::rpc::server::BlockCacheEntry = Arc::new(raw);
    block_cache[shard]
        .lock()
        .put((slot, cfg_hash), Arc::clone(&entry));
    Some(entry)
}

pub fn register(module: &mut RpcModule<RpcContext>) -> Result<()> {
    module.register_async_method("getBlock", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let slot: Slot = p
            .first()
            .and_then(|v| v.as_u64())
            .ok_or_else(|| err(-32602, "Invalid slot"))?;
        let cfg = p.get(1).and_then(|v| v.as_object());
        let tx_details = cfg
            .and_then(|o| o.get("transactionDetails"))
            .and_then(|v| v.as_str())
            .unwrap_or("full");
        let include_rewards = cfg
            .and_then(|o| o.get("rewards"))
            .and_then(|v| v.as_bool())
            .unwrap_or(true);

        // Config fingerprint: (tx_details, include_rewards) fold into a u64.
        // Common configs are ~10 combinations, so a tiny hash suffices.
        let cfg_hash: u64 = {
            let mut h: u64 = include_rewards as u64;
            for b in tx_details.as_bytes() {
                h = h.wrapping_mul(31).wrapping_add(*b as u64);
            }
            h
        };

        let shard = crate::rpc::server::block_cache_shard(slot, cfg_hash);
        if let Some(cached) = ctx.block_cache[shard]
            .lock()
            .get(&(slot, cfg_hash))
            .cloned()
        {
            return Ok::<crate::rpc::server::BlockCacheEntry, jsonrpsee::types::ErrorObjectOwned>(
                cached,
            );
        }

        // Singleflight on (slot, cfg_hash) — N concurrent first-time callers
        // share one build pass.
        let reader = std::sync::Arc::clone(&ctx.reader);
        let block_cache = std::sync::Arc::clone(&ctx.block_cache);
        let tx_details_owned = tx_details.to_string();
        let built = ctx
            .block_coalescer
            .run((slot, cfg_hash), move || async move {
                build_block_raw(
                    &reader,
                    slot,
                    cfg_hash,
                    &tx_details_owned,
                    include_rewards,
                    shard,
                    &block_cache,
                )
            })
            .await;
        match (*built).clone() {
            Some(entry) => {
                Ok::<crate::rpc::server::BlockCacheEntry, jsonrpsee::types::ErrorObjectOwned>(entry)
            }
            None => Err(err(
                -32009,
                "Slot was skipped, or missing in long-term storage",
            )),
        }
    })?;

    module.register_async_method("getBlockHeight", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        match ctx.reader.get_block_height(commitment) {
            Ok(Some(h)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(h)),
            Ok(None) => Ok(serde_json::json!(0)),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    module.register_async_method("getSlot", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(ctx
            .reader
            .get_slot(commitment)))
    })?;

    module.register_async_method("getLatestBlockhash", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        let slot = ctx.reader.get_slot(commitment);
        match ctx.reader.get_latest_blockhash(commitment) {
            Some((blockhash, bh_slot)) => {
                let last_valid = match ctx.reader.get_block_height(commitment) {
                    Ok(Some(h)) => h + 150,
                    _ => bh_slot + 150,
                };
                Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(
                    slot,
                    serde_json::json!({
                        "blockhash": blockhash,
                        "lastValidBlockHeight": last_valid
                    }),
                ))
            }
            None => Err(err(-32603, "No blockhash available")),
        }
    })?;

    module.register_async_method("isBlockhashValid", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let blockhash = p.first().and_then(|v| v.as_str()).unwrap_or("").to_string();
        let slot = ctx.reader.cache().processed_slot();
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(rpc_response(
            slot,
            serde_json::json!(ctx.reader.is_blockhash_valid(&blockhash)),
        ))
    })?;

    module.register_async_method("getBlockTime", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let slot: Slot = p
            .first()
            .and_then(|v| v.as_u64())
            .ok_or_else(|| err(-32602, "Invalid slot"))?;
        match ctx.reader.get_block_time(slot).await {
            Ok(Some(t)) => Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(t)),
            Ok(None) => Ok(serde_json::Value::Null),
            Err(e) => Err(err(-32603, &e.to_string())),
        }
    })?;

    // Pre-encoded static JSON — jsonrpsee emits verbatim.
    static VERSION_RAW: std::sync::LazyLock<Box<serde_json::value::RawValue>> =
        std::sync::LazyLock::new(|| {
            serde_json::value::RawValue::from_string(format!(
                r#"{{"solana-core":"2.2.4","feature-set":0,"light-rpc":"{}"}}"#,
                env!("CARGO_PKG_VERSION"),
            ))
            .unwrap()
        });
    module.register_async_method("getVersion", |_, _, _| async move {
        Ok::<Box<serde_json::value::RawValue>, jsonrpsee::types::ErrorObjectOwned>(
            VERSION_RAW.clone(),
        )
    })?;

    // --- Trivial methods for Solana RPC parity ---

    // Mainnet genesis hash is a fixed constant.
    static GENESIS_HASH: &str = "5eykt4UsFv8P8NJdTREpY1vzqKqZKvdpKuc147dw2N9d";
    module.register_async_method("getGenesisHash", |_, _, _| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(GENESIS_HASH))
    })?;

    module.register_async_method(
        "getMinimumBalanceForRentExemption",
        |params, _, _| async move {
            let p: Vec<serde_json::Value> = params.parse()?;
            let data_len = p.first().and_then(|v| v.as_u64()).unwrap_or(0);
            let lamports = (128 + data_len) * 6960;
            Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(lamports))
        },
    )?;

    module.register_async_method("getEpochInfo", |params, ctx, _| async move {
        let commitment = parse_commitment(&params);
        let slot = ctx.reader.get_slot(commitment);
        // Mainnet epoch schedule: 432,000 slots per epoch
        let slots_per_epoch: u64 = 432_000;
        let epoch = slot / slots_per_epoch;
        let slot_index = slot % slots_per_epoch;
        let block_height = ctx
            .reader
            .get_block_height(commitment)
            .ok()
            .flatten()
            .unwrap_or(slot);
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "epoch": epoch,
            "slotIndex": slot_index,
            "slotsInEpoch": slots_per_epoch,
            "absoluteSlot": slot,
            "blockHeight": block_height,
            "transactionCount": null,
        }))
    })?;

    module.register_async_method("getEpochSchedule", |_, _, _| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "slotsPerEpoch": 432_000,
            "leaderScheduleSlotOffset": 432_000,
            "warmup": false,
            "firstNormalEpoch": 0,
            "firstNormalSlot": 0,
        }))
    })?;

    module.register_async_method("getHealth", |_, ctx, _| async move {
        let age = ctx.reader.cache().finalized_slot_age_secs();
        if age < 120 {
            Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!("ok"))
        } else {
            Err(err(-32005, &format!("Node is behind by {age} seconds")))
        }
    })?;

    module.register_async_method("getIdentity", |_, _, _| async move {
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!({
            "identity": "light-rpc"
        }))
    })?;

    module.register_async_method("getFirstAvailableBlock", |_, ctx, _| async move {
        let first = ctx.reader.get_first_available_slot();
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(first))
    })?;

    module.register_async_method("getBlocks", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let start: Slot = p
            .first()
            .and_then(|v| v.as_u64())
            .ok_or_else(|| err(-32602, "Invalid start slot"))?;
        let end: Slot = p
            .get(1)
            .and_then(|v| v.as_u64())
            .unwrap_or_else(|| ctx.reader.cache().finalized_slot());
        if end.saturating_sub(start) > 500_000 {
            return Err(err(-32602, "Slot range too large (max 500,000)"));
        }
        let slots = ctx
            .reader
            .get_blocks_in_range(start, end)
            .await
            .map_err(|e| err(-32603, &e.to_string()))?;
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(slots))
    })?;

    module.register_async_method("getBlocksWithLimit", |params, ctx, _| async move {
        let p: Vec<serde_json::Value> = params.parse()?;
        let start: Slot = p
            .first()
            .and_then(|v| v.as_u64())
            .ok_or_else(|| err(-32602, "Invalid start slot"))?;
        let limit = p
            .get(1)
            .and_then(|v| v.as_u64())
            .unwrap_or(500_000)
            .min(500_000);
        let end = start + limit;
        let slots = ctx
            .reader
            .get_blocks_in_range(start, end)
            .await
            .map_err(|e| err(-32603, &e.to_string()))?;
        Ok::<_, jsonrpsee::types::ErrorObjectOwned>(serde_json::json!(slots))
    })?;

    Ok(())
}

fn parse_commitment(params: &Params) -> Commitment {
    if let Ok(p) = params.parse::<Vec<serde_json::Value>>() {
        if let Some(obj) = p.first().and_then(|v| v.as_object()) {
            if let Some(c) = obj.get("commitment").and_then(|v| v.as_str()) {
                return match c {
                    "processed" => Commitment::Processed,
                    "confirmed" => Commitment::Confirmed,
                    _ => Commitment::Finalized,
                };
            }
        }
    }
    Commitment::Finalized
}
