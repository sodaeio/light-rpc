//! tx_index value layout:
//!   [u64 slot LE][u32 tx_index LE][i64 block_time LE][u8 err_len][err][proto]
//! `proto` is SubscribeUpdateTransactionInfo. `err_len` is u8; the human-
//! readable err string is truncated past 255 bytes (does not affect
//! `meta.err` in the response, which decodes from the proto).

use anyhow::{anyhow, Result};
use serde_json::{json, Value};
use yellowstone_grpc_proto::prelude::{
    CompiledInstruction, InnerInstruction, InnerInstructions, MessageAddressTableLookup,
    SubscribeUpdateTransactionInfo, TokenBalance, TransactionError, TransactionStatusMeta,
};
use yellowstone_grpc_proto::prost::Message;

const HEADER_LEN: usize = 21;

pub fn decode_tx_index(bytes: &[u8]) -> Result<Value> {
    // Backfilled values are JSON, starting `{"`. Stream prost values start
    // with slot u64 LE; checking two bytes disambiguates against `{` collisions.
    if bytes.starts_with(b"{\"") {
        return serde_json::from_slice(bytes).map_err(|e| anyhow!("json tx_index: {e}"));
    }

    if bytes.len() < HEADER_LEN {
        return Err(anyhow!("tx_index value too short ({} bytes)", bytes.len()));
    }
    let slot = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let block_time_raw = i64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let err_len = bytes[20] as usize;

    let payload_start = HEADER_LEN + err_len;
    if bytes.len() < payload_start {
        return Err(anyhow!("tx_index err_len exceeds value length"));
    }
    let err_str = if err_len > 0 {
        Some(std::str::from_utf8(&bytes[HEADER_LEN..payload_start])?.to_string())
    } else {
        None
    };

    let info = SubscribeUpdateTransactionInfo::decode(&bytes[payload_start..])?;
    Ok(build_tx_components(
        slot,
        if block_time_raw == 0 {
            None
        } else {
            Some(block_time_raw)
        },
        err_str,
        info,
    )
    .full)
}

/// Decode a raw `SubscribeUpdateTransactionInfo` (no tx_index header) into
/// the `{transaction, meta, version}` shape used by getBlock.
pub fn decode_payload(payload: &[u8], err_str: Option<String>) -> Result<Value> {
    let info = SubscribeUpdateTransactionInfo::decode(payload)?;
    Ok(build_tx_components(0, None, err_str, info).full_no_slot())
}

/// Build the `{transaction, meta, version}` shape from an already-parsed
/// proto. Called at ingest to populate `TransactionEntry::prebuilt`.
pub fn prebuild_tx_value(info: SubscribeUpdateTransactionInfo, err_str: Option<String>) -> Value {
    build_tx_components(0, None, err_str, info).full_no_slot()
}

/// Variant of `prebuild_tx_value` that includes slot + blockTime.
pub fn prebuild_gettx_value(
    info: SubscribeUpdateTransactionInfo,
    slot: u64,
    block_time: Option<i64>,
    err_str: Option<String>,
) -> Value {
    build_tx_components(slot, block_time, err_str, info).full
}

/// All fields the writer + RPC layer need from a single proto decode.
/// `message` and `meta` are JSON strings ready for ClickHouse storage.
pub struct TxComponents {
    pub full: Value,
    pub message: String,
    pub meta: String,
    pub compute_units: u32,
    pub log_messages: Vec<String>,
}

impl TxComponents {
    fn full_no_slot(mut self) -> Value {
        if let Value::Object(ref mut map) = self.full {
            map.remove("slot");
            map.remove("blockTime");
        }
        self.full
    }
}

/// Single-pass builder: produces the agave-shape Value plus the pre-serialized
/// message/meta strings + log_messages and compute_units the CH writer stores.
/// Decoding the proto twice (once here, again for storage) would waste CPU on
/// every tx, so callers should produce both products from this one call.
pub fn build_tx_components(
    slot: u64,
    block_time: Option<i64>,
    err_str: Option<String>,
    info: SubscribeUpdateTransactionInfo,
) -> TxComponents {
    let tx = info.transaction.unwrap_or_default();
    let meta = info.meta.unwrap_or_default();

    let signatures: Vec<String> = tx
        .signatures
        .iter()
        .map(|s| bs58::encode(s).into_string())
        .collect();

    let versioned = tx.message.as_ref().map(|m| m.versioned).unwrap_or(false);
    let message_json = if let Some(msg) = tx.message {
        let account_keys: Vec<String> = msg
            .account_keys
            .iter()
            .map(|k| bs58::encode(k).into_string())
            .collect();
        let recent_blockhash = bs58::encode(&msg.recent_blockhash).into_string();
        let header = msg.header.unwrap_or_default();
        let instructions: Vec<Value> = msg.instructions.iter().map(compiled_ix_json).collect();
        let mut message = json!({
            "header": {
                "numRequiredSignatures": header.num_required_signatures,
                "numReadonlySignedAccounts": header.num_readonly_signed_accounts,
                "numReadonlyUnsignedAccounts": header.num_readonly_unsigned_accounts,
            },
            "accountKeys": account_keys,
            "recentBlockhash": recent_blockhash,
            "instructions": instructions,
        });
        if msg.versioned && !msg.address_table_lookups.is_empty() {
            let atl: Vec<Value> = msg.address_table_lookups.iter().map(atl_json).collect();
            message["addressTableLookups"] = json!(atl);
        }
        message
    } else {
        json!({})
    };

    let compute_units = meta.compute_units_consumed.unwrap_or(0) as u32;
    let log_messages = if meta.log_messages_none {
        Vec::new()
    } else {
        meta.log_messages.clone()
    };
    let meta_value = meta_json(&meta, err_str);

    let message_str = serde_json::to_string(&message_json).unwrap_or_default();
    let meta_str = serde_json::to_string(&meta_value).unwrap_or_default();

    let version = if versioned { json!(0) } else { json!("legacy") };
    let full = json!({
        "slot": slot,
        "blockTime": block_time,
        "version": version,
        "transaction": {
            "signatures": signatures,
            "message": message_json,
        },
        "meta": meta_value,
    });

    TxComponents {
        full,
        message: message_str,
        meta: meta_str,
        compute_units,
        log_messages,
    }
}

fn compiled_ix_json(ix: &CompiledInstruction) -> Value {
    json!({
        "programIdIndex": ix.program_id_index,
        "accounts": ix.accounts.to_vec(),
        "data": bs58::encode(&ix.data).into_string(),
    })
}

fn inner_ix_json(ix: &InnerInstruction) -> Value {
    let mut v = json!({
        "programIdIndex": ix.program_id_index,
        "accounts": ix.accounts.to_vec(),
        "data": bs58::encode(&ix.data).into_string(),
    });
    if let Some(sh) = ix.stack_height {
        v["stackHeight"] = json!(sh);
    }
    v
}

fn atl_json(l: &MessageAddressTableLookup) -> Value {
    json!({
        "accountKey": bs58::encode(&l.account_key).into_string(),
        "writableIndexes": l.writable_indexes.to_vec(),
        "readonlyIndexes": l.readonly_indexes.to_vec(),
    })
}

fn token_balance_json(b: &TokenBalance) -> Value {
    let ui = b.ui_token_amount.clone().unwrap_or_default();
    json!({
        "accountIndex": b.account_index,
        "mint": b.mint,
        "owner": b.owner,
        "programId": b.program_id,
        "uiTokenAmount": {
            "amount": ui.amount,
            "decimals": ui.decimals,
            "uiAmount": ui.ui_amount,
            "uiAmountString": ui.ui_amount_string,
        },
    })
}

fn inner_ixs_json(groups: &[InnerInstructions]) -> Value {
    let v: Vec<Value> = groups
        .iter()
        .map(|g| {
            json!({
                "index": g.index,
                "instructions": g.instructions.iter().map(inner_ix_json).collect::<Vec<_>>(),
            })
        })
        .collect();
    json!(v)
}

fn meta_json(meta: &TransactionStatusMeta, err_str: Option<String>) -> Value {
    // proto.err is bincode(TransactionError); re-encode to agave JSON.
    let err = match (&meta.err, err_str.as_ref()) {
        (Some(TransactionError { err }), _) if !err.is_empty() => {
            match bincode::deserialize::<solana_transaction_error::TransactionError>(err) {
                Ok(te) => serde_json::to_value(&te).unwrap_or(Value::Null),
                Err(_) => err_str.as_ref().map(|s| json!(s)).unwrap_or(Value::Null),
            }
        }
        (_, Some(s)) => json!(s),
        _ => Value::Null,
    };

    let pre_token: Vec<Value> = meta
        .pre_token_balances
        .iter()
        .map(token_balance_json)
        .collect();
    let post_token: Vec<Value> = meta
        .post_token_balances
        .iter()
        .map(token_balance_json)
        .collect();
    let loaded = json!({
        "writable": meta
            .loaded_writable_addresses
            .iter()
            .map(|a| bs58::encode(a).into_string())
            .collect::<Vec<_>>(),
        "readonly": meta
            .loaded_readonly_addresses
            .iter()
            .map(|a| bs58::encode(a).into_string())
            .collect::<Vec<_>>(),
    });

    let mut v = json!({
        "err": err,
        "fee": meta.fee,
        "preBalances": meta.pre_balances,
        "postBalances": meta.post_balances,
        "logMessages": if meta.log_messages_none { Value::Null } else { json!(meta.log_messages) },
        "innerInstructions": if meta.inner_instructions_none {
            Value::Null
        } else {
            inner_ixs_json(&meta.inner_instructions)
        },
        "preTokenBalances": pre_token,
        "postTokenBalances": post_token,
        "rewards": Value::Array(Vec::new()),
        "loadedAddresses": loaded,
        "status": if err.is_null() { json!({"Ok": Value::Null}) } else { json!({"Err": err.clone()}) },
    });
    if let Some(cu) = meta.compute_units_consumed {
        v["computeUnitsConsumed"] = json!(cu);
    }
    if meta.return_data_none {
        v["returnData"] = Value::Null;
    } else if let Some(rd) = &meta.return_data {
        v["returnData"] = json!({
            "programId": bs58::encode(&rd.program_id).into_string(),
            "data": [bs58::encode(&rd.data).into_string(), "base58"],
        });
    }
    v
}
