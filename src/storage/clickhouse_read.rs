//! ClickHouse read fallback for historical queries.
//!
//! When RocksDB misses (data pruned beyond retention window), these
//! functions query ClickHouse. All behind `#[cfg(feature = "clickhouse")]`.
//!
//! Tables used:
//!   transactions    — getTransaction, getBlock
//!   gsfa_mv         — getSignaturesForAddress
//!   gsfa_hot_mv     — getSignaturesForAddress (recent, faster)
//!   sig_status_mv   — getSignatureStatuses

use anyhow::Result;
use clickhouse::Client;
use serde::Deserialize;

use crate::types::Slot;

#[derive(Debug, Deserialize, clickhouse::Row)]
pub struct ChTransaction {
    pub slot: u64,
    pub block_time: i64,
    pub err: bool,
    pub fee: u64,
    pub message: String,
    pub meta: String,
}

#[derive(Debug, Deserialize, clickhouse::Row)]
pub struct ChSignatureEntry {
    pub slot: u64,
    pub tx_index: u32,
    pub block_time: i64,
    pub err: bool,
}

#[derive(Debug, Deserialize, clickhouse::Row)]
pub struct ChSlot {
    pub slot: u64,
}

/// Fetch a transaction by signature from ClickHouse.
pub async fn get_transaction(
    client: &Client,
    signature: &[u8; 64],
) -> Result<Option<serde_json::Value>> {
    let sig_b58 = bs58::encode(signature).into_string();
    let rows: Vec<ChTransaction> = client
        .query(
            "SELECT slot, block_time, err, fee, message, meta \
             FROM transactions WHERE signature = unhex(?) LIMIT 1",
        )
        .bind(hex::encode(signature))
        .fetch_all()
        .await?;

    let Some(row) = rows.into_iter().next() else {
        return Ok(None);
    };

    let meta_val: serde_json::Value = serde_json::from_str(&row.meta).unwrap_or_default();
    let msg_val: serde_json::Value = serde_json::from_str(&row.message).unwrap_or_default();

    Ok(Some(serde_json::json!({
        "slot": row.slot,
        "blockTime": row.block_time,
        "transaction": {
            "signatures": [sig_b58],
            "message": msg_val,
        },
        "meta": meta_val,
        "version": "legacy",
    })))
}

/// Fetch signatures for an address from ClickHouse gsfa_mv.
pub async fn get_signatures_for_address(
    client: &Client,
    address: &[u8; 32],
    before_slot: Option<Slot>,
    limit: usize,
) -> Result<Vec<ChSignatureEntry>> {
    let query = if let Some(before) = before_slot {
        format!(
            "SELECT slot, tx_index, block_time, err \
             FROM gsfa_mv WHERE address = unhex(?) AND slot < {} \
             ORDER BY slot DESC, tx_index DESC LIMIT {}",
            before, limit
        )
    } else {
        format!(
            "SELECT slot, tx_index, block_time, err \
             FROM gsfa_mv WHERE address = unhex(?) \
             ORDER BY slot DESC, tx_index DESC LIMIT {}",
            limit
        )
    };

    let rows: Vec<ChSignatureEntry> = client
        .query(&query)
        .bind(hex::encode(address))
        .fetch_all()
        .await?;

    Ok(rows)
}

/// Fetch block time for a slot from ClickHouse.
pub async fn get_block_time(client: &Client, slot: Slot) -> Result<Option<i64>> {
    let rows: Vec<ChTransaction> = client
        .query(
            "SELECT slot, block_time, err, fee, message, meta \
                FROM transactions WHERE slot = ? LIMIT 1",
        )
        .bind(slot)
        .fetch_all()
        .await?;

    Ok(rows.into_iter().next().map(|r| r.block_time))
}

/// Fetch slots that have transactions in a range.
pub async fn get_blocks_in_range(client: &Client, start: Slot, end: Slot) -> Result<Vec<Slot>> {
    let rows: Vec<ChSlot> = client
        .query(
            "SELECT DISTINCT slot FROM transactions \
             WHERE slot >= ? AND slot <= ? ORDER BY slot",
        )
        .bind(start)
        .bind(end)
        .fetch_all()
        .await?;

    Ok(rows.into_iter().map(|r| r.slot).collect())
}
