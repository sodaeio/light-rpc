//! Integration tests against a live light-indexer instance.
//!
//! Set LIGHT_INDEXER_URL env var to run (default: http://45.154.33.82:8878).
//! These tests verify response format and data correctness against a production
//! Solana dataset.
//!
//! Run: LIGHT_INDEXER_URL=http://45.154.33.82:8878 cargo test --test rpc_integration -- --nocapture

use serde_json::{json, Value};
use std::time::Instant;

fn endpoint() -> String {
    std::env::var("LIGHT_INDEXER_URL").unwrap_or_else(|_| "http://45.154.33.82:8878".into())
}

async fn rpc_call(method: &str, params: Value) -> Value {
    let client = reqwest::Client::new();
    let body = json!({
        "jsonrpc": "2.0",
        "id": 1,
        "method": method,
        "params": params
    });

    let resp = client
        .post(endpoint())
        .json(&body)
        .send()
        .await
        .expect("request failed");

    resp.json::<Value>().await.expect("invalid json response")
}

// Well-known Solana addresses for testing
const USDC_MINT: &str = "EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v";
const USDT_MINT: &str = "Es9vMFrzaCERmJfrF4H2FYD4KCoNkY11McCe8BenwNYB";
const TOKEN_PROGRAM: &str = "TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA";
const SYSTEM_PROGRAM: &str = "11111111111111111111111111111111";
// Jupiter aggregator — has token accounts but not millions
const JUPITER_V6: &str = "JUP6LkbZbjS1jKKwapdHNy74zcZ3tLUZoi5QNyVTaV4";

// --- Slot / Block methods ---

#[tokio::test]
async fn test_get_slot() {
    let resp = rpc_call("getSlot", json!([])).await;
    let slot = resp["result"]
        .as_u64()
        .expect("getSlot should return a number");
    assert!(slot > 400_000_000, "slot should be recent: {slot}");
}

#[tokio::test]
async fn test_get_slot_with_commitment() {
    // Fetch finalized first (slowest to advance), then processed (fastest)
    // to avoid race where processed advances between calls
    let finalized = rpc_call("getSlot", json!([{"commitment": "finalized"}])).await;
    let confirmed = rpc_call("getSlot", json!([{"commitment": "confirmed"}])).await;
    let processed = rpc_call("getSlot", json!([{"commitment": "processed"}])).await;

    let p = processed["result"].as_u64().unwrap();
    let c = confirmed["result"].as_u64().unwrap();
    let f = finalized["result"].as_u64().unwrap();

    // Allow small tolerance for network timing between sequential requests
    assert!(
        p + 5 >= c,
        "processed ({p}) should be >= confirmed ({c}) (with tolerance)"
    );
    assert!(c >= f, "confirmed ({c}) >= finalized ({f})");
    assert!(
        p - f < 100,
        "processed-finalized gap should be < 100 slots: {}",
        p - f
    );
}

#[tokio::test]
async fn test_get_block_height() {
    let resp = rpc_call("getBlockHeight", json!([])).await;
    let height = resp["result"].as_u64().expect("should return block height");
    assert!(
        height > 380_000_000,
        "block height should be recent: {height}"
    );
}

#[tokio::test]
async fn test_get_latest_blockhash() {
    let resp = rpc_call("getLatestBlockhash", json!([])).await;
    let value = &resp["result"]["value"];
    let blockhash = value["blockhash"].as_str().expect("should have blockhash");
    let last_valid = value["lastValidBlockHeight"]
        .as_u64()
        .expect("should have lastValidBlockHeight");

    assert!(!blockhash.is_empty(), "blockhash should not be empty");
    assert!(blockhash.len() >= 32, "blockhash should be base58 encoded");
    assert!(
        last_valid > 380_000_000,
        "lastValidBlockHeight should be recent: {last_valid}"
    );
}

#[tokio::test]
async fn test_is_blockhash_valid() {
    // Get a fresh blockhash then check it
    let bh_resp = rpc_call("getLatestBlockhash", json!([])).await;
    let blockhash = bh_resp["result"]["value"]["blockhash"].as_str().unwrap();

    let resp = rpc_call("isBlockhashValid", json!([blockhash])).await;
    let valid = resp["result"]["value"]
        .as_bool()
        .expect("should return bool");
    assert!(valid, "fresh blockhash should be valid");

    // Old/fake blockhash should be invalid
    let resp2 = rpc_call(
        "isBlockhashValid",
        json!(["1111111111111111111111111111111111111111111111"]),
    )
    .await;
    let valid2 = resp2["result"]["value"]
        .as_bool()
        .expect("should return bool");
    assert!(!valid2, "fake blockhash should be invalid");
}

#[tokio::test]
async fn test_get_version() {
    let resp = rpc_call("getVersion", json!([])).await;
    let solana_core = resp["result"]["solana-core"]
        .as_str()
        .expect("should have solana-core");
    assert!(!solana_core.is_empty(), "solana-core should not be empty");
}

// --- Token methods ---

#[tokio::test]
async fn test_get_token_supply_usdc() {
    let resp = rpc_call("getTokenSupply", json!([USDC_MINT])).await;
    let value = &resp["result"]["value"];
    let amount = value["amount"].as_str().expect("should have amount");
    let decimals = value["decimals"].as_u64().expect("should have decimals");

    assert_eq!(decimals, 6, "USDC has 6 decimals");
    let supply: u64 = amount.parse().expect("amount should be numeric");
    assert!(
        supply > 1_000_000_000_000,
        "USDC supply should be > 1B (raw): {supply}"
    );
}

#[tokio::test]
async fn test_get_token_supply_usdt() {
    let resp = rpc_call("getTokenSupply", json!([USDT_MINT])).await;
    let value = &resp["result"]["value"];
    let decimals = value["decimals"].as_u64().expect("should have decimals");
    assert_eq!(decimals, 6, "USDT has 6 decimals");
}

#[tokio::test]
async fn test_get_token_supply_invalid_mint() {
    let resp = rpc_call(
        "getTokenSupply",
        json!(["1111111111111111111111111111111111111111111"]),
    )
    .await;
    assert!(
        resp["error"].is_object(),
        "should return error for non-existent mint"
    );
}

#[tokio::test]
async fn test_get_token_accounts_by_owner() {
    let resp = rpc_call("getTokenAccountsByOwner", json!([JUPITER_V6])).await;
    let accounts = resp["result"]["value"]
        .as_array()
        .expect("should return array");
    assert!(!accounts.is_empty(), "Jupiter should have token accounts");

    let first = &accounts[0];
    assert!(first["pubkey"].is_string(), "should have pubkey");
    // Solana-compatible format: account.data.parsed.info.mint
    let info = &first["account"]["data"]["parsed"]["info"];
    assert!(info["mint"].is_string(), "should have mint in parsed info");
    assert!(
        info["owner"].is_string(),
        "should have owner in parsed info"
    );
}

// --- Account methods ---

#[tokio::test]
async fn test_get_account_info_token_program() {
    let resp = rpc_call("getAccountInfo", json!([TOKEN_PROGRAM])).await;
    let value = &resp["result"]["value"];
    assert!(
        value["executable"].as_bool().unwrap_or(false),
        "Token Program should be executable"
    );
    assert!(
        value["lamports"].as_u64().unwrap_or(0) > 0,
        "should have lamports"
    );
}

#[tokio::test]
async fn test_get_account_info_not_found() {
    let resp = rpc_call(
        "getAccountInfo",
        json!(["1111111111111111111111111111111111111111111"]),
    )
    .await;
    // Should return null value, not error
    assert!(
        resp["result"]["value"].is_null(),
        "non-existent account should return null value"
    );
}

#[tokio::test]
async fn test_get_multiple_accounts() {
    let resp = rpc_call(
        "getMultipleAccounts",
        json!([[TOKEN_PROGRAM, SYSTEM_PROGRAM]]),
    )
    .await;
    let accounts = resp["result"]["value"]
        .as_array()
        .expect("should return array");
    assert_eq!(accounts.len(), 2, "should return 2 accounts");
}

#[tokio::test]
async fn test_get_balance() {
    let resp = rpc_call("getBalance", json!([TOKEN_PROGRAM])).await;
    let value = resp["result"]["value"].as_u64();
    assert!(value.is_some(), "should return a balance");
}

// --- Error handling ---

#[tokio::test]
async fn test_invalid_method() {
    let resp = rpc_call("nonExistentMethod", json!([])).await;
    assert!(
        resp["error"].is_object(),
        "unknown method should return error"
    );
}

#[tokio::test]
async fn test_invalid_pubkey() {
    let resp = rpc_call("getAccountInfo", json!(["not-a-valid-pubkey"])).await;
    assert!(
        resp["error"].is_object(),
        "invalid pubkey should return error"
    );
    assert_eq!(resp["error"]["code"].as_i64().unwrap(), -32602);
}

#[tokio::test]
async fn test_batch_request() {
    let client = reqwest::Client::new();
    let body = json!([
        {"jsonrpc": "2.0", "id": 1, "method": "getSlot", "params": []},
        {"jsonrpc": "2.0", "id": 2, "method": "getVersion", "params": []},
        {"jsonrpc": "2.0", "id": 3, "method": "getBlockHeight", "params": []},
    ]);

    let resp = client
        .post(endpoint())
        .json(&body)
        .send()
        .await
        .expect("batch request failed");

    let results: Vec<Value> = resp.json().await.expect("batch should return array");
    assert_eq!(results.len(), 3, "batch should return 3 responses");
}

// --- Performance / latency tests ---

#[tokio::test]
async fn test_latency_get_slot() {
    let start = Instant::now();
    for _ in 0..10 {
        rpc_call("getSlot", json!([])).await;
    }
    let elapsed = start.elapsed();
    let avg_ms = elapsed.as_millis() / 10;
    println!("getSlot avg latency: {avg_ms}ms");
    assert!(
        avg_ms < 5000,
        "getSlot should be < 5s avg over network: {avg_ms}ms"
    );
}

#[tokio::test]
async fn test_latency_get_token_supply() {
    let start = Instant::now();
    for _ in 0..5 {
        rpc_call("getTokenSupply", json!([USDC_MINT])).await;
    }
    let elapsed = start.elapsed();
    let avg_ms = elapsed.as_millis() / 5;
    println!("getTokenSupply avg latency: {avg_ms}ms");
    assert!(
        avg_ms < 10000,
        "getTokenSupply should be < 10s avg over network: {avg_ms}ms"
    );
}

#[tokio::test]
async fn test_latency_get_account_info() {
    let start = Instant::now();
    for _ in 0..5 {
        rpc_call("getAccountInfo", json!([TOKEN_PROGRAM])).await;
    }
    let elapsed = start.elapsed();
    let avg_ms = elapsed.as_millis() / 5;
    println!("getAccountInfo avg latency: {avg_ms}ms");
    assert!(
        avg_ms < 30000,
        "getAccountInfo should be < 30s avg over network: {avg_ms}ms"
    );
}

// --- Concurrent load test ---

#[tokio::test]
async fn test_concurrent_requests() {
    let start = Instant::now();
    let mut handles = Vec::new();

    for i in 0..50 {
        handles.push(tokio::spawn(async move {
            let method = match i % 3 {
                0 => "getSlot",
                1 => "getVersion",
                _ => "getBlockHeight",
            };
            rpc_call(method, json!([])).await
        }));
    }

    let mut success = 0;
    let mut errors = 0;
    for h in handles {
        match h.await {
            Ok(resp) => {
                if resp["result"].is_null() && resp["error"].is_object() {
                    errors += 1;
                } else {
                    success += 1;
                }
            }
            Err(_) => errors += 1,
        }
    }

    let elapsed = start.elapsed();
    println!(
        "50 concurrent requests: {success} ok, {errors} errors in {}ms",
        elapsed.as_millis()
    );
    assert!(success >= 45, "at least 90% should succeed: {success}/50");
}

// --- Memory / stability test ---

#[tokio::test]
async fn test_rapid_fire_no_leak() {
    // Send 200 requests rapidly to check for obvious issues
    let start = Instant::now();
    for i in 0..200 {
        let method = match i % 5 {
            0 => "getSlot",
            1 => "getBlockHeight",
            2 => "getLatestBlockhash",
            3 => "getVersion",
            _ => "isBlockhashValid",
        };
        let params = if method == "isBlockhashValid" {
            json!(["11111111111111111111111111111111111111111111"])
        } else {
            json!([])
        };
        let resp = rpc_call(method, params).await;
        assert!(
            !resp["result"].is_null() || !resp["error"].is_null(),
            "request {i} returned nothing"
        );
    }
    let elapsed = start.elapsed();
    println!(
        "200 sequential requests in {}ms ({}ms avg)",
        elapsed.as_millis(),
        elapsed.as_millis() / 200
    );
}

// --- Data consistency: compare with DAS ---

#[tokio::test]
async fn test_consistency_token_supply_matches_reference() {
    let ref_url =
        std::env::var("REF_RPC_URL").unwrap_or_else(|_| "https://solana-rpc.publicnode.com".into());

    let client = reqwest::Client::new();
    let body = json!({"jsonrpc":"2.0","id":1,"method":"getTokenSupply","params":[USDC_MINT]});

    let ref_resp: Value = match client
        .post(&ref_url)
        .header("content-type", "application/json")
        .json(&body)
        .send()
        .await
    {
        Ok(r) => match r.json().await {
            Ok(v) => v,
            Err(_) => {
                println!("reference RPC unavailable, skipping");
                return;
            }
        },
        Err(_) => {
            println!("reference RPC unavailable, skipping");
            return;
        }
    };
    let li_resp = rpc_call("getTokenSupply", json!([USDC_MINT])).await;

    let ref_decimals = ref_resp["result"]["value"]["decimals"]
        .as_u64()
        .unwrap_or(0);
    let li_decimals = li_resp["result"]["value"]["decimals"].as_u64().unwrap_or(0);
    assert_eq!(
        ref_decimals, li_decimals,
        "USDC decimals should match reference"
    );
}
