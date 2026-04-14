//! Stress test for light-indexer RPC throughput.
//!
//! Run: cargo test --test stress --release -- --nocapture

use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn endpoint() -> String {
    std::env::var("LIGHT_INDEXER_URL").unwrap_or_else(|_| "http://45.154.33.82:8878".into())
}

async fn rpc_call_raw(
    client: &reqwest::Client,
    method: &str,
    params: Value,
) -> Result<Value, reqwest::Error> {
    let body = json!({"jsonrpc": "2.0", "id": 1, "method": method, "params": params});
    client
        .post(&endpoint())
        .json(&body)
        .send()
        .await?
        .json()
        .await
}

struct BenchResult {
    total_requests: u64,
    successful: u64,
    failed: u64,
    duration: Duration,
    p50_us: u64,
    p99_us: u64,
}

impl BenchResult {
    fn rps(&self) -> f64 {
        self.successful as f64 / self.duration.as_secs_f64()
    }
}

async fn bench(
    name: &str,
    method: &str,
    params: Value,
    concurrency: usize,
    duration_secs: u64,
) -> BenchResult {
    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(concurrency)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let success = Arc::new(AtomicU64::new(0));
    let fail = Arc::new(AtomicU64::new(0));
    let latencies = Arc::new(parking_lot::Mutex::new(Vec::with_capacity(100_000)));
    let deadline = Instant::now() + Duration::from_secs(duration_secs);

    let mut handles = Vec::new();
    for _ in 0..concurrency {
        let client = client.clone();
        let method = method.to_string();
        let params = params.clone();
        let success = Arc::clone(&success);
        let fail = Arc::clone(&fail);
        let latencies = Arc::clone(&latencies);

        handles.push(tokio::spawn(async move {
            while Instant::now() < deadline {
                let start = Instant::now();
                match rpc_call_raw(&client, &method, params.clone()).await {
                    Ok(resp) => {
                        if resp.get("error").is_some() {
                            fail.fetch_add(1, Ordering::Relaxed);
                        } else {
                            success.fetch_add(1, Ordering::Relaxed);
                            latencies.lock().push(start.elapsed().as_micros() as u64);
                        }
                    }
                    Err(_) => {
                        fail.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    let start = Instant::now();
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();

    let s = success.load(Ordering::Relaxed);
    let f = fail.load(Ordering::Relaxed);
    let mut lats = latencies.lock().clone();
    lats.sort();

    let p50 = if !lats.is_empty() {
        lats[lats.len() / 2]
    } else {
        0
    };
    let p99 = if !lats.is_empty() {
        lats[lats.len() * 99 / 100]
    } else {
        0
    };

    let result = BenchResult {
        total_requests: s + f,
        successful: s,
        failed: f,
        duration: elapsed,
        p50_us: p50,
        p99_us: p99,
    };

    println!(
        "  {name}: {:.0} req/s | {s} ok, {f} err | p50={:.1}ms p99={:.1}ms | {:.1}s",
        result.rps(),
        p50 as f64 / 1000.0,
        p99 as f64 / 1000.0,
        elapsed.as_secs_f64(),
    );

    result
}

#[tokio::test]
async fn stress_test_getslot() {
    println!("\n=== getSlot (lightweight, in-memory) ===");
    for c in [1, 10, 50, 100] {
        let r = bench(&format!("c={c}"), "getSlot", json!([]), c, 5).await;
        assert!(r.failed == 0, "no errors expected");
    }
}

#[tokio::test]
async fn stress_test_getbalance() {
    println!("\n=== getBalance (RocksDB point lookup) ===");
    for c in [1, 10, 50] {
        let r = bench(
            &format!("c={c}"),
            "getBalance",
            json!(["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]),
            c,
            5,
        )
        .await;
        assert!(r.failed == 0, "no errors expected");
    }
}

#[tokio::test]
async fn stress_test_getaccountinfo() {
    println!("\n=== getAccountInfo base64 (RocksDB read) ===");
    for c in [1, 10, 50] {
        let r = bench(
            &format!("c={c}"),
            "getAccountInfo",
            json!(["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA", {"encoding": "base64"}]),
            c,
            5,
        )
        .await;
        assert!(r.failed == 0, "no errors expected");
    }
}

#[tokio::test]
async fn stress_test_gettokensupply() {
    println!("\n=== getTokenSupply (PostgreSQL read) ===");
    for c in [1, 10, 50] {
        let r = bench(
            &format!("c={c}"),
            "getTokenSupply",
            json!(["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]),
            c,
            5,
        )
        .await;
        assert!(r.failed == 0, "no errors expected");
    }
}

#[tokio::test]
async fn stress_test_getlatestblockhash() {
    println!("\n=== getLatestBlockhash (memory cache) ===");
    for c in [1, 10, 50, 100] {
        let r = bench(&format!("c={c}"), "getLatestBlockhash", json!([]), c, 5).await;
        assert!(r.failed == 0, "no errors expected");
    }
}

#[tokio::test]
async fn stress_test_mixed_workload() {
    println!("\n=== Mixed workload (realistic traffic pattern) ===");

    let client = reqwest::Client::builder()
        .pool_max_idle_per_host(100)
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap();

    let success = Arc::new(AtomicU64::new(0));
    let fail = Arc::new(AtomicU64::new(0));
    let deadline = Instant::now() + Duration::from_secs(10);
    let concurrency = 100;

    let mut handles = Vec::new();
    for i in 0..concurrency {
        let client = client.clone();
        let success = Arc::clone(&success);
        let fail = Arc::clone(&fail);

        handles.push(tokio::spawn(async move {
            let mut counter = i;
            while Instant::now() < deadline {
                let (method, params) = match counter % 5 {
                    0 => ("getSlot", json!([])),
                    1 => ("getLatestBlockhash", json!([])),
                    2 => (
                        "getBalance",
                        json!(["TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"]),
                    ),
                    3 => ("getBlockHeight", json!([])),
                    _ => (
                        "getTokenSupply",
                        json!(["EPjFWdd5AufqSSqeM2qN1xzybapC8G4wEGGkZwyTDt1v"]),
                    ),
                };
                counter += 1;

                match rpc_call_raw(&client, method, params).await {
                    Ok(resp) if resp.get("error").is_none() => {
                        success.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        fail.fetch_add(1, Ordering::Relaxed);
                    }
                }
            }
        }));
    }

    let start = Instant::now();
    for h in handles {
        let _ = h.await;
    }
    let elapsed = start.elapsed();
    let s = success.load(Ordering::Relaxed);
    let f = fail.load(Ordering::Relaxed);

    println!(
        "  mixed c={concurrency}: {:.0} req/s | {s} ok, {f} err | {:.1}s",
        s as f64 / elapsed.as_secs_f64(),
        elapsed.as_secs_f64(),
    );
    assert!(f == 0, "no errors in mixed workload");
}
