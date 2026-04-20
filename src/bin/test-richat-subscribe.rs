//! Standalone richat subscribe probe.
//!
//! Mirrors light-indexer's subscribe_richat call exactly. Useful for
//! diagnosing whether the issue is in light-indexer or upstream richat
//! when the indexer's stream times out at 90s with no data.
//!
//! Usage:
//!     cargo run --release --bin test-richat-subscribe -- http://45.154.33.73:10100
//!     cargo run --release --bin test-richat-subscribe -- http://127.0.0.1:10200 --duration 30

use std::time::{Duration, Instant};

use anyhow::Result;
use futures::StreamExt;
use richat_client::grpc::ConfigGrpcClient;
use richat_proto::richat::GrpcSubscribeRequest;
use yellowstone_grpc_proto::geyser::subscribe_update::UpdateOneof;

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    let endpoint = args
        .get(1)
        .cloned()
        .unwrap_or_else(|| "http://45.154.33.73:10100".to_string());
    let duration_secs: u64 = args
        .iter()
        .position(|a| a == "--duration")
        .and_then(|i| args.get(i + 1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(20);

    println!("→ endpoint: {endpoint}");
    println!("→ duration: {duration_secs}s");
    println!();

    let cfg = ConfigGrpcClient {
        endpoint: endpoint.clone(),
        x_token: None,
        max_decoding_message_size: 67_108_864,
        connect_timeout: Some(Duration::from_secs(10)),
        timeout: Some(Duration::from_secs(30)),
        tcp_nodelay: true,
        tcp_keepalive: Some(Duration::from_secs(15)),
        keep_alive_while_idle: true,
        ..Default::default()
    };

    let t0 = Instant::now();
    println!("[+{:.2}s] connecting…", t0.elapsed().as_secs_f64());
    let mut client = tokio::time::timeout(Duration::from_secs(10), cfg.connect())
        .await
        .map_err(|_| anyhow::anyhow!("connect timed out"))??;
    println!("[+{:.2}s] connected", t0.elapsed().as_secs_f64());

    println!(
        "[+{:.2}s] subscribing (filter=None, replay_from_slot=None)…",
        t0.elapsed().as_secs_f64()
    );
    let stream = tokio::time::timeout(
        Duration::from_secs(10),
        client.subscribe_richat(GrpcSubscribeRequest {
            replay_from_slot: None,
            filter: None,
        }),
    )
    .await
    .map_err(|_| anyhow::anyhow!("subscribe timed out"))??;
    println!("[+{:.2}s] subscribed", t0.elapsed().as_secs_f64());

    let parsed = stream.into_parsed();
    tokio::pin!(parsed);

    let deadline = Instant::now() + Duration::from_secs(duration_secs);
    let mut total: u64 = 0;
    let mut last_print = Instant::now();
    let mut last_count: u64 = 0;
    let mut first_msg_at: Option<f64> = None;
    let mut by_kind = std::collections::BTreeMap::<&'static str, u64>::new();

    loop {
        let now = Instant::now();
        if now >= deadline {
            break;
        }
        let remaining = deadline.saturating_duration_since(now);
        match tokio::time::timeout(remaining.min(Duration::from_secs(1)), parsed.next()).await {
            Ok(Some(Ok(update))) => {
                if first_msg_at.is_none() {
                    first_msg_at = Some(t0.elapsed().as_secs_f64());
                    println!("[+{:.2}s] FIRST MESSAGE", first_msg_at.unwrap());
                }
                total += 1;
                let kind = match &update.update_oneof {
                    Some(UpdateOneof::Account(_))           => "account",
                    Some(UpdateOneof::Slot(_))              => "slot",
                    Some(UpdateOneof::Transaction(_))       => "transaction",
                    Some(UpdateOneof::TransactionStatus(_)) => "tx_status",
                    Some(UpdateOneof::Entry(_))             => "entry",
                    Some(UpdateOneof::BlockMeta(_))         => "block_meta",
                    Some(UpdateOneof::Block(_))             => "block",
                    Some(UpdateOneof::Ping(_))              => "ping",
                    Some(UpdateOneof::Pong(_))              => "pong",
                    None                                    => "empty",
                };
                *by_kind.entry(kind).or_insert(0) += 1;

                if last_print.elapsed() >= Duration::from_secs(2) {
                    let dt = last_print.elapsed().as_secs_f64();
                    let rate = (total - last_count) as f64 / dt;
                    println!(
                        "[+{:.2}s] msgs={:>8}  rate={:>7.0}/s  by_kind={:?}",
                        t0.elapsed().as_secs_f64(),
                        total,
                        rate,
                        by_kind
                    );
                    last_print = Instant::now();
                    last_count = total;
                }
            }
            Ok(Some(Err(e))) => {
                println!("[+{:.2}s] STREAM ERROR: {e}", t0.elapsed().as_secs_f64());
                break;
            }
            Ok(None) => {
                println!("[+{:.2}s] STREAM ENDED", t0.elapsed().as_secs_f64());
                break;
            }
            Err(_) => {
                if first_msg_at.is_none() {
                    print!(".");
                    use std::io::Write;
                    std::io::stdout().flush().ok();
                }
            }
        }
    }

    println!();
    println!("════════ summary ════════");
    println!("endpoint:        {endpoint}");
    println!("duration:        {:.2}s", t0.elapsed().as_secs_f64());
    println!(
        "first msg at:    {}",
        match first_msg_at {
            Some(t) => format!("{t:.2}s"),
            None => "(never — subscribe call returned OK but server delivered nothing)".to_string(),
        }
    );
    println!("total messages:  {total}");
    println!("by kind:         {by_kind:?}");
    if total == 0 && first_msg_at.is_none() {
        println!();
        println!("⚠️  ZERO messages received during the entire window.");
        println!("    light-indexer would also see this (90s timeout → reconnect loop).");
    }
    Ok(())
}
