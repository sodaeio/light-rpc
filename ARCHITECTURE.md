# Architecture

Single-binary Solana indexer + JSON-RPC server. Replaces a validator, RPC node, history backend, and DAS API service for the read path.

## Process layout

```
              ┌─────────────────────────────────────────────────┐
              │  light-indexer (one OS process)                  │
              │                                                  │
Richat gRPC ──▶ source::stream                                   │
              │      │ mpsc(2048)                                │
              │      ▼                                           │
              │  storage::write   ──┬──▶ RocksDB (6 CFs)         │
              │      │              ├──▶ block files (LZ4)       │
              │      │              └──▶ pg_writer ─▶ Postgres   │
              │      │ broadcast(1024)                           │
              │      ▼                                           │
              │  storage::read::MemoryCache (slot/blockhash hot) │
              │      │                                           │
              │      ▼                                           │
              │  rpc::server (axum, 32 SO_REUSEPORT listeners) ──▶ JSON-RPC clients
              └─────────────────────────────────────────────────┘
```

All four pipeline stages are independent tokio tasks linked by bounded channels. RPC load and ingestion never share resources beyond the broadcast channel that informs read-side caches of new blocks.

## Storage layout

### RocksDB (six column families, single DB instance)

| CF | Key | Value | Access |
|---|---|---|---|
| `slot_index` | `slot u64 BE` | small JSON `{block_time, block_height, blockhash, parent_slot}` | point lookup |
| `tx_index` | `signature [u8;64]` | header + prost-encoded `SubscribeUpdateTransactionInfo` (see `rpc/tx_format.rs`) | point lookup |
| `sfa_index` | `address[32] \| slot u64 BE` | bincode `Vec<SignatureEntry>` | prefix scan, descending |
| `accounts` | `pubkey [u8;32]` | classified `StoredAccount` | point lookup |
| `program_index` | `program_id[32] \| pubkey[32]` | empty | prefix scan |
| `owner_atas` | `owner[32] \| token_account_pubkey[32]` | empty | prefix scan |

Shared 1 GiB LRU block cache across CFs. Per-CF tuning:

- **slot_index** — 4 KiB blocks, no bloom (key is small, scans rare)
- **tx_index** — 16 KiB blocks, bloom 15 bits/key
- **sfa_index** — 16 KiB blocks, prefix bloom (32-byte address), L0 trigger 1, 256 KiB read-ahead
- **accounts** — 8 KiB blocks, bloom 15 bits/key
- **program_index** — 4 KiB blocks, prefix bloom, L0 trigger 1
- **owner_atas** — 4 KiB blocks, prefix bloom, L0 trigger 1

Compaction style is level with `level_compaction_dynamic_level_bytes`, ZSTD on the bottom level. Background I/O is rate-limited at 200 MB/s. Partitioned indexes + filters are enabled on every CF.

### PostgreSQL (Auxiliary, used for token state and DAS)

Schema: `solanadb`. Tables managed by the indexer:

- `slot_metas(slot)` — slot bookkeeping
- `tokens(mint, supply, decimals, …)` — SPL token mint state
- `token_accounts(pubkey, mint, owner, amount, …)` with `idx (mint, owner)` and (operator-built) `idx_token_accounts_owner` covering `(mint, amount, slot_updated)`
- `address_transactions` — historically used for gTFA; now superseded by RocksDB `sfa_index` + `owner_atas`. Range-partitioned by slot for retention drops.
- DAS schema (`asset`, `asset_data`, `asset_creators`, …) — read-only queries

Per-connection guards (set via `after_connect`):

```
statement_timeout = 60s
lock_timeout = 10s
idle_in_transaction_session_timeout = 5min
jit = off
application_name = light-indexer
```

### Block files

LZ4-compressed full encoded blocks at `data/blocks/{slot/10000}/{slot}.blk`. Lazy decompress on demand. Used by `getBlock` for historical (non-cached) requests.

## Read-path routing

| Method | Path |
|---|---|
| `getSlot`, `getBlockHeight`, `getLatestBlockhash`, `isBlockhashValid` | MemoryCache (atomic / RwLock<BTreeMap>) |
| `getBlock` | LRU cache (sharded) → MemoryCache → block file |
| `getAccountInfo`, `getBalance` | native registry → RocksDB `accounts` → PG `program_accounts` fallback |
| `getMultipleAccounts` | native registry → RocksDB `multi_get_cf` → PG `= ANY(bytea[])` for misses |
| `getProgramAccounts` | denylist check → RocksDB `program_index` prefix → `multi_get_cf` for `accounts` |
| `getTransaction` | RocksDB `tx_index` → prost decode → JSON shape (`rpc/tx_format.rs`) |
| `getSignaturesForAddress` | RocksDB `sfa_index` prefix scan |
| `getTransactionsForAddress` | RocksDB `owner_atas` prefix → N × `iter_sfa` → slot-DESC merge → `tx_index` hydrate |
| `getTokenAccountsByOwner`, `getTokenSupply`, `getTokenLargestAccounts` | Postgres |
| `getAssetsByOwner` and other DAS | Postgres |

## Write-path routing

For each `SourceMessage::Block`:

1. Encoded block → block file (LZ4)
2. Slot metadata → `slot_index`
3. Per-tx → `tx_index` (header + prost)
4. Per-address sigs → `sfa_index` (bincode)
5. Per-(address, signature) → PG `address_transactions` partition (legacy reader; ingest also writes for backfill purposes — read path no longer touches it)
6. Broadcast `WriteToReadMessage::NewBlock`

For account batches (flushed every 200 ms or 1000 entries):

1. Dedup by pubkey, keep highest slot
2. Classify into `TokenMint`, `TokenAccount`, `ProgramAccount`
3. Program accounts → RocksDB batch (`accounts` + `program_index`)
4. Token accounts → RocksDB `owner_atas` batch + Postgres `token_accounts` upsert (via channel)
5. Token mints → Postgres `tokens` upsert (via channel)

## Broadcast / cache invalidation

`tokio::sync::broadcast::Sender<WriteToReadMessage>` (capacity 1024). Subscribers:

- `MemoryCache::run_cache_updater` — applies `NewBlock` (insert), `BlockConfirmed`, `SlotFinalized` (advance atomic counters and GC blocks past `finalized - 64`).

## RPC server

- Axum 0.7 + jsonrpsee 0.24 + tower-http compression
- One TCP listener per CPU via `socket2` `SO_REUSEPORT`. Per-listener accept queue 32 768. Removes cross-worker contention at `c≥2000`.
- `TCP_NODELAY` disables Nagle for sub-ms response latency
- Compression: gzip + brotli at fastest quality. The tradeoff favors low CPU per request over best ratio.
- Block response cache: 16 shards × 64-entry LRU keyed on `(slot, fnv(transactionDetails+rewards))`. Stored as `Arc<Value>` so cache hits are a refcount bump and a clone (cheap for a `Value` tree).
- `getProgramAccounts` denylist: 8 program IDs (Token, Token-2022, ATA, System, Vote, Stake, ALT, Compute Budget). Returns `-32602` with a hint pointing to `getTokenAccountsByOwner`.

## Retention

- `address_transactions` (PG): range-partitioned by `(slot / 1_500_000)`. Retention loop creates next 4 partitions and drops partitions whose upper bound is older than `current_finalized - retention_slots` (default ~30 d). Bootstrap creates current + 4 ahead at startup using `slot_metas` for the seed.
- `sfa_index` (RocksDB): app-level prune via `delete_range_cf` per address before `cutoff_slot`.

## Observability

Prometheus on `metrics.endpoint`:

- `li_ingested_blocks_total` / `li_ingested_accounts_total` / `li_ingested_txs_total`
- `li_latest_slot{commitment}`
- `li_pipeline_channel_size{channel}`
- `li_rpc_requests_total{method}` / `li_rpc_latency_seconds{method}` / `li_rpc_errors_total{method}`
- `li_storage_write_seconds`, `li_pg_write_seconds`
- `li_memory_cached_blocks`
- `li_rocksdb_l0_files{cf}`, `li_rocksdb_sst_count{cf}`, `li_rocksdb_live_data_bytes{cf}`

`/healthz` always 200. `/readyz` returns 503 if no finalized slot update in 60 s.

## Configuration

YAML — see `config.example.yml`. Key sections:

- `source` — Richat gRPC endpoint, commitment, message-size cap
- `storage.rocksdb` — path, write buffer, max open files
- `storage.blocks` — path, compression, max stored blocks
- `storage.postgres` — connection string, pool sizing, address retention slots
- `storage.pipeline` — channel capacities (`source_to_write` 2048, `write_to_read` 1024, `pg_write_buffer` 10000)
- `rpc` — bind address, blocked programs, gpa max accounts, upstream forward
- `metrics` — bind address
- `threads` — worker counts (defaults `0` = num_cpus)

## Optional features

- `clickhouse` cargo feature gates an additional historical writer that mirrors the RPC 2.0 design: base `transactions` table + materialized views `gsfa_mv`, `gsfa_hot_mv`, `sig_status_mv`, plus `token_owner_signatures` and `program_invocations`. See `src/storage/clickhouse.rs`. Disabled by default.
