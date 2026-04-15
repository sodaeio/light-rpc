# Performance

Benchmarks and the optimizations behind them. All numbers from `bm82` (48 cores, NVMe), measured with `hey`.

## Headline numbers

Latest build, measured against a fresh RocksDB after ~90 s of ingest:

| Method | Concurrency | rps | p50 | p99 |
|---|---|---|---|---|
| `getSlot` | 2000 | **42,470** | 21.4 ms | 142 ms |
| `getSlot` | 100 | 23,278 | 3.4 ms | 23.4 ms |
| `gTFA(wallet, 100)` | 50 | **16,933** | 2.4 ms | **7.2 ms** |
| `getTransaction` | 50 | **16,101** | 2.9 ms | 7.3 ms |
| `gAI(USDC jsonParsed)` | 50 | 16,074 | 2.3 ms | 20 ms |
| `getBlock` cached | 200 | 13,723 | 11.9 ms | 58 ms |
| `gPA(Jupiter)` | 20 | 11,714 | 1.4 ms | 4.3 ms |
| `gTFA(whale, 10)` | 50 | **9,775** | 4.3 ms | **9.4 ms** |
| `gAI(Token 17 KB)` | 50 | 6,247 | 7.3 ms | 13.9 ms |
| `gMA(10 accts)` | 50 | 6,231 | 7.2 ms | 14.1 ms |
| `gSFA(Jupiter, 100)` | 20 | 3,351 | 5.4 ms | 10.1 ms |
| `gTFA(whale, 100)` | 20 | **2,492** | 7.4 ms | **13.5 ms** |
| `blocked gPA(Token)` | 100 | 22,215 | 3.5 ms | 31.5 ms |

## Data accuracy

Spot-checked 5 fresh transactions against `solana-rpc.publicnode.com`. **70/70 fields match** across:

```
slot, blockTime, version, meta.fee, meta.err, meta.computeUnitsConsumed,
transaction.signatures[0], transaction.message.accountKeys[0],
transaction.message.recentBlockhash, transaction.message.instructions|length,
meta.preBalances|sum, meta.postBalances|sum, meta.logMessages|length,
meta.preTokenBalances|length
```

Failed transactions match agave's exact error format (`{"InstructionError":[3,{"Custom":21}]}`) via `bincode::deserialize::<TransactionError>` then re-serialize as JSON.

## Optimization journey

Each row is a build that shipped to bm82 and was measured.

### Phase 1 — Foundations

| Optimization | What it did | Effort |
|---|---|---|
| RocksDB compaction tuning | L0 trigger 2, slowdown 20, stop 36, dynamic level bytes, bottommost ZSTD | 1 h |
| Per-CF block sizes (4/8/16 KiB) | Match block size to access pattern | 1 h |
| Partitioned index + filters, format v5 | Top-level index pinned, leaf blocks demand-loaded; massive memory saving on large DBs | 1 h |
| Prefix bloom for prefix-scan CFs | `iter_sfa` / `get_program_accounts` skip SSTs whose prefix range can't match | 30 m |
| Periodic compaction (1 h) | Drains L0 even when ingest is idle | 5 m |
| RocksDB rate limiter (200 MB/s) | Compaction can't starve reads during burst ingest | 5 m |

### Phase 2 — Postgres hardening

| Optimization | What it did | Effort |
|---|---|---|
| `statement_timeout = 60 s` | Surfaces runaway queries instead of pinning workers indefinitely | 5 m |
| `lock_timeout = 10 s` | Stops cascading stalls during migrations / `pg_repack` | 5 m |
| Token accounts `FILLFACTOR = 85` | Balance UPDATEs stay HOT, ~60% less index bloat | 5 m |
| Aggressive autovacuum on `token_accounts` | Triggers at 5% dead tuples vs 20% default | 5 m |
| `BRIN(slot)` on `address_transactions` | ~1000× smaller than btree, exploits monotonic slot-order within partitions | 5 m |
| `address_transactions` partitioned by slot | Drop old partitions to bound disk; nightly retention loop | 4 h |

### Phase 3 — RPC compatibility

| Optimization | What it did | Effort |
|---|---|---|
| `getTransaction` proto on disk + JSON shape on read | Stored `SubscribeUpdateTransactionInfo` as prost; on read, decoded and shaped to the agave RPC format. Verified byte-identical to mainnet across 70 fields | 8 h |
| `getBlock` accepts the standard config object | Previous tuple destructure rejected `(slot, {...})`; broke every wallet | 30 m |
| Bincode-decode of `TransactionError` | Matches agave's `{"InstructionError":[3,{"Custom":21}]}` shape exactly | 1 h |

### Phase 4 — Hot path micro-optimizations

| Optimization | What it did | Measured impact |
|---|---|---|
| `mimalloc` global allocator | Halved RSS under load; helped JSON-heavy hot paths | RSS 4.7 GB → 2.0 GB |
| `Arc<str>` blockhash cache | No String clone on every read | small |
| `getVersion` `LazyLock` | Build the tiny static JSON once | small |
| `multi_get_cf` for `getMultipleAccounts` | Replaced N serial `get_cf` calls | gMA p99 112 ms → 15 ms (-87%) |
| Batched PG fallback `= ANY(bytea[])` | Replaced N serial PG queries on cache-miss path | same |
| `native::lookup` HashMap | Was linear scan over a 40-entry Vec | small |
| Bloom filter 10 → 15 bits/key | FP rate 0.9% → 0.04% on point CFs | small |
| Reusable Prometheus encoder buffer | Stop allocating 64 KiB per scrape | small |
| `multi_get_cf` for `getProgramAccounts` | Collect pubkeys from prefix scan, then batch fetch | gPA(Memo empty) 1.5 s → 7 ms |
| Drop legacy PG fallback in `gPA` | Was hitting an unindexed `program_accounts` table on empty-result paths | gPA(Memo empty) c=50: 2.8 rps → 18 905 rps |
| Compression at `Fastest` quality | Tiny responses pay heavy compression no longer | small |
| `tokio` `max_blocking_threads = 512` | Headroom for blocking RocksDB calls | helps under load |

### Phase 5 — Network

| Optimization | What it did | Measured impact |
|---|---|---|
| `TCP_NODELAY` on listener | No Nagle delay on small responses | getSlot c=1000: 17 858 → 28 357 rps |
| Listen backlog 8 192 → 32 768 | Absorb burst traffic before kernel drops | small |
| `SO_REUSEPORT` × `num_cpus` listeners | One accept queue per worker; zero cross-worker contention | getSlot c=2000: new ceiling 39 077 → 42 470 rps |
| TCP_NODELAY on each per-listener socket | Same | applied per shard |

### Phase 6 — Cache layers

| Optimization | What it did | Measured impact |
|---|---|---|
| `getBlock` response cache (LRU) | `(slot, cfg_hash) → Arc<Value>`, hit serves Arc clone instead of rebuild | getBlock c=50 cached: 10 446 → 16 952 rps, p99 22 → 7.4 ms |
| 16-shard sharded LRU | Removes single-mutex contention at 9k+ rps | getBlock c=200 cached: scales to 13 723 rps |
| `tx_index` proto storage with header | Decode on demand; ~40% smaller on disk than equivalent JSON | enables `getTransaction` 16k rps |

### Phase 7 — Architectural overhauls

| Overhaul | What it did | Measured impact |
|---|---|---|
| **`gTFA` from PG → RocksDB** | New `owner_atas` CF written at token-account classification time. gTFA reads do owner-prefix scan + N × `iter_sfa` + slot-DESC merge + `tx_index` hydrate. PG entirely off the gTFA critical path. | **whale 302 → 9 775 rps (32×), p99 130 → 9.4 ms (14×)** |
| **`sfa_index` JSON → bincode** | Per-entry deserialization is the hot loop in `gSFA` and `gTFA`. Bincode is roughly 5× faster on the same `Vec<SignatureEntry>` | **gSFA(Jupiter, 100) 1 699 → 3 351 rps (+97%), p99 19 → 10 ms** **gTFA(whale, 100) 936 → 2 492 rps (+166%)** |
| `iter_sfa` with 256 KiB read-ahead + `prefix_same_as_start` | Cold scans on the `sfa_index` prefetch large chunks instead of doing 4 KiB reads | helps cold whale-100 |

### Skipped — reasoned-out bad ROI

| Idea | Why skipped |
|---|---|
| simd-json drop-in | Hot serialization happens inside jsonrpsee which owns the serde_json stack; would need either a fork or a parallel pipeline. Risk vs ~15% gain doesn't justify it. |
| Hand-serialized response bodies (`write!` directly to bytes) | Maintenance burden + bug risk; only material on getSlot which is already memory-bound by atomics. |
| LRU account cache in front of `accounts` CF | Required new broadcast plumbing for invalidation. Single-key reads on hot accounts already 16k rps. |
| Streaming `getProgramAccounts` body | jsonrpsee has no streaming support; would require a parallel handler outside the RPC module. Niche; gPA blocked on the most pathological programs anyway. |
| Pre-encode bs58 at ingest | Doubles `tx_index` size per entry. Skip in favor of fast `bs58` at decode time. |
| HTTP/2 | JSON-RPC is short request/response; multiplexing helps long-polled streams, not POSTs. |
| Direct I/O for cold gPA | Page cache is helping; dropping it regresses warm reads. |
| CPU pinning | Single-socket bm82, no NUMA. |
| Append-only LSM for accounts table | Architectural rebuild for negative measured benefit. |

## Methodology

- One process under `systemd` on bm82, ingesting from the production Richat gRPC at `45.154.33.73:10100`.
- `hey` driver from the same machine (loopback). Network adds nothing.
- Each bench warmed by 30 s of ingest before measuring.
- Repeat measurements are taken with a fresh RocksDB to remove warm-cache bias on the first runs; subsequent runs measure steady state.
- `/readyz` is monitored throughout; any test that drives ingestion lag above 0 is invalidated.

## Service health under sustained load

Mixed workload (5 methods interleaved at ~50 rps each for 60 s):

| Time | Lag | RSS |
|---|---|---|
| baseline | 0 s | 4 755 MB |
| 30 s | 0 s | 5 585 MB |
| 60 s | 0 s | 5 753 MB |

Memory grew 1 GB (cache warming) then stabilized. Zero ingestion lag throughout.

L0 file counts after 60 s of mixed load:

| CF | L0 files |
|---|---|
| accounts | 0 |
| program_index | 1 |
| sfa_index | 0 |
| slot_index | 1 |
| tx_index | 0 |
| owner_atas | 0 |

## Future levers (not yet applied)

- **k-way merge in gTFA** — replace `Vec::sort_unstable` with a `BinaryHeap` over per-address iterators. Wins above ~32 ATAs. ~4 h.
- **Block cache shard count tuning** — currently 16 shards × 64 entries. Total 1024 — at very high concurrency, more shards reduce contention further.
- **`use_direct_io_for_flush_and_compaction`** — bypasses page cache for compaction; can let more page cache go to read paths.
- **Move historical reads to ClickHouse** (`clickhouse` feature) — already scaffolded with the writer wired but disabled by default. Would offload `getSignaturesForAddress` analytical patterns to a columnar store.
- **gSFA with k=many addresses** — prefix bloom helps but the descending iterator is still serial per address. A multi-address parallel iterator would help long-tail `getSignaturesForAddress` calls.
