# Design Decisions

Why each major call was made the way it was. Reading-order: most consequential first.

## RocksDB owner_atas CF instead of Postgres for gTFA

`getTransactionsForAddress` was the slowest method by 30×. Original implementation: `SELECT pubkey FROM token_accounts WHERE owner = $1 LIMIT 2048` then `SELECT … FROM address_transactions WHERE address = ANY($1::bytea[]) ORDER BY slot DESC LIMIT N`. On a custodial wallet with millions of ATAs the planner couldn't push through `ANY` cleanly and we got 130 ms p99 at 302 rps.

The data wasn't unavailable — RocksDB's `sfa_index` already had per-address signatures. The missing piece was the owner→ATAs mapping, which was only in Postgres. We added a `owner_atas` CF (`[owner|pubkey] → empty`, prefix-scannable) populated at token-account classification time using the SPL Token data layout (owner is bytes [32..64]). gTFA now scans owner_atas to enumerate ATAs, calls `iter_sfa` for each address, sort-merges by slot DESC, and hydrates from `tx_index`. Postgres is off the gTFA critical path.

Result: 302 rps p99=130 ms → 9 775 rps p99=9.4 ms (whale-10), 936 → 2 492 rps (whale-100).

## Bincode on `sfa_index` with JSON fallback

Per-entry deserialization is the inner loop in both `gSFA` and `gTFA`. JSON decode of `Vec<SignatureEntry>` is ~5× slower than bincode for the same struct. Switching the on-disk format gave +97% rps on `gSFA(Jupiter, 100)` and +166% on `gTFA(whale, 100)`.

The read path tries bincode first and falls back to `serde_json::from_slice` so existing JSON entries keep decoding through the migration window. After full retention rotation the fallback is dead code; we kept it so fresh deploys don't have to wipe RocksDB.

## SO_REUSEPORT × num_cpus listeners

A single `axum::serve(listener, app)` puts every inbound connection through one kernel accept queue and one tokio task. At c≥2000 the queue became the bottleneck.

We spawn one listener per CPU using `socket2::Socket` with `SO_REUSEPORT`. The kernel hashes by 5-tuple so connections stick to a worker. Each listener has its own 32k accept queue, removing the shared-queue contention.

Result: getSlot c=2000 went from saturating at ~28k rps to peaking at 42 470 rps.

## Sharded `getBlock` response cache

Most clients re-query the same recent block dozens of times per second. The first version had a single `Mutex<LruCache<(slot, cfg), Bytes>>`. At 9k rps the mutex became the bottleneck; the cached-bytes path also re-serialized the response into JSON twice (cache held bytes, jsonrpsee re-serializes to the wire).

We changed to:

- 16 sharded LRUs (64 entries each, total 1024)
- shard index = `mix_64(slot, cfg_hash) % 16`
- cache holds `Arc<Value>`; hits return `(*arc).clone()` — a refcount bump on the outer Value plus shallow clones of fields (Vec lengths are precomputed)

The double-serialization is avoided because we never round-trip through bytes.

## Per-CF compaction tuning, especially `level_zero_file_num_compaction_trigger = 1` on write-heavy CFs

RocksDB defaults are tuned for moderate-write workloads and let L0 grow to 4 files before compacting. Solana's per-block volume on `sfa_index` and `tx_index` accumulated L0 files faster than compaction merged them; reads paid bigger merge costs.

Per-CF `apply_cf_compaction_tuning_with(opts, 1)` on write-heavy CFs forces L0 → L1 promotion as soon as a file lands. Combined with rate-limited background I/O (200 MB/s), L0 stays at ≤1 file per CF even under heavy ingest.

Single biggest correctness/operational win was adding `set_level_zero_stop_writes_trigger(36)`. Without it, runaway ingest could fill the disk to 100% before the indexer noticed; we'd lose the database. With it, writes get backpressured before things go bad.

## prost-on-disk + JSON-shape-on-read for `getTransaction`

The first version stored the in-memory `BlockWithData::transactions` Vec — barely-decoded transaction entries with offsets into the encoded block. Reads returned that internal shape directly, which was useless to clients (no signatures, no fees, no logs).

We switched to:

- At ingest, serialize the full `SubscribeUpdateTransactionInfo` (yellowstone proto) using `prost::Message::encode` and a 21-byte fixed header (`slot, tx_index, block_time, err_len`). Stored in `tx_index`.
- At read, decode the proto, then build the agave RPC shape (`{slot, blockTime, version, transaction:{signatures, message}, meta:{…}}`) on demand.

The proto encoding is ~40% smaller than equivalent JSON and ~10× faster to materialize on the read side. Failed-tx error fields are bincode-decoded via `solana_transaction_error::TransactionError` so the JSON shape matches agave exactly (`{"InstructionError":[3,{"Custom":21}]}`).

Verified against `solana-rpc.publicnode.com` — 70/70 fields match across 5 sampled txs.

## `getProgramAccounts` denylist returning `-32602`

Token Program has hundreds of millions of accounts; one `getProgramAccounts` call against it can return tens of GB. Every production Solana RPC denies these implicitly.

We block 8 program IDs by default (Token, Token-2022, ATA, System, Vote, Stake, ALT, Compute Budget). The error code is `-32602` (JSON-RPC standard "Invalid params") rather than `-32010` (which agave uses for "Node is unhealthy" — would trigger client retry storms). The error message points at the right typed alternative (`getTokenAccountsByOwner`).

A secondary `gpa_max_accounts` cap (default 100k) catches anything else that grows pathologically.

## Mimalloc as the global allocator

Rust's default system allocator (jemalloc on Linux distros that use it; glibc `malloc` otherwise) was holding ~5 GB RSS under sustained load with significant fragmentation (lots of small JSON allocations from response building).

`mimalloc` as the global allocator halved RSS to ~2.5 GB at the same load and improved tail latency on heavy gSFA/gTFA where allocation churn dominated.

## Postgres connection guards via `after_connect`

Without `statement_timeout`, a single bad gPA query (or a forgotten manual query) could pin a worker forever and starve the connection pool. We surface that pathology after 60 s. `lock_timeout = 10 s` and `idle_in_transaction_session_timeout = 5 m` cover the other failure modes.

`jit = off` matters for our query mix — the planner's JIT cost almost always exceeds the savings on these lookup-shaped queries.

## Block files separate from RocksDB

RocksDB block cache and compaction would not handle the ~1-10 MB encoded-block payloads gracefully. We store them as flat LZ4 files at `data/blocks/{slot/10000}/{slot}.blk` with lazy decompression. RocksDB only stores per-tx offsets and metadata, which fit cleanly in its block cache.

## `address_transactions` partitioned by slot

Even with the above fixes that took it off the read path, `address_transactions` is still being written by the ingester (legacy backfill purposes) and could grow unboundedly. We range-partition by `(slot / 1_500_000)` (about 1 week per partition) and the retention loop drops partitions older than the configured window (default 30 days).

If we wanted, we could now stop writing to `address_transactions` entirely; we left the dual-write in place during the migration window so reads from clients that haven't switched still work.

## Single binary, single process

Splitting the indexer into separate ingest and query processes is a real option. We didn't because:

- All four pipeline stages already isolate via bounded channels; RPC saturation cannot stall ingest.
- Operational complexity of two coordinated processes (file-based handoff, restart ordering) outweighs the marginal scaling benefit at our current load.
- The biggest scaling problems weren't process boundaries, they were per-handler cost.

If we needed to scale RPC independently of ingest, the cheaper option is to run multiple read-only replicas pointing at the same RocksDB (read-only mode) and have a primary that handles ingest + writes.

## Configuration philosophy

Defaults aim to be production-correct out of the box. Knobs exist where the right value is genuinely deployment-dependent (RocksDB paths, retention window, blocked program list). We do not expose every internal threshold as a config — those that have a single right answer (compaction triggers, bloom bits) stay in code.
