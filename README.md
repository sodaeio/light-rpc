# light-rpc

A unified Solana indexer and RPC server. Single binary that replaces the need for separate block history, account indexing, and DAS API services.

## What it does

light-rpc subscribes to a Solana validator's stream (Yellowstone Dragon's Mouth gRPC, native [Richat](https://github.com/lamports-dev/richat) gRPC, or QUIC firehose) and indexes everything into a tiered storage system. It then serves the full Solana JSON-RPC API from one HTTP endpoint — no validator node required. Cold start can rebuild full account state from a local Solana snapshot (`source.snapshot_dir`).

```
Validator (Richat gRPC)
        │
        ▼
┌─ StreamSource ───────────────────────────────┐
│  gRPC subscription, commitment tracking,      │
│  block accumulation, account updates           │
└──────────┬───────────────────────────────────┘
           │ mpsc channel
           ▼
┌─ StorageWriter ──────────────────────────────┐
│                                               │
│  Block pipeline:                              │
│    → LZ4 block files (historical data)        │
│    → RocksDB indexes (slot, tx, signatures)   │
│                                               │
│  Account pipeline:                            │
│    → RocksDB (program accounts, fast lookup)  │
│    → PostgreSQL (tokens, mints, relational)   │
│                                               │
└──────────┬───────────────────────────────────┘
           │ broadcast channel
           ▼
┌─ MemoryCache ────────────────────────────────┐
│  ~260 recent blocks, 512 blockhashes,         │
│  atomic slot tracking per commitment level     │
└──────────┬───────────────────────────────────┘
           │
           ▼
┌─ RPC Server (Axum + jsonrpsee) ──────────────┐
│  35 JSON-RPC methods on single endpoint       │
│  Compression (gzip, brotli), CORS, metrics    │
└──────────────────────────────────────────────┘
```

## Key design decisions

**Single binary, single process.** No separate ingest and API services to coordinate. The gRPC source, storage writer, and RPC server run as isolated async tasks connected by bounded channels. If the RPC server is under heavy load, the ingestion pipeline is unaffected — they share no resources except the broadcast channel.

**Tiered read path.** Every query checks memory first (sub-ms), then RocksDB indexes (single disk read), then block files or PostgreSQL. Recent data is always fast.

**Isolated PostgreSQL writes.** Token and account writes to PostgreSQL go through a separate bounded channel to a dedicated writer task. If PG is slow, the channel buffers and the main pipeline never stalls. Account state is last-write-wins, so dropped updates during backpressure are safe.

**Unified RocksDB.** Block indexes and account data share one RocksDB instance with 7 column families. One compaction budget, one memory pool, one thing to tune.

## Supported RPC methods

### Block / History
- `getBlock` — full block with transactions
- `getBlocks` — confirmed slots in a range
- `getBlocksWithLimit` — confirmed slots starting at a slot, limited count
- `getBlockHeight` — current block height by commitment
- `getBlockTime` — unix timestamp for a slot
- `getSlot` — current slot by commitment
- `getLatestBlockhash` — latest blockhash with validity window
- `isBlockhashValid` — check if a blockhash is still recent
- `getFirstAvailableBlock` — oldest slot still served from local storage

### Transactions
- `getTransaction` — transaction details by signature
- `getSignaturesForAddress` — transaction history for an address
- `getSignatureStatuses` — confirmation status of signatures
- `getTransactionsForAddress` — full decoded transactions for a wallet, auto-expanding owned token accounts (ATAs) in one paginated call

### Account State
- `getAccountInfo` — account data by pubkey
- `getMultipleAccounts` — batch account lookup
- `getProgramAccounts` — all accounts owned by a program
- `getBalance` — SOL balance for a pubkey

### Tokens
- `getTokenAccountsByOwner` — token accounts for a wallet
- `getTokenAccountsByDelegate` — delegated token accounts
- `getTokenSupply` — total supply of a mint
- `getTokenLargestAccounts` — largest holders of a mint

### DAS (Digital Asset Standard)
- `getAsset` — NFT/compressed NFT metadata
- `getAssetsByOwner` — assets owned by a wallet
- `getAssetsByCreator` — assets by creator address
- `getAssetsByGroup` — assets by collection/group
- `getAssetsByAuthority` — assets by authority
- `searchAssets` — full-text asset search
- `getAssetProof` — merkle proof for compressed NFTs

### Chain metadata
- `getVersion` — node version info
- `getGenesisHash` — cluster genesis hash
- `getIdentity` — node identity pubkey
- `getHealth` — service health
- `getEpochInfo` — current epoch / slot index / slots-in-epoch
- `getEpochSchedule` — epoch schedule constants
- `getMinimumBalanceForRentExemption` — rent-exempt minimum for a data length

### Forwarded
Methods like `sendTransaction` and `simulateTransaction` are forwarded to a configured upstream validator RPC.

## Storage layout

```
data/
├── rocksdb/              # Unified RocksDB (7 column families)
│   ├── slot_index        # slot → block metadata (time, height, hash)
│   ├── tx_index          # signature → transaction location
│   ├── sfa_index         # address + slot → signatures (prefix scan)
│   ├── accounts          # pubkey → serialized account data (all kinds)
│   ├── program_index     # program_id + pubkey → (prefix scan)
│   ├── owner_atas        # owner + token_account → (SPL ATA enumeration)
│   └── mint_top_holders  # mint → top-K (amount, token_account)
│
└── blocks/               # LZ4-compressed block files
    └── s{shard}/         # sharded by slot / 10000
        └── {slot}.blk
```

PostgreSQL stores relational data that benefits from SQL queries:
- `token_mints` — SPL token mint metadata
- `token_accounts` — SPL token account balances (indexed by owner, mint)
- `address_transactions` — address → transaction history with block times
- `slot_status` — slot commitment progression

## Configuration

```yaml
source:
  # gRPC endpoint (Yellowstone Dragon's Mouth or Richat). Set exactly one
  # of `endpoint` or `quic`.
  endpoint: "http://127.0.0.1:10000"
  grpc_mode: yellowstone                 # "yellowstone" (default) or "richat"
  # quic:                                # QUIC firehose (richat-client::quic)
  #   endpoint: "127.0.0.1:10101"
  x_token: ~                             # Optional auth token
  commitment: finalized
  max_message_size: 67108864             # 64MB
  snapshot_dir: "/tmp/light-rpc-snapshots"  # Local Solana snapshot dir for cold-start

storage:
  rocksdb:
    path: "data/rocksdb"
    write_buffer_size: 268435456         # 256MB
    max_open_files: -1                   # unlimited; required so compaction
                                         # can drain a large L0 backlog

  blocks:
    path: "data/blocks"
    compression: lz4
    max_stored_blocks: 500000

  postgres:
    url: "postgres://user:pass@localhost:5432/light_rpc"
    max_connections: 50

  pipeline:
    source_to_write: 2048                # Channel capacities
    write_to_read: 1024
    pg_write_buffer: 10000

rpc:
  endpoint: "0.0.0.0:8876"
  request_timeout_secs: 60
  upstream: ~                            # Optional validator RPC for forwarding
  forwarded_methods:
    - sendTransaction
    - simulateTransaction

metrics:
  endpoint: "0.0.0.0:9090"              # Prometheus metrics
```

See [config.example.yml](config.example.yml) for all options.

## Build

```bash
cargo build --release
```

The release binary is ~21MB (LTO + stripped).

## Run

```bash
# Validate config
./target/release/light-rpc --config config.yml --check

# Run
./target/release/light-rpc --config config.yml

# With debug logging
RUST_LOG=debug ./target/release/light-rpc --config config.yml
```

## Docker

```bash
docker build -t light-rpc .
docker run -v ./config.yml:/etc/light-rpc/config.yml light-rpc
```

## Metrics

Prometheus metrics are served on the configured metrics endpoint (default `:9090`):

| Metric | Type | Description |
|--------|------|-------------|
| `li_ingested_blocks_total` | counter | Total blocks ingested from gRPC |
| `li_ingested_accounts_total` | counter | Total account updates ingested |
| `li_ingested_txs_total` | counter | Total transactions ingested |
| `li_latest_slot{commitment}` | gauge | Latest slot by commitment level |
| `li_rpc_requests_total{method}` | counter | RPC requests by method |
| `li_rpc_errors_total{method}` | counter | RPC errors by method |
| `li_rpc_latency_seconds{method}` | histogram | RPC method latency |
| `li_storage_write_seconds` | histogram | Storage write latency per block |
| `li_pg_write_seconds` | histogram | PostgreSQL batch write latency |
| `li_memory_cached_blocks` | gauge | Blocks currently in memory cache |
| `li_pipeline_channel_size{channel}` | gauge | Pipeline channel utilization |
| `li_pg_drop_total{kind}` | counter | PG write jobs dropped (channel full) |
| `li_clickhouse_drop_total` | counter | ClickHouse write jobs dropped (channel full) |
| `li_source_malformed_total` | counter | Stream messages skipped (bad pubkey/signature) |
| `li_rocksdb_quarantined_total` | counter | Corrupt SSTs auto-quarantined |
| `li_rocksdb_l0_files{cf}` | gauge | L0 file count per column family |
| `li_rocksdb_live_data_bytes{cf}` | gauge | Live data size per column family |

## Project structure

```
src/
├── main.rs                # Entry point, pipeline orchestration
├── lib.rs                 # Crate root, module exports
├── config.rs              # YAML configuration types
├── types.rs               # Shared types (Slot, BlockWithData, AccountUpdate, etc.)
├── metrics.rs             # Prometheus metric definitions
├── source/
│   ├── stream.rs          # Richat gRPC subscription, block accumulation
│   └── commitment.rs      # Slot commitment state machine
├── storage/
│   ├── rocks.rs           # Unified RocksDB (7 column families)
│   ├── files.rs           # LZ4 block file storage
│   ├── postgres.rs        # PostgreSQL operations (tokens, migrations)
│   ├── accounts.rs        # Account classification and serialization
│   ├── write.rs           # Write worker (blocks + accounts → storage)
│   └── read.rs            # Read worker (memory cache + tiered lookup)
└── rpc/
    ├── server.rs          # Axum HTTP server, JSON-RPC dispatch
    ├── upstream.rs         # Upstream validator RPC forwarding
    └── methods/
        ├── blocks.rs      # getBlock, getSlot, getLatestBlockhash, ...
        ├── transactions.rs # getTransaction, getSignaturesForAddress, ...
        ├── accounts.rs    # getAccountInfo, getProgramAccounts, ...
        ├── tokens.rs      # getTokenAccountsByOwner, getTokenSupply, ...
        └── assets.rs      # getAsset, getAssetsByOwner, searchAssets, ...
```

## Requirements

- Rust stable (1.86+)
- PostgreSQL 14+
- A Richat/Yellowstone gRPC source (validator with geyser plugin)

## Hardware

light-rpc is much lighter than a full validator. A single mainnet node fully indexing all accounts, transactions, and blocks comfortably fits on:

| Resource | Minimum | Recommended |
|----------|---------|-------------|
| CPU      | 4 cores | 8 cores     |
| RAM      | 16 GB   | 32 GB       |
| Disk     | 500 GB NVMe SSD | 1 TB NVMe SSD |
| Network  | 1 Gbps  | 1 Gbps      |

Observed steady-state on a mainnet node (2-day retention, gRPC source, all account CFs populated): ~1–3 cores sustained, ~50 GB RES (most of it RocksDB block cache and mmap'd SSTs — the OS reclaims it under pressure), ~390 GB on-disk for RocksDB. PostgreSQL adds ~30 GB.

Disk grows roughly linearly with retention (`storage.blocks.max_stored_blocks`) and with how aggressively you keep `tx_index` / `sfa_index` history. Account CFs are bounded by chain state, not retention.

## License

AGPL-3.0-only. See [LICENSE](LICENSE) for the full text and [LICENSE.md](LICENSE.md) for licensing notes.
