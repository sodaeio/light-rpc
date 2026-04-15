# Testing light-indexer

Commands for running the indexer locally and exercising the JSON-RPC surface, with a focus on the new `getTransactionsForAddress` method.

## 1. Build and run

```bash
cd /media/LocalDisk/servers/nodes-project/light-indexer

# Quick compile check
cargo check

# Debug build (fast compile, slow runtime)
cargo run -- --config config.yml

# Release build (slow compile, fast runtime — use for benchmarking)
cargo build --release
./target/release/light-indexer --config config.yml

# With debug logging
RUST_LOG=light_indexer=debug ./target/release/light-indexer --config config.yml

# Validate config without running
./target/release/light-indexer --config config.yml --check
```

Default ports (from `config.yml`):
- RPC: `0.0.0.0:8876`
- Metrics: `0.0.0.0:9090`

```bash
export RPC=http://127.0.0.1:8876
```

## 2. Sanity checks

```bash
curl -s $RPC -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getHealth","params":[]
}' | jq

curl -s $RPC -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getSlot","params":[]
}' | jq

curl -s $RPC -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getVersion","params":[]
}' | jq

curl -s $RPC -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getLatestBlockhash","params":[]
}' | jq
```

## 3. Pick a wallet to test against

Query Postgres directly for an owner that has both token accounts and transaction history:

```bash
psql "postgres://solanadb:solanadb@localhost:5432/solanadb" <<'SQL'
SELECT encode(ta.owner, 'hex')    AS owner_hex,
       COUNT(DISTINCT ta.pubkey)  AS ata_count,
       COUNT(at.signature)        AS direct_txns
FROM token_accounts ta
LEFT JOIN address_transactions at ON at.address = ta.owner
GROUP BY ta.owner
HAVING COUNT(DISTINCT ta.pubkey) > 2
ORDER BY direct_txns DESC NULLS LAST
LIMIT 5;
SQL
```

Convert a hex owner to base58 (Python one-liner):

```bash
python3 -c "import base58; print(base58.b58encode(bytes.fromhex('HEX_HERE')).decode())"
```

Then:

```bash
export WALLET=REPLACE_WITH_BASE58_PUBKEY
```

## 4. getTransactionsForAddress

```bash
# Latest 100 txns for owner + all owned ATAs
curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransactionsForAddress\",
  \"params\":[\"$WALLET\",{\"limit\":100}]
}" | jq '.result | length, .result[0]'

# Paginated — pass last slot from previous response as `before`
curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransactionsForAddress\",
  \"params\":[\"$WALLET\",{\"limit\":50,\"before\":SLOT_FROM_PREVIOUS}]
}" | jq '.result[] | {sig:.signature, slot, status:.confirmationStatus}'

# Max limit (clamps to 10_000)
curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransactionsForAddress\",
  \"params\":[\"$WALLET\",{\"limit\":10000}]
}" | jq '.result | length'

# Error: invalid encoding
curl -s $RPC -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getTransactionsForAddress","params":["notabase58",{}]
}' | jq

# Error: missing address
curl -s $RPC -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getTransactionsForAddress","params":[]
}' | jq
```

Expected response shape:

```json
{
  "jsonrpc": "2.0",
  "result": [
    {
      "signature": "5x...",
      "slot": 123456789,
      "blockTime": 1713100000,
      "err": false,
      "confirmationStatus": "finalized",
      "transaction": { "...full decoded transaction..." }
    }
  ],
  "id": 1
}
```

## 5. Prove the N+1 win vs. the classic pattern

```bash
# OLD WAY: getSignaturesForAddress then N× getTransaction
time (
  SIGS=$(curl -s $RPC -H 'content-type: application/json' -d "{
    \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSignaturesForAddress\",
    \"params\":[\"$WALLET\",{\"limit\":100}]
  }" | jq -r '.result[].signature')
  for s in $SIGS; do
    curl -s $RPC -H 'content-type: application/json' -d "{
      \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransaction\",\"params\":[\"$s\"]
    }" > /dev/null
  done
)

# NEW WAY: one call
time curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransactionsForAddress\",
  \"params\":[\"$WALLET\",{\"limit\":100}]
}" > /dev/null
```

## 6. Verify ATA expansion

```bash
DIRECT=$(curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSignaturesForAddress\",
  \"params\":[\"$WALLET\",{\"limit\":10000}]
}" | jq '.result | length')

EXPANDED=$(curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTransactionsForAddress\",
  \"params\":[\"$WALLET\",{\"limit\":10000}]
}" | jq '.result | length')

echo "getSignaturesForAddress (owner only): $DIRECT"
echo "getTransactionsForAddress  (owner + ATAs): $EXPANDED"
```

If the wallet has token activity, `EXPANDED` should be `>= DIRECT`.

## 7. Related methods (regression sanity)

```bash
# getTransaction — single signature
curl -s $RPC -H 'content-type: application/json' -d '{
  "jsonrpc":"2.0","id":1,"method":"getTransaction","params":["SIGNATURE_HERE"]
}' | jq

# getSignaturesForAddress
curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSignaturesForAddress\",
  \"params\":[\"$WALLET\",{\"limit\":10}]
}" | jq

# getAccountInfo
curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAccountInfo\",
  \"params\":[\"$WALLET\",{\"encoding\":\"base64\"}]
}" | jq

# getBalance
curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getBalance\",\"params\":[\"$WALLET\"]
}" | jq

# getTokenAccountsByOwner
curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getTokenAccountsByOwner\",
  \"params\":[\"$WALLET\",{\"programId\":\"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA\"},{\"encoding\":\"jsonParsed\"}]
}" | jq

# getAssetsByOwner (DAS)
curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getAssetsByOwner\",
  \"params\":{\"ownerAddress\":\"$WALLET\",\"page\":1,\"limit\":50}
}" | jq '.result.items | length'
```

## 8. Observability

```bash
# Prometheus metrics
curl -s http://127.0.0.1:9090/metrics | grep -E "li_(rpc|ingested|latest_slot)"

# Watch ingestion lag live
watch -n 1 'curl -s http://127.0.0.1:9090/metrics | grep -E "li_latest_slot|li_ingested_blocks_total"'

# Per-method RPC latency histogram
curl -s http://127.0.0.1:9090/metrics | grep 'li_rpc_latency_seconds.*method="getTransactionsForAddress"'
```

## 9. Load test

```bash
# Install oha (faster than wrk for JSON-RPC, one-liner config)
cargo install oha

# Save the payload once
cat > /tmp/gtfa.json <<EOF
{"jsonrpc":"2.0","id":1,"method":"getTransactionsForAddress","params":["$WALLET",{"limit":100}]}
EOF

# 30 seconds, 50 concurrent
oha -z 30s -c 50 -m POST -T application/json -d "$(cat /tmp/gtfa.json)" $RPC
```

## 10. Compare vs. a public endpoint (for launch demo)

```bash
# light-indexer
time curl -s $RPC -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSignaturesForAddress\",
  \"params\":[\"$WALLET\",{\"limit\":1000}]
}" | jq '.result | length'

# Public Solana RPC (baseline)
time curl -s https://api.mainnet-beta.solana.com -H 'content-type: application/json' -d "{
  \"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"getSignaturesForAddress\",
  \"params\":[\"$WALLET\",{\"limit\":1000}]
}" | jq '.result | length'
```

Record both terminals side by side — this is the footage for the launch video.
