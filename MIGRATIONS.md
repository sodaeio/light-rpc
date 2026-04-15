# Migrations & Manual Maintenance

Most migrations run automatically at startup in `PgStorage::migrate()`. A few operations are **too expensive to run inside a startup transaction** on large deployments and must be applied manually with `CREATE INDEX CONCURRENTLY` or scheduled maintenance windows.

## Manual: owner-only covering index on `token_accounts`

**Why it's not in auto-migrate:** on deployments with 100M+ token accounts, building this index takes 30-60 minutes and holds a ShareLock on the table. Running it inside startup migrations blocks the indexer from accepting traffic and triggers the `statement_timeout = 60s` connection guard.

**What it does:** serves `getTokenAccountsByOwner` without a heap fetch. The existing `(mint, owner)` index can't be used when the query leads with owner alone.

**Apply it:**

```bash
# Run as the indexer's PG user, OUTSIDE a transaction.
psql "postgres://solanadb:solanapwd@localhost:5432/solanadb" <<'SQL'
SET statement_timeout = 0;
SET lock_timeout = 0;
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_token_accounts_owner
    ON token_accounts(owner)
    INCLUDE (mint, amount, slot_updated);
SQL
```

Run during low traffic. The indexer will keep serving reads the whole time; writes are briefly blocked during the final MERGE phase.

Verify after:

```sql
SELECT indexrelid::regclass, indisvalid, pg_size_pretty(pg_relation_size(indexrelid))
FROM pg_index WHERE indexrelid = 'idx_token_accounts_owner'::regclass;
```

## Manual: drop legacy `address_transactions_legacy`

After the partitioning migration has been running 30+ days (your retention window), the legacy non-partitioned table holds nothing newer than the retention cutoff. Drop it to reclaim disk:

```sql
DROP TABLE IF EXISTS address_transactions_legacy CASCADE;
```

On a 2TB table this takes ~1 minute and frees the same amount of disk.

## Manual: `pg_repack token_accounts`

Run once to reclaim accumulated bloat from before `FILLFACTOR=85` was applied. Needs the `pg_repack` extension. Alternative: `VACUUM FULL` (requires downtime).

```bash
pg_repack -d solanadb -t token_accounts --no-superuser-check
```

## Automatic (run by light-indexer on startup)

These are cheap and safe to run every start:

- `CREATE TABLE IF NOT EXISTS` for all schemas
- Index creation on **empty** (new) tables like partitions
- `ALTER TABLE ... SET (fillfactor = N)` — metadata only
- `ALTER TABLE ... SET (autovacuum_*)` — metadata only
- `ALTER TABLE address_transactions RENAME TO address_transactions_legacy` — metadata only, one-time
- BRIN index on the new empty partitioned `address_transactions`

## Connection-level guards

Every connection gets these via `after_connect`:

- `statement_timeout = 60s`
- `lock_timeout = 10s`
- `idle_in_transaction_session_timeout = 5min`
- `jit = off` (JIT hurts more than helps on our query patterns)
- `application_name = light-indexer`

If you're running a long maintenance query (`CREATE INDEX CONCURRENTLY`, `VACUUM FULL`, `REINDEX`), set `SET statement_timeout = 0; SET lock_timeout = 0;` at the start of the psql session — these only affect that session.
