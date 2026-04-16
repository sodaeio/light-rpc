use anyhow::{Context, Result};
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::ConnectOptions;
use std::str::FromStr;
use std::time::Duration;
use tracing::info;

use crate::config::PostgresConfig;
use crate::metrics;
use crate::types::*;

pub struct PgStorage {
    pool: PgPool,
}

impl PgStorage {
    pub async fn connect(config: &PostgresConfig) -> Result<Self> {
        let connect_opts = PgConnectOptions::from_str(&config.url)
            .context("parsing postgres url")?
            .application_name("light-indexer")
            .log_statements(tracing::log::LevelFilter::Trace)
            .log_slow_statements(
                tracing::log::LevelFilter::Warn,
                Duration::from_secs(1),
            );

        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.connect_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .max_lifetime(Duration::from_secs(1800))
            .after_connect(|conn, _meta| {
                Box::pin(async move {
                    sqlx::Executor::execute(
                        conn,
                        "SET statement_timeout = '60s';
                         SET lock_timeout = '10s';
                         SET idle_in_transaction_session_timeout = '5min';
                         SET jit = off;",
                    )
                    .await?;
                    Ok(())
                })
            })
            .connect_with(connect_opts)
            .await
            .context("connecting to postgres")?;

        info!(max_conn = config.max_connections, "connected to postgres");
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    pub async fn migrate(&self) -> Result<()> {
        let statements = [
            "CREATE TABLE IF NOT EXISTS slot_metas (
                slot BIGINT PRIMARY KEY
            )",
            "CREATE INDEX IF NOT EXISTS idx_slot_desc ON slot_metas(slot DESC)",

            "CREATE TABLE IF NOT EXISTS tokens (
                mint BYTEA PRIMARY KEY,
                supply NUMERIC(20,0) NOT NULL DEFAULT 0,
                decimals INTEGER NOT NULL DEFAULT 0,
                token_program BYTEA NOT NULL,
                mint_authority BYTEA,
                freeze_authority BYTEA,
                close_authority BYTEA,
                extension_data BYTEA,
                slot_updated BIGINT NOT NULL,
                extensions JSONB
            )",

            "CREATE TABLE IF NOT EXISTS token_accounts (
                pubkey BYTEA PRIMARY KEY,
                mint BYTEA NOT NULL,
                amount BIGINT NOT NULL DEFAULT 0,
                owner BYTEA NOT NULL,
                frozen BOOLEAN NOT NULL DEFAULT false,
                close_authority BYTEA,
                delegate BYTEA,
                delegated_amount BIGINT NOT NULL DEFAULT 0,
                slot_updated BIGINT NOT NULL,
                token_program BYTEA NOT NULL,
                lamports BIGINT NOT NULL DEFAULT 2039280,
                extensions JSONB
            )",
            "ALTER TABLE token_accounts ADD COLUMN IF NOT EXISTS lamports BIGINT NOT NULL DEFAULT 2039280",
            "CREATE INDEX IF NOT EXISTS idx_token_accounts_mint_owner ON token_accounts(mint, owner)",
            "ALTER TABLE token_accounts SET (fillfactor = 85)",
            "ALTER TABLE token_accounts SET (
                autovacuum_vacuum_scale_factor = 0.05,
                autovacuum_analyze_scale_factor = 0.02,
                autovacuum_vacuum_cost_delay = 10
            )",
            // idx_token_accounts_owner must be built manually — see MIGRATIONS.md.

        ];

        for stmt in statements {
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .with_context(|| format!("migration: {}", &stmt[..stmt.len().min(60)]))?;
        }

        self.migrate_address_transactions_partitioned().await?;
        self.bootstrap_address_partitions().await?;

        info!("postgres migrations complete");
        Ok(())
    }

    async fn bootstrap_address_partitions(&self) -> Result<()> {
        // Use slot_metas (has idx_slot_desc); MAX(slot) on the legacy
        // table is a full seq scan.
        let latest: Option<i64> = sqlx::query_scalar(
            "SELECT slot FROM slot_metas ORDER BY slot DESC LIMIT 1",
        )
        .fetch_optional(&self.pool)
        .await?
        .flatten();

        let seed_slot = latest.unwrap_or(413_000_000).max(0) as u64;
        let current_partition = (seed_slot as i64) / Self::SLOTS_PER_PARTITION;
        let start = (current_partition - 2).max(0);
        for p in start..=current_partition + 4 {
            let lower = p * Self::SLOTS_PER_PARTITION;
            let upper = (p + 1) * Self::SLOTS_PER_PARTITION;
            let name = format!("address_transactions_p{p:07}");
            let sql = format!(
                "CREATE TABLE IF NOT EXISTS {name}
                 PARTITION OF address_transactions
                 FOR VALUES FROM ({lower}) TO ({upper})"
            );
            sqlx::query(&sql).execute(&self.pool).await?;
        }

        info!(
            seed_slot,
            partitions_created = (current_partition + 4 - start + 1),
            "bootstrapped address_transactions partitions"
        );
        Ok(())
    }

    async fn migrate_address_transactions_partitioned(&self) -> Result<()> {
        let partitioned: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM pg_partitioned_table pt
                JOIN pg_class c ON c.oid = pt.partrelid
                WHERE c.relname = 'address_transactions'
            )",
        )
        .fetch_one(&self.pool)
        .await?;

        if partitioned {
            return Ok(());
        }

        let exists: bool = sqlx::query_scalar(
            "SELECT EXISTS(
                SELECT 1 FROM pg_tables
                WHERE tablename = 'address_transactions' AND schemaname = 'public'
            )",
        )
        .fetch_one(&self.pool)
        .await?;

        if exists {
            sqlx::query(
                "ALTER TABLE address_transactions RENAME TO address_transactions_legacy",
            )
            .execute(&self.pool)
            .await?;
            info!("renamed address_transactions to address_transactions_legacy");
        }

        sqlx::query(
            "CREATE TABLE address_transactions (
                address BYTEA NOT NULL,
                signature BYTEA NOT NULL,
                slot BIGINT NOT NULL,
                tx_index INTEGER,
                block_time BIGINT,
                err BOOLEAN NOT NULL DEFAULT false,
                balance_changed BOOLEAN NOT NULL DEFAULT false,
                post_balance BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (address, signature, slot)
            ) PARTITION BY RANGE (slot)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_addr_txn_address_slot
             ON address_transactions(address, slot DESC, tx_index DESC)
             INCLUDE (signature, block_time, err, balance_changed)",
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            "CREATE INDEX IF NOT EXISTS idx_addr_txn_slot_brin
             ON address_transactions USING BRIN (slot) WITH (pages_per_range = 16)",
        )
        .execute(&self.pool)
        .await?;

        info!("address_transactions is now partitioned by slot");
        Ok(())
    }

    /// ~1 week at 2.5 slots/sec.
    const SLOTS_PER_PARTITION: i64 = 1_500_000;

    pub async fn ensure_address_partitions(&self, current_slot: Slot, ahead: i64) -> Result<()> {
        let current_partition = (current_slot as i64) / Self::SLOTS_PER_PARTITION;
        for i in 0..=ahead {
            let p = current_partition + i;
            let lower = p * Self::SLOTS_PER_PARTITION;
            let upper = (p + 1) * Self::SLOTS_PER_PARTITION;
            let name = format!("address_transactions_p{p:07}");
            let sql = format!(
                "CREATE TABLE IF NOT EXISTS {name}
                 PARTITION OF address_transactions
                 FOR VALUES FROM ({lower}) TO ({upper})"
            );
            sqlx::query(&sql).execute(&self.pool).await?;
        }
        Ok(())
    }

    /// Returns names of partitions dropped.
    pub async fn drop_address_partitions_before(
        &self,
        cutoff_slot: Slot,
    ) -> Result<Vec<String>> {
        let cutoff = cutoff_slot as i64;
        let partitions: Vec<String> = sqlx::query_scalar(
            "SELECT c.relname FROM pg_class c
             JOIN pg_inherits i ON c.oid = i.inhrelid
             JOIN pg_class parent ON parent.oid = i.inhparent
             WHERE parent.relname = 'address_transactions'
               AND c.relkind = 'r'
             ORDER BY c.relname",
        )
        .fetch_all(&self.pool)
        .await?;

        let mut dropped = Vec::new();
        for name in partitions {
            let Some(rest) = name.strip_prefix("address_transactions_p") else {
                continue;
            };
            let Ok(p) = rest.parse::<i64>() else {
                continue;
            };
            let upper = (p + 1) * Self::SLOTS_PER_PARTITION;
            if upper <= cutoff {
                let sql = format!("DROP TABLE IF EXISTS {name}");
                sqlx::query(&sql).execute(&self.pool).await?;
                dropped.push(name);
            }
        }
        Ok(dropped)
    }

    // --- Write operations (called from pg_writer_loop) ---

    /// Batch upsert token mints using UNNEST arrays — single round-trip.
    pub async fn upsert_token_mints(&self, mints: &[AccountUpdate]) -> Result<()> {
        if mints.is_empty() {
            return Ok(());
        }
        let timer = metrics::PG_WRITE_LATENCY.start_timer();

        let mut p_mint: Vec<Vec<u8>> = Vec::with_capacity(mints.len());
        let mut p_supply: Vec<i64> = Vec::with_capacity(mints.len());
        let mut p_decimals: Vec<i32> = Vec::with_capacity(mints.len());
        let mut p_program: Vec<Vec<u8>> = Vec::with_capacity(mints.len());
        let mut p_mint_auth: Vec<Option<Vec<u8>>> = Vec::with_capacity(mints.len());
        let mut p_freeze_auth: Vec<Option<Vec<u8>>> = Vec::with_capacity(mints.len());
        let mut p_slot: Vec<i64> = Vec::with_capacity(mints.len());

        for m in mints {
            let supply = if m.data.len() >= 44 {
                u64::from_le_bytes(m.data[36..44].try_into().unwrap_or([0; 8]))
            } else {
                0
            };
            let decimals = if m.data.len() >= 45 {
                m.data[44] as i32
            } else {
                0
            };
            let mint_auth = if m.data.len() >= 36 && m.data[0] == 1 {
                Some(m.data[4..36].to_vec())
            } else {
                None
            };
            let freeze_auth = if m.data.len() >= 82 && m.data[46] == 1 {
                Some(m.data[50..82].to_vec())
            } else {
                None
            };

            p_mint.push(m.pubkey.as_ref().to_vec());
            p_supply.push(supply as i64);
            p_decimals.push(decimals);
            p_program.push(m.owner.as_ref().to_vec());
            p_mint_auth.push(mint_auth);
            p_freeze_auth.push(freeze_auth);
            p_slot.push(m.slot as i64);
        }

        sqlx::query(
            "INSERT INTO tokens (mint, supply, decimals, token_program, mint_authority, freeze_authority, slot_updated)
             SELECT * FROM UNNEST($1::bytea[], $2::bigint[], $3::int[], $4::bytea[], $5::bytea[], $6::bytea[], $7::bigint[])
             ON CONFLICT (mint) DO UPDATE SET
                 supply = EXCLUDED.supply,
                 decimals = EXCLUDED.decimals,
                 mint_authority = EXCLUDED.mint_authority,
                 freeze_authority = EXCLUDED.freeze_authority,
                 slot_updated = EXCLUDED.slot_updated
             WHERE EXCLUDED.slot_updated >= tokens.slot_updated",
        )
        .bind(&p_mint)
        .bind(&p_supply)
        .bind(&p_decimals)
        .bind(&p_program)
        .bind(&p_mint_auth)
        .bind(&p_freeze_auth)
        .bind(&p_slot)
        .execute(&self.pool)
        .await?;

        timer.observe_duration();
        Ok(())
    }

    /// Batch upsert token accounts using UNNEST arrays — single round-trip.
    pub async fn upsert_token_accounts(&self, accounts: &[AccountUpdate]) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }
        let timer = metrics::PG_WRITE_LATENCY.start_timer();

        let mut p_pubkey: Vec<Vec<u8>> = Vec::with_capacity(accounts.len());
        let mut p_mint: Vec<Vec<u8>> = Vec::with_capacity(accounts.len());
        let mut p_owner: Vec<Vec<u8>> = Vec::with_capacity(accounts.len());
        let mut p_amount: Vec<i64> = Vec::with_capacity(accounts.len());
        let mut p_frozen: Vec<bool> = Vec::with_capacity(accounts.len());
        let mut p_delegate: Vec<Option<Vec<u8>>> = Vec::with_capacity(accounts.len());
        let mut p_delegated_amount: Vec<i64> = Vec::with_capacity(accounts.len());
        let mut p_slot: Vec<i64> = Vec::with_capacity(accounts.len());
        let mut p_program: Vec<Vec<u8>> = Vec::with_capacity(accounts.len());
        let mut p_lamports: Vec<i64> = Vec::with_capacity(accounts.len());

        for a in accounts {
            let acct_mint = if a.data.len() >= 32 {
                a.data[..32].to_vec()
            } else {
                vec![0; 32]
            };
            let acct_owner = if a.data.len() >= 64 {
                a.data[32..64].to_vec()
            } else {
                vec![0; 32]
            };
            let amount = if a.data.len() >= 72 {
                u64::from_le_bytes(a.data[64..72].try_into().unwrap_or([0; 8]))
            } else {
                0
            };
            let has_delegate = a.data.len() >= 76
                && u32::from_le_bytes(a.data[72..76].try_into().unwrap_or([0; 4])) == 1;
            let delegate = if has_delegate && a.data.len() >= 108 {
                Some(a.data[76..108].to_vec())
            } else {
                None
            };
            let state = if a.data.len() >= 109 { a.data[108] } else { 0 };
            let delegated_amount = if a.data.len() >= 121 && has_delegate {
                u64::from_le_bytes(a.data[113..121].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            p_pubkey.push(a.pubkey.as_ref().to_vec());
            p_mint.push(acct_mint);
            p_owner.push(acct_owner);
            p_amount.push(amount as i64);
            p_frozen.push(state == 2);
            p_delegate.push(delegate);
            p_delegated_amount.push(delegated_amount as i64);
            p_slot.push(a.slot as i64);
            p_program.push(a.owner.as_ref().to_vec());
            p_lamports.push(a.lamports as i64);
        }

        sqlx::query(
            "INSERT INTO token_accounts (pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program, lamports)
             SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::bytea[], $4::bigint[], $5::bool[], $6::bytea[], $7::bigint[], $8::bigint[], $9::bytea[], $10::bigint[])
             ON CONFLICT (pubkey) DO UPDATE SET
                 mint = EXCLUDED.mint,
                 owner = EXCLUDED.owner,
                 amount = EXCLUDED.amount,
                 frozen = EXCLUDED.frozen,
                 delegate = EXCLUDED.delegate,
                 delegated_amount = EXCLUDED.delegated_amount,
                 slot_updated = EXCLUDED.slot_updated,
                 lamports = EXCLUDED.lamports
             WHERE EXCLUDED.slot_updated >= token_accounts.slot_updated",
        )
        .bind(&p_pubkey)
        .bind(&p_mint)
        .bind(&p_owner)
        .bind(&p_amount)
        .bind(&p_frozen)
        .bind(&p_delegate)
        .bind(&p_delegated_amount)
        .bind(&p_slot)
        .bind(&p_program)
        .bind(&p_lamports)
        .execute(&self.pool)
        .await?;

        timer.observe_duration();
        Ok(())
    }

    /// Batch insert address transactions using UNNEST — single round-trip.
    #[allow(clippy::type_complexity)]
    pub async fn insert_address_transactions(
        &self,
        entries: &[(
            solana_pubkey::Pubkey,
            Slot,
            solana_signature::Signature,
            Option<UnixTimestamp>,
            Option<String>,
        )],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        let mut p_addr: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
        let mut p_sig: Vec<Vec<u8>> = Vec::with_capacity(entries.len());
        let mut p_slot: Vec<i64> = Vec::with_capacity(entries.len());
        let mut p_time: Vec<Option<i64>> = Vec::with_capacity(entries.len());
        let mut p_err: Vec<bool> = Vec::with_capacity(entries.len());

        for (address, slot, signature, block_time, err) in entries {
            p_addr.push(address.as_ref().to_vec());
            p_sig.push(signature.as_ref().to_vec());
            p_slot.push(*slot as i64);
            p_time.push(*block_time);
            p_err.push(err.is_some());
        }

        sqlx::query(
            "INSERT INTO address_transactions (address, signature, slot, block_time, err)
             SELECT * FROM UNNEST($1::bytea[], $2::bytea[], $3::bigint[], $4::bigint[], $5::bool[])
             ON CONFLICT DO NOTHING",
        )
        .bind(&p_addr)
        .bind(&p_sig)
        .bind(&p_slot)
        .bind(&p_time)
        .bind(&p_err)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn upsert_slot(&self, slot: Slot) -> Result<()> {
        sqlx::query("INSERT INTO slot_metas (slot) VALUES ($1) ON CONFLICT DO NOTHING")
            .bind(slot as i64)
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // --- Read operations ---

    pub async fn get_latest_slot(&self) -> Result<Option<Slot>> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT MAX(slot) FROM slot_metas")
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.and_then(|(s,)| if s > 0 { Some(s as Slot) } else { None }))
    }

    pub async fn get_token_accounts_by_owner(&self, owner: &[u8]) -> Result<Vec<TokenAccountRow>> {
        let rows: Vec<TokenAccountRow> = sqlx::query_as(
            "SELECT pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program, lamports
             FROM token_accounts WHERE owner = $1 LIMIT 10000",
        )
        .bind(owner)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_token_accounts_by_mint(&self, mint: &[u8]) -> Result<Vec<TokenAccountRow>> {
        let rows: Vec<TokenAccountRow> = sqlx::query_as(
            "SELECT pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program, lamports
             FROM token_accounts WHERE mint = $1 LIMIT 10000",
        )
        .bind(mint)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_token_mint(&self, mint_pubkey: &[u8]) -> Result<Option<TokenMintRow>> {
        let row: Option<TokenMintRow> = sqlx::query_as(
            "SELECT mint, supply, decimals, token_program, mint_authority, freeze_authority, slot_updated
             FROM tokens WHERE mint = $1",
        )
        .bind(mint_pubkey)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_signatures_for_address(
        &self,
        address: &[u8],
        before_slot: Option<Slot>,
        limit: i64,
    ) -> Result<Vec<AddressTransactionRow>> {
        let rows: Vec<AddressTransactionRow> = if let Some(before) = before_slot {
            sqlx::query_as(
                "SELECT address, signature, slot, tx_index, block_time, err
                 FROM address_transactions
                 WHERE address = $1 AND slot < $2
                 ORDER BY slot DESC, tx_index DESC
                 LIMIT $3",
            )
            .bind(address)
            .bind(before as i64)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                "SELECT address, signature, slot, tx_index, block_time, err
                 FROM address_transactions
                 WHERE address = $1
                 ORDER BY slot DESC, tx_index DESC
                 LIMIT $2",
            )
            .bind(address)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows)
    }

    /// Guards against custodial wallets with millions of ATAs.
    const GTFA_ATA_CAP: i64 = 2048;

    pub async fn get_transactions_for_owner_with_atas(
        &self,
        owner: &[u8],
        before_slot: Option<Slot>,
        limit: i64,
    ) -> Result<Vec<AddressTransactionRow>> {
        let ata_pubkeys: Vec<Vec<u8>> = sqlx::query_scalar(
            "SELECT pubkey FROM token_accounts
             WHERE owner = $1
             LIMIT $2",
        )
        .bind(owner)
        .bind(Self::GTFA_ATA_CAP)
        .fetch_all(&self.pool)
        .await?;

        let mut addrs: Vec<Vec<u8>> = Vec::with_capacity(ata_pubkeys.len() + 1);
        addrs.push(owner.to_vec());
        addrs.extend(ata_pubkeys);

        let rows: Vec<AddressTransactionRow> = if let Some(before) = before_slot {
            sqlx::query_as(
                "SELECT address, signature, slot, tx_index, block_time, err
                 FROM address_transactions
                 WHERE address = ANY($1::bytea[]) AND slot < $2
                 ORDER BY slot DESC, tx_index DESC
                 LIMIT $3",
            )
            .bind(&addrs)
            .bind(before as i64)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                "SELECT address, signature, slot, tx_index, block_time, err
                 FROM address_transactions
                 WHERE address = ANY($1::bytea[])
                 ORDER BY slot DESC, tx_index DESC
                 LIMIT $2",
            )
            .bind(&addrs)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows)
    }

    pub async fn get_token_largest_accounts(
        &self,
        mint: &[u8],
        limit: i64,
    ) -> Result<Vec<TokenAccountRow>> {
        let rows: Vec<TokenAccountRow> = sqlx::query_as(
            "SELECT pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program, lamports
             FROM token_accounts WHERE mint = $1 ORDER BY amount DESC LIMIT $2",
        )
        .bind(mint)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_token_accounts_by_delegate(
        &self,
        delegate: &[u8],
    ) -> Result<Vec<TokenAccountRow>> {
        let rows: Vec<TokenAccountRow> = sqlx::query_as(
            "SELECT pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program, lamports
             FROM token_accounts WHERE delegate = $1 LIMIT 10000",
        )
        .bind(delegate)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_program_account_by_pubkey(
        &self,
        pubkey: &[u8],
    ) -> Result<Option<ProgramAccountRow>> {
        let row: Option<ProgramAccountRow> = sqlx::query_as(
            "SELECT pubkey, program_id, lamports, data, owner, executable, rent_epoch, slot_updated
             FROM program_accounts WHERE pubkey = $1",
        )
        .bind(pubkey)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_program_accounts_by_pubkeys(
        &self,
        pubkeys: &[Vec<u8>],
    ) -> Result<Vec<ProgramAccountRow>> {
        let rows: Vec<ProgramAccountRow> = sqlx::query_as(
            "SELECT pubkey, program_id, lamports, data, owner, executable, rent_epoch, slot_updated
             FROM program_accounts WHERE pubkey = ANY($1::bytea[])",
        )
        .bind(pubkeys)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_program_accounts_pg(
        &self,
        program_id: &[u8],
    ) -> Result<Vec<ProgramAccountRow>> {
        let rows: Vec<ProgramAccountRow> = sqlx::query_as(
            "SELECT pubkey, program_id, lamports, data, owner, executable, rent_epoch, slot_updated
             FROM program_accounts WHERE program_id = $1 LIMIT 1000",
        )
        .bind(program_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    // --- DAS Asset queries ---

    pub async fn get_asset(&self, id: &[u8]) -> Result<Option<AssetRow>> {
        let row: Option<AssetRow> = sqlx::query_as(
            "SELECT a.id, a.specification_asset_class::text, a.owner, a.delegate, a.frozen, a.supply,
                    a.compressed, a.tree_id, a.leaf, a.nonce, a.royalty_amount, a.burnt,
                    a.slot_updated, a.seq,
                    ad.chain_data, ad.metadata_url, ad.metadata, ad.raw_name, ad.raw_symbol
             FROM asset a
             LEFT JOIN asset_data ad ON a.asset_data = ad.id
             WHERE a.id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_asset_creators(&self, asset_id: &[u8]) -> Result<Vec<AssetCreatorRow>> {
        let rows: Vec<AssetCreatorRow> = sqlx::query_as(
            "SELECT asset_id, creator, share, verified, position
             FROM asset_creators WHERE asset_id = $1 ORDER BY position",
        )
        .bind(asset_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_asset_authority(&self, asset_id: &[u8]) -> Result<Option<AssetAuthorityRow>> {
        let row: Option<AssetAuthorityRow> = sqlx::query_as(
            "SELECT asset_id, authority, scopes FROM asset_authority WHERE asset_id = $1",
        )
        .bind(asset_id)
        .fetch_optional(&self.pool)
        .await?;
        Ok(row)
    }

    pub async fn get_asset_grouping(&self, asset_id: &[u8]) -> Result<Vec<AssetGroupingRow>> {
        let rows: Vec<AssetGroupingRow> = sqlx::query_as(
            "SELECT asset_id, group_key, group_value, verified FROM asset_grouping WHERE asset_id = $1",
        )
        .bind(asset_id)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_assets_by_owner(
        &self,
        owner: &[u8],
        page: i64,
        limit: i64,
    ) -> Result<(i64, Vec<AssetRow>)> {
        let count: (i64,) =
            sqlx::query_as("SELECT count(*) FROM asset WHERE owner = $1 AND burnt = false")
                .bind(owner)
                .fetch_one(&self.pool)
                .await?;
        let offset = (page - 1) * limit;
        let rows: Vec<AssetRow> = sqlx::query_as(
            "SELECT a.id, a.specification_asset_class::text, a.owner, a.delegate, a.frozen, a.supply,
                    a.compressed, a.tree_id, a.leaf, a.nonce, a.royalty_amount, a.burnt,
                    a.slot_updated, a.seq,
                    ad.chain_data, ad.metadata_url, ad.metadata, ad.raw_name, ad.raw_symbol
             FROM asset a
             LEFT JOIN asset_data ad ON a.asset_data = ad.id
             WHERE a.owner = $1 AND a.burnt = false
             ORDER BY a.slot_updated DESC
             LIMIT $2 OFFSET $3",
        )
        .bind(owner)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        Ok((count.0, rows))
    }

    pub async fn get_assets_by_creator(
        &self,
        creator: &[u8],
        page: i64,
        limit: i64,
    ) -> Result<(i64, Vec<AssetRow>)> {
        let count: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM asset_creators ac JOIN asset a ON a.id = ac.asset_id WHERE ac.creator = $1 AND a.burnt = false")
            .bind(creator)
            .fetch_one(&self.pool)
            .await?;
        let offset = (page - 1) * limit;
        let rows: Vec<AssetRow> = sqlx::query_as(
            "SELECT a.id, a.specification_asset_class::text, a.owner, a.delegate, a.frozen, a.supply,
                    a.compressed, a.tree_id, a.leaf, a.nonce, a.royalty_amount, a.burnt,
                    a.slot_updated, a.seq,
                    ad.chain_data, ad.metadata_url, ad.metadata, ad.raw_name, ad.raw_symbol
             FROM asset a
             JOIN asset_creators ac ON ac.asset_id = a.id
             LEFT JOIN asset_data ad ON a.asset_data = ad.id
             WHERE ac.creator = $1 AND a.burnt = false
             ORDER BY a.slot_updated DESC
             LIMIT $2 OFFSET $3",
        )
        .bind(creator)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        Ok((count.0, rows))
    }

    pub async fn get_assets_by_group(
        &self,
        group_key: &str,
        group_value: &str,
        page: i64,
        limit: i64,
    ) -> Result<(i64, Vec<AssetRow>)> {
        let count: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM asset_grouping ag JOIN asset a ON a.id = ag.asset_id
             WHERE ag.group_key = $1 AND ag.group_value = $2 AND a.burnt = false",
        )
        .bind(group_key)
        .bind(group_value)
        .fetch_one(&self.pool)
        .await?;
        let offset = (page - 1) * limit;
        let rows: Vec<AssetRow> = sqlx::query_as(
            "SELECT a.id, a.specification_asset_class::text, a.owner, a.delegate, a.frozen, a.supply,
                    a.compressed, a.tree_id, a.leaf, a.nonce, a.royalty_amount, a.burnt,
                    a.slot_updated, a.seq,
                    ad.chain_data, ad.metadata_url, ad.metadata, ad.raw_name, ad.raw_symbol
             FROM asset a
             JOIN asset_grouping ag ON ag.asset_id = a.id
             LEFT JOIN asset_data ad ON a.asset_data = ad.id
             WHERE ag.group_key = $1 AND ag.group_value = $2 AND a.burnt = false
             ORDER BY a.slot_updated DESC
             LIMIT $3 OFFSET $4",
        )
        .bind(group_key)
        .bind(group_value)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        Ok((count.0, rows))
    }

    pub async fn get_assets_by_authority(
        &self,
        authority: &[u8],
        page: i64,
        limit: i64,
    ) -> Result<(i64, Vec<AssetRow>)> {
        let count: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM asset_authority aa JOIN asset a ON a.id = aa.asset_id WHERE aa.authority = $1 AND a.burnt = false")
            .bind(authority)
            .fetch_one(&self.pool)
            .await?;
        let offset = (page - 1) * limit;
        let rows: Vec<AssetRow> = sqlx::query_as(
            "SELECT a.id, a.specification_asset_class::text, a.owner, a.delegate, a.frozen, a.supply,
                    a.compressed, a.tree_id, a.leaf, a.nonce, a.royalty_amount, a.burnt,
                    a.slot_updated, a.seq,
                    ad.chain_data, ad.metadata_url, ad.metadata, ad.raw_name, ad.raw_symbol
             FROM asset a
             JOIN asset_authority aa ON aa.asset_id = a.id
             LEFT JOIN asset_data ad ON a.asset_data = ad.id
             WHERE aa.authority = $1 AND a.burnt = false
             ORDER BY a.slot_updated DESC
             LIMIT $2 OFFSET $3",
        )
        .bind(authority)
        .bind(limit)
        .bind(offset)
        .fetch_all(&self.pool)
        .await?;
        Ok((count.0, rows))
    }

    pub async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("health check failed")?;
        Ok(())
    }
}

impl Clone for PgStorage {
    fn clone(&self) -> Self {
        Self {
            pool: self.pool.clone(),
        }
    }
}

// Row types matching the DAS schema

#[derive(sqlx::FromRow)]
pub struct TokenAccountRow {
    pub pubkey: Vec<u8>,
    pub mint: Vec<u8>,
    pub owner: Vec<u8>,
    pub amount: i64,
    pub frozen: bool,
    pub delegate: Option<Vec<u8>>,
    pub delegated_amount: i64,
    pub slot_updated: i64,
    pub token_program: Vec<u8>,
    pub lamports: i64,
}

#[derive(sqlx::FromRow)]
pub struct TokenMintRow {
    pub mint: Vec<u8>,
    pub supply: bigdecimal::BigDecimal,
    pub decimals: i32,
    pub token_program: Vec<u8>,
    pub mint_authority: Option<Vec<u8>>,
    pub freeze_authority: Option<Vec<u8>>,
    pub slot_updated: i64,
}

#[derive(sqlx::FromRow)]
pub struct ProgramAccountRow {
    pub pubkey: Vec<u8>,
    pub program_id: Vec<u8>,
    pub lamports: i64,
    pub data: Vec<u8>,
    pub owner: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: i64,
    pub slot_updated: i64,
}

#[derive(sqlx::FromRow)]
pub struct AssetRow {
    pub id: Vec<u8>,
    pub specification_asset_class: Option<String>,
    pub owner: Option<Vec<u8>>,
    pub delegate: Option<Vec<u8>>,
    pub frozen: bool,
    pub supply: bigdecimal::BigDecimal,
    pub compressed: bool,
    pub tree_id: Option<Vec<u8>>,
    pub leaf: Option<Vec<u8>>,
    pub nonce: Option<i64>,
    pub royalty_amount: i32,
    pub burnt: bool,
    pub slot_updated: Option<i64>,
    pub seq: Option<i64>,
    // from asset_data join
    pub chain_data: Option<serde_json::Value>,
    pub metadata_url: Option<String>,
    pub metadata: Option<serde_json::Value>,
    pub raw_name: Option<Vec<u8>>,
    pub raw_symbol: Option<Vec<u8>>,
}

#[derive(sqlx::FromRow)]
pub struct AssetCreatorRow {
    pub asset_id: Vec<u8>,
    pub creator: Vec<u8>,
    pub share: i32,
    pub verified: bool,
    pub position: i16,
}

#[derive(sqlx::FromRow)]
pub struct AssetAuthorityRow {
    pub asset_id: Vec<u8>,
    pub authority: Vec<u8>,
    pub scopes: Option<Vec<String>>,
}

#[derive(sqlx::FromRow)]
pub struct AssetGroupingRow {
    pub asset_id: Vec<u8>,
    pub group_key: String,
    pub group_value: Option<String>,
    pub verified: bool,
}

#[derive(sqlx::FromRow)]
pub struct AddressTransactionRow {
    pub address: Vec<u8>,
    pub signature: Vec<u8>,
    pub slot: i64,
    pub tx_index: Option<i32>,
    pub block_time: Option<i64>,
    pub err: bool,
}
