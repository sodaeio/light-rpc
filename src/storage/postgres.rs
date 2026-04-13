use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::time::Duration;
use tracing::info;

use crate::config::PostgresConfig;
use crate::types::*;
use crate::metrics;

pub struct PgStorage {
    pool: PgPool,
}

impl PgStorage {
    pub async fn connect(config: &PostgresConfig) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.connect_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .connect(&config.url)
            .await
            .context("connecting to postgres")?;

        info!(
            max_conn = config.max_connections,
            "connected to postgres"
        );

        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Run schema migrations to ensure tables exist.
    pub async fn migrate(&self) -> Result<()> {
        let statements = [
            "CREATE TABLE IF NOT EXISTS slot_status (
                slot BIGINT PRIMARY KEY,
                parent_slot BIGINT,
                block_time BIGINT,
                status SMALLINT NOT NULL DEFAULT 0
            )",
            "CREATE TABLE IF NOT EXISTS token_mints (
                pubkey BYTEA PRIMARY KEY,
                slot BIGINT NOT NULL,
                supply BIGINT,
                decimals SMALLINT,
                mint_authority BYTEA,
                freeze_authority BYTEA,
                raw_data BYTEA NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            "CREATE TABLE IF NOT EXISTS token_accounts (
                pubkey BYTEA PRIMARY KEY,
                slot BIGINT NOT NULL,
                mint BYTEA NOT NULL,
                owner BYTEA NOT NULL,
                amount BIGINT NOT NULL,
                delegate BYTEA,
                state SMALLINT NOT NULL DEFAULT 0,
                raw_data BYTEA NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )",
            "CREATE TABLE IF NOT EXISTS address_transactions (
                address BYTEA NOT NULL,
                slot BIGINT NOT NULL,
                signature BYTEA NOT NULL,
                block_time BIGINT,
                err TEXT,
                PRIMARY KEY (address, slot, signature)
            )",
            "CREATE INDEX IF NOT EXISTS idx_token_accounts_owner ON token_accounts(owner)",
            "CREATE INDEX IF NOT EXISTS idx_token_accounts_mint ON token_accounts(mint)",
            "CREATE INDEX IF NOT EXISTS idx_address_transactions_slot ON address_transactions(address, slot DESC)",
        ];

        for stmt in statements {
            sqlx::query(stmt).execute(&self.pool).await
                .with_context(|| format!("migration failed: {}", &stmt[..stmt.len().min(60)]))?;
        }

        info!("postgres migrations complete");
        Ok(())
    }

    /// Batch upsert token mints.
    pub async fn upsert_token_mints(&self, mints: &[AccountUpdate]) -> Result<()> {
        if mints.is_empty() {
            return Ok(());
        }

        let timer = metrics::PG_WRITE_LATENCY.start_timer();

        for mint in mints {
            sqlx::query(
                r#"
                INSERT INTO token_mints (pubkey, slot, raw_data)
                VALUES ($1, $2, $3)
                ON CONFLICT (pubkey) DO UPDATE SET
                    slot = EXCLUDED.slot,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = NOW()
                WHERE EXCLUDED.slot >= token_mints.slot
                "#,
            )
            .bind(mint.pubkey.as_ref())
            .bind(mint.slot as i64)
            .bind(&mint.data)
            .execute(&self.pool)
            .await?;
        }

        timer.observe_duration();
        Ok(())
    }

    /// Batch upsert token accounts.
    pub async fn upsert_token_accounts(&self, accounts: &[AccountUpdate]) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }

        let timer = metrics::PG_WRITE_LATENCY.start_timer();

        for account in accounts {
            // Extract mint and owner from SPL token account data layout
            let mint = if account.data.len() >= 32 {
                &account.data[..32]
            } else {
                &[0u8; 32]
            };
            let owner = if account.data.len() >= 64 {
                &account.data[32..64]
            } else {
                &[0u8; 32]
            };
            let amount = if account.data.len() >= 72 {
                u64::from_le_bytes(account.data[64..72].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            sqlx::query(
                r#"
                INSERT INTO token_accounts (pubkey, slot, mint, owner, amount, raw_data)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (pubkey) DO UPDATE SET
                    slot = EXCLUDED.slot,
                    mint = EXCLUDED.mint,
                    owner = EXCLUDED.owner,
                    amount = EXCLUDED.amount,
                    raw_data = EXCLUDED.raw_data,
                    updated_at = NOW()
                WHERE EXCLUDED.slot >= token_accounts.slot
                "#,
            )
            .bind(account.pubkey.as_ref())
            .bind(account.slot as i64)
            .bind(mint)
            .bind(owner)
            .bind(amount as i64)
            .bind(&account.data)
            .execute(&self.pool)
            .await?;
        }

        timer.observe_duration();
        Ok(())
    }

    /// Batch insert address-transaction mappings.
    pub async fn insert_address_transactions(
        &self,
        entries: &[(solana_pubkey::Pubkey, Slot, solana_signature::Signature, Option<UnixTimestamp>, Option<String>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        for (address, slot, signature, block_time, err) in entries {
            sqlx::query(
                r#"
                INSERT INTO address_transactions (address, slot, signature, block_time, err)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(address.as_ref())
            .bind(*slot as i64)
            .bind(signature.as_ref())
            .bind(block_time.map(|t| t))
            .bind(err.as_deref())
            .execute(&self.pool)
            .await?;
        }

        Ok(())
    }

    /// Update slot status tracking.
    pub async fn upsert_slot(&self, meta: &SlotMeta) -> Result<()> {
        sqlx::query(
            r#"
            INSERT INTO slot_status (slot, parent_slot, block_time, status)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (slot) DO UPDATE SET
                block_time = COALESCE(EXCLUDED.block_time, slot_status.block_time),
                status = GREATEST(EXCLUDED.status, slot_status.status)
            "#,
        )
        .bind(meta.slot as i64)
        .bind(meta.parent_slot as i64)
        .bind(meta.block_time)
        .bind(meta.status as i16)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    /// Get latest known slot.
    pub async fn get_latest_slot(&self) -> Result<Option<Slot>> {
        let row: Option<(i64,)> = sqlx::query_as(
            "SELECT MAX(slot) FROM slot_status WHERE status >= 2", // confirmed+
        )
        .fetch_optional(&self.pool)
        .await?;
        Ok(row.map(|(s,)| s as Slot))
    }

    /// Get token accounts by owner.
    pub async fn get_token_accounts_by_owner(
        &self,
        owner: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows: Vec<(Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT pubkey, raw_data FROM token_accounts WHERE owner = $1",
        )
        .bind(owner)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Get token accounts by mint.
    pub async fn get_token_accounts_by_mint(
        &self,
        mint: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let rows: Vec<(Vec<u8>, Vec<u8>)> = sqlx::query_as(
            "SELECT pubkey, raw_data FROM token_accounts WHERE mint = $1",
        )
        .bind(mint)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    /// Get transactions for an address.
    pub async fn get_transactions_for_address(
        &self,
        address: &[u8],
        before_slot: Option<Slot>,
        limit: i64,
    ) -> Result<Vec<(i64, Vec<u8>, Option<i64>, Option<String>)>> {
        let rows = if let Some(before) = before_slot {
            sqlx::query_as(
                r#"
                SELECT slot, signature, block_time, err
                FROM address_transactions
                WHERE address = $1 AND slot < $2
                ORDER BY slot DESC
                LIMIT $3
                "#,
            )
            .bind(address)
            .bind(before as i64)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        } else {
            sqlx::query_as(
                r#"
                SELECT slot, signature, block_time, err
                FROM address_transactions
                WHERE address = $1
                ORDER BY slot DESC
                LIMIT $2
                "#,
            )
            .bind(address)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?
        };
        Ok(rows)
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
