use anyhow::{Context, Result};
use sqlx::postgres::{PgPool, PgPoolOptions};
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
        let pool = PgPoolOptions::new()
            .max_connections(config.max_connections)
            .min_connections(config.min_connections)
            .acquire_timeout(Duration::from_secs(config.connect_timeout_secs))
            .idle_timeout(Duration::from_secs(config.idle_timeout_secs))
            .connect(&config.url)
            .await
            .context("connecting to postgres")?;

        info!(max_conn = config.max_connections, "connected to postgres");
        Ok(Self { pool })
    }

    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Create tables matching the existing DAS schema (solanadb).
    /// Safe to run against an already-populated database — all IF NOT EXISTS.
    pub async fn migrate(&self) -> Result<()> {
        let statements = [
            // Slot tracking
            "CREATE TABLE IF NOT EXISTS slot_metas (
                slot BIGINT PRIMARY KEY
            )",
            "CREATE INDEX IF NOT EXISTS idx_slot_desc ON slot_metas(slot DESC)",

            // Token mints — matches DAS `tokens` table
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

            // Token accounts — matches DAS `token_accounts` table
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
                extensions JSONB
            )",
            "CREATE INDEX IF NOT EXISTS idx_token_accounts_mint_owner ON token_accounts(mint, owner)",

            // Address transactions — matches DAS `address_transactions` table
            "CREATE TABLE IF NOT EXISTS address_transactions (
                address BYTEA NOT NULL,
                signature BYTEA NOT NULL,
                slot BIGINT NOT NULL,
                tx_index INTEGER,
                block_time BIGINT,
                err BOOLEAN NOT NULL DEFAULT false,
                balance_changed BOOLEAN NOT NULL DEFAULT false,
                post_balance BIGINT NOT NULL DEFAULT 0,
                PRIMARY KEY (address, signature)
            )",
            "CREATE INDEX IF NOT EXISTS idx_addr_txn_address_slot
                ON address_transactions(address, slot DESC, tx_index DESC)
                INCLUDE (signature, block_time, err, balance_changed)",
        ];

        for stmt in statements {
            sqlx::query(stmt)
                .execute(&self.pool)
                .await
                .with_context(|| format!("migration: {}", &stmt[..stmt.len().min(60)]))?;
        }

        info!("postgres migrations complete");
        Ok(())
    }

    // --- Write operations (called from pg_writer_loop) ---

    pub async fn upsert_token_mints(&self, mints: &[AccountUpdate]) -> Result<()> {
        if mints.is_empty() {
            return Ok(());
        }
        let timer = metrics::PG_WRITE_LATENCY.start_timer();

        for mint in mints {
            // SPL mint layout: [mint_authority(36) | supply(8) | decimals(1) | ...]
            let supply = if mint.data.len() >= 44 {
                u64::from_le_bytes(mint.data[36..44].try_into().unwrap_or([0; 8]))
            } else {
                0
            };
            let decimals = if mint.data.len() >= 45 {
                mint.data[44] as i32
            } else {
                0
            };
            let mint_authority = if mint.data.len() >= 36 && mint.data[0] == 1 {
                Some(&mint.data[4..36])
            } else {
                None
            };
            let freeze_authority = if mint.data.len() >= 82 && mint.data[46] == 1 {
                Some(&mint.data[50..82])
            } else {
                None
            };

            sqlx::query(
                "INSERT INTO tokens (mint, supply, decimals, token_program, mint_authority, freeze_authority, slot_updated)
                 VALUES ($1, $2, $3, $4, $5, $6, $7)
                 ON CONFLICT (mint) DO UPDATE SET
                     supply = EXCLUDED.supply,
                     decimals = EXCLUDED.decimals,
                     mint_authority = EXCLUDED.mint_authority,
                     freeze_authority = EXCLUDED.freeze_authority,
                     slot_updated = EXCLUDED.slot_updated
                 WHERE EXCLUDED.slot_updated >= tokens.slot_updated",
            )
            .bind(mint.pubkey.as_ref())
            .bind(supply as i64)
            .bind(decimals)
            .bind(mint.owner.as_ref())
            .bind(mint_authority)
            .bind(freeze_authority)
            .bind(mint.slot as i64)
            .execute(&self.pool)
            .await?;
        }

        timer.observe_duration();
        Ok(())
    }

    pub async fn upsert_token_accounts(&self, accounts: &[AccountUpdate]) -> Result<()> {
        if accounts.is_empty() {
            return Ok(());
        }
        let timer = metrics::PG_WRITE_LATENCY.start_timer();

        for account in accounts {
            // SPL token account layout: [mint(32) | owner(32) | amount(8) | delegate_option(4) | delegate(32) | state(1) | ...]
            let acct_mint = if account.data.len() >= 32 { &account.data[..32] } else { &[0u8; 32][..] };
            let acct_owner = if account.data.len() >= 64 { &account.data[32..64] } else { &[0u8; 32][..] };
            let amount = if account.data.len() >= 72 {
                u64::from_le_bytes(account.data[64..72].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            let has_delegate = account.data.len() >= 76 && u32::from_le_bytes(account.data[72..76].try_into().unwrap_or([0; 4])) == 1;
            let delegate = if has_delegate && account.data.len() >= 108 {
                Some(&account.data[76..108])
            } else {
                None
            };

            let state = if account.data.len() >= 109 { account.data[108] } else { 0 };
            let frozen = state == 2;

            let delegated_amount = if account.data.len() >= 121 && has_delegate {
                u64::from_le_bytes(account.data[113..121].try_into().unwrap_or([0; 8]))
            } else {
                0
            };

            sqlx::query(
                "INSERT INTO token_accounts (pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program)
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
                 ON CONFLICT (pubkey) DO UPDATE SET
                     mint = EXCLUDED.mint,
                     owner = EXCLUDED.owner,
                     amount = EXCLUDED.amount,
                     frozen = EXCLUDED.frozen,
                     delegate = EXCLUDED.delegate,
                     delegated_amount = EXCLUDED.delegated_amount,
                     slot_updated = EXCLUDED.slot_updated
                 WHERE EXCLUDED.slot_updated >= token_accounts.slot_updated",
            )
            .bind(account.pubkey.as_ref())
            .bind(acct_mint)
            .bind(acct_owner)
            .bind(amount as i64)
            .bind(frozen)
            .bind(delegate)
            .bind(delegated_amount as i64)
            .bind(account.slot as i64)
            .bind(account.owner.as_ref())
            .execute(&self.pool)
            .await?;
        }

        timer.observe_duration();
        Ok(())
    }

    pub async fn insert_address_transactions(
        &self,
        entries: &[(solana_pubkey::Pubkey, Slot, solana_signature::Signature, Option<UnixTimestamp>, Option<String>)],
    ) -> Result<()> {
        if entries.is_empty() {
            return Ok(());
        }

        for (address, slot, signature, block_time, err) in entries {
            let has_err = err.is_some();
            sqlx::query(
                "INSERT INTO address_transactions (address, signature, slot, block_time, err)
                 VALUES ($1, $2, $3, $4, $5)
                 ON CONFLICT DO NOTHING",
            )
            .bind(address.as_ref())
            .bind(signature.as_ref())
            .bind(*slot as i64)
            .bind(*block_time)
            .bind(has_err)
            .execute(&self.pool)
            .await?;
        }

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
        let row: Option<(i64,)> =
            sqlx::query_as("SELECT MAX(slot) FROM slot_metas")
                .fetch_optional(&self.pool)
                .await?;
        Ok(row.and_then(|(s,)| if s > 0 { Some(s as Slot) } else { None }))
    }

    pub async fn get_token_accounts_by_owner(&self, owner: &[u8]) -> Result<Vec<TokenAccountRow>> {
        let rows: Vec<TokenAccountRow> = sqlx::query_as(
            "SELECT pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program
             FROM token_accounts WHERE owner = $1",
        )
        .bind(owner)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_token_accounts_by_mint(&self, mint: &[u8]) -> Result<Vec<TokenAccountRow>> {
        let rows: Vec<TokenAccountRow> = sqlx::query_as(
            "SELECT pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program
             FROM token_accounts WHERE mint = $1",
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

    pub async fn get_token_largest_accounts(&self, mint: &[u8], limit: i64) -> Result<Vec<TokenAccountRow>> {
        let rows: Vec<TokenAccountRow> = sqlx::query_as(
            "SELECT pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program
             FROM token_accounts WHERE mint = $1 ORDER BY amount DESC LIMIT $2",
        )
        .bind(mint)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_token_accounts_by_delegate(&self, delegate: &[u8]) -> Result<Vec<TokenAccountRow>> {
        let rows: Vec<TokenAccountRow> = sqlx::query_as(
            "SELECT pubkey, mint, owner, amount, frozen, delegate, delegated_amount, slot_updated, token_program
             FROM token_accounts WHERE delegate = $1",
        )
        .bind(delegate)
        .fetch_all(&self.pool)
        .await?;
        Ok(rows)
    }

    pub async fn get_program_accounts_pg(&self, program_id: &[u8]) -> Result<Vec<ProgramAccountRow>> {
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

    pub async fn get_assets_by_owner(&self, owner: &[u8], page: i64, limit: i64) -> Result<(i64, Vec<AssetRow>)> {
        let count: (i64,) = sqlx::query_as("SELECT count(*) FROM asset WHERE owner = $1 AND burnt = false")
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

    pub async fn get_assets_by_creator(&self, creator: &[u8], page: i64, limit: i64) -> Result<(i64, Vec<AssetRow>)> {
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

    pub async fn get_assets_by_group(&self, group_key: &str, group_value: &str, page: i64, limit: i64) -> Result<(i64, Vec<AssetRow>)> {
        let count: (i64,) = sqlx::query_as(
            "SELECT count(*) FROM asset_grouping ag JOIN asset a ON a.id = ag.asset_id
             WHERE ag.group_key = $1 AND ag.group_value = $2 AND a.burnt = false")
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

    pub async fn get_assets_by_authority(&self, authority: &[u8], page: i64, limit: i64) -> Result<(i64, Vec<AssetRow>)> {
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
        Self { pool: self.pool.clone() }
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
