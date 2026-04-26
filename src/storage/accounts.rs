use super::rocks::{StoredAccountEntry, UnifiedRocksDb};
use crate::types::*;

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredAccount {
    pub owner: [u8; 32],
    pub lamports: u64,
    pub data: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: u64,
    pub slot: Slot,
}

impl StoredAccount {
    pub fn from_update(update: &AccountUpdate) -> Self {
        Self {
            owner: update.owner.to_bytes(),
            lamports: update.lamports,
            data: update.data.clone(),
            executable: update.executable,
            rent_epoch: update.rent_epoch,
            slot: update.slot,
        }
    }

    pub fn serialize(&self) -> Vec<u8> {
        bincode::serialize(self).expect("account serialization cannot fail")
    }

    pub fn deserialize(data: &[u8]) -> Option<Self> {
        bincode::deserialize(data).ok()
    }
}

pub struct AccountProcessor;

impl AccountProcessor {
    pub fn classify_batch(
        updates: &[AccountUpdate],
    ) -> (
        Vec<&AccountUpdate>,
        Vec<&AccountUpdate>,
        Vec<&AccountUpdate>,
    ) {
        let mut mints = Vec::new();
        let mut token_accounts = Vec::new();
        let mut program_accounts = Vec::new();

        for update in updates {
            match update.classify() {
                AccountKind::TokenMint => mints.push(update),
                AccountKind::TokenAccount => token_accounts.push(update),
                AccountKind::ProgramAccount => program_accounts.push(update),
            }
        }

        (mints, token_accounts, program_accounts)
    }

    /// No read-before-write: stream order guarantees newer data wins.
    pub fn write_program_accounts(
        db: &UnifiedRocksDb,
        accounts: &[&AccountUpdate],
    ) -> anyhow::Result<usize> {
        let entries: Vec<StoredAccountEntry> = accounts
            .iter()
            .map(|update| {
                let stored = StoredAccount::from_update(update);
                StoredAccountEntry {
                    pubkey: update.pubkey.to_bytes(),
                    owner: update.owner.to_bytes(),
                    data: stored.serialize(),
                    slot: update.slot,
                }
            })
            .collect();

        let count = entries.len();
        if !entries.is_empty() {
            db.write_account_batch(&entries)?;
        }
        Ok(count)
    }
}
