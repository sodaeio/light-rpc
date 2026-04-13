use serde::{Deserialize, Serialize};
use solana_pubkey::Pubkey;
use std::collections::HashMap;
use std::sync::Arc;

pub type Slot = u64;
pub type UnixTimestamp = i64;

/// Well-known Solana program IDs (avoids pulling in heavy SPL crates).
pub mod programs {
    use solana_pubkey::Pubkey;
    use std::str::FromStr;

    pub fn token_program() -> Pubkey {
        Pubkey::from_str("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA").unwrap()
    }

    pub fn token_2022_program() -> Pubkey {
        Pubkey::from_str("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb").unwrap()
    }

    pub fn metaplex_metadata() -> Pubkey {
        Pubkey::from_str("metaqbxxUerdq28cj1RbAWkYQm3ybzjb6a8bt518x1s").unwrap()
    }

    pub fn system_program() -> Pubkey {
        Pubkey::from_str("11111111111111111111111111111111").unwrap()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Commitment {
    Processed,
    Confirmed,
    Finalized,
}

impl Default for Commitment {
    fn default() -> Self {
        Self::Finalized
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SlotStatus {
    ProcessedOrSkipped,
    Dead,
    Confirmed,
    Finalized,
}

#[derive(Debug, Clone)]
pub struct BlockInfo {
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: Option<UnixTimestamp>,
    pub block_height: Option<u64>,
    pub blockhash: String,
}

#[derive(Debug, Clone)]
pub struct TransactionInfo {
    pub signature: solana_signature::Signature,
    pub slot: Slot,
    pub block_time: Option<UnixTimestamp>,
    pub err: Option<String>,
    pub memo: Option<String>,
}

#[derive(Debug)]
pub struct BlockWithData {
    pub info: BlockInfo,
    pub encoded_block: bytes::Bytes,
    pub transactions: Vec<TransactionEntry>,
    pub address_signatures: HashMap<Pubkey, Vec<SignatureEntry>>,
    pub fees: Vec<u64>,
}

#[derive(Debug, Clone)]
pub struct TransactionEntry {
    pub signature: solana_signature::Signature,
    pub offset: u32,
    pub length: u32,
    pub err: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignatureEntry {
    pub signature: solana_signature::Signature,
    pub err: Option<String>,
    pub memo: Option<String>,
}

#[derive(Debug, Clone)]
pub struct AccountUpdate {
    pub pubkey: Pubkey,
    pub slot: Slot,
    pub owner: Pubkey,
    pub lamports: u64,
    pub data: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: u64,
    pub write_version: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccountKind {
    TokenMint,
    TokenAccount,
    ProgramAccount,
}

impl AccountUpdate {
    pub fn classify(&self) -> AccountKind {
        let is_token_program = self.owner == programs::token_program()
            || self.owner == programs::token_2022_program();

        if is_token_program && self.data.len() == 82 {
            AccountKind::TokenMint
        } else if is_token_program && self.data.len() == 165 {
            AccountKind::TokenAccount
        } else {
            AccountKind::ProgramAccount
        }
    }
}

#[derive(Debug, Clone)]
pub struct SlotMeta {
    pub slot: Slot,
    pub parent_slot: Slot,
    pub block_time: Option<UnixTimestamp>,
    pub status: SlotStatus,
}

pub enum SourceMessage {
    Block {
        slot: Slot,
        block: Arc<BlockWithData>,
    },
    SlotStatus {
        slot: Slot,
        parent_slot: Option<Slot>,
        status: SlotStatus,
    },
    AccountUpdate(AccountUpdate),
}

#[derive(Debug, Clone)]
pub enum WriteToReadMessage {
    Init {
        latest_slot: Slot,
    },
    NewBlock {
        slot: Slot,
        block: Arc<BlockWithData>,
    },
    BlockConfirmed {
        slot: Slot,
    },
    SlotFinalized {
        slot: Slot,
    },
    BlockDead {
        slot: Slot,
    },
}
