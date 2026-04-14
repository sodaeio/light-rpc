//! Hardcoded native program and sysvar accounts.
//!
//! These accounts are validator-internal state and never appear in the gRPC
//! account update stream. Without this registry, a query for them would miss
//! since they're not in RocksDB or PostgreSQL.
//!
//! Values captured from mainnet. Dynamic sysvars (Clock, EpochSchedule) change
//! per slot but their lamports/owner/executable fields are stable.

use base64::Engine;
use std::sync::OnceLock;

use crate::storage::accounts::StoredAccount;

/// Lookup a native program or sysvar by pubkey.
/// Returns Some(StoredAccount) if the address is a known native account.
pub fn lookup(pubkey: &[u8; 32]) -> Option<StoredAccount> {
    let registry = REGISTRY.get_or_init(build_registry);
    let entry = registry.iter().find(|e| &e.pubkey == pubkey)?;
    Some(StoredAccount {
        owner: entry.owner,
        lamports: entry.lamports,
        data: entry.data.clone(),
        executable: entry.executable,
        rent_epoch: 0,
        slot: 0,
    })
}

struct NativeEntry {
    pubkey: [u8; 32],
    lamports: u64,
    owner: [u8; 32],
    executable: bool,
    data: Vec<u8>,
}

static REGISTRY: OnceLock<Vec<NativeEntry>> = OnceLock::new();

fn pk(s: &str) -> [u8; 32] {
    let bytes = bs58::decode(s).into_vec().expect("valid base58");
    bytes.try_into().expect("32 bytes")
}

fn b64(s: &str) -> Vec<u8> {
    base64::engine::general_purpose::STANDARD
        .decode(s)
        .unwrap_or_default()
}

fn build_registry() -> Vec<NativeEntry> {
    let native_loader = pk("NativeLoader1111111111111111111111111111111");
    let bpf_upgradeable = pk("BPFLoaderUpgradeab1e11111111111111111111111");
    let sysvar_parent = pk("11111111111111111111111111111111");

    vec![
        // System Program
        NativeEntry {
            pubkey: pk("11111111111111111111111111111111"),
            lamports: 1,
            owner: native_loader,
            executable: true,
            data: b64("c29sYW5hX3N5c3RlbV9wcm9ncmFt"),
        },
        // Vote Program
        NativeEntry {
            pubkey: pk("Vote111111111111111111111111111111111111111"),
            lamports: 1,
            owner: native_loader,
            executable: true,
            data: b64("c29sYW5hX3ZvdGVfcHJvZ3JhbQ=="),
        },
        // BPF Loader Upgradeable
        NativeEntry {
            pubkey: bpf_upgradeable,
            lamports: 1,
            owner: native_loader,
            executable: true,
            data: b64("c29sYW5hX2JwZl9sb2FkZXJfdXBncmFkZWFibGVfcHJvZ3JhbQ=="),
        },
        // Compute Budget
        NativeEntry {
            pubkey: pk("ComputeBudget111111111111111111111111111111"),
            lamports: 1,
            owner: native_loader,
            executable: true,
            data: b64("Y29tcHV0ZV9idWRnZXRfcHJvZ3JhbQ=="),
        },
        // Stake Program (upgraded to BPF)
        NativeEntry {
            pubkey: pk("Stake11111111111111111111111111111111111111"),
            lamports: 1_141_440,
            owner: bpf_upgradeable,
            executable: true,
            data: b64("AgAAAFHW83CdNkiAbBM/KbbTGo51AzqfNvqotBJrLfsRYocN"),
        },
        // Address Lookup Table
        NativeEntry {
            pubkey: pk("AddressLookupTab1e1111111111111111111111111"),
            lamports: 1_141_440,
            owner: bpf_upgradeable,
            executable: true,
            data: b64("AgAAADtKXMfRSUXkVugHTEht+fDZAO8WZH0jhJ0wNdftoyOf"),
        },
        // Config Program
        NativeEntry {
            pubkey: pk("Config1111111111111111111111111111111111111"),
            lamports: 1_141_440,
            owner: bpf_upgradeable,
            executable: true,
            data: b64("AgAAAKeepvf6/N81xCQjn/tKO2u2Qt7HM6w45+yDq1ky4kn4"),
        },
        // Sysvar parent meta-account
        NativeEntry {
            pubkey: pk("Sysvar1111111111111111111111111111111111111"),
            lamports: 57_760_221,
            owner: sysvar_parent,
            executable: false,
            data: vec![],
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_system_program_lookup() {
        let system = [0u8; 32];
        let acc = lookup(&system).expect("System program should be in registry");
        assert_eq!(acc.lamports, 1);
        assert!(acc.executable);
    }

    #[test]
    fn test_vote_program_lookup() {
        let vote = pk("Vote111111111111111111111111111111111111111");
        let acc = lookup(&vote).expect("Vote program should be in registry");
        assert_eq!(acc.lamports, 1);
    }

    #[test]
    fn test_stake_program_lookup() {
        let stake = pk("Stake11111111111111111111111111111111111111");
        let acc = lookup(&stake).expect("Stake program should be in registry");
        assert_eq!(acc.lamports, 1_141_440);
    }

    #[test]
    fn test_unknown_returns_none() {
        let random = [1u8; 32];
        assert!(lookup(&random).is_none());
    }
}
