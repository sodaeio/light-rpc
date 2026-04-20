//! Snapshot-based recovery: account state from local snapshot files.
//!
//! Snapshots are read from a local directory (mounted from the validator
//! or rsync'd periodically). Zero RPC calls — pure file I/O.
//!
//! Flow:
//!   1. Scan snapshot_dir for the latest incremental (or full) snapshot
//!   2. Parse the tar.zst → extract AppendVec account entries
//!   3. Batch-write to all RocksDB CFs
//!
//! Block/tx/sig history is NOT recoverable from snapshots — those come
//! exclusively from the gRPC stream via replay_from_slot.

use anyhow::{Context, Result};
use std::io::Read;
use std::path::{Path, PathBuf};
use tracing::info;

use crate::types::*;

#[derive(Debug)]
pub struct SnapshotAccount {
    pub pubkey: [u8; 32],
    pub lamports: u64,
    pub owner: [u8; 32],
    pub data: Vec<u8>,
    pub executable: bool,
    pub rent_epoch: u64,
}

#[derive(Debug)]
pub struct SnapshotInfo {
    pub slot: Slot,
    pub base_slot: Option<Slot>,
    pub path: PathBuf,
    pub is_incremental: bool,
}

/// Scan a directory for the latest snapshot file. Prefers incremental
/// over full. Filenames follow Solana convention:
///   incremental-snapshot-{base}-{slot}-{hash}.tar.zst
///   snapshot-{slot}-{hash}.tar.zst
pub fn find_latest_snapshot(dir: &Path) -> Result<SnapshotInfo> {
    let entries = std::fs::read_dir(dir)
        .with_context(|| format!("reading snapshot dir {}", dir.display()))?;

    let mut best_incremental: Option<(Slot, Slot, PathBuf)> = None; // (slot, base, path)
    let mut best_full: Option<(Slot, PathBuf)> = None;

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if name.starts_with("incremental-snapshot-") && name.ends_with(".tar.zst") {
            // incremental-snapshot-{base}-{slot}-{hash}.tar.zst
            let parts: Vec<&str> = name
                .trim_start_matches("incremental-snapshot-")
                .trim_end_matches(".tar.zst")
                .splitn(3, '-')
                .collect();
            if parts.len() >= 2 {
                if let (Ok(base), Ok(slot)) = (parts[0].parse::<Slot>(), parts[1].parse::<Slot>()) {
                    if best_incremental.as_ref().is_none_or(|(s, _, _)| slot > *s) {
                        best_incremental = Some((slot, base, entry.path()));
                    }
                }
            }
        } else if name.starts_with("snapshot-") && name.ends_with(".tar.zst") {
            // snapshot-{slot}-{hash}.tar.zst
            let parts: Vec<&str> = name
                .trim_start_matches("snapshot-")
                .trim_end_matches(".tar.zst")
                .splitn(2, '-')
                .collect();
            if let Some(Ok(slot)) = parts.first().map(|s| s.parse::<Slot>()) {
                if best_full.as_ref().is_none_or(|(s, _)| slot > *s) {
                    best_full = Some((slot, entry.path()));
                }
            }
        }
    }

    // Prefer incremental (smaller, faster)
    if let Some((slot, base, path)) = best_incremental {
        info!(slot, base, path = %path.display(), "found incremental snapshot");
        return Ok(SnapshotInfo {
            slot,
            base_slot: Some(base),
            path,
            is_incremental: true,
        });
    }

    if let Some((slot, path)) = best_full {
        info!(slot, path = %path.display(), "found full snapshot");
        return Ok(SnapshotInfo {
            slot,
            base_slot: None,
            path,
            is_incremental: false,
        });
    }

    anyhow::bail!("no snapshot files found in {}", dir.display())
}

/// Parse accounts from a snapshot archive (tar.zst containing AppendVec files).
pub fn parse_snapshot_accounts(path: &Path) -> Result<Vec<SnapshotAccount>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening snapshot {}", path.display()))?;
    let zstd_reader = zstd::Decoder::new(file)?;
    let mut archive = tar::Archive::new(zstd_reader);

    let mut accounts = Vec::new();
    let mut files_parsed = 0;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_path_buf();
        let path_str = entry_path.to_string_lossy();

        if !path_str.contains("accounts/") || path_str.ends_with('/') {
            continue;
        }

        let mut data = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut data)?;
        parse_append_vec(&data, &mut accounts);

        files_parsed += 1;
        if files_parsed % 100 == 0 {
            info!(
                files = files_parsed,
                accounts = accounts.len(),
                "parsing snapshot"
            );
        }
    }

    info!(
        files = files_parsed,
        accounts = accounts.len(),
        "snapshot parsing complete"
    );
    Ok(accounts)
}

/// Apply parsed snapshot accounts to RocksDB CFs.
pub fn apply_snapshot_to_rocks(
    rocks: &crate::storage::rocks::UnifiedRocksDb,
    accounts: &[SnapshotAccount],
    slot: Slot,
) -> Result<usize> {
    use crate::storage::accounts::StoredAccount;

    let mut applied = 0;

    for acc in accounts {
        // accounts CF
        let stored = StoredAccount {
            owner: acc.owner,
            lamports: acc.lamports,
            data: acc.data.clone(),
            executable: acc.executable,
            rent_epoch: acc.rent_epoch,
            slot,
        };
        let _ = rocks.put_account(&acc.pubkey, &stored.serialize());

        // program_index CF
        let _ = rocks.put_program_index(&acc.owner, &acc.pubkey);

        // SPL Token Account (165 bytes): owner_atas + mint_top_holders
        if acc.data.len() == 165 {
            let is_token = acc.owner
                == bs58::decode("TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA")
                    .into_vec()
                    .unwrap_or_default()
                    .as_slice()
                || acc.owner
                    == bs58::decode("TokenzQdBNbLqP5VEhdkAS6EPFLC1PHnBqCXEpPxuEb")
                        .into_vec()
                        .unwrap_or_default()
                        .as_slice();
            if is_token && acc.data.len() >= 72 {
                let mut owner_pk = [0u8; 32];
                owner_pk.copy_from_slice(&acc.data[32..64]);
                let _ = rocks.put_owner_atas_batch(&[(owner_pk, acc.pubkey)]);

                let mut mint = [0u8; 32];
                mint.copy_from_slice(&acc.data[0..32]);
                let amount = u64::from_le_bytes(acc.data[64..72].try_into().unwrap_or([0; 8]));
                let _ = rocks.update_mint_top_holders(&mint, &[(amount, acc.pubkey)]);
            }
        }

        applied += 1;
        if applied % 1_000_000 == 0 {
            info!(
                applied,
                total = accounts.len(),
                pct = applied * 100 / accounts.len(),
                "snapshot apply progress"
            );
        }
    }

    Ok(applied)
}

/// AppendVec binary layout per entry:
///   StoredMeta:   write_version(u64) + data_len(u64) + pubkey([u8;32]) = 48 bytes
///   AccountMeta:  lamports(u64) + rent_epoch(u64) + owner([u8;32]) + executable(u8) + padding(7) = 56 bytes
///   data:         [u8; data_len]
///   padding:      align to 8 bytes
fn parse_append_vec(buf: &[u8], out: &mut Vec<SnapshotAccount>) {
    let mut offset = 0;
    let min_entry = 48 + 56;

    while offset + min_entry <= buf.len() {
        let _write_version =
            u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap_or([0; 8]));
        offset += 8;
        let data_len =
            u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap_or([0; 8])) as usize;
        offset += 8;

        if data_len > 10_000_000 {
            break;
        }

        if offset + 32 > buf.len() {
            break;
        }
        let mut pubkey = [0u8; 32];
        pubkey.copy_from_slice(&buf[offset..offset + 32]);
        offset += 32;

        if offset + 56 > buf.len() {
            break;
        }
        let lamports = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap_or([0; 8]));
        offset += 8;
        let rent_epoch = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap_or([0; 8]));
        offset += 8;
        let mut owner = [0u8; 32];
        owner.copy_from_slice(&buf[offset..offset + 32]);
        offset += 32;
        let executable = buf[offset] != 0;
        offset += 1;
        offset += 7;

        if offset + data_len > buf.len() {
            break;
        }
        let data = buf[offset..offset + data_len].to_vec();
        offset += data_len;

        let rem = offset % 8;
        if rem != 0 {
            offset += 8 - rem;
        }

        if lamports == 0 {
            continue;
        }

        out.push(SnapshotAccount {
            pubkey,
            lamports,
            owner,
            data,
            executable,
            rent_epoch,
        });
    }
}
