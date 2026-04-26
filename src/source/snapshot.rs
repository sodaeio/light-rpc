//! Account-state recovery from local Solana snapshot files.
//! Block/tx/sig history is not in snapshots; that comes from the stream.

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
pub struct SnapshotSet {
    pub full: PathBuf,
    pub full_slot: Slot,
    /// Optional incremental layered on top of `full`. Its base slot equals `full_slot`.
    pub incremental: Option<(PathBuf, Slot)>,
}

impl SnapshotSet {
    /// Tip slot of the set: incremental's slot if present, otherwise the full's.
    pub fn tip_slot(&self) -> Slot {
        self.incremental.as_ref().map(|(_, s)| *s).unwrap_or(self.full_slot)
    }
}

/// Solana snapshot filenames:
///   snapshot-{slot}-{hash}.tar.zst
///   incremental-snapshot-{base}-{slot}-{hash}.tar.zst
///
/// Returns the newest full plus the newest incremental whose base matches it.
/// Orphan incrementals (no matching local full) are ignored — applying one
/// without its base produces a near-empty account state.
pub fn find_latest_snapshot(dir: &Path) -> Result<SnapshotSet> {
    let entries = std::fs::read_dir(dir)
        .with_context(|| format!("reading snapshot dir {}", dir.display()))?;

    let mut best_full: Option<(Slot, PathBuf)> = None;
    let mut incrementals: Vec<(Slot, Slot, PathBuf)> = Vec::new();

    for entry in entries.flatten() {
        let name = entry.file_name();
        let name = name.to_string_lossy();

        if let Some(rest) = name
            .strip_prefix("incremental-snapshot-")
            .and_then(|n| n.strip_suffix(".tar.zst"))
        {
            let parts: Vec<&str> = rest.splitn(3, '-').collect();
            if parts.len() >= 2 {
                if let (Ok(base), Ok(slot)) = (parts[0].parse::<Slot>(), parts[1].parse::<Slot>()) {
                    incrementals.push((base, slot, entry.path()));
                }
            }
        } else if let Some(rest) = name
            .strip_prefix("snapshot-")
            .and_then(|n| n.strip_suffix(".tar.zst"))
        {
            let parts: Vec<&str> = rest.splitn(2, '-').collect();
            if let Some(Ok(slot)) = parts.first().map(|s| s.parse::<Slot>()) {
                if best_full.as_ref().is_none_or(|(s, _)| slot > *s) {
                    best_full = Some((slot, entry.path()));
                }
            }
        }
    }

    let (full_slot, full) = best_full
        .with_context(|| format!("no full snapshot found in {}", dir.display()))?;

    let incremental = incrementals
        .into_iter()
        .filter(|(base, _, _)| *base == full_slot)
        .max_by_key(|(_, slot, _)| *slot)
        .map(|(_, slot, path)| (path, slot));

    info!(
        full_slot,
        full = %full.display(),
        incremental_slot = ?incremental.as_ref().map(|(_, s)| *s),
        "found snapshot set"
    );

    Ok(SnapshotSet { full, full_slot, incremental })
}

type MintAgg = std::collections::HashMap<[u8; 32], Vec<(u64, [u8; 32])>>;

/// Parse + apply a SnapshotSet to RocksDB. Full first, then incremental on top.
///
/// Pipeline: a single tar reader thread streams AppendVec buffers into a
/// bounded MPMC channel; N worker threads parse + write to RocksDB in parallel.
/// Each worker accumulates its own mint-top-holders aggregator; aggregators
/// are merged at the end and flushed in one WriteBatch. RocksDB writes are
/// thread-safe — multiple workers' WriteBatches serialize internally.
///
/// Per-account writes use `apply_snapshot_batch` (WriteBatch + WAL off).
/// Mint RMW is replaced by the aggregator: ~150M read+writes → ~num_mints writes.
///
/// Returns (tip_slot, total_accounts_applied).
pub fn apply_snapshot_set(
    rocks: &crate::storage::rocks::UnifiedRocksDb,
    set: &SnapshotSet,
) -> Result<(Slot, usize)> {
    let workers = num_cpus::get().clamp(4, 32);

    info!(
        slot = set.full_slot,
        path = %set.full.display(),
        workers,
        "applying full snapshot"
    );
    let mut total = apply_one_parallel(rocks, &set.full, set.full_slot, workers)?;
    let (full_applied, full_agg) = total;
    info!(applied = full_applied, mints = full_agg.len(), "full snapshot applied");

    let mut applied = full_applied;
    let mut merged_agg = full_agg;
    let mut tip = set.full_slot;

    if let Some((path, slot)) = &set.incremental {
        info!(slot = *slot, path = %path.display(), workers, "applying incremental snapshot");
        total = apply_one_parallel(rocks, path, *slot, workers)?;
        let (inc_applied, inc_agg) = total;
        applied += inc_applied;
        merge_mint_aggs(&mut merged_agg, inc_agg);
        tip = *slot;
    }

    info!(mints = merged_agg.len(), "flushing mint_top_holders");
    let written = rocks.flush_mint_top_holders(merged_agg)?;
    info!(written, "mint_top_holders flushed");

    Ok((tip, applied))
}

/// Reader-and-N-workers pipeline for a single snapshot file.
fn apply_one_parallel(
    rocks: &crate::storage::rocks::UnifiedRocksDb,
    path: &Path,
    slot: Slot,
    workers: usize,
) -> Result<(usize, MintAgg)> {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    // Bounded channel: keep at most `workers * 2` AppendVec buffers in flight
    // so the reader doesn't run away from the workers and balloon RAM.
    let (tx, rx) = crossbeam_channel::bounded::<Vec<u8>>(workers * 2);

    let total = Arc::new(AtomicUsize::new(0));
    let next_log = Arc::new(AtomicUsize::new(1_000_000));

    let aggs: Result<Vec<MintAgg>> = std::thread::scope(|s| {
        let mut handles = Vec::with_capacity(workers);
        for _ in 0..workers {
            let rx = rx.clone();
            let rocks = rocks.clone();
            let total = total.clone();
            let next_log = next_log.clone();
            handles.push(s.spawn(move || -> Result<MintAgg> {
                let mut local_agg: MintAgg = std::collections::HashMap::new();
                let mut accounts: Vec<SnapshotAccount> = Vec::new();
                while let Ok(buf) = rx.recv() {
                    accounts.clear();
                    parse_append_vec(&buf, &mut accounts);
                    if accounts.is_empty() {
                        continue;
                    }
                    rocks.apply_snapshot_batch(&accounts, slot, &mut local_agg)?;
                    let n = total.fetch_add(accounts.len(), Ordering::Relaxed) + accounts.len();
                    let log_at = next_log.load(Ordering::Relaxed);
                    if n >= log_at
                        && next_log
                            .compare_exchange(
                                log_at,
                                n + 1_000_000,
                                Ordering::Relaxed,
                                Ordering::Relaxed,
                            )
                            .is_ok()
                    {
                        info!(applied = n, mints = local_agg.len(), "snapshot apply progress");
                    }
                }
                Ok(local_agg)
            }));
        }
        // Drop our receiver clone so workers can detect EOF on `drop(tx)`.
        drop(rx);

        // Reader (this thread): stream tar entries, send each AppendVec buffer.
        let file = std::fs::File::open(path)
            .with_context(|| format!("opening snapshot {}", path.display()))?;
        let zstd_reader = zstd::Decoder::new(file)?;
        let mut archive = tar::Archive::new(zstd_reader);
        let mut files = 0usize;
        for entry in archive.entries()? {
            let mut entry = entry?;
            let entry_path = entry.path()?.to_path_buf();
            let path_str = entry_path.to_string_lossy();
            if !path_str.contains("accounts/") || path_str.ends_with('/') {
                continue;
            }
            let mut data = Vec::with_capacity(entry.size() as usize);
            entry.read_to_end(&mut data)?;
            tx.send(data).ok();
            files += 1;
            if files % 5000 == 0 {
                info!(files, "tar reader progress");
            }
        }
        info!(files, "tar read complete");
        drop(tx);

        handles.into_iter().map(|h| h.join().unwrap()).collect()
    });

    let aggs = aggs?;
    let mut merged: MintAgg = std::collections::HashMap::new();
    for a in aggs {
        merge_mint_aggs(&mut merged, a);
    }
    Ok((total.load(Ordering::Relaxed), merged))
}

fn merge_mint_aggs(into: &mut MintAgg, from: MintAgg) {
    use crate::storage::rocks::MINT_TOP_HOLDERS_K;
    for (mint, holders) in from {
        let entry = into.entry(mint).or_default();
        entry.extend(holders);
        if entry.len() >= MINT_TOP_HOLDERS_K * 2 {
            entry.sort_unstable_by(|a, b| b.0.cmp(&a.0));
            entry.truncate(MINT_TOP_HOLDERS_K);
        }
    }
}

/// Stream per-AppendVec batches through `on_batch` instead of buffering all
/// accounts in memory. Mainnet has ~300M accounts; loading them all peaks
/// well past the host's RAM. Per-AppendVec batches keep working set bounded
/// to one file (~10s of MB).
pub fn stream_snapshot_accounts(
    path: &Path,
    mut on_batch: impl FnMut(&[SnapshotAccount]) -> Result<()>,
) -> Result<usize> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening snapshot {}", path.display()))?;
    let zstd_reader = zstd::Decoder::new(file)?;
    let mut archive = tar::Archive::new(zstd_reader);

    let mut batch: Vec<SnapshotAccount> = Vec::new();
    let mut files_parsed = 0usize;
    let mut total = 0usize;

    for entry in archive.entries()? {
        let mut entry = entry?;
        let entry_path = entry.path()?.to_path_buf();
        let path_str = entry_path.to_string_lossy();

        if !path_str.contains("accounts/") || path_str.ends_with('/') {
            continue;
        }

        let mut data = Vec::with_capacity(entry.size() as usize);
        entry.read_to_end(&mut data)?;
        batch.clear();
        parse_append_vec(&data, &mut batch);

        if !batch.is_empty() {
            on_batch(&batch)?;
            total += batch.len();
        }

        files_parsed += 1;
        if files_parsed % 1000 == 0 {
            info!(files = files_parsed, accounts = total, "parsing snapshot");
        }
    }

    info!(files = files_parsed, accounts = total, "snapshot parsing complete");
    Ok(total)
}

pub fn apply_snapshot_to_rocks(
    rocks: &crate::storage::rocks::UnifiedRocksDb,
    accounts: &[SnapshotAccount],
    slot: Slot,
) -> Result<usize> {
    use crate::storage::accounts::StoredAccount;

    let mut applied = 0;

    for acc in accounts {
        let stored = StoredAccount {
            owner: acc.owner,
            lamports: acc.lamports,
            data: acc.data.clone(),
            executable: acc.executable,
            rent_epoch: acc.rent_epoch,
            slot,
        };
        let _ = rocks.put_account(&acc.pubkey, &stored.serialize());
        let _ = rocks.put_program_index(&acc.owner, &acc.pubkey);

        // SPL Token Account: populate owner_atas + mint_top_holders.
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

/// AppendVec binary layout per entry (agave `STORE_META_OVERHEAD = 136`):
///   StoredMeta:           write_version(u64) + data_len(u64) + pubkey([u8;32]) = 48
///   AccountMeta:          lamports(u64) + rent_epoch(u64) + owner([u8;32]) + executable(u8) + pad(7) = 56
///   ObsoleteAccountHash:  [u8; 32] = 32  (kept on-disk for backward compat; ignored)
///   data:                 [u8; data_len]
///   padding:              align to 8 bytes
const STORE_META_OVERHEAD: usize = 48 + 56 + 32;

fn parse_append_vec(buf: &[u8], out: &mut Vec<SnapshotAccount>) {
    let mut offset = 0;

    while offset + STORE_META_OVERHEAD <= buf.len() {
        let _write_version =
            u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap_or([0; 8]));
        offset += 8;
        let data_len =
            u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap_or([0; 8])) as usize;
        offset += 8;

        if data_len > 10_000_000 {
            break;
        }

        let mut pubkey = [0u8; 32];
        pubkey.copy_from_slice(&buf[offset..offset + 32]);
        offset += 32;

        let lamports = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap_or([0; 8]));
        offset += 8;
        let rent_epoch = u64::from_le_bytes(buf[offset..offset + 8].try_into().unwrap_or([0; 8]));
        offset += 8;
        let mut owner = [0u8; 32];
        owner.copy_from_slice(&buf[offset..offset + 32]);
        offset += 32;
        let executable = buf[offset] != 0;
        offset += 1 + 7;

        // ObsoleteAccountHash — present on disk, not used here.
        offset += 32;

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
