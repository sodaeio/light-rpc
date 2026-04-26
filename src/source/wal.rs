//! Rolling write-ahead log for raw gRPC stream messages. Survives restarts;
//! does not cover messages never received (use snapshots for that).

use anyhow::{Context, Result};
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use tracing::info;

use crate::types::Slot;

const WAL_MAGIC: &[u8; 4] = b"LIWA";
const WAL_VERSION: u8 = 1;
const ENTRY_HEADER_LEN: usize = 13; // marker(1) + slot(8) + len(4)

pub struct WalWriter {
    dir: PathBuf,
    current: Option<BufWriter<std::fs::File>>,
    current_slot: Slot,
    bytes_written: u64,
    max_file_bytes: u64,
}

impl WalWriter {
    pub fn new(dir: &Path, max_file_bytes: u64) -> Result<Self> {
        std::fs::create_dir_all(dir)?;
        Ok(Self {
            dir: dir.to_path_buf(),
            current: None,
            current_slot: 0,
            bytes_written: 0,
            max_file_bytes,
        })
    }

    pub fn append(&mut self, slot: Slot, data: &[u8]) -> Result<()> {
        if self.current.is_none() || self.bytes_written >= self.max_file_bytes {
            self.rotate(slot)?;
        }
        let w = self.current.as_mut().unwrap();
        w.write_all(&[0xFE])?;
        w.write_all(&slot.to_le_bytes())?;
        w.write_all(&(data.len() as u32).to_le_bytes())?;
        w.write_all(data)?;
        self.bytes_written += (ENTRY_HEADER_LEN + data.len()) as u64;
        self.current_slot = slot;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<()> {
        if let Some(ref mut w) = self.current {
            w.flush()?;
        }
        Ok(())
    }

    fn rotate(&mut self, slot: Slot) -> Result<()> {
        let filename = format!("wal-{slot}.bin");
        let path = self.dir.join(&filename);
        let file = std::fs::File::create(&path)
            .with_context(|| format!("creating WAL file {}", path.display()))?;
        let mut w = BufWriter::new(file);
        w.write_all(WAL_MAGIC)?;
        w.write_all(&[WAL_VERSION])?;
        self.current = Some(w);
        self.bytes_written = 5;
        info!(path = %path.display(), slot, "rotated WAL file");
        Ok(())
    }

    pub fn prune(&self, cutoff_slot: Slot) -> Result<usize> {
        let mut removed = 0;
        for entry in std::fs::read_dir(&self.dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if name.starts_with("wal-") && name.ends_with(".bin") {
                let slot_str = name.trim_start_matches("wal-").trim_end_matches(".bin");
                if let Ok(slot) = slot_str.parse::<Slot>() {
                    if slot < cutoff_slot {
                        let _ = std::fs::remove_file(entry.path());
                        removed += 1;
                    }
                }
            }
        }
        Ok(removed)
    }
}

pub fn replay_wal(dir: &Path, after_slot: Slot) -> Result<Vec<(Slot, Vec<u8>)>> {
    let mut entries = Vec::new();
    let mut wal_files: Vec<PathBuf> = Vec::new();

    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if name.starts_with("wal-") && name.ends_with(".bin") {
            wal_files.push(entry.path());
        }
    }
    wal_files.sort();

    for path in &wal_files {
        let file = std::fs::File::open(path)?;
        let mut reader = BufReader::new(file);

        let mut magic = [0u8; 4];
        if reader.read_exact(&mut magic).is_err() {
            continue;
        }
        if &magic != WAL_MAGIC {
            continue;
        }
        let mut version = [0u8; 1];
        if reader.read_exact(&mut version).is_err() || version[0] != WAL_VERSION {
            continue;
        }

        loop {
            let mut marker = [0u8; 1];
            if reader.read_exact(&mut marker).is_err() {
                break;
            }
            if marker[0] != 0xFE {
                break;
            }

            let mut slot_buf = [0u8; 8];
            let mut len_buf = [0u8; 4];
            if reader.read_exact(&mut slot_buf).is_err() || reader.read_exact(&mut len_buf).is_err()
            {
                break;
            }
            let slot = Slot::from_le_bytes(slot_buf);
            let len = u32::from_le_bytes(len_buf) as usize;

            if len > 100_000_000 {
                break;
            }

            let mut data = vec![0u8; len];
            if reader.read_exact(&mut data).is_err() {
                break;
            }

            if slot > after_slot {
                entries.push((slot, data));
            }
        }
    }

    info!(
        entries = entries.len(),
        files = wal_files.len(),
        after_slot,
        "WAL replay complete"
    );
    Ok(entries)
}
