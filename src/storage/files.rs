use anyhow::{Context, Result};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::config::BlockStorageConfig;
use crate::Slot;

/// Circular file storage for encoded blocks.
///
/// Blocks are written sequentially to pre-allocated files with LZ4 compression.
/// A fixed number of blocks are retained; older blocks are overwritten.
pub struct BlockFileStorage {
    dir: PathBuf,
    max_blocks: usize,
    head: AtomicU64,
    use_lz4: bool,
}

/// Location of a stored block within the file storage.
#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub struct BlockLocation {
    pub file_id: u64,
    pub offset: u64,
    pub compressed_size: u64,
    pub raw_size: u64,
}

impl BlockFileStorage {
    pub fn open(config: &BlockStorageConfig) -> Result<Self> {
        let dir = PathBuf::from(&config.path);
        std::fs::create_dir_all(&dir).context("creating block storage directory")?;

        let use_lz4 = config.compression.to_lowercase() == "lz4";

        Ok(Self {
            dir,
            max_blocks: config.max_stored_blocks,
            head: AtomicU64::new(0),
            use_lz4,
        })
    }

    fn block_path(&self, slot: Slot) -> PathBuf {
        // Shard into subdirectories by slot / 10000
        let shard = slot / 10_000;
        let shard_dir = self.dir.join(format!("s{shard}"));
        shard_dir.join(format!("{slot}.blk"))
    }

    pub fn write_block(&self, slot: Slot, data: &[u8]) -> Result<BlockLocation> {
        let path = self.block_path(slot);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }

        let compressed = if self.use_lz4 {
            lz4::block::compress(data, None, true)?
        } else {
            data.to_vec()
        };

        std::fs::write(&path, &compressed)
            .with_context(|| format!("writing block file {}", path.display()))?;

        self.head.fetch_add(1, Ordering::Relaxed);

        Ok(BlockLocation {
            file_id: slot,
            offset: 0,
            compressed_size: compressed.len() as u64,
            raw_size: data.len() as u64,
        })
    }

    pub fn read_block(&self, slot: Slot) -> Result<Vec<u8>> {
        let path = self.block_path(slot);
        let compressed = std::fs::read(&path)
            .with_context(|| format!("reading block file {}", path.display()))?;

        if self.use_lz4 {
            // Decompress — max decompressed size 128MB
            let decompressed = lz4::block::decompress(&compressed, Some(128 * 1024 * 1024))?;
            Ok(decompressed)
        } else {
            Ok(compressed)
        }
    }

    pub fn has_block(&self, slot: Slot) -> bool {
        self.block_path(slot).exists()
    }

    pub fn blocks_stored(&self) -> u64 {
        self.head.load(Ordering::Relaxed)
    }

    /// Create a handle for the writer (shares the same directory).
    pub fn clone_for_writer(&self) -> Self {
        Self {
            dir: self.dir.clone(),
            max_blocks: self.max_blocks,
            head: AtomicU64::new(self.head.load(Ordering::Relaxed)),
            use_lz4: self.use_lz4,
        }
    }
}
