//! Store / dedup reporting.

use std::fs;
use std::path::Path;

use crate::error::Result;
use crate::store::{list_manifests, store_paths};

#[derive(Debug, Clone)]
pub struct StoreStats {
    pub manifests: usize,
    pub unique_chunks: usize,
    pub total_chunk_refs: u64,
    pub logical_bytes: u64,
    pub compressed_bytes: u64,
    pub on_disk_chunk_bytes: u64,
}

impl StoreStats {
    pub fn dedup_saved_logical(&self) -> u64 {
        // Rough: if every chunk ref were stored once vs raw file bytes.
        self.logical_bytes.saturating_sub(self.compressed_bytes)
    }

    pub fn format_summary(&self) -> String {
        format!(
            "{} files · {} unique chunks · logical {} · compressed meta {} · on-disk {} · est. saved {}",
            self.manifests,
            self.unique_chunks,
            crate::progress::format_bytes(self.logical_bytes),
            crate::progress::format_bytes(self.compressed_bytes),
            crate::progress::format_bytes(self.on_disk_chunk_bytes),
            crate::progress::format_bytes(self.dedup_saved_logical()),
        )
    }
}

pub fn store_stats(store_root: impl AsRef<Path>) -> Result<StoreStats> {
    let store_root = store_root.as_ref();
    let manifests = list_manifests(store_root)?;
    let mut unique = std::collections::BTreeSet::new();
    let mut total_refs = 0u64;
    let mut logical = 0u64;
    let mut compressed_meta = 0u64;

    for m in &manifests {
        logical += m.size;
        for c in &m.chunks {
            total_refs += 1;
            unique.insert(c.hash.clone());
            compressed_meta += c.compressed_size as u64;
        }
    }

    let mut on_disk = 0u64;
    let chunks = store_paths(store_root).chunks;
    if chunks.exists() {
        on_disk = dir_size(&chunks)?;
    }

    Ok(StoreStats {
        manifests: manifests.len(),
        unique_chunks: unique.len(),
        total_chunk_refs: total_refs,
        logical_bytes: logical,
        compressed_bytes: compressed_meta,
        on_disk_chunk_bytes: on_disk,
    })
}

fn dir_size(path: &Path) -> Result<u64> {
    let mut total = 0u64;
    if path.is_file() {
        return Ok(fs::metadata(path)?.len());
    }
    for e in fs::read_dir(path)? {
        let e = e?;
        let p = e.path();
        if p.is_dir() {
            total += dir_size(&p)?;
        } else {
            total += e.metadata()?.len();
        }
    }
    Ok(total)
}
