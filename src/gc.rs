//! Manifest deletion and chunk garbage collection.

use std::collections::HashMap;
use std::fs;
use std::path::Path;

use crate::error::Result;
use crate::lock::with_store_file_lock;
use crate::manifest::Manifest;
use crate::store::{find_manifest, list_manifests, store_paths};

fn read_refs(root: &Path) -> Result<HashMap<String, u64>> {
    let paths = store_paths(root);
    match fs::read_to_string(&paths.refs) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(_) => Ok(HashMap::new()),
    }
}

fn write_refs(root: &Path, refs: &HashMap<String, u64>) -> Result<()> {
    let paths = store_paths(root);
    fs::write(&paths.refs, serde_json::to_string(refs)?)?;
    Ok(())
}

fn chunk_path(chunks_root: &Path, hash: &str) -> std::path::PathBuf {
    let (a, b) = hash.split_at(2.min(hash.len()));
    chunks_root.join(a).join(b)
}

#[derive(Debug, Clone)]
pub struct DeleteResult {
    pub manifest_id: String,
    pub dropped_refs: usize,
    pub deleted_chunks: usize,
}

/// Delete a manifest and decrement its chunk refs. Chunks at 0 are removed.
pub fn delete_manifest(store_root: impl AsRef<Path>, query: &str) -> Result<DeleteResult> {
    let store_root = store_root.as_ref().to_path_buf();
    with_store_file_lock(&store_root, || {
        let manifest = find_manifest(&store_root, query)?;
        delete_manifest_inner(&store_root, &manifest)
    })
}

fn delete_manifest_inner(store_root: &Path, manifest: &Manifest) -> Result<DeleteResult> {
    let paths = store_paths(store_root);
    let mut refs = read_refs(store_root)?;
    let mut dropped = 0usize;
    let mut deleted_chunks = 0usize;

    for entry in &manifest.chunks {
        dropped += 1;
        let count = refs.get(&entry.hash).copied().unwrap_or(0);
        if count <= 1 {
            refs.remove(&entry.hash);
            let dest = chunk_path(&paths.chunks, &entry.hash);
            if dest.exists() {
                fs::remove_file(&dest)?;
                deleted_chunks += 1;
                // best-effort cleanup empty parent
                if let Some(parent) = dest.parent() {
                    let _ = fs::remove_dir(parent);
                }
            }
        } else {
            refs.insert(entry.hash.clone(), count - 1);
        }
    }

    write_refs(store_root, &refs)?;
    let man_path = paths.manifests.join(format!("{}.json", manifest.id));
    if man_path.exists() {
        fs::remove_file(man_path)?;
    }

    Ok(DeleteResult {
        manifest_id: manifest.id.clone(),
        dropped_refs: dropped,
        deleted_chunks,
    })
}

#[derive(Debug, Clone)]
pub struct GcResult {
    pub scanned_chunks: usize,
    pub deleted_chunks: usize,
    pub kept_chunks: usize,
}

/// Remove chunk files not referenced by any manifest / refs map.
pub fn gc_store(store_root: impl AsRef<Path>) -> Result<GcResult> {
    let store_root = store_root.as_ref().to_path_buf();
    with_store_file_lock(&store_root, || {
        // Rebuild refs from manifests for safety, then delete unreferenced files.
        let manifests = list_manifests(&store_root)?;
        let mut refs: HashMap<String, u64> = HashMap::new();
        for m in &manifests {
            for c in &m.chunks {
                *refs.entry(c.hash.clone()).or_insert(0) += 1;
            }
        }
        write_refs(&store_root, &refs)?;

        let chunks_root = store_paths(&store_root).chunks;
        let mut scanned = 0usize;
        let mut deleted = 0usize;
        let mut kept = 0usize;

        if chunks_root.exists() {
            gc_walk(&chunks_root, &refs, &mut scanned, &mut deleted, &mut kept)?;
        }

        Ok(GcResult {
            scanned_chunks: scanned,
            deleted_chunks: deleted,
            kept_chunks: kept,
        })
    })
}

fn gc_walk(
    dir: &Path,
    refs: &HashMap<String, u64>,
    scanned: &mut usize,
    deleted: &mut usize,
    kept: &mut usize,
) -> Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.is_dir() {
            gc_walk(&path, refs, scanned, deleted, kept)?;
            let _ = fs::remove_dir(&path); // only succeeds if empty
            continue;
        }
        *scanned += 1;
        let name = path
            .file_name()
            .map(|s| s.to_string_lossy().to_string())
            .unwrap_or_default();
        // hash files are stored as full hex under ab/abcdef...
        let hash = if let Some(parent) = path.parent().and_then(|p| p.file_name()) {
            format!("{}{}", parent.to_string_lossy(), name)
        } else {
            name.clone()
        };
        if refs.get(&hash).copied().unwrap_or(0) == 0 {
            fs::remove_file(&path)?;
            *deleted += 1;
        } else {
            *kept += 1;
        }
    }
    Ok(())
}

pub fn find_manifest_by_logical_path(
    store_root: impl AsRef<Path>,
    logical_path: &str,
) -> Result<Option<Manifest>> {
    let logical_path = logical_path.replace('\\', "/");
    for m in list_manifests(store_root)? {
        if m.logical_path() == logical_path {
            return Ok(Some(m));
        }
    }
    Ok(None)
}

pub fn already_ingested(
    store_root: impl AsRef<Path>,
    logical_path: &str,
    content_hash: &str,
) -> Result<bool> {
    if let Some(m) = find_manifest_by_logical_path(store_root, logical_path)? {
        return Ok(m.hash == content_hash);
    }
    Ok(false)
}

