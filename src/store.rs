use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::error::{DiceError, Result};
use crate::manifest::Manifest;

/// Serializes store mutations (refs RMW, chunk create, manifest write) across pool workers.
fn store_lock() -> &'static Mutex<()> {
    static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| Mutex::new(()))
}

pub fn with_store_lock<T>(f: impl FnOnce() -> T) -> T {
    let _guard = store_lock().lock().unwrap_or_else(|e| e.into_inner());
    f()
}

#[derive(Debug, Clone)]
pub struct StorePaths {
    pub root: PathBuf,
    pub chunks: PathBuf,
    pub manifests: PathBuf,
    pub refs: PathBuf,
}

pub fn store_paths(root: impl AsRef<Path>) -> StorePaths {
    let root = root.as_ref().to_path_buf();
    StorePaths {
        chunks: root.join("chunks"),
        manifests: root.join("manifests"),
        refs: root.join("refs.json"),
        root,
    }
}

pub fn init_store(root: impl AsRef<Path>) -> Result<StorePaths> {
    let paths = store_paths(root);
    fs::create_dir_all(&paths.chunks)?;
    fs::create_dir_all(&paths.manifests)?;
    if !paths.refs.exists() {
        fs::write(&paths.refs, "{}\n")?;
    }
    Ok(paths)
}

fn chunk_path(chunks_root: &Path, hash: &str) -> PathBuf {
    let (a, b) = hash.split_at(2.min(hash.len()));
    chunks_root.join(a).join(b)
}

pub fn has_chunk(root: impl AsRef<Path>, hash: &str) -> bool {
    chunk_path(&store_paths(root).chunks, hash).exists()
}

pub fn list_missing(root: impl AsRef<Path>, hashes: &[String]) -> Vec<String> {
    hashes
        .iter()
        .filter(|h| !has_chunk(root.as_ref(), h))
        .cloned()
        .collect()
}

pub fn read_chunk(root: impl AsRef<Path>, hash: &str) -> Result<Vec<u8>> {
    Ok(fs::read(chunk_path(&store_paths(root).chunks, hash))?)
}

/// Returns true if the chunk was newly written.
pub fn write_chunk(root: impl AsRef<Path>, hash: &str, compressed: &[u8]) -> Result<bool> {
    let paths = store_paths(root);
    let dest = chunk_path(&paths.chunks, hash);
    if dest.exists() {
        return Ok(false);
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(dest, compressed)?;
    Ok(true)
}

fn read_refs(root: &Path) -> Result<HashMap<String, u64>> {
    let paths = store_paths(root);
    match fs::read_to_string(&paths.refs) {
        Ok(raw) => Ok(serde_json::from_str(&raw)?),
        Err(_) => Ok(HashMap::new()),
    }
}

fn write_refs(root: &Path, refs: &HashMap<String, u64>) -> Result<()> {
    let paths = store_paths(root);
    // Compact JSON — refs grows large with many chunks; pretty print was a major stall.
    fs::write(&paths.refs, serde_json::to_string(refs)?)?;
    Ok(())
}

pub fn bump_refs(root: impl AsRef<Path>, hashes: &[String]) -> Result<()> {
    let root = root.as_ref();
    let mut refs = read_refs(root)?;
    for h in hashes {
        *refs.entry(h.clone()).or_insert(0) += 1;
    }
    write_refs(root, &refs)
}

pub fn save_manifest(root: impl AsRef<Path>, manifest: &Manifest) -> Result<PathBuf> {
    let paths = store_paths(root);
    let dest = paths.manifests.join(format!("{}.json", manifest.id));
    fs::write(&dest, serde_json::to_string_pretty(manifest)? + "\n")?;
    Ok(dest)
}

pub fn load_manifest(root: impl AsRef<Path>, id: &str) -> Result<Manifest> {
    let dest = store_paths(root).manifests.join(format!("{id}.json"));
    let raw = fs::read_to_string(dest)?;
    Ok(serde_json::from_str(&raw)?)
}

pub fn find_manifest(root: impl AsRef<Path>, query: &str) -> Result<Manifest> {
    let paths = store_paths(root);
    let exact = paths.manifests.join(format!("{query}.json"));
    if exact.exists() {
        let raw = fs::read_to_string(exact)?;
        return Ok(serde_json::from_str(&raw)?);
    }

    let mut matches = Vec::new();
    for entry in fs::read_dir(&paths.manifests)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.ends_with(".json") && (name.starts_with(query) || name.contains(query)) {
            matches.push(entry.path());
        }
    }
    if matches.is_empty() {
        return Err(DiceError::msg(format!("manifest not found: {query}")));
    }
    if matches.len() > 1 {
        return Err(DiceError::msg(format!(
            "ambiguous manifest \"{query}\": {} matches",
            matches.len()
        )));
    }
    let raw = fs::read_to_string(&matches[0])?;
    Ok(serde_json::from_str(&raw)?)
}

pub fn list_manifests(root: impl AsRef<Path>) -> Result<Vec<Manifest>> {
    let paths = store_paths(root);
    let mut out = Vec::new();
    if !paths.manifests.exists() {
        return Ok(out);
    }
    for entry in fs::read_dir(&paths.manifests)? {
        let entry = entry?;
        let name = entry.file_name().to_string_lossy().to_string();
        if !name.ends_with(".json") {
            continue;
        }
        let raw = fs::read_to_string(entry.path())?;
        out.push(serde_json::from_str(&raw)?);
    }
    out.sort_by(|a, b| a.logical_path().cmp(b.logical_path()));
    Ok(out)
}
