use std::fs;
use std::path::{Path, PathBuf};

use crate::codec::decompress;
use crate::error::{DiceError, Result};
use crate::manifest::Manifest;
use crate::paths::safe_join;
use crate::store::{find_manifest, read_chunk};

/// Resolve restore output path under `dest_dir` using the manifest logical path.
pub fn restore_output_path(dest_dir: impl AsRef<Path>, manifest: &Manifest) -> Result<PathBuf> {
    let dest_dir = dest_dir.as_ref();
    let out = safe_join(dest_dir, manifest.logical_path())?;
    if out.exists() && out.is_dir() {
        return Err(DiceError::msg(format!(
            "restore target is a directory: {}",
            out.display()
        )));
    }
    Ok(out)
}

#[derive(Debug, Clone)]
pub struct AssembleResult {
    pub manifest: Manifest,
    pub output_path: String,
    pub bytes: u64,
}

pub fn assemble_file<F>(
    store_root: impl AsRef<Path>,
    manifest_query: &str,
    output_path: impl AsRef<Path>,
    mut on_progress: F,
) -> Result<AssembleResult>
where
    F: FnMut(usize, usize),
{
    let store_root = store_root.as_ref();
    let output_path = output_path.as_ref();
    let manifest = find_manifest(store_root, manifest_query)?;

    if let Some(parent) = output_path.parent() {
        if !parent.as_os_str().is_empty() {
            fs::create_dir_all(parent)?;
        }
    }

    let total = manifest.chunks.len().max(1);
    let mut hasher = blake3::Hasher::new();
    let mut parts: Vec<Vec<u8>> = Vec::with_capacity(manifest.chunks.len());

    for (i, entry) in manifest.chunks.iter().enumerate() {
        let compressed = read_chunk(store_root, &entry.hash)?;
        let raw = decompress(&compressed)?;
        if raw.len() != entry.raw_size {
            return Err(DiceError::msg(format!(
                "chunk size mismatch {}: expected {}, got {}",
                entry.hash,
                entry.raw_size,
                raw.len()
            )));
        }
        let got = blake3::hash(&raw).to_hex().to_string();
        if got != entry.hash {
            return Err(DiceError::msg(format!(
                "chunk hash mismatch {}: got {got}",
                entry.hash
            )));
        }
        hasher.update(&raw);
        parts.push(raw);
        on_progress(i + 1, total);
    }

    let digest = hasher.finalize().to_hex().to_string();
    if digest != manifest.hash {
        return Err(DiceError::msg(format!(
            "file hash mismatch: expected {}, got {digest}",
            manifest.hash
        )));
    }

    let mut whole = Vec::with_capacity(manifest.size as usize);
    for p in parts {
        whole.extend_from_slice(&p);
    }
    if whole.len() as u64 != manifest.size {
        return Err(DiceError::msg(format!(
            "file size mismatch: expected {}, got {}",
            manifest.size,
            whole.len()
        )));
    }

    fs::write(output_path, &whole)?;
    on_progress(total, total);
    Ok(AssembleResult {
        manifest,
        output_path: output_path.display().to_string(),
        bytes: whole.len() as u64,
    })
}
