//! Store and restored-file verification.

use std::fs;
use std::path::Path;

use crate::codec::decompress;
use crate::error::{DiceError, Result};
use crate::manifest::Manifest;
use crate::store::{list_manifests, read_chunk, store_paths};

#[derive(Debug, Clone)]
pub struct VerifyIssue {
    pub manifest_id: String,
    pub path: String,
    pub message: String,
}

#[derive(Debug, Clone)]
pub struct VerifyReport {
    pub manifests: usize,
    pub chunks_checked: usize,
    pub ok: usize,
    pub issues: Vec<VerifyIssue>,
}

pub fn verify_store(store_root: impl AsRef<Path>) -> Result<VerifyReport> {
    let store_root = store_root.as_ref();
    let manifests = list_manifests(store_root)?;
    let mut report = VerifyReport {
        manifests: manifests.len(),
        chunks_checked: 0,
        ok: 0,
        issues: Vec::new(),
    };

    for m in manifests {
        match verify_manifest(store_root, &m) {
            Ok(n) => {
                report.chunks_checked += n;
                report.ok += 1;
            }
            Err(issues) => {
                report.chunks_checked += issues.len();
                report.issues.extend(issues);
            }
        }
    }
    Ok(report)
}

pub fn verify_manifest(store_root: &Path, manifest: &Manifest) -> std::result::Result<usize, Vec<VerifyIssue>> {
    let mut issues = Vec::new();
    let mut hasher = blake3::Hasher::new();
    let mut total = 0usize;

    for entry in &manifest.chunks {
        total += 1;
        match read_chunk(store_root, &entry.hash) {
            Ok(compressed) => match decompress(&compressed) {
                Ok(raw) => {
                    if raw.len() != entry.raw_size {
                        issues.push(VerifyIssue {
                            manifest_id: manifest.id.clone(),
                            path: manifest.logical_path().to_string(),
                            message: format!(
                                "chunk {} size mismatch: expected {}, got {}",
                                entry.hash,
                                entry.raw_size,
                                raw.len()
                            ),
                        });
                        continue;
                    }
                    let got = blake3::hash(&raw).to_hex().to_string();
                    if got != entry.hash {
                        issues.push(VerifyIssue {
                            manifest_id: manifest.id.clone(),
                            path: manifest.logical_path().to_string(),
                            message: format!("chunk {} hash mismatch: got {got}", entry.hash),
                        });
                        continue;
                    }
                    hasher.update(&raw);
                }
                Err(e) => issues.push(VerifyIssue {
                    manifest_id: manifest.id.clone(),
                    path: manifest.logical_path().to_string(),
                    message: format!("chunk {} decompress: {e}", entry.hash),
                }),
            },
            Err(e) => issues.push(VerifyIssue {
                manifest_id: manifest.id.clone(),
                path: manifest.logical_path().to_string(),
                message: format!("chunk {} missing: {e}", entry.hash),
            }),
        }
    }

    if issues.is_empty() {
        let digest = hasher.finalize().to_hex().to_string();
        if digest != manifest.hash {
            issues.push(VerifyIssue {
                manifest_id: manifest.id.clone(),
                path: manifest.logical_path().to_string(),
                message: format!("file hash mismatch: expected {}, got {digest}", manifest.hash),
            });
        } else if manifest.size
            != manifest
                .chunks
                .iter()
                .map(|c| c.raw_size as u64)
                .sum::<u64>()
        {
            issues.push(VerifyIssue {
                manifest_id: manifest.id.clone(),
                path: manifest.logical_path().to_string(),
                message: "manifest size does not equal sum of chunk sizes".into(),
            });
        }
    }

    if issues.is_empty() {
        Ok(total)
    } else {
        Err(issues)
    }
}

pub fn verify_restored_file(path: impl AsRef<Path>, expected_hash: &str) -> Result<()> {
    use memmap2::MmapOptions;
    use std::fs::File;
    let path = path.as_ref();
    let file = File::open(path)?;
    let meta = file.metadata()?;
    let got = if meta.len() == 0 {
        blake3::hash(b"").to_hex().to_string()
    } else {
        let mmap = unsafe { MmapOptions::new().map(&file) }
            .map_err(|e| DiceError::msg(format!("mmap {}: {e}", path.display())))?;
        blake3::hash(&mmap).to_hex().to_string()
    };
    if got != expected_hash {
        return Err(DiceError::msg(format!(
            "restored hash mismatch for {}: expected {expected_hash}, got {got}",
            path.display()
        )));
    }
    Ok(())
}

pub fn count_chunk_files(store_root: impl AsRef<Path>) -> Result<usize> {
    let chunks = store_paths(store_root).chunks;
    let mut n = 0usize;
    if !chunks.exists() {
        return Ok(0);
    }
    fn walk(dir: &Path, n: &mut usize) -> Result<()> {
        for e in fs::read_dir(dir)? {
            let e = e?;
            let p = e.path();
            if p.is_dir() {
                walk(&p, n)?;
            } else {
                *n += 1;
            }
        }
        Ok(())
    }
    walk(&chunks, &mut n)?;
    Ok(n)
}
