use serde::{Deserialize, Serialize};

use crate::chunker::ChunkerOptions;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ManifestChunk {
    pub hash: String,
    pub raw_size: usize,
    pub compressed_size: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ChunkerMeta {
    pub algo: String,
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Manifest {
    pub version: u32,
    pub id: String,
    pub name: String,
    /// Portable relative path with `/` separators (e.g. `docs/a.txt`).
    /// Empty for older manifests — treat as `name` via [`Manifest::logical_path`].
    #[serde(default)]
    pub relative_path: String,
    pub size: u64,
    pub hash: String,
    pub codec: String,
    pub level: i32,
    pub chunker: ChunkerMeta,
    pub chunks: Vec<ManifestChunk>,
    pub created_at: String,
}

impl Manifest {
    /// Path used for restore and display (relative tree when present).
    pub fn logical_path(&self) -> &str {
        if self.relative_path.is_empty() {
            &self.name
        } else {
            &self.relative_path
        }
    }
}

pub fn create_manifest_id(file_hash: &str, name: &str) -> String {
    let safe: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(40)
        .collect();
    format!("{}-{}", &file_hash[..16.min(file_hash.len())], safe)
}

pub fn chunker_meta(opts: ChunkerOptions) -> ChunkerMeta {
    ChunkerMeta {
        algo: "fastcdc".into(),
        min_size: opts.min_size,
        avg_size: opts.avg_size,
        max_size: opts.max_size,
    }
}
