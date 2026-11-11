use std::collections::BTreeSet;
use std::fs::File;
use std::path::Path;

use memmap2::MmapOptions;

use crate::chunker::ChunkerOptions;
use crate::codec::{compress, DEFAULT_ZSTD_LEVEL};
use crate::error::{DiceError, Result};
use crate::gc::{already_ingested, find_manifest_by_logical_path};
use crate::lock::with_store_file_lock;
use crate::manifest::{chunker_meta, create_manifest_id, Manifest, ManifestChunk};
use crate::paths::normalize_relative_path;
use crate::store::{bump_refs, init_store, save_manifest, write_chunk};
use fastcdc::v2020::FastCDC;

#[derive(Debug, Clone)]
pub struct IngestResult {
    pub manifest: Manifest,
    pub new_chunks: usize,
    pub reused_chunks: usize,
    pub raw_bytes: u64,
    pub stored_bytes: u64,
    pub skipped: bool,
}

fn validate_chunker(opts: ChunkerOptions) -> Result<()> {
    if opts.min_size < 64 || opts.avg_size < opts.min_size || opts.max_size < opts.avg_size {
        return Err(DiceError::msg(
            "invalid chunker options: require min <= avg <= max",
        ));
    }
    Ok(())
}

/// Ingest a single file using memory-mapped I/O (low RAM for large files).
/// Chunks are written incrementally; the manifest is committed last.
/// If the same logical path + content hash already exists, returns skipped.
pub fn ingest_file<F>(
    store_root: impl AsRef<Path>,
    file_path: impl AsRef<Path>,
    relative_path: Option<&str>,
    chunker: ChunkerOptions,
    level: i32,
    mut on_progress: F,
) -> Result<IngestResult>
where
    F: FnMut(usize, usize),
{
    let store_root = store_root.as_ref();
    let file_path = file_path.as_ref();
    init_store(store_root)?;
    validate_chunker(chunker)?;

    let name = file_path
        .file_name()
        .map(|s| s.to_string_lossy().to_string())
        .unwrap_or_else(|| "file".into());
    let relative_path = match relative_path {
        Some(p) => normalize_relative_path(p)?,
        None => normalize_relative_path(&name)?,
    };
    let leaf = relative_path
        .rsplit('/')
        .next()
        .unwrap_or(name.as_str())
        .to_string();

    let level = if level == 0 {
        DEFAULT_ZSTD_LEVEL
    } else {
        level
    };

    on_progress(0, 1);

    let file = File::open(file_path).map_err(|e| {
        DiceError::msg(format!("open {}: {e}", file_path.display()))
    })?;
    let meta = file.metadata()?;
    let size = meta.len();

    // Empty file: no mmap.
    if size == 0 {
        let file_hash = blake3::hash(b"").to_hex().to_string();
        if already_ingested(store_root, &relative_path, &file_hash)? {
            let manifest = find_manifest_by_logical_path(store_root, &relative_path)?
                .ok_or_else(|| DiceError::msg("skipped but manifest missing"))?;
            on_progress(1, 1);
            return Ok(IngestResult {
                manifest,
                new_chunks: 0,
                reused_chunks: 0,
                raw_bytes: 0,
                stored_bytes: 0,
                skipped: true,
            });
        }
        return commit_manifest(
            store_root,
            relative_path,
            leaf,
            file_hash,
            0,
            Vec::new(),
            0,
            0,
            0,
            chunker,
            level,
            &mut on_progress,
        );
    }

    let mmap = unsafe { MmapOptions::new().map(&file) }.map_err(|e| {
        DiceError::msg(format!("mmap {}: {e}", file_path.display()))
    })?;
    let data: &[u8] = &mmap;
    let file_hash = blake3::hash(data).to_hex().to_string();

    if already_ingested(store_root, &relative_path, &file_hash)? {
        let manifest = find_manifest_by_logical_path(store_root, &relative_path)?
            .ok_or_else(|| DiceError::msg("skipped but manifest missing"))?;
        on_progress(1, 1);
        return Ok(IngestResult {
            manifest,
            new_chunks: 0,
            reused_chunks: 0,
            raw_bytes: size,
            stored_bytes: 0,
            skipped: true,
        });
    }

    let cdc = FastCDC::new(data, chunker.min_size, chunker.avg_size, chunker.max_size);
    let mut manifest_chunks = Vec::new();
    let mut new_chunks = 0usize;
    let mut reused_chunks = 0usize;
    let mut stored_bytes = 0u64;
    let mut unique = BTreeSet::new();
    let mut index = 0usize;

    // Collect chunk ranges first for progress denominator.
    let ranges: Vec<(usize, usize)> = cdc
        .map(|c| {
            let start = c.offset as usize;
            (start, start + c.length as usize)
        })
        .collect();
    let total = ranges.len().max(1);

    for (start, end) in ranges {
        let slice = &data[start..end];
        let hash = blake3::hash(slice).to_hex().to_string();
        let compressed = compress(slice, level)?;
        let compressed_size = compressed.len();
        let raw_size = slice.len();

        let created = with_store_file_lock(store_root, || {
            write_chunk(store_root, &hash, &compressed)
        })?;
        if created {
            new_chunks += 1;
            stored_bytes += compressed_size as u64;
        } else {
            reused_chunks += 1;
        }
        unique.insert(hash.clone());
        manifest_chunks.push(ManifestChunk {
            hash,
            raw_size,
            compressed_size,
        });
        index += 1;
        on_progress(index, total);
    }

    let unique_vec: Vec<String> = unique.into_iter().collect();
    with_store_file_lock(store_root, || {
        bump_refs(store_root, &unique_vec)?;
        let manifest = Manifest {
            version: 1,
            id: create_manifest_id(&file_hash, &relative_path),
            name: leaf.clone(),
            relative_path: relative_path.clone(),
            size,
            hash: file_hash.clone(),
            codec: "zstd".into(),
            level,
            chunker: chunker_meta(chunker),
            chunks: manifest_chunks.clone(),
            created_at: chrono::Utc::now().to_rfc3339(),
        };
        save_manifest(store_root, &manifest)?;
        Ok(manifest)
    })
    .map(|manifest| {
        on_progress(total, total);
        IngestResult {
            manifest,
            new_chunks,
            reused_chunks,
            raw_bytes: size,
            stored_bytes,
            skipped: false,
        }
    })
}

fn commit_manifest<F>(
    store_root: &Path,
    relative_path: String,
    leaf: String,
    file_hash: String,
    size: u64,
    manifest_chunks: Vec<ManifestChunk>,
    new_chunks: usize,
    reused_chunks: usize,
    stored_bytes: u64,
    chunker: ChunkerOptions,
    level: i32,
    on_progress: &mut F,
) -> Result<IngestResult>
where
    F: FnMut(usize, usize),
{
    let unique: Vec<String> = manifest_chunks.iter().map(|c| c.hash.clone()).collect();
    let manifest = with_store_file_lock(store_root, || {
        if !unique.is_empty() {
            bump_refs(store_root, &unique)?;
        }
        let manifest = Manifest {
            version: 1,
            id: create_manifest_id(&file_hash, &relative_path),
            name: leaf.clone(),
            relative_path: relative_path.clone(),
            size,
            hash: file_hash.clone(),
            codec: "zstd".into(),
            level,
            chunker: chunker_meta(chunker),
            chunks: manifest_chunks,
            created_at: chrono::Utc::now().to_rfc3339(),
        };
        save_manifest(store_root, &manifest)?;
        Ok(manifest)
    })?;
    on_progress(1, 1);
    Ok(IngestResult {
        manifest,
        new_chunks,
        reused_chunks,
        raw_bytes: size,
        stored_bytes,
        skipped: false,
    })
}
