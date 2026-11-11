use std::fs;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::error::{DiceError, Result};
use crate::exclude::ExcludeSet;
use crate::paths::relative_under;

#[derive(Debug, Clone)]
pub struct FsEntry {
    pub name: String,
    pub path: PathBuf,
    pub is_dir: bool,
    pub size: u64,
    pub modified: Option<SystemTime>,
}

/// One file to convert, with a portable relative path under the Target base folder.
#[derive(Debug, Clone)]
pub struct SelectedFile {
    pub path: PathBuf,
    pub relative_path: String,
    pub size: u64,
}

/// Expand Target selection: files as-is; folders walked recursively (symlinks skipped).
/// Relative paths are under `base` (the current Target folder).
pub fn expand_selection(
    base: impl AsRef<Path>,
    selected: impl IntoIterator<Item = PathBuf>,
) -> Result<Vec<SelectedFile>> {
    expand_selection_filtered(base, selected, &ExcludeSet::from_defaults())
}

pub fn expand_selection_filtered(
    base: impl AsRef<Path>,
    selected: impl IntoIterator<Item = PathBuf>,
    excludes: &ExcludeSet,
) -> Result<Vec<SelectedFile>> {
    let base = base.as_ref();
    if !base.is_dir() {
        return Err(DiceError::msg(format!(
            "base is not a directory: {}",
            base.display()
        )));
    }

    let mut out: Vec<SelectedFile> = Vec::new();
    let mut seen = std::collections::BTreeSet::new();

    for path in selected {
        if excludes.matches_path(&path) {
            continue;
        }
        let meta = match fs::symlink_metadata(&path) {
            Ok(m) => m,
            Err(e) => {
                return Err(DiceError::msg(format!(
                    "cannot read {}: {e}",
                    path.display()
                )));
            }
        };
        if meta.file_type().is_symlink() {
            continue;
        }
        if meta.is_file() {
            push_file(base, &path, meta.len(), excludes, &mut out, &mut seen)?;
        } else if meta.is_dir() {
            walk_dir(base, &path, excludes, &mut out, &mut seen)?;
        }
    }

    out.sort_by(|a, b| a.relative_path.cmp(&b.relative_path));
    Ok(out)
}

fn push_file(
    base: &Path,
    path: &Path,
    size: u64,
    excludes: &ExcludeSet,
    out: &mut Vec<SelectedFile>,
    seen: &mut std::collections::BTreeSet<PathBuf>,
) -> Result<()> {
    if excludes.matches_path(path) {
        return Ok(());
    }
    if !seen.insert(path.to_path_buf()) {
        return Ok(());
    }
    let relative_path = relative_under(base, path)?;
    if excludes.matches_path(Path::new(&relative_path)) {
        return Ok(());
    }
    out.push(SelectedFile {
        path: path.to_path_buf(),
        relative_path,
        size,
    });
    Ok(())
}

fn walk_dir(
    base: &Path,
    dir: &Path,
    excludes: &ExcludeSet,
    out: &mut Vec<SelectedFile>,
    seen: &mut std::collections::BTreeSet<PathBuf>,
) -> Result<()> {
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        if excludes.matches_path(&current) {
            continue;
        }
        let rd = fs::read_dir(&current).map_err(|e| {
            DiceError::msg(format!("cannot read {}: {e}", current.display()))
        })?;
        for entry in rd {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().to_string();
            if excludes.matches_name(&name) {
                continue;
            }
            let path = entry.path();
            let meta = match fs::symlink_metadata(&path) {
                Ok(m) => m,
                Err(e) => {
                    return Err(DiceError::msg(format!(
                        "cannot read {}: {e}",
                        path.display()
                    )));
                }
            };
            if meta.file_type().is_symlink() {
                continue;
            }
            if meta.is_dir() {
                stack.push(path);
            } else if meta.is_file() {
                push_file(base, &path, meta.len(), excludes, out, seen)?;
            }
        }
    }
    Ok(())
}

pub fn list_dir(path: impl AsRef<Path>) -> Result<Vec<FsEntry>> {
    let path = path.as_ref();
    if !path.exists() {
        return Err(DiceError::msg(format!(
            "path does not exist: {}",
            path.display()
        )));
    }
    if !path.is_dir() {
        return Err(DiceError::msg(format!(
            "not a directory: {}",
            path.display()
        )));
    }

    let mut entries = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let meta = entry.metadata()?;
        let name = entry.file_name().to_string_lossy().to_string();
        if name.starts_with('.') && name != ".." {
            // keep visible files; still show normal dirs/files including hidden if wanted
        }
        entries.push(FsEntry {
            name,
            path: entry.path(),
            is_dir: meta.is_dir(),
            size: if meta.is_file() { meta.len() } else { 0 },
            modified: meta.modified().ok(),
        });
    }

    entries.sort_by(|a, b| match (a.is_dir, b.is_dir) {
        (true, false) => std::cmp::Ordering::Less,
        (false, true) => std::cmp::Ordering::Greater,
        _ => a.name.to_lowercase().cmp(&b.name.to_lowercase()),
    });
    Ok(entries)
}

pub fn default_local_path() -> PathBuf {
    std::env::current_dir()
        .or_else(|_| {
            std::env::var_os("USERPROFILE")
                .or_else(|| std::env::var_os("HOME"))
                .map(PathBuf::from)
                .ok_or_else(|| std::io::Error::other("no home"))
        })
        .unwrap_or_else(|_| PathBuf::from("."))
}

pub fn format_modified(t: Option<SystemTime>) -> String {
    let Some(t) = t else {
        return "—".into();
    };
    let Ok(duration) = t.duration_since(SystemTime::UNIX_EPOCH) else {
        return "—".into();
    };
    // simple UTC-ish display without extra deps beyond chrono already in project
    let secs = duration.as_secs() as i64;
    chrono::DateTime::from_timestamp(secs, 0)
        .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
        .unwrap_or_else(|| "—".into())
}

pub fn file_type_label(entry: &FsEntry) -> String {
    if entry.is_dir {
        return "Folder".into();
    }
    entry
        .path
        .extension()
        .map(|e| e.to_string_lossy().to_string())
        .filter(|s| !s.is_empty())
        .unwrap_or_else(|| "File".into())
}
