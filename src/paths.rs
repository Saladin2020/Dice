//! Safe relative-path helpers for nested folder ingest/restore.

use std::path::{Component, Path, PathBuf};

use crate::error::{DiceError, Result};

/// Normalize a logical store path to portable form: `a/b/c.txt` (forward slashes).
/// Rejects absolute paths, `..`, empty segments, and Windows prefixes.
pub fn normalize_relative_path(input: &str) -> Result<String> {
    let trimmed = input.trim().trim_matches(|c| c == '/' || c == '\\');
    if trimmed.is_empty() {
        return Err(DiceError::msg("relative path is empty"));
    }
    if trimmed.contains('\0') {
        return Err(DiceError::msg("relative path contains NUL"));
    }

    let path = Path::new(trimmed);
    if path.is_absolute() {
        return Err(DiceError::msg(format!(
            "absolute path not allowed: {trimmed}"
        )));
    }

    let mut parts: Vec<String> = Vec::new();
    for comp in path.components() {
        match comp {
            Component::Normal(s) => {
                let s = s.to_string_lossy();
                if s.is_empty() || s == "." {
                    continue;
                }
                if s == ".." {
                    return Err(DiceError::msg("relative path must not contain '..'"));
                }
                // Block Windows drive-like segments sneaked into relative strings.
                if s.contains(':') {
                    return Err(DiceError::msg(format!(
                        "invalid path segment \"{s}\""
                    )));
                }
                parts.push(s.into_owned());
            }
            Component::CurDir => {}
            Component::ParentDir => {
                return Err(DiceError::msg("relative path must not contain '..'"));
            }
            Component::RootDir | Component::Prefix(_) => {
                return Err(DiceError::msg(format!(
                    "absolute path not allowed: {trimmed}"
                )));
            }
        }
    }

    if parts.is_empty() {
        return Err(DiceError::msg("relative path is empty"));
    }

    Ok(parts.join("/"))
}

/// Build a relative path of `file` under `base` using `/` separators.
pub fn relative_under(base: impl AsRef<Path>, file: impl AsRef<Path>) -> Result<String> {
    let base = base.as_ref();
    let file = file.as_ref();
    let rel = file.strip_prefix(base).map_err(|_| {
        DiceError::msg(format!(
            "path {} is outside base {}",
            file.display(),
            base.display()
        ))
    })?;
    normalize_relative_path(&rel.to_string_lossy())
}

/// Join `dest` with a normalized relative path and ensure the result stays under `dest`.
pub fn safe_join(dest: impl AsRef<Path>, relative: &str) -> Result<PathBuf> {
    let dest = dest.as_ref();
    let norm = normalize_relative_path(relative)?;
    let mut out = dest.to_path_buf();
    for part in norm.split('/') {
        out.push(part);
    }

    let dest_canon = canonicalize_existing(dest)?;
    // Parent dirs of out may not exist yet — walk up to first existing ancestor,
    // then verify that ancestor is under dest; final path must start with dest.
    let out_check = if out.exists() {
        canonicalize_existing(&out)?
    } else if let Some(parent) = out.parent() {
        let parent_existing = first_existing_ancestor(parent)?;
        let parent_canon = canonicalize_existing(&parent_existing)?;
        if !is_under(&parent_canon, &dest_canon) {
            return Err(DiceError::msg(format!(
                "refusing path escape: {}",
                out.display()
            )));
        }
        // Reconstruct expected canonical child path for prefix check.
        let suffix = out
            .strip_prefix(&parent_existing)
            .unwrap_or(Path::new(""));
        parent_canon.join(suffix)
    } else {
        return Err(DiceError::msg("invalid destination path"));
    };

    if !is_under(&out_check, &dest_canon) && out_check != dest_canon {
        return Err(DiceError::msg(format!(
            "refusing path escape: {}",
            out.display()
        )));
    }

    Ok(out)
}

fn first_existing_ancestor(path: &Path) -> Result<PathBuf> {
    let mut cur = path.to_path_buf();
    loop {
        if cur.exists() {
            return Ok(cur);
        }
        if !cur.pop() {
            return Err(DiceError::msg(format!(
                "no existing ancestor for {}",
                path.display()
            )));
        }
    }
}

fn canonicalize_existing(path: &Path) -> Result<PathBuf> {
    fs_canonicalize(path).map_err(|e| {
        DiceError::msg(format!("canonicalize {}: {e}", path.display()))
    })
}

fn fs_canonicalize(path: &Path) -> std::io::Result<PathBuf> {
    // Prefer dunce-free std canonicalize; on Windows this returns \\?\ paths.
    std::fs::canonicalize(path)
}

fn is_under(path: &Path, root: &Path) -> bool {
    if path == root {
        return true;
    }
    path.starts_with(root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_accepts_nested() {
        assert_eq!(
            normalize_relative_path(r"docs\2024\a.txt").unwrap(),
            "docs/2024/a.txt"
        );
        assert_eq!(
            normalize_relative_path("a/b/c.txt").unwrap(),
            "a/b/c.txt"
        );
    }

    #[test]
    fn normalize_rejects_dotdot_and_absolute() {
        assert!(normalize_relative_path("../x").is_err());
        assert!(normalize_relative_path("a/../b").is_err());
        #[cfg(windows)]
        assert!(normalize_relative_path(r"C:\x").is_err());
        #[cfg(unix)]
        assert!(normalize_relative_path("/etc/passwd").is_err());
    }

    #[test]
    fn safe_join_keeps_under_dest() {
        let dest = std::env::temp_dir().join(format!(
            "dice-safe-join-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dest);
        std::fs::create_dir_all(&dest).unwrap();
        let out = safe_join(&dest, "a/b/c.txt").unwrap();
        assert!(out.ends_with(Path::new("a").join("b").join("c.txt")));
        assert!(out.starts_with(&dest));
        let _ = std::fs::remove_dir_all(&dest);
    }
}
