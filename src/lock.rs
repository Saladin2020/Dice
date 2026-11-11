//! Cross-process store lock (CLI + GUI safe).

use std::fs::{File, OpenOptions};
use std::path::{Path, PathBuf};

use fs4::fs_std::FileExt;

use crate::error::{DiceError, Result};
use crate::store::store_paths;

pub struct StoreLock {
    _file: File,
    path: PathBuf,
}

impl StoreLock {
    /// Acquire an exclusive lock on `<store>/.lock` (blocks until available).
    pub fn acquire(store_root: impl AsRef<Path>) -> Result<Self> {
        let paths = store_paths(store_root);
        std::fs::create_dir_all(&paths.root)?;
        let path = paths.root.join(".lock");
        let file = OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(&path)
            .map_err(|e| DiceError::msg(format!("open lock {}: {e}", path.display())))?;
        file.lock_exclusive()
            .map_err(|e| DiceError::msg(format!("lock {}: {e}", path.display())))?;
        Ok(Self { _file: file, path })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl Drop for StoreLock {
    fn drop(&mut self) {
        let _ = self._file.unlock();
    }
}

/// Run `f` while holding both the in-process mutex and the cross-process file lock.
pub fn with_store_file_lock<T>(
    store_root: impl AsRef<Path>,
    f: impl FnOnce() -> Result<T>,
) -> Result<T> {
    let root = store_root.as_ref().to_path_buf();
    crate::store::with_store_lock(|| {
        let _flock = StoreLock::acquire(&root)?;
        f()
    })
}
