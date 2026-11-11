//! Interrupt-safe convert journal for resume.

use std::fs;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::error::{DiceError, Result};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum JobFileStatus {
    Pending,
    Done,
    Skipped,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobFile {
    pub absolute: String,
    pub relative_path: String,
    pub size: u64,
    pub status: JobFileStatus,
    #[serde(default)]
    pub error: String,
    #[serde(default)]
    pub manifest_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ConvertJob {
    pub version: u32,
    pub store: String,
    pub base: String,
    pub zstd_level: i32,
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
    pub workers: usize,
    pub files: Vec<JobFile>,
    pub updated_at: String,
}

impl ConvertJob {
    pub fn journal_path(store: impl AsRef<Path>) -> PathBuf {
        store.as_ref().join("jobs").join("convert-resume.json")
    }

    pub fn save(&self, path: impl AsRef<Path>) -> Result<()> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let tmp = path.with_extension("json.tmp");
        fs::write(&tmp, serde_json::to_string_pretty(self)? + "\n")?;
        fs::rename(&tmp, path)?;
        Ok(())
    }

    pub fn load(path: impl AsRef<Path>) -> Result<Self> {
        let raw = fs::read_to_string(path)?;
        Ok(serde_json::from_str(&raw)?)
    }

    pub fn pending_count(&self) -> usize {
        self.files
            .iter()
            .filter(|f| f.status == JobFileStatus::Pending || f.status == JobFileStatus::Failed)
            .count()
    }

    pub fn mark(
        &mut self,
        relative_path: &str,
        status: JobFileStatus,
        manifest_id: impl Into<String>,
        error: impl Into<String>,
    ) {
        if let Some(f) = self
            .files
            .iter_mut()
            .find(|f| f.relative_path == relative_path)
        {
            f.status = status;
            f.manifest_id = manifest_id.into();
            f.error = error.into();
        }
        self.updated_at = chrono::Utc::now().to_rfc3339();
    }

    pub fn clear_journal(store: impl AsRef<Path>) -> Result<()> {
        let path = Self::journal_path(store);
        if path.exists() {
            fs::remove_file(path)?;
        }
        Ok(())
    }
}

pub fn ensure_jobs_dir(store: impl AsRef<Path>) -> Result<PathBuf> {
    let dir = store.as_ref().join("jobs");
    fs::create_dir_all(&dir)?;
    Ok(dir)
}

pub fn default_log_path(store: impl AsRef<Path>) -> PathBuf {
    store.as_ref().join("logs").join(format!(
        "dice-{}.log",
        chrono::Utc::now().format("%Y%m%d-%H%M%S")
    ))
}

pub fn append_log(path: impl AsRef<Path>, line: &str) -> Result<()> {
    use std::io::Write;
    let path = path.as_ref();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let mut f = fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .map_err(|e| DiceError::msg(format!("log {}: {e}", path.display())))?;
    writeln!(f, "{}", line)?;
    Ok(())
}
