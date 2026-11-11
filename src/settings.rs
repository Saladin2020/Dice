//! Persisted app settings for Dice desktop.

use std::fs;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::error::{DiceError, Result};
use crate::exclude::{ExcludeSet, DEFAULT_EXCLUDES};
use crate::pool::default_pool_size;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct AppSettings {
    pub min_size: u32,
    pub avg_size: u32,
    pub max_size: u32,
    pub zstd_level: i32,
    pub workers: usize,
    pub send_host: String,
    pub serve_bind: String,
    /// Shared secret for Send/Serve auth (empty = auth disabled).
    pub auth_token: String,
    /// Comma-separated exclude names for nested folder walks.
    pub excludes: String,
    pub tls_cert: String,
    pub tls_key: String,
    pub tls_ca: String,
    /// Max send rate in bytes/sec (0 = unlimited).
    pub send_rate_bps: u64,
    /// Verify content hash after restore.
    pub verify_restore: bool,
}

impl Default for AppSettings {
    fn default() -> Self {
        Self {
            min_size: 256 * 1024,
            avg_size: 1024 * 1024,
            max_size: 4 * 1024 * 1024,
            zstd_level: 1,
            workers: default_pool_size(),
            send_host: "127.0.0.1:7788".into(),
            serve_bind: "0.0.0.0:7788".into(),
            auth_token: String::new(),
            excludes: DEFAULT_EXCLUDES.join(","),
            tls_cert: String::new(),
            tls_key: String::new(),
            tls_ca: String::new(),
            send_rate_bps: 0,
            verify_restore: true,
        }
    }
}

impl AppSettings {
    pub fn validate(&self) -> Result<()> {
        if self.min_size < 64 {
            return Err(DiceError::msg("min chunk size must be ≥ 64"));
        }
        if self.avg_size < self.min_size || self.max_size < self.avg_size {
            return Err(DiceError::msg("require min ≤ avg ≤ max chunk sizes"));
        }
        if !(1..=22).contains(&self.zstd_level) {
            return Err(DiceError::msg("zstd level must be 1–22"));
        }
        if !(1..=8).contains(&self.workers) {
            return Err(DiceError::msg("workers must be 1–8"));
        }
        if self.send_host.trim().is_empty() {
            return Err(DiceError::msg("send host is empty"));
        }
        if self.serve_bind.trim().is_empty() {
            return Err(DiceError::msg("serve bind is empty"));
        }
        Ok(())
    }

    pub fn exclude_set(&self) -> ExcludeSet {
        if self.excludes.trim().is_empty() {
            ExcludeSet::from_defaults()
        } else {
            ExcludeSet::parse_csv(&self.excludes)
        }
    }
}

pub fn settings_path() -> PathBuf {
    if let Some(base) = std::env::var_os("APPDATA") {
        PathBuf::from(base).join("Dice").join("settings.json")
    } else if let Some(home) = std::env::var_os("HOME") {
        PathBuf::from(home)
            .join(".config")
            .join("dice")
            .join("settings.json")
    } else {
        PathBuf::from("dice-settings.json")
    }
}

pub fn load_settings() -> AppSettings {
    let path = settings_path();
    match fs::read_to_string(&path) {
        Ok(raw) => serde_json::from_str(&raw).unwrap_or_default(),
        Err(_) => AppSettings::default(),
    }
}

pub fn save_settings(settings: &AppSettings) -> Result<()> {
    settings.validate()?;
    let path = settings_path();
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)?;
    }
    let raw = serde_json::to_string_pretty(settings)? + "\n";
    fs::write(path, raw)?;
    Ok(())
}
