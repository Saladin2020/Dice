//! Self-signed TLS certificate helpers for Dice sync.

use std::fs;
use std::path::{Path, PathBuf};

use crate::error::{DiceError, Result};

#[derive(Debug, Clone)]
pub struct GeneratedCerts {
    pub cert_path: PathBuf,
    pub key_path: PathBuf,
    pub dir: PathBuf,
}

/// Generate a self-signed cert+key under `dir` (creates dir if needed).
pub fn generate_self_signed(dir: impl AsRef<Path>, hostnames: &[&str]) -> Result<GeneratedCerts> {
    let dir = dir.as_ref();
    fs::create_dir_all(dir)?;
    let mut names: Vec<String> = hostnames.iter().map(|s| (*s).to_string()).collect();
    if names.is_empty() {
        names.push("localhost".into());
        names.push("127.0.0.1".into());
    }
    let certified = rcgen::generate_simple_self_signed(names)
        .map_err(|e| DiceError::msg(format!("generate cert: {e}")))?;
    let cert_path = dir.join("dice-cert.pem");
    let key_path = dir.join("dice-key.pem");
    fs::write(&cert_path, certified.cert.pem())?;
    fs::write(&key_path, certified.key_pair.serialize_pem())?;
    Ok(GeneratedCerts {
        cert_path,
        key_path,
        dir: dir.to_path_buf(),
    })
}

pub fn default_cert_dir() -> PathBuf {
    if let Some(base) = std::env::var_os("APPDATA") {
        PathBuf::from(base).join("Dice").join("tls")
    } else if let Some(home) = std::env::var_os("HOME") {
        PathBuf::from(home).join(".config").join("dice").join("tls")
    } else {
        PathBuf::from("dice-tls")
    }
}
