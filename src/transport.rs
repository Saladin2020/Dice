use std::collections::BTreeSet;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;

use serde::{Deserialize, Serialize};

use crate::codec::decompress;
use crate::error::{DiceError, Result};
use crate::lock::with_store_file_lock;
use crate::manifest::Manifest;
use crate::store::{
    bump_refs, has_chunk, init_store, list_missing, load_manifest, read_chunk, save_manifest,
    write_chunk,
};
use crate::throttle::RateLimiter;
use crate::tlsutil::{
    accept_tls, client_config, client_config_with_ca, connect_tls, server_config, SyncStream,
};

#[derive(Debug, Clone, Default)]
pub struct NetOptions {
    pub token: Option<String>,
    pub tls_cert: Option<PathBuf>,
    pub tls_key: Option<PathBuf>,
    pub tls_ca: Option<PathBuf>,
    /// Client: wrap connection in TLS.
    pub use_tls: bool,
    /// Max bytes/sec for outbound chunk payload (0 = unlimited).
    pub rate_limit_bps: u64,
}

#[derive(Debug, Clone)]
pub struct SyncStats {
    pub manifest_id: String,
    pub sent_chunks: usize,
    pub reused_chunks: usize,
    pub sent_bytes: u64,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum Frame {
    #[serde(rename = "hello")]
    Hello {
        role: String,
        #[serde(default)]
        token: String,
    },
    #[serde(rename = "manifest")]
    ManifestMsg { manifest: Manifest },
    #[serde(rename = "need")]
    Need { hashes: Vec<String> },
    #[serde(rename = "chunk")]
    Chunk { hash: String, size: usize },
    #[serde(rename = "done")]
    Done,
    #[serde(rename = "ok")]
    Ok { hash: String, size: u64 },
    #[serde(rename = "error")]
    Error { message: String },
}

fn write_frame(stream: &mut impl Write, frame: &Frame, payload: Option<&[u8]>) -> Result<()> {
    let json = serde_json::to_vec(frame)?;
    let payload_len = payload.map(|p| p.len()).unwrap_or(0);
    let mut header = [0u8; 8];
    header[0..4].copy_from_slice(&(json.len() as u32).to_be_bytes());
    header[4..8].copy_from_slice(&(payload_len as u32).to_be_bytes());
    stream.write_all(&header)?;
    stream.write_all(&json)?;
    if let Some(p) = payload {
        stream.write_all(p)?;
    }
    stream.flush()?;
    Ok(())
}

fn read_frame(stream: &mut impl Read) -> Result<(Frame, Vec<u8>)> {
    let mut header = [0u8; 8];
    stream.read_exact(&mut header)?;
    let json_len = u32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
    let payload_len = u32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
    if json_len > 32 * 1024 * 1024 || payload_len > 64 * 1024 * 1024 {
        return Err(DiceError::msg("frame too large"));
    }
    let mut json = vec![0u8; json_len];
    stream.read_exact(&mut json)?;
    let mut payload = vec![0u8; payload_len];
    if payload_len > 0 {
        stream.read_exact(&mut payload)?;
    }
    let frame: Frame = serde_json::from_slice(&json)?;
    Ok((frame, payload))
}

fn check_token(expected: &Option<String>, got: &str) -> Result<()> {
    match expected {
        Some(exp) if !exp.is_empty() => {
            if got != exp {
                return Err(DiceError::msg("auth failed: invalid token"));
            }
        }
        _ => {}
    }
    Ok(())
}

pub fn push_to_store<F>(
    local_root: impl AsRef<Path>,
    remote_root: impl AsRef<Path>,
    manifest_id: &str,
    mut on_progress: F,
) -> Result<SyncStats>
where
    F: FnMut(usize, usize),
{
    let local_root = local_root.as_ref();
    let remote_root = remote_root.as_ref().to_path_buf();
    init_store(&remote_root)?;
    let manifest = load_manifest(local_root, manifest_id)?;
    let unique: Vec<String> = manifest
        .chunks
        .iter()
        .map(|c| c.hash.clone())
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect();
    let missing = list_missing(&remote_root, &unique);
    let total = missing.len().max(1);

    let mut sent_bytes = 0u64;
    for (i, hash) in missing.iter().enumerate() {
        let data = read_chunk(local_root, hash)?;
        with_store_file_lock(&remote_root, || write_chunk(&remote_root, hash, &data))?;
        sent_bytes += data.len() as u64;
        on_progress(i + 1, total);
    }

    with_store_file_lock(&remote_root, || {
        bump_refs(&remote_root, &unique)?;
        save_manifest(&remote_root, &manifest)?;
        Ok(())
    })?;
    on_progress(total, total);

    Ok(SyncStats {
        manifest_id: manifest.id,
        sent_chunks: missing.len(),
        reused_chunks: unique.len().saturating_sub(missing.len()),
        sent_bytes,
    })
}

fn accept_chunk(store_root: &Path, hash: &str, compressed: &[u8]) -> Result<()> {
    let raw = decompress(compressed)?;
    let got = blake3::hash(&raw).to_hex().to_string();
    if got != hash {
        return Err(DiceError::msg(format!(
            "received chunk hash mismatch {hash}: got {got}"
        )));
    }
    with_store_file_lock(store_root, || write_chunk(store_root, hash, compressed))?;
    Ok(())
}

fn handle_client(mut stream: SyncStream, store_root: &Path, token: Option<String>) -> Result<()> {
    let mut pending: Option<Manifest> = None;
    let mut authed = match &token {
        None => true,
        Some(t) if t.is_empty() => true,
        Some(_) => false,
    };
    loop {
        let (frame, payload) = read_frame(&mut stream)?;
        match frame {
            Frame::Hello { token: got, .. } => {
                check_token(&token, &got)?;
                authed = true;
                write_frame(
                    &mut stream,
                    &Frame::Hello {
                        role: "server".into(),
                        token: String::new(),
                    },
                    None,
                )?;
            }
            Frame::ManifestMsg { manifest } => {
                if !authed {
                    return Err(DiceError::msg("auth required"));
                }
                let hashes: Vec<String> = manifest
                    .chunks
                    .iter()
                    .map(|c| c.hash.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                let missing = list_missing(store_root, &hashes);
                pending = Some(manifest);
                write_frame(&mut stream, &Frame::Need { hashes: missing }, None)?;
            }
            Frame::Chunk { hash, size } => {
                if !authed {
                    return Err(DiceError::msg("auth required"));
                }
                if pending.is_none() {
                    return Err(DiceError::msg("chunk before manifest"));
                }
                if payload.len() != size {
                    return Err(DiceError::msg(format!(
                        "chunk size header mismatch for {hash}"
                    )));
                }
                accept_chunk(store_root, &hash, &payload)?;
            }
            Frame::Done => {
                if !authed {
                    return Err(DiceError::msg("auth required"));
                }
                let Some(manifest) = pending.take() else {
                    return Err(DiceError::msg("done before manifest"));
                };
                for entry in &manifest.chunks {
                    if !has_chunk(store_root, &entry.hash) {
                        return Err(DiceError::msg(format!(
                            "missing chunk after transfer: {}",
                            entry.hash
                        )));
                    }
                }
                let unique: Vec<String> = manifest
                    .chunks
                    .iter()
                    .map(|c| c.hash.clone())
                    .collect::<BTreeSet<_>>()
                    .into_iter()
                    .collect();
                with_store_file_lock(store_root, || {
                    bump_refs(store_root, &unique)?;
                    save_manifest(store_root, &manifest)?;
                    Ok(())
                })?;
                write_frame(
                    &mut stream,
                    &Frame::Ok {
                        hash: manifest.hash.clone(),
                        size: manifest.size,
                    },
                    None,
                )?;
            }
            Frame::Error { message } => return Err(DiceError::msg(message)),
            _ => {}
        }
    }
}

pub fn serve_store(
    store_root: impl AsRef<Path> + Send + Sync + 'static,
    bind: &str,
    stop: Arc<AtomicBool>,
) -> Result<u16> {
    serve_store_with(store_root, bind, stop, NetOptions::default())
}

pub fn serve_store_with(
    store_root: impl AsRef<Path> + Send + Sync + 'static,
    bind: &str,
    stop: Arc<AtomicBool>,
    opts: NetOptions,
) -> Result<u16> {
    let store_root = store_root.as_ref().to_path_buf();
    init_store(&store_root)?;
    let listener = TcpListener::bind(bind)?;
    listener.set_nonblocking(true)?;
    let port = listener.local_addr()?.port();

    let tls_cfg = match (&opts.tls_cert, &opts.tls_key) {
        (Some(cert), Some(key)) => Some(server_config(cert, key)?),
        (None, None) => None,
        _ => {
            return Err(DiceError::msg(
                "TLS requires both --tls-cert and --tls-key",
            ))
        }
    };
    let token = opts.token.clone();

    thread::spawn(move || {
        while !stop.load(Ordering::SeqCst) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let root = store_root.clone();
                    let tls_cfg = tls_cfg.clone();
                    let token = token.clone();
                    thread::spawn(move || {
                        let _ = stream.set_nonblocking(false);
                        let sync = match tls_cfg {
                            Some(cfg) => match accept_tls(stream, cfg) {
                                Ok(s) => SyncStream::TlsServer(s),
                                Err(e) => {
                                    eprintln!("tls accept: {e}");
                                    return;
                                }
                            },
                            None => SyncStream::Plain(stream),
                        };
                        if let Err(err) = handle_client(sync, &root, token) {
                            let msg = err.to_string();
                            if !msg.contains("failed to fill whole buffer")
                                && !msg.contains("early eof")
                                && !msg.contains("UnexpectedEof")
                            {
                                eprintln!("client error: {msg}");
                            }
                        }
                    });
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(std::time::Duration::from_millis(50));
                }
                Err(e) => {
                    eprintln!("accept error: {e}");
                    break;
                }
            }
        }
    });

    Ok(port)
}

pub fn push_over_network<F>(
    local_root: impl AsRef<Path>,
    host: &str,
    port: u16,
    manifest_id: &str,
    on_progress: F,
) -> Result<SyncStats>
where
    F: FnMut(usize, usize),
{
    push_over_network_with(
        local_root,
        host,
        port,
        manifest_id,
        NetOptions::default(),
        on_progress,
    )
}

pub fn push_over_network_with<F>(
    local_root: impl AsRef<Path>,
    host: &str,
    port: u16,
    manifest_id: &str,
    opts: NetOptions,
    mut on_progress: F,
) -> Result<SyncStats>
where
    F: FnMut(usize, usize),
{
    let local_root = local_root.as_ref();
    let manifest = load_manifest(local_root, manifest_id)?;
    let tcp = TcpStream::connect((host, port))?;
    let mut stream = if opts.use_tls || opts.tls_ca.is_some() {
        let cfg = if let Some(ca) = &opts.tls_ca {
            client_config_with_ca(ca)?
        } else {
            client_config(false)?
        };
        SyncStream::TlsClient(connect_tls(tcp, host, cfg)?)
    } else {
        SyncStream::Plain(tcp)
    };

    let token = opts.token.clone().unwrap_or_default();
    write_frame(
        &mut stream,
        &Frame::Hello {
            role: "client".into(),
            token,
        },
        None,
    )?;
    write_frame(
        &mut stream,
        &Frame::ManifestMsg {
            manifest: manifest.clone(),
        },
        None,
    )?;
    on_progress(0, 1);

    let mut sent_chunks = 0usize;
    let mut sent_bytes = 0u64;
    let mut reused_chunks = 0usize;
    let mut limiter = RateLimiter::new(opts.rate_limit_bps);

    loop {
        let (frame, _) = read_frame(&mut stream)?;
        match frame {
            Frame::Hello { .. } => continue,
            Frame::Need { hashes } => {
                let unique_count = manifest
                    .chunks
                    .iter()
                    .map(|c| c.hash.as_str())
                    .collect::<BTreeSet<_>>()
                    .len();
                reused_chunks = unique_count.saturating_sub(hashes.len());
                let total = hashes.len().max(1);
                if hashes.is_empty() {
                    on_progress(1, 1);
                }
                for (i, hash) in hashes.into_iter().enumerate() {
                    let payload = read_chunk(local_root, &hash)?;
                    let size = payload.len();
                    limiter.take(size as u64);
                    write_frame(
                        &mut stream,
                        &Frame::Chunk {
                            hash: hash.clone(),
                            size,
                        },
                        Some(&payload),
                    )?;
                    sent_chunks += 1;
                    sent_bytes += size as u64;
                    on_progress(i + 1, total);
                }
                write_frame(&mut stream, &Frame::Done, None)?;
            }
            Frame::Ok { .. } => {
                return Ok(SyncStats {
                    manifest_id: manifest.id,
                    sent_chunks,
                    reused_chunks,
                    sent_bytes,
                });
            }
            Frame::Error { message } => return Err(DiceError::msg(message)),
            other => {
                return Err(DiceError::msg(format!("unexpected frame: {other:?}")));
            }
        }
    }
}
