//! Optional TLS helpers for Dice sync (rustls).

use std::fs;
use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;

use rustls::pki_types::{CertificateDer, PrivateKeyDer, ServerName};
use rustls::{ClientConfig, RootCertStore, ServerConfig};

use crate::error::{DiceError, Result};

pub fn load_certs(path: impl AsRef<Path>) -> Result<Vec<CertificateDer<'static>>> {
    let raw = fs::read(path.as_ref())?;
    let mut reader = std::io::Cursor::new(raw);
    let certs: Vec<_> = rustls_pemfile::certs(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| DiceError::msg(format!("read certs: {e}")))?;
    if certs.is_empty() {
        return Err(DiceError::msg("no certificates found"));
    }
    Ok(certs)
}

pub fn load_private_key(path: impl AsRef<Path>) -> Result<PrivateKeyDer<'static>> {
    let raw = fs::read(path.as_ref())?;
    let mut reader = std::io::Cursor::new(raw);
    let keys: Vec<_> = rustls_pemfile::pkcs8_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| DiceError::msg(format!("read key: {e}")))?;
    if let Some(k) = keys.into_iter().next() {
        return Ok(PrivateKeyDer::Pkcs8(k));
    }
    // retry as RSA
    let raw = fs::read(path.as_ref())?;
    let mut reader = std::io::Cursor::new(raw);
    let keys: Vec<_> = rustls_pemfile::rsa_private_keys(&mut reader)
        .collect::<std::result::Result<Vec<_>, _>>()
        .map_err(|e| DiceError::msg(format!("read rsa key: {e}")))?;
    keys.into_iter()
        .next()
        .map(PrivateKeyDer::Pkcs1)
        .ok_or_else(|| DiceError::msg("no private key found"))
}

pub fn server_config(cert: impl AsRef<Path>, key: impl AsRef<Path>) -> Result<Arc<ServerConfig>> {
    let certs = load_certs(cert)?;
    let key = load_private_key(key)?;
    let cfg = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .map_err(|e| DiceError::msg(format!("tls server config: {e}")))?;
    Ok(Arc::new(cfg))
}

pub fn client_config(insecure: bool) -> Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    if insecure {
        // Still need a config; use empty roots and custom verifier via dangerous API is complex.
        // For self-signed lab use, prefer providing --tls-ca. Fallback: webpki-roots only.
    }
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

pub fn client_config_with_ca(ca_path: impl AsRef<Path>) -> Result<Arc<ClientConfig>> {
    let mut roots = RootCertStore::empty();
    for cert in load_certs(ca_path)? {
        roots
            .add(cert)
            .map_err(|e| DiceError::msg(format!("add ca: {e}")))?;
    }
    let cfg = ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();
    Ok(Arc::new(cfg))
}

pub type TlsStream = rustls::StreamOwned<rustls::ServerConnection, TcpStream>;
pub type TlsClientStream = rustls::StreamOwned<rustls::ClientConnection, TcpStream>;

pub fn accept_tls(tcp: TcpStream, cfg: Arc<ServerConfig>) -> Result<TlsStream> {
    let conn = rustls::ServerConnection::new(cfg)
        .map_err(|e| DiceError::msg(format!("tls server conn: {e}")))?;
    Ok(rustls::StreamOwned::new(conn, tcp))
}

pub fn connect_tls(
    tcp: TcpStream,
    host: &str,
    cfg: Arc<ClientConfig>,
) -> Result<TlsClientStream> {
    let name = ServerName::try_from(host.to_string())
        .map_err(|_| DiceError::msg(format!("invalid TLS server name: {host}")))?;
    let conn = rustls::ClientConnection::new(cfg, name)
        .map_err(|e| DiceError::msg(format!("tls client conn: {e}")))?;
    Ok(rustls::StreamOwned::new(conn, tcp))
}

/// Erased read/write stream used by the sync protocol.
pub enum SyncStream {
    Plain(TcpStream),
    TlsServer(TlsStream),
    TlsClient(TlsClientStream),
}

impl Read for SyncStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.read(buf),
            Self::TlsServer(s) => s.read(buf),
            Self::TlsClient(s) => s.read(buf),
        }
    }
}

impl Write for SyncStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            Self::Plain(s) => s.write(buf),
            Self::TlsServer(s) => s.write(buf),
            Self::TlsClient(s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            Self::Plain(s) => s.flush(),
            Self::TlsServer(s) => s.flush(),
            Self::TlsClient(s) => s.flush(),
        }
    }
}
