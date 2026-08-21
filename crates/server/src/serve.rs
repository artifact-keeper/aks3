// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The accept loop: everything between a TCP connection and the S3 service.
//!
//! [`run`] assembles the three crates below it (the engine that holds the data,
//! the service that speaks S3, the auth provider that decides whose request it
//! is), binds a listener, and hands each connection to `hyper`. Nothing in this
//! module knows what an S3 operation is.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context as _;
use hyper_util::rt::{TokioExecutor, TokioIo};
use hyper_util::server::conn::auto::Builder as ConnBuilder;
use hyper_util::server::graceful::{GracefulShutdown, Watcher};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio_rustls::rustls::pki_types::{CertificateDer, PrivateKeyDer};
use tokio_rustls::rustls::ServerConfig;
use tokio_rustls::TlsAcceptor;

use aks3_api::Aks3;
use aks3_engine::FsEngine;
use aks3_iam::{IamAuth, RootCredentials};
use s3s::service::{S3Service, S3ServiceBuilder};

use crate::config::{Config, TlsConfig};

/// How long a shutdown waits for connections that are still in flight before
/// dropping them. A `GET` of a large object is a long-lived connection, so this
/// is generous enough to let one finish rather than tuned for a fast exit.
const GRACE_PERIOD: Duration = Duration::from_secs(10);

/// The protocols offered during a TLS handshake, best first.
const ALPN_PROTOCOLS: [&[u8]; 2] = [b"h2", b"http/1.1"];

/// What has to happen to an accepted socket before HTTP is spoken on it.
///
/// Cloned per connection, which is why the TLS arm holds a [`TlsAcceptor`]
/// (itself an `Arc` over the server config) rather than the config.
#[derive(Clone)]
enum Transport {
    /// Speak HTTP directly on the socket.
    Plain,
    /// Complete a TLS handshake first.
    Tls(TlsAcceptor),
}

/// Runs the server until `Ctrl-C`.
///
/// The bound address is sent on `bound` before the first connection is
/// accepted, which is how a caller that asked for port 0 learns which port it
/// got. A caller that does not care can drop the receiver.
///
/// Errors if the store cannot be opened, the root credentials are rejected, the
/// TLS material is unusable, or the listen address cannot be bound. An error
/// after that point belongs to one connection and is logged, not returned:
/// a client that hangs up mid-request does not stop the server.
pub async fn run(cfg: Config, bound: oneshot::Sender<SocketAddr>) -> anyhow::Result<()> {
    let engine = FsEngine::open(&cfg.data_dir)
        .await
        .with_context(|| format!("opening the store at {}", cfg.data_dir.display()))?;
    let credentials = RootCredentials::new(&cfg.root_access_key, &cfg.root_secret_key)
        .context("the root credentials were rejected")?;

    let service = {
        let mut builder = S3ServiceBuilder::new(Aks3::new(Arc::new(engine)));
        builder.set_auth(IamAuth::new(credentials));
        builder.build()
    };

    let transport = match cfg.tls.as_ref() {
        Some(tls) => Transport::Tls(tls_acceptor(tls)?),
        None => Transport::Plain,
    };

    let listener = TcpListener::bind(&cfg.listen)
        .await
        .with_context(|| format!("binding {}", cfg.listen))?;
    let addr = listener
        .local_addr()
        .context("reading the bound address back")?;

    let scheme = match transport {
        Transport::Plain => "http",
        Transport::Tls(_) => "https",
    };
    tracing::info!(
        "aks3 listening on {scheme}://{addr}, data dir {}",
        cfg.data_dir.display()
    );
    // The receiver is optional; a caller that already knows the address drops it.
    let _ = bound.send(addr);

    accept_loop(listener, transport, service).await;
    Ok(())
}

/// Accepts connections until `Ctrl-C`, then waits out [`GRACE_PERIOD`] for the
/// ones still running.
async fn accept_loop(listener: TcpListener, transport: Transport, service: S3Service) {
    let graceful = GracefulShutdown::new();
    let mut interrupt = std::pin::pin!(tokio::signal::ctrl_c());

    loop {
        let stream = tokio::select! {
            result = listener.accept() => match result {
                Ok((stream, peer)) => {
                    tracing::debug!("accepted a connection from {peer}");
                    stream
                }
                // Per-connection failures (the peer reset before the accept
                // completed, the process is out of descriptors) are not the
                // listener's death. Log and take the next one.
                Err(err) => {
                    tracing::warn!("accept failed: {err}");
                    continue;
                }
            },
            _ = interrupt.as_mut() => {
                tracing::info!("interrupted, draining connections");
                break;
            }
        };

        tokio::spawn(serve_connection(
            stream,
            transport.clone(),
            service.clone(),
            graceful.watcher(),
        ));
    }

    tokio::select! {
        () = graceful.shutdown() => tracing::info!("all connections closed"),
        () = tokio::time::sleep(GRACE_PERIOD) => {
            tracing::warn!("connections still open after {GRACE_PERIOD:?}, exiting anyway");
        }
    }
}

/// Brings one accepted socket up to HTTP and serves it.
///
/// The TLS handshake happens here rather than in the accept loop on purpose: it
/// is a round trip with the client, and a peer that stalls halfway through one
/// must not stop the server from accepting anybody else.
async fn serve_connection(
    stream: TcpStream,
    transport: Transport,
    service: S3Service,
    watcher: Watcher,
) {
    match transport {
        Transport::Plain => serve_http(TokioIo::new(stream), service, watcher).await,
        Transport::Tls(acceptor) => match acceptor.accept(stream).await {
            Ok(stream) => serve_http(TokioIo::new(stream), service, watcher).await,
            Err(err) => tracing::warn!("TLS handshake failed: {err}"),
        },
    }
}

/// Serves HTTP/1 or HTTP/2 on `io`, whichever the client turns out to speak.
async fn serve_http<I>(io: I, service: S3Service, watcher: Watcher)
where
    I: hyper::rt::Read + hyper::rt::Write + Unpin + Send + 'static,
{
    let builder = ConnBuilder::new(TokioExecutor::new());
    let connection = builder.serve_connection(io, service).into_owned();
    if let Err(err) = watcher.watch(connection).await {
        // A client that hangs up mid-response lands here, so this is a normal
        // occurrence rather than a fault, and is logged as one.
        tracing::debug!("connection ended: {err}");
    }
}

/// Builds the TLS acceptor described by `tls`.
fn tls_acceptor(tls: &TlsConfig) -> anyhow::Result<TlsAcceptor> {
    // `rustls` refuses to build a config until a crypto provider is installed
    // and installing is once per process, so an `Err` here means somebody
    // already did it and there is nothing to do.
    let _ = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider().install_default();

    let certs = load_certs(&tls.cert_pem)?;
    let key = load_private_key(&tls.key_pem)?;
    let mut config = ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("the certificate and private key do not go together")?;
    config.alpn_protocols = ALPN_PROTOCOLS.iter().map(|p| p.to_vec()).collect();
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// Reads a PEM certificate chain, leaf first.
fn load_certs(path: &Path) -> anyhow::Result<Vec<CertificateDer<'static>>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening the certificate {}", path.display()))?;
    let certs = rustls_pemfile::certs(&mut std::io::BufReader::new(file))
        .collect::<Result<Vec<_>, _>>()
        .with_context(|| format!("reading the certificate {}", path.display()))?;
    anyhow::ensure!(
        !certs.is_empty(),
        "no certificate found in {}",
        path.display()
    );
    Ok(certs)
}

/// Reads the first PEM private key in the file, in any of the encodings
/// `rustls` accepts.
fn load_private_key(path: &Path) -> anyhow::Result<PrivateKeyDer<'static>> {
    let file = std::fs::File::open(path)
        .with_context(|| format!("opening the private key {}", path.display()))?;
    rustls_pemfile::private_key(&mut std::io::BufReader::new(file))
        .with_context(|| format!("reading the private key {}", path.display()))?
        .with_context(|| format!("no private key found in {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::{load_certs, load_private_key};

    /// Writes `body` to a scratch file, returning it with the directory that
    /// owns it.
    fn scratch(name: &str, body: &str) -> (tempfile::TempDir, std::path::PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(name);
        std::fs::write(&path, body).unwrap();
        (dir, path)
    }

    /// The TLS material is read at startup so that a bad path stops the server
    /// then, rather than at the first handshake. These check that reading it
    /// fails loudly, and that the message says which file was at fault: an
    /// operator reading it has two paths in the config and needs to know which
    /// one to fix.
    #[test]
    fn a_certificate_path_that_does_not_exist_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent.pem");
        let err = load_certs(&missing).unwrap_err();
        assert!(err.to_string().contains("absent.pem"), "{err:#}");
    }

    #[test]
    fn a_file_with_no_certificate_in_it_is_an_error() {
        let (_dir, path) = scratch("cert.pem", "not a certificate\n");
        let err = load_certs(&path).unwrap_err();
        assert!(err.to_string().contains("cert.pem"), "{err:#}");
    }

    #[test]
    fn a_key_path_that_does_not_exist_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let missing = dir.path().join("absent.pem");
        let err = load_private_key(&missing).unwrap_err();
        assert!(err.to_string().contains("absent.pem"), "{err:#}");
    }

    /// A certificate where the key should be is the likely mix-up, and it has
    /// to be caught: `rustls_pemfile` reports "no key here", not an error.
    #[test]
    fn a_file_with_no_private_key_in_it_is_an_error() {
        let (_dir, path) = scratch(
            "key.pem",
            "-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n",
        );
        let err = load_private_key(&path).unwrap_err();
        assert!(err.to_string().contains("key.pem"), "{err:#}");
    }
}
