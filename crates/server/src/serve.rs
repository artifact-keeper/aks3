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
use tokio::signal::unix::{signal, Signal, SignalKind};
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
///
/// It has to stay under the timeout of whatever sent the signal, because that
/// timeout ends in SIGKILL. `docker stop` waits ten seconds by default, so a
/// ten second window here would be a tie, and a tie is lost: the drain is still
/// deciding when the process is killed under it, which is the outcome the drain
/// exists to avoid. Eight seconds leaves the margin that makes the common case
/// resolve on the server's terms. Anything still open when it expires is
/// dropped, so raising a supervisor's timeout protects this window rather than
/// lengthening it.
const GRACE_PERIOD: Duration = Duration::from_secs(8);

/// How long the accept loop waits after a failed `accept` before trying again.
///
/// Most accept failures are one connection's problem and the next call
/// succeeds, but some are not: out of file descriptors, or out of memory for
/// the socket, and those persist until an operator or a closing connection
/// fixes them. Retrying immediately turns such a condition into a loop spinning
/// at the speed of the syscall, burning a core and filling the log with the
/// same line, which is the state least likely to leave room for the connections
/// already open to finish and free what is exhausted. The pause is short enough
/// that a transient failure costs one client nothing it would notice.
const ACCEPT_ERROR_BACKOFF: Duration = Duration::from_millis(100);

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

/// Runs the server until SIGINT or SIGTERM arrives, then drains.
///
/// The bound address is sent on `bound` before the first connection is
/// accepted, which is how a caller that asked for port 0 learns which port it
/// got. A caller that does not care can drop the receiver.
///
/// Errors if the signal handlers cannot be installed, the store cannot be
/// opened, the root credentials are rejected, the TLS material is unusable, or
/// the listen address cannot be bound. An error after that point belongs to one
/// connection and is logged, not returned: a client that hangs up mid-request
/// does not stop the server.
pub async fn run(cfg: Config, bound: oneshot::Sender<SocketAddr>) -> anyhow::Result<()> {
    // Before anything slow, so that a stop request arriving during startup is
    // held rather than lost. Opening a large store sweeps its temp directory,
    // and a container told to stop in that window has no handler to catch the
    // SIGTERM yet if this waits until the accept loop.
    let signals = ShutdownSignals::install()?;

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

    accept_loop(listener, transport, service, signals).await;
    Ok(())
}

/// Accepts connections until a shutdown signal arrives, then waits out
/// [`GRACE_PERIOD`] for the ones still running.
async fn accept_loop(
    listener: TcpListener,
    transport: Transport,
    service: S3Service,
    mut signals: ShutdownSignals,
) {
    let graceful = GracefulShutdown::new();

    loop {
        let stream = tokio::select! {
            result = listener.accept() => match result {
                Ok((stream, peer)) => {
                    tracing::debug!("accepted a connection from {peer}");
                    stream
                }
                // Per-connection failures (the peer reset before the accept
                // completed, the process is out of descriptors) are not the
                // listener's death. Log, pause, and take the next one. The
                // pause is unconditional because the error alone does not say
                // whether the cause has gone away; see ACCEPT_ERROR_BACKOFF.
                // It delays a shutdown by at most one backoff, since the next
                // pass through the loop selects on the signal again.
                Err(err) => {
                    tracing::warn!("accept failed: {err}");
                    tokio::time::sleep(ACCEPT_ERROR_BACKOFF).await;
                    continue;
                }
            },
            name = signals.recv() => {
                tracing::info!("received {name}, draining connections");
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

/// The two signals that ask this server to stop.
///
/// SIGINT is `Ctrl-C` at a terminal. SIGTERM is what everything else uses:
/// `docker stop`, a Kubernetes pod deletion and `systemctl stop` all send it
/// first and only reach for SIGKILL once their grace period runs out. It has to
/// be handled explicitly, because a process running as PID 1 in a container
/// gets no default dispositions from the kernel, so a SIGTERM with no handler
/// installed is discarded rather than fatal. Left unhandled there, every stop
/// request is ignored, every stop takes the full grace period, and the drain in
/// [`accept_loop`] never runs: an in-flight `GetObject` is cut off by the
/// SIGKILL instead of being allowed to finish.
///
/// `tokio::signal::unix` limits this to Unix, which the rest of the server
/// already requires (see `aks3_engine::atomic`).
struct ShutdownSignals {
    interrupt: Signal,
    terminate: Signal,
}

impl ShutdownSignals {
    /// Installs both handlers.
    ///
    /// Errors if either cannot be registered. That is fatal rather than a
    /// warning: a server nobody can ask to stop is one an operator can only
    /// kill, and finding that out at startup beats finding it out during a
    /// deploy.
    fn install() -> anyhow::Result<Self> {
        Ok(Self {
            interrupt: signal(SignalKind::interrupt()).context("listening for SIGINT")?,
            terminate: signal(SignalKind::terminate()).context("listening for SIGTERM")?,
        })
    }

    /// Resolves at whichever arrives first, naming it for the log.
    ///
    /// Cancel-safe, which is what lets the accept loop select on it: a signal
    /// that has not arrived yet is not consumed by a poll that loses the race
    /// to an incoming connection.
    async fn recv(&mut self) -> &'static str {
        tokio::select! {
            _ = self.interrupt.recv() => "SIGINT",
            _ = self.terminate.recv() => "SIGTERM",
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
