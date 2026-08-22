// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// SPDX-License-Identifier: AGPL-3.0-only

//! The server has to stop when it is asked to, and drain rather than die.
//!
//! Out of process on purpose. A signal handler is installed for the whole
//! process, so a test that sent SIGTERM to itself would be asserting about
//! whichever handler the test binary happened to have installed, and would take
//! the test runner down with it if the answer was "none". Driving the real
//! `aks3` binary as a child is the only form of this that sees what a
//! supervisor sees: the process notices the signal, drains, and exits 0 on its
//! own rather than being killed.

use std::io::{BufRead as _, BufReader};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

/// How long to wait for a log line that is expected to appear.
const LOG_TIMEOUT: Duration = Duration::from_secs(30);

/// How long the child gets to exit once it has been signalled.
///
/// Far above the near-instant exit expected of a server holding no connections,
/// and above the eight second grace period it would wait out even if it thought
/// it held one, so reaching this means the signal was ignored rather than that
/// the machine was busy.
const EXIT_TIMEOUT: Duration = Duration::from_secs(30);

/// How often the child is checked for having exited.
const POLL_INTERVAL: Duration = Duration::from_millis(10);

/// A running `aks3` child process and everything it has logged so far.
struct Server {
    child: Child,
    lines: Receiver<String>,
    log: Vec<String>,
    /// The store, which has to outlive the process writing into it.
    _dir: tempfile::TempDir,
}

impl Server {
    /// Starts the binary this crate builds over a fresh store, on a port the
    /// operating system picks, and returns once it says it is listening.
    fn start() -> Self {
        let dir = tempfile::tempdir().expect("a scratch directory");
        let mut child = Command::new(env!("CARGO_BIN_EXE_aks3"))
            .env("AKS3_LISTEN", "127.0.0.1:0")
            .env("AKS3_DATA_DIR", dir.path())
            .env("AKS3_ROOT_USER", "shutdowntest")
            .env("AKS3_ROOT_PASSWORD", "shutdowntestsecret")
            .env("RUST_LOG", "info")
            // The logs are the assertion, and `tracing_subscriber::fmt` writes
            // them to stdout. stderr is left inherited so that a panic in the
            // child shows up in the test output.
            .stdout(Stdio::piped())
            .spawn()
            .expect("starting the aks3 binary");

        // Drained by a thread rather than read on demand, because a pipe left
        // unread fills and blocks the child, and the child has to stay free to
        // run for the whole test.
        let (tx, lines) = mpsc::channel();
        let stdout = child.stdout.take().expect("piped stdout");
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if tx.send(line).is_err() {
                    return;
                }
            }
        });

        let mut server = Self {
            child,
            lines,
            log: Vec::new(),
            _dir: dir,
        };
        // Both handlers are installed before this line is printed, which is
        // what stops the signal below from arriving too early to be caught.
        server.wait_for("aks3 listening on");
        server
    }

    /// Blocks until some log line contains `needle`, failing the test if it
    /// does not arrive within [`LOG_TIMEOUT`].
    ///
    /// Lines already read stay in `log`, so this can be called repeatedly for
    /// needles that arrive in any order.
    fn wait_for(&mut self, needle: &str) {
        let deadline = Instant::now() + LOG_TIMEOUT;
        while !self.log.iter().any(|line| line.contains(needle)) {
            let remaining = deadline.saturating_duration_since(Instant::now());
            match self.lines.recv_timeout(remaining) {
                Ok(line) => self.log.push(line),
                Err(RecvTimeoutError::Timeout) => {
                    panic!("nothing logged containing {needle:?}\n{}", self.log())
                }
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("the log ended without {needle:?}\n{}", self.log())
                }
            }
        }
    }

    /// Sends `signal` to the child and waits for it to exit on its own.
    ///
    /// `kill(1)` rather than the system call, because the workspace forbids
    /// `unsafe` and the standard library will only send SIGKILL.
    fn signal(&mut self, signal: &str) -> ExitStatus {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .arg(signal)
            .arg(&pid)
            .status()
            .expect("running kill(1)");
        assert!(sent.success(), "kill {signal} {pid} exited {sent}");

        let deadline = Instant::now() + EXIT_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().expect("checking on the server") {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "still running {EXIT_TIMEOUT:?} after {signal}\n{}",
                self.log()
            );
            std::thread::sleep(POLL_INTERVAL);
        }
    }

    /// The log so far, for a failure message.
    fn log(&self) -> String {
        self.log.join("\n")
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        // A test that failed before signalling would otherwise leave a server
        // running: `Child` does not kill what it owns when it goes away.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The regression this file exists for. SIGTERM is what `docker stop`, a
/// Kubernetes pod deletion and `systemctl stop` send, and a server that ignores
/// it is hard-killed at the end of the supervisor's grace period with its
/// in-flight requests cut off.
#[test]
fn sigterm_drains_and_exits_cleanly() {
    let mut server = Server::start();
    let status = server.signal("-TERM");

    server.wait_for("received SIGTERM, draining connections");
    // Not just "it exited": this is the line at the end of the drain, so it
    // distinguishes a graceful stop from one that gave up on open connections.
    server.wait_for("all connections closed");
    assert!(
        status.success(),
        "SIGTERM left exit status {status}\n{}",
        server.log()
    );
}

/// SIGINT is what a terminal sends on `Ctrl-C`, and handling SIGTERM must not
/// have cost it anything.
#[test]
fn sigint_drains_and_exits_cleanly() {
    let mut server = Server::start();
    let status = server.signal("-INT");

    server.wait_for("received SIGINT, draining connections");
    server.wait_for("all connections closed");
    assert!(
        status.success(),
        "SIGINT left exit status {status}\n{}",
        server.log()
    );
}
