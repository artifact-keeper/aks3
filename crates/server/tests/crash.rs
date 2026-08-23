// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// SPDX-License-Identifier: AGPL-3.0-only

//! Crash recovery: what the store looks like after the writer is killed outright.
//!
//! # What a `SIGKILL` proves here, and what it does not
//!
//! Every kill below is `SIGKILL` to a process writing to a real filesystem. The
//! kernel keeps the page cache when a process dies, so everything the dying
//! process wrote is still visible to the next process that opens the directory,
//! whether or not it was ever synced. What that falsifies is the *publication*
//! discipline: that a `rename` is the only way anything becomes visible, that
//! the data file is committed before the manifest naming it, that a delete
//! removes the manifest before the bytes, and that startup sweeps its staging
//! directory. Deleting a `sync_all` from `crates/engine/src/atomic.rs` fails
//! nothing here, and this file makes no claim that it should.
//!
//! A green run is therefore not durability evidence. Testing the sync discipline
//! needs a filesystem that can be told to forget writes that were never synced
//! (`LazyFS`), which is a separate, nightly job.
//!
//! # Two roles, one harness
//!
//! 1. **The engine-precision loop** ([`engine_crash_loop`]) works against
//!    [`FsEngine`] directly, so a failure names an engine invariant rather than
//!    an HTTP symptom. An iteration crashes a run of short-lived writers over
//!    one store and then a run of deleters over what they left, verifying the
//!    whole store after every one of the thirteen crashes it makes.
//! 2. **The end-to-end iteration** ([`e2e_sigkill_mid_put_recovers`]) runs once
//!    per test run through the real `aks3` binary and the real AWS SDK, which is
//!    what checks that the server's own restart path over a crashed data
//!    directory works at all.
//!
//! # The child process
//!
//! The engine loop needs a process it can kill, and the workspace's single gate
//! is `cargo test --workspace`: no new crate, no new binary target. So the test
//! binary re-executes *itself*. [`crash_child_entry_point`] is an ordinary
//! `#[test]` that does nothing at all unless [`CHILD_MODE`] is set in its
//! environment, and the parent re-invokes [`std::env::current_exe`] with that
//! variable set plus libtest's own `--exact` and `--nocapture` flags, so the
//! child runs that one test and streams its acknowledgements over a pipe.
//!
//! # Determinism and evidence
//!
//! Every object's bytes are derived from its key and the run's seed, so
//! verification needs no record of what was written, only the key. The seed is
//! printed for every iteration, and a failure archives the whole post-crash
//! scratch directory (the data directory plus the log of acknowledgements the
//! parent observed) under `CARGO_TARGET_TMPDIR` and prints the path.
//!
//! Kills are triggered by an observed acknowledgement count, never by a timer:
//! "acknowledged" means the parent read the line, so a slow machine changes how
//! much work the child got through, not what the harness asserts.
//!
//! # Power, and why the children run several operations at once
//!
//! Where inside an operation a kill lands is a race, so this loop samples an
//! ordering rather than proving one, and the sample is not the uniform one it
//! looks like. A kill arrives a roughly fixed interval after the
//! acknowledgement that triggered it, and an operation's wall clock is
//! dominated by its first sync, which on both `APFS` and `ext4` costs far more
//! than the ones after it. A writer working one object at a time is therefore
//! killed at nearly the same point of nearly every operation, and that point is
//! before anything has been published, which is the state a correct order and a
//! reversed one both produce.
//!
//! Measured, not assumed: with one-at-a-time writers, an engine that published
//! the manifest before the bytes it names went undetected in ten of ten Linux
//! runs at this budget. With [`PUT_CONCURRENCY`] operations in flight, which
//! staggers them against each other so a kill has one of them at a later point,
//! the same bug failed ten of ten. Concurrency is also what the server does, so
//! it is not a contrivance. It does not generalise: deleters stay sequential,
//! for the reason given at [`SEQUENTIAL_PUT_ROUNDS`].
//!
//! The narrowest window left is a delete's, which is two `unlink` calls wide.
//! A reversed delete order failed twenty of twenty Linux runs at this budget and
//! nine of ten on `APFS`, where the sync cost is heaviest and the phase a kill
//! lands in is most concentrated. That last number is the reason
//! [`ITERATIONS_VAR`] exists: on the nightly schedule the count goes well past
//! what a pull request can afford, and nine of ten becomes every time.

use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::hash::{BuildHasher as _, RandomState};
use std::io::{BufRead as _, BufReader, Read as _, Write as _};
use std::net::SocketAddr;
use std::os::unix::process::ExitStatusExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use aks3_engine::meta::{load_manifest, VersionManifest};
use aks3_engine::paths::{data_file_name, rel_path_to_key, META_FILE, RESERVED_PREFIX};
use aks3_engine::{EngineError, FsEngine, ObjectLayer as _, PutOpts};
use futures::StreamExt as _;

/// Set in the child's environment to select what it does; absent in the parent.
const CHILD_MODE: &str = "AKS3_CRASH_CHILD_MODE";
/// The data directory the child opens.
const CHILD_DIR: &str = "AKS3_CRASH_CHILD_DIR";
/// The run seed, so the child derives the same bytes the parent verifies.
const CHILD_SEED: &str = "AKS3_CRASH_CHILD_SEED";
/// The key index the child starts from.
const CHILD_START: &str = "AKS3_CRASH_CHILD_START";
/// How many operations the child keeps in flight at once.
const CHILD_CONCURRENCY: &str = "AKS3_CRASH_CHILD_CONCURRENCY";
/// Name of the `#[test]` the child runs, passed to libtest as an exact filter.
const CHILD_TEST: &str = "crash_child_entry_point";
/// Prefix of the line the child prints after an operation has returned `Ok`.
const ACK: &str = "aks3-crash-ack ";
/// Printed by the child once it runs out of work, before it blocks until killed.
const EXHAUSTED: &str = "aks3-crash-exhausted";

/// How many iterations the engine loop runs when nothing says otherwise.
///
/// Sized for the pull-request path: ten iterations are about a hundred and
/// thirty crashes, costing a second on Linux and under eight on a laptop where
/// every sync is a full device flush. It is still a sample rather than a proof,
/// which is what [`ITERATIONS_VAR`] is for on the nightly schedule.
const DEFAULT_ITERATIONS: usize = 10;
/// Overrides [`DEFAULT_ITERATIONS`].
const ITERATIONS_VAR: &str = "AKS3_CRASH_ITERATIONS";
/// Reproduces a previous run when set to a seed the harness printed.
const SEED_VAR: &str = "AKS3_CRASH_SEED";

/// Keys one engine child walks through. Comfortably more than any kill point,
/// so the child is still working when the kill lands.
const KEYS: usize = 48;
/// Writers crashed per iteration, and the largest number of acknowledged `PUT`s
/// any one of them is allowed before the kill.
const PUT_ROUNDS: usize = 5;
const MAX_PUT_ACKS: u64 = 3;
/// The first writers of an iteration run one `PUT` at a time, so that the bound
/// on data files no manifest names is one per crash and genuinely tight. The
/// rest run [`PUT_CONCURRENCY`] at once, which is what gives a kill more than
/// one operation to interrupt, at the cost of loosening that bound.
///
/// Deleters stay sequential. Concurrency was tried there and made things worse:
/// a delete is cheap, so eight at a time empties what the writers left in the
/// first batch, and the kill then lands on a deleter whose remaining work is
/// removing keys that were never there. Measured, a reversed delete order went
/// from caught in six of ten Linux runs to none of ten.
const SEQUENTIAL_PUT_ROUNDS: usize = 2;
const PUT_CONCURRENCY: usize = 8;
/// Deleters crashed per iteration. Bounded rather than run until the store is
/// empty, because how many objects the writers left is up to how fast the
/// machine is, and an iteration whose cost tracks that is an iteration whose
/// cost is unpredictable.
const DELETE_ROUNDS: usize = 8;
/// Objects the end-to-end leg queues, and the acknowledgements it waits for.
const E2E_KEYS: usize = 32;
const E2E_ACKS: usize = 4;
/// Body size for the end-to-end leg: large enough that a kill arriving one
/// acknowledgement later is very likely to land inside the next transfer.
const E2E_BODY: usize = 192 * 1024;

/// The bucket everything here writes to.
const BUCKET: &str = "crash";
const ROOT_USER: &str = "crashtest";
const ROOT_PASSWORD: &str = "crashtestsecret";

/// How long any wait for an expected event may take before it is a failure.
/// Far above what the work costs, so reaching it means the event is not coming.
const DEADLINE: Duration = Duration::from_secs(60);

// ---------------------------------------------------------------------------
// Deterministic content
// ---------------------------------------------------------------------------

/// One step of `SplitMix64`. Enough of a generator for choosing kill points and
/// filling bodies, and small enough to keep the harness dependency-free.
fn splitmix64(state: &mut u64) -> u64 {
    *state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
    let mut z = *state;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// A seeded generator whose whole state is one printable number.
struct Rng(u64);

impl Rng {
    /// A value in `0..n`. `n` must not be zero.
    fn below(&mut self, n: u64) -> u64 {
        splitmix64(&mut self.0) % n
    }
}

fn fnv1a(bytes: &[u8]) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325_u64;
    for b in bytes {
        h ^= u64::from(*b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

/// The key at `index`. Two directory levels, so every key writes a fresh
/// directory chain and the commit path that syncs a whole created chain runs on
/// every object rather than only the first.
fn key_for(index: usize) -> String {
    format!("crash/{index:04}/object-{index}")
}

/// The bytes `key` holds in a store built with `seed`.
///
/// Derived rather than recorded: the verifier reconstructs what any key should
/// contain without a side channel from the process that wrote it, which is what
/// keeps the check honest across a crash that lost the writer's own state.
fn content_for(key: &str, seed: u64, len: usize) -> Vec<u8> {
    let mut state = seed ^ fnv1a(key.as_bytes());
    let mut out = Vec::with_capacity(len + 8);
    while out.len() < len {
        out.extend_from_slice(&splitmix64(&mut state).to_le_bytes());
    }
    out.truncate(len);
    out
}

/// Body length for the engine leg: 4 to 12 KiB, varying per key so that the
/// window in which a staged file exists varies with it.
fn engine_len(key: &str, seed: u64) -> usize {
    let mut state = seed ^ fnv1a(key.as_bytes()) ^ 0x5bf0_3635;
    let spread = usize::try_from(splitmix64(&mut state) % 8192).expect("a 13-bit value fits usize");
    4096 + spread
}

fn engine_body(key: &str, seed: u64) -> Vec<u8> {
    content_for(key, seed, engine_len(key, seed))
}

// ---------------------------------------------------------------------------
// The child
// ---------------------------------------------------------------------------

/// The re-executed child, and otherwise nothing at all.
///
/// Run as part of an ordinary `cargo test`, [`CHILD_MODE`] is unset and this
/// returns immediately. The parent sets it and passes this test's name to
/// libtest as an exact filter, which is how a test binary with no binary target
/// of its own gets a process it can kill. The body is not meant to return: it is
/// killed mid-operation, and in the rare case that it runs out of work first it
/// blocks on standard input, which the parent holds open, rather than exiting
/// into a race over whether the kill found a live process.
///
/// Ignored, so an ordinary `cargo test` does not run a test whose only outcome
/// is to return immediately: an always-passing no-op is noise in the count, and
/// one that would do something quite different if a stray `AKS3_CRASH_*` were
/// exported is worse than noise. The parent passes `--ignored` alongside the
/// filter, which is how it gets the one test it wants.
#[test]
#[ignore = "the re-exec entry point of the crash loop, not a test of its own"]
fn crash_child_entry_point() {
    let Ok(mode) = std::env::var(CHILD_MODE) else {
        return;
    };
    let dir = PathBuf::from(std::env::var(CHILD_DIR).expect("the child's data directory"));
    let seed: u64 = std::env::var(CHILD_SEED)
        .expect("the child's seed")
        .parse()
        .expect("a numeric seed");
    let start: usize = std::env::var(CHILD_START)
        .expect("the child's start index")
        .parse()
        .expect("a numeric start index");
    let concurrency: usize = std::env::var(CHILD_CONCURRENCY)
        .expect("the child's concurrency")
        .parse()
        .expect("a numeric concurrency");

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("a tokio runtime");
    runtime.block_on(async move {
        let engine = FsEngine::open(&dir)
            .await
            .expect("opening the data directory");
        match mode.as_str() {
            "put" => child_put(&engine, seed, start, concurrency).await,
            "delete" => child_delete(&engine, start, concurrency).await,
            other => panic!("unknown child mode {other:?}"),
        }
    });

    println!("{EXHAUSTED}");
    let _ = std::io::stdout().flush();
    // Blocks until the parent's kill arrives: the parent keeps the write end of
    // this pipe, so it never reaches end of file on its own. If the parent dies
    // instead of killing, the pipe closes with it and this returns, so no child
    // outlives the run that started it.
    let _ = std::io::stdin().read_to_end(&mut Vec::new());
}

/// Report one completed operation, and make sure the parent can see it before
/// anything else happens.
fn ack(index: usize) {
    println!("{ACK}{index}");
    let _ = std::io::stdout().flush();
}

/// Write every key from `start` on, `concurrency` at a time, acknowledging each
/// one the engine reports stored.
///
/// Concurrency is what gives the kill something to land on. A kill arrives a
/// roughly fixed interval after the acknowledgement that triggered it, and one
/// operation's cost is dominated by its first sync, so a single-file-at-a-time
/// writer is killed at nearly the same point of nearly every operation: before
/// anything has been published. Several operations in flight are staggered
/// against each other, so the same kill catches one of them at a later point.
/// It is also what the server does, since a `PUT` per connection is the normal
/// case rather than the exceptional one.
async fn child_put(engine: &FsEngine, seed: u64, start: usize, concurrency: usize) {
    match engine.create_bucket(BUCKET).await {
        Ok(()) | Err(EngineError::BucketAlreadyExists) => {}
        Err(e) => panic!("creating the bucket: {e}"),
    }
    let mut puts = futures::stream::iter(start..KEYS)
        .map(|index| async move {
            let key = key_for(index);
            let body = engine_body(&key, seed);
            let stream = futures::stream::once(async move { Ok(bytes::Bytes::from(body)) });
            engine
                .put_object(BUCKET, &key, Box::pin(stream), PutOpts::default())
                .await
                .expect("storing an object");
            index
        })
        .buffer_unordered(concurrency);
    while let Some(index) = puts.next().await {
        ack(index);
    }
}

/// Delete every key from `start` on, `concurrency` at a time, acknowledging each
/// delete the engine reports done.
///
/// Starting part way up is what lets one set of written objects feed several
/// crashed deleters: each one begins above the last one's reach, so its first
/// delete is of an object that is really there rather than a vacuous success.
/// `concurrency` is a parameter for symmetry with [`child_put`]; the harness
/// passes one. A delete is interruptible between exactly two `unlink` calls,
/// the narrowest window anything in the engine has, and running several at once
/// exhausts the objects there are to delete faster than it samples that window.
async fn child_delete(engine: &FsEngine, start: usize, concurrency: usize) {
    let mut deletes = futures::stream::iter(start..KEYS)
        .map(|index| async move {
            engine
                .delete_object(BUCKET, &key_for(index))
                .await
                .expect("deleting an object");
            index
        })
        .buffer_unordered(concurrency);
    while let Some(index) = deletes.next().await {
        ack(index);
    }
}

// ---------------------------------------------------------------------------
// Driving and killing a child process
// ---------------------------------------------------------------------------

/// A spawned process whose output the parent is reading line by line.
struct Killable {
    child: Child,
    /// Held, never written to, so the child's blocking read never sees end of
    /// file and the process stays alive until it is killed.
    _stdin: ChildStdin,
    lines: Receiver<String>,
    log: Vec<String>,
}

impl Killable {
    /// Re-execute this test binary in `mode` over `dir`, from key `start`, with
    /// `concurrency` operations in flight.
    fn spawn_child(mode: &str, dir: &Path, seed: u64, start: usize, concurrency: usize) -> Self {
        let exe = std::env::current_exe().expect("the path of this test binary");
        let child = Command::new(exe)
            .arg(CHILD_TEST)
            .arg("--exact")
            // The entry point carries `#[ignore]`, so it takes asking for.
            .arg("--ignored")
            .arg("--nocapture")
            .arg("--test-threads=1")
            .env(CHILD_MODE, mode)
            .env(CHILD_DIR, dir)
            .env(CHILD_SEED, seed.to_string())
            .env(CHILD_START, start.to_string())
            .env(CHILD_CONCURRENCY, concurrency.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("re-executing this test binary");
        Self::adopt(child)
    }

    /// Take over a spawned process, draining its output on a thread.
    ///
    /// Draining is not optional: a pipe nobody reads fills up and blocks the
    /// writer, and a blocked child is one that stopped doing the work being
    /// crashed.
    fn adopt(mut child: Child) -> Self {
        let stdin = child.stdin.take().expect("piped standard input");
        let stdout = child.stdout.take().expect("piped standard output");
        let (tx, lines) = mpsc::channel();
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let Ok(line) = line else { return };
                if tx.send(line).is_err() {
                    return;
                }
            }
        });
        Self {
            child,
            _stdin: stdin,
            lines,
            log: Vec::new(),
        }
    }

    /// Read output until `count` acknowledgements have been seen, returning them.
    ///
    /// This is the kill trigger: the returned indices are exactly the operations
    /// the parent knows completed, which is what the durability assertions are
    /// stated over.
    fn wait_for_acks(&mut self, count: usize) -> Vec<usize> {
        let deadline = Instant::now() + DEADLINE;
        let mut acked = Vec::with_capacity(count);
        while acked.len() < count {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = match self.lines.recv_timeout(remaining) {
                Ok(line) => line,
                Err(RecvTimeoutError::Timeout) => {
                    panic!(
                        "only {} of {count} acks arrived\n{}",
                        acked.len(),
                        self.log()
                    )
                }
                Err(RecvTimeoutError::Disconnected) => {
                    panic!(
                        "the child stopped after {} acks\n{}",
                        acked.len(),
                        self.log()
                    )
                }
            };
            if let Some(index) = line.strip_prefix(ACK) {
                acked.push(index.parse().expect("an ack carries an index"));
            }
            self.log.push(line);
        }
        acked
    }

    /// Read the rest of what the child printed, and report whether it had run
    /// out of work before the kill landed.
    ///
    /// The child is already dead and its pipe already closed, so this returns as
    /// soon as the reader thread reaches end of file. The answer is printed, not
    /// asserted on: the durability claims are stated only over what the parent
    /// acknowledged, and a kill is no less valid for having arrived late. It
    /// still matters, because a kill that interrupted nothing tested nothing.
    fn drain_exhausted(&mut self) -> bool {
        while let Ok(line) = self.lines.recv_timeout(DEADLINE) {
            self.log.push(line);
        }
        self.log.iter().any(|line| line == EXHAUSTED)
    }

    /// Read the startup log until the bound address appears, and parse it.
    ///
    /// The address is what makes a restart usable: the port is chosen by the
    /// operating system and differs between the two runs.
    fn wait_for_listening(&mut self) -> SocketAddr {
        let deadline = Instant::now() + DEADLINE;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            let line = match self.lines.recv_timeout(remaining) {
                Ok(line) => line,
                Err(RecvTimeoutError::Timeout) => {
                    panic!("the server never listened\n{}", self.log())
                }
                Err(RecvTimeoutError::Disconnected) => {
                    panic!("the server stopped before listening\n{}", self.log())
                }
            };
            let found = line
                .split_once("listening on http://")
                .and_then(|(_, rest)| rest.split(',').next())
                .and_then(|addr| addr.parse().ok());
            self.log.push(line);
            if let Some(addr) = found {
                return addr;
            }
        }
    }

    /// `SIGKILL` the child and reap it.
    ///
    /// `kill(1)` rather than the system call, because the workspace forbids
    /// `unsafe`; the same reason `shutdown.rs` shells out.
    fn kill_9(&mut self) {
        let pid = self.child.id().to_string();
        let sent = Command::new("kill")
            .arg("-KILL")
            .arg(&pid)
            .status()
            .expect("running kill(1)");
        assert!(sent.success(), "kill -KILL {pid} exited {sent}");
        let status = self.child.wait().expect("reaping the child");
        assert_eq!(
            status.signal(),
            Some(9),
            "the child exited on its own ({status}) rather than being killed\n{}",
            self.log()
        );
    }

    fn log(&self) -> String {
        self.log.join("\n")
    }
}

impl Drop for Killable {
    fn drop(&mut self) {
        // A test that panicked before killing would otherwise leave the process
        // running: `Child` does not kill what it owns when it goes away.
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

// ---------------------------------------------------------------------------
// Verifying a crashed store
// ---------------------------------------------------------------------------

/// Everything one key's directory holds on disk.
#[derive(Default)]
struct KeyState {
    manifest: Option<VersionManifest>,
    /// Data file name to its size on disk.
    data: BTreeMap<String, u64>,
}

/// What a reopened store must look like, given what the parent saw acknowledged.
struct Expectation<'a> {
    /// Keys whose `PUT` was acknowledged and must therefore read back.
    present: &'a [usize],
    /// Keys whose `DELETE` was acknowledged and must therefore be gone.
    absent: &'a [usize],
    /// Data files no manifest names. Every operation in flight when a process
    /// dies can leave one behind, and the next write to that key reuses the same
    /// name rather than picking a new one, so the number of operations that were
    /// ever in flight across all the crashes so far is the bound that says
    /// orphans do not accumulate.
    ///
    /// The delete leg does better than that sum. Deleters are sequential, so it
    /// carries the count the writers actually left and allows one more per
    /// crashed deleter, which is a bound on what the delete leg itself can add
    /// rather than on everything either leg was permitted.
    max_orphans: usize,
}

/// Read a whole object back through the engine's own `GET` path.
async fn read_object(engine: &FsEngine, key: &str) -> Result<Vec<u8>, String> {
    let (_, _, _, mut body) = engine
        .get_object(BUCKET, key, None)
        .await
        .map_err(|e| format!("GET {key}: {e}"))?;
    let mut out = Vec::new();
    while let Some(chunk) = body.next().await {
        out.extend_from_slice(&chunk.map_err(|e| format!("reading {key}: {e}"))?);
    }
    Ok(out)
}

/// Read every key directory under the bucket.
///
/// Fails on a manifest that does not parse, which is the torn-manifest check:
/// `load_manifest` reports a truncated or half-written file as invalid data
/// rather than as an absent object.
async fn collect_keys(objects: &Path) -> Result<BTreeMap<String, KeyState>, String> {
    let mut found = BTreeMap::new();
    let mut stack = vec![objects.to_path_buf()];
    while let Some(dir) = stack.pop() {
        let mut state = KeyState::default();
        let entries = std::fs::read_dir(&dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        for entry in entries {
            let entry = entry.map_err(|e| format!("{}: {e}", dir.display()))?;
            let meta = entry
                .metadata()
                .map_err(|e| format!("{}: {e}", entry.path().display()))?;
            if meta.is_dir() {
                stack.push(entry.path());
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if name == META_FILE {
                state.manifest = load_manifest(&entry.path()).await.map_err(|e| {
                    format!(
                        "torn or unreadable manifest {}: {e}",
                        entry.path().display()
                    )
                })?;
            } else if name.starts_with(RESERVED_PREFIX) {
                state.data.insert(name, meta.len());
            } else {
                return Err(format!(
                    "stray file {} in the object tree",
                    entry.path().display()
                ));
            }
        }
        if state.manifest.is_none() && state.data.is_empty() {
            continue;
        }
        let rel = dir.strip_prefix(objects).map_err(|e| format!("{e}"))?;
        let key = rel_path_to_key(rel).map_err(|e| format!("{}: {e}", rel.display()))?;
        found.insert(key, state);
    }
    Ok(found)
}

/// No manifest may name bytes that are not there, at the size it recorded.
///
/// This is the invariant a `SIGKILL` can actually falsify, and the one that
/// catches a publication order written the wrong way round: a manifest reaching
/// disk before the data it describes leaves exactly this state behind.
///
/// The returned orphan count leans on a Phase 0 fact: a key has one data file
/// name, so a crash can strand at most one file per key and the next write to
/// that key reuses the name rather than adding to it. Versioning mints a name
/// per version, at which case orphans really can pile up under one key, and both
/// this count and the bounds stated over it need revisiting then.
fn check_manifests(found: &BTreeMap<String, KeyState>) -> Result<usize, String> {
    let mut orphans = 0;
    for (key, state) in found {
        let mut named = Vec::new();
        for entry in state.manifest.iter().flat_map(|m| &m.versions) {
            if entry.delete_marker {
                continue;
            }
            let name = data_file_name(&entry.version_id);
            match state.data.get(&name) {
                None => {
                    return Err(format!(
                        "{key}: the manifest names version {} but {name} is not there",
                        entry.version_id
                    ))
                }
                Some(&size) if size != entry.size => {
                    return Err(format!(
                        "{key}: the manifest says version {} is {} bytes, {name} is {size}",
                        entry.version_id, entry.size
                    ))
                }
                Some(_) => named.push(name),
            }
        }
        orphans += state.data.keys().filter(|n| !named.contains(n)).count();
    }
    Ok(orphans)
}

/// Whatever is present must be whole: correct length, correct bytes.
///
/// Stated over every key rather than only the acknowledged ones, so the
/// operation that was in flight when the process died is covered: it may be
/// absent, and it may be complete, and nothing in between is allowed.
async fn check_all_or_nothing(
    engine: &FsEngine,
    found: &BTreeMap<String, KeyState>,
    seed: u64,
) -> Result<(), String> {
    for (key, state) in found {
        let Some(manifest) = state.manifest.as_ref() else {
            continue;
        };
        let Some(entry) = manifest.latest() else {
            continue;
        };
        if entry.delete_marker {
            continue;
        }
        let want = engine_body(key, seed);
        if usize::try_from(entry.size).map_err(|e| format!("{key}: {e}"))? != want.len() {
            return Err(format!(
                "{key}: published at {} bytes, should be {}",
                entry.size,
                want.len()
            ));
        }
        let got = read_object(engine, key).await?;
        if got != want {
            return Err(format!("{key}: {} bytes read back do not match", got.len()));
        }
    }
    Ok(())
}

/// What a store held when it was last verified.
struct Surveyed {
    /// Keys that read back. What the next round is aimed at: a child racing
    /// ahead of its own acknowledgements means the parent cannot predict what a
    /// crash left, but it can read it back and pick up from there.
    alive: Vec<usize>,
    /// Data files no manifest names, as counted by [`check_manifests`]. Carried
    /// forward so a later round can bound its own share of them rather than
    /// re-allowing everything the earlier rounds were allowed.
    orphans: usize,
}

/// Reopen the store and check everything that must hold after a crash.
async fn verify(root: &Path, seed: u64, expect: &Expectation<'_>) -> Result<Surveyed, String> {
    // A staged file that a crash left behind is swept by `open`, but whether a
    // crash left one depends on where it landed. Seeding one makes the sweep
    // assertion below bite on every iteration rather than only the lucky ones.
    let tmp = root.join(".aks3").join("tmp");
    std::fs::write(
        tmp.join("planted-by-the-crash-harness.tmp"),
        b"staged bytes",
    )
    .map_err(|e| format!("planting a staged file: {e}"))?;

    let engine = FsEngine::open(root)
        .await
        .map_err(|e| format!("reopening the data directory: {e}"))?;

    let left = std::fs::read_dir(&tmp)
        .map_err(|e| format!("{}: {e}", tmp.display()))?
        .count();
    if left != 0 {
        return Err(format!("{left} files left in {} after open", tmp.display()));
    }

    let objects = root.join("buckets").join(BUCKET).join("objects");
    let found = collect_keys(&objects).await?;
    let orphans = check_manifests(&found)?;
    if orphans > expect.max_orphans {
        return Err(format!(
            "{orphans} data files no manifest names, at most {} expected",
            expect.max_orphans
        ));
    }
    check_all_or_nothing(&engine, &found, seed).await?;

    // Only that they are there. What they hold was checked above, over every
    // key rather than only these, so reading them a second time here would cost
    // the loop its budget and buy nothing.
    for &index in expect.present {
        let key = key_for(index);
        engine
            .head_object(BUCKET, &key)
            .await
            .map_err(|e| format!("{key}: acknowledged stored, HEAD failed with {e}"))?;
    }
    for &index in expect.absent {
        let key = key_for(index);
        match engine.head_object(BUCKET, &key).await {
            Err(EngineError::NoSuchKey) => {}
            Ok(_) => return Err(format!("{key}: acknowledged deleted, still readable")),
            Err(e) => return Err(format!("{key}: acknowledged deleted, HEAD failed with {e}")),
        }
    }

    let alive = (0..KEYS)
        .filter(|i| {
            found
                .get(&key_for(*i))
                .and_then(|state| state.manifest.as_ref())
                .and_then(VersionManifest::latest)
                .is_some_and(|entry| !entry.delete_marker)
        })
        .collect();
    Ok(Surveyed { alive, orphans })
}

// ---------------------------------------------------------------------------
// Failure artifacts
// ---------------------------------------------------------------------------

/// Copy the whole scratch directory somewhere a CI job can pick it up.
///
/// Under `CARGO_TARGET_TMPDIR`, which is inside the target directory and so is
/// both writable and already the place build artifacts live.
fn archive(scratch: &Path, label: &str) -> String {
    let out_dir = Path::new(env!("CARGO_TARGET_TMPDIR")).join("crash-artifacts");
    if let Err(e) = std::fs::create_dir_all(&out_dir) {
        return format!("<could not create {}: {e}>", out_dir.display());
    }
    let out = out_dir.join(format!("{label}.tar.gz"));
    let (Some(parent), Some(name)) = (scratch.parent(), scratch.file_name()) else {
        return format!("<{} has no parent to archive from>", scratch.display());
    };
    let status = Command::new("tar")
        .arg("-czf")
        .arg(&out)
        .arg("-C")
        .arg(parent)
        .arg(name)
        .status();
    match status {
        Ok(s) if s.success() => out.display().to_string(),
        Ok(s) => format!("<tar exited {s}>"),
        Err(e) => format!("<running tar: {e}>"),
    }
}

/// Archive the evidence, then fail, saying what reproduces the run.
///
/// `reproduce` is the caller's, because what to set [`SEED_VAR`] to is not the
/// same question in both legs. It seeds a whole run, and the engine loop derives
/// a fresh seed per iteration from it, so printing the derived one there would
/// hand the reader a value that replays a different store. The derived seed
/// still labels the archive, since that is a name rather than an instruction.
fn fail(scratch: &Path, label: &str, reproduce: &str, problem: &str) -> ! {
    // Into the archive as well as onto the terminal: whoever opens the tarball
    // later should not need the log of the run that produced it.
    record(&scratch.join("acked.log"), &format!("FAILED: {problem}"));
    let path = archive(scratch, label);
    panic!(
        "{problem}\n\
         reproduce with {reproduce}\n\
         post-crash scratch directory archived at {path}"
    );
}

/// [`fail`] with the end-to-end leg's labels filled in. It has one iteration and
/// uses the run seed directly, so what reproduces it is just that seed.
fn fail_e2e(scratch: &Path, seed: u64, problem: &str) -> ! {
    fail(
        scratch,
        &format!("e2e-{seed}"),
        &format!("{SEED_VAR}={seed}"),
        problem,
    )
}

/// The message out of a caught panic, whatever shape the payload came in.
fn panic_message(payload: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = payload.downcast_ref::<&str>() {
        (*s).to_owned()
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.clone()
    } else {
        "a panic carrying no message".to_owned()
    }
}

// ---------------------------------------------------------------------------
// The engine-precision loop
// ---------------------------------------------------------------------------

fn iterations() -> usize {
    std::env::var(ITERATIONS_VAR)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(DEFAULT_ITERATIONS)
}

/// A seed from the environment if one was given, otherwise a fresh one.
fn run_seed() -> u64 {
    std::env::var(SEED_VAR)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or_else(|| RandomState::new().hash_one("aks3-crash"))
}

/// Append what the parent observed, so the archive of a failure carries it.
fn record(log: &Path, note: &str) {
    if let Ok(mut f) = std::fs::OpenOptions::new()
        .create(true)
        .append(true)
        .open(log)
    {
        let _ = writeln!(f, "{note}");
    }
}

/// One iteration: a run of crashed writers over one store, then a run of
/// crashed deleters over what they left, verifying after every crash.
///
/// Repeated crashes on the *same* directory are the point rather than an
/// economy. Every child after the first opens a directory a crash already
/// damaged, works in it and dies in turn, so a store that degrades by a file
/// or a stale entry per crash shows up as growth across the round rather than
/// as a single tolerable leftover.
fn one_iteration(scratch: &Path, seed: u64, rng: &mut Rng) -> Result<String, String> {
    let root = scratch.join("data");
    std::fs::create_dir_all(&root).map_err(|e| format!("{e}"))?;
    let log = scratch.join("acked.log");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("{e}"))?;

    // Several short writers rather than one long one. Where inside an operation
    // a kill lands is decided by where that operation spends its time, and on a
    // filesystem whose first sync of a transaction costs far more than the ones
    // after it that is nowhere near uniform. The number of crash points, not the
    // number of objects, is what samples the ordering, and a writer that is
    // killed after two acknowledgements costs two objects.
    let mut crashes = 0;
    let mut in_flight = 0;
    let mut acked: Vec<usize> = Vec::new();
    let mut store = Surveyed {
        alive: Vec::new(),
        orphans: 0,
    };
    let mut exhausted = false;
    let mut next = 0;
    for round in 0..PUT_ROUNDS {
        let concurrency = if round < SEQUENTIAL_PUT_ROUNDS {
            1
        } else {
            PUT_CONCURRENCY
        };
        let acks = usize::try_from(1 + rng.below(MAX_PUT_ACKS)).expect("a small count fits usize");
        // From above the last writer's acknowledgements. A key an earlier writer
        // had finished without acknowledging is simply written again with the
        // same bytes, which is the overwrite path and changes nothing the
        // assertions look at.
        let mut writer = Killable::spawn_child("put", &root, seed, next, concurrency);
        acked.extend(writer.wait_for_acks(acks));
        writer.kill_9();
        exhausted |= writer.drain_exhausted();
        record(&log, &format!("put from {next} acked {acked:?}"));
        next = acked.iter().max().map_or(0, |top| top + 1);

        crashes += 1;
        in_flight += concurrency;
        store = runtime.block_on(verify(
            &root,
            seed,
            &Expectation {
                present: &acked,
                absent: &[],
                max_orphans: in_flight,
            },
        ))?;
    }

    // One crashed writer feeds several crashed deleters. The window a delete
    // has to be interrupted in is two unlink calls wide, far narrower than a
    // PUT's, so sampling it once per iteration would leave a reordered delete
    // mostly undetected; deleting through what one writer left costs no further
    // PUTs and multiplies the sample.
    //
    // The bound on orphans tightens here rather than carrying the writers'
    // allowance forward. What the writers were permitted is one thing; what they
    // actually left is another, and the delete leg is judged against the second
    // plus one per crashed deleter, which is all a sequential deleter can add.
    let left_by_writers = store.orphans;
    let mut removed: Vec<usize> = Vec::new();
    let mut deleters = 0;
    while store.alive.len() >= 2 && deleters < DELETE_ROUNDS {
        deleters += 1;
        // From the middle: everything below is untouched by this deleter and
        // must still read back, since a crashed delete may not take its
        // neighbours with it, and everything above is work it can really do.
        let split = store.alive.len() / 2;
        let survivors: Vec<usize> = store.alive[..split].to_vec();
        let start = store.alive[split];

        let acks = usize::try_from(1 + rng.below(2)).expect("a small count fits usize");
        let mut deleter = Killable::spawn_child("delete", &root, seed, start, 1);
        removed.extend(deleter.wait_for_acks(acks));
        deleter.kill_9();
        exhausted |= deleter.drain_exhausted();
        record(&log, &format!("delete from {start} acked {removed:?}"));

        crashes += 1;
        store = runtime.block_on(verify(
            &root,
            seed,
            &Expectation {
                present: &survivors,
                absent: &removed,
                max_orphans: left_by_writers + deleters,
            },
        ))?;
    }

    let mut note = format!(
        "{crashes} crashes: {PUT_ROUNDS} writers left {} keys acknowledged stored, \
         then {deleters} deleters left {} acknowledged deleted",
        acked.len(),
        removed.len()
    );
    if exhausted {
        let _ = write!(note, "; a child ran out of work before its kill landed");
    }
    Ok(note)
}

/// The loop. Each iteration gets its own store and its own seed, and prints the
/// seed before it runs.
///
/// [`SEED_VAR`] seeds the loop, not an iteration: it fixes the bytes of every
/// object and every kill point, so a rerun replays the same decisions in the
/// same order. What it cannot replay is where inside an operation each kill
/// landed, which is a race with the machine. A crash bug is therefore
/// reproducible in the sense that matters, which is that the store that
/// exhibited it is in the archive the failure names.
///
/// Every failure goes through [`fail`], including one that arrived as a panic.
/// A panic left to propagate would unwind past `scratch` and `TempDir` would
/// delete the post-crash store on the way out, which is exactly backwards: a
/// child that died on its own, or one that stopped acknowledging, is the case
/// where what is on disk is the whole of the evidence. Catching is preferred
/// over turning the two panicking helpers into `Result`, because it also covers
/// the sites that are not theirs, a `tempfile` that cannot be made or a
/// `kill(1)` that will not run, and the payload carries the same diagnostics
/// the `Result` would have.
#[test]
fn engine_crash_loop() {
    let base = run_seed();
    let total = iterations();
    println!("crash harness: {total} iterations, base seed {base} ({SEED_VAR} to reproduce)");

    for i in 0..total {
        let mut rng = Rng(base ^ (u64::try_from(i).expect("an iteration count fits u64")));
        let seed = splitmix64(&mut rng.0);
        let scratch = tempfile::tempdir().expect("a scratch directory");
        println!("crash iteration {}/{total} seed={seed}", i + 1);

        let ran = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            one_iteration(scratch.path(), seed, &mut rng)
        }));
        let problem = match ran {
            Ok(Ok(note)) => {
                println!("crash iteration {}/{total} ok: {note}", i + 1);
                continue;
            }
            Ok(Err(problem)) => problem,
            Err(payload) => panic_message(&payload),
        };
        fail(
            scratch.path(),
            &format!("engine-{seed}"),
            &format!(
                "{SEED_VAR}={base}, which fails at iteration {} of {total}",
                i + 1
            ),
            &format!("crash iteration {}/{total}: {problem}", i + 1),
        );
    }
}

// ---------------------------------------------------------------------------
// The end-to-end iteration
// ---------------------------------------------------------------------------

/// A running `aks3` binary, and the address it reported.
struct ServerProc {
    proc: Killable,
    addr: SocketAddr,
}

impl ServerProc {
    /// Start the real binary over `dir` on a port the operating system picks,
    /// and return once it has logged the address it bound.
    fn start(dir: &Path) -> Self {
        let child = Command::new(env!("CARGO_BIN_EXE_aks3"))
            .env("AKS3_LISTEN", "127.0.0.1:0")
            .env("AKS3_DATA_DIR", dir)
            .env("AKS3_ROOT_USER", ROOT_USER)
            .env("AKS3_ROOT_PASSWORD", ROOT_PASSWORD)
            .env("RUST_LOG", "info")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("starting the aks3 binary");
        let mut proc = Killable::adopt(child);
        let addr = proc.wait_for_listening();
        Self { proc, addr }
    }
}

fn client(addr: SocketAddr) -> aws_sdk_s3::Client {
    let creds = aws_sdk_s3::config::Credentials::new(ROOT_USER, ROOT_PASSWORD, None, None, "crash");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(aws_sdk_s3::config::Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// The whole stack, once per run: `SIGKILL` the real binary while the real AWS
/// SDK is uploading, restart it over the same data directory, and read back
/// everything the SDK was told had been stored.
///
/// One iteration on purpose. The engine loop above is where iteration count buys
/// coverage; what this adds is the parts the engine loop cannot see, namely that
/// the server starts at all over a directory a crash left behind and serves the
/// objects it acknowledged before dying.
#[tokio::test]
async fn e2e_sigkill_mid_put_recovers() {
    let seed = run_seed();
    println!("crash e2e: seed={seed}, killing after {E2E_ACKS} acknowledged PUTs");
    let scratch = tempfile::tempdir().expect("a scratch directory");
    let root = scratch.path().join("data");
    std::fs::create_dir_all(&root).expect("the data directory");

    let mut server = ServerProc::start(&root);
    let c = client(server.addr);
    c.create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .expect("creating the bucket");

    // The uploads run in their own task so that the kill can land while one is
    // still in flight; the channel is the acknowledgement the kill waits on.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel();
    let uploader = c.clone();
    let writer = tokio::spawn(async move {
        for index in 0..E2E_KEYS {
            let key = key_for(index);
            let body = content_for(&key, seed, E2E_BODY);
            let sent = uploader
                .put_object()
                .bucket(BUCKET)
                .key(&key)
                .body(body.into())
                .send()
                .await;
            // Once the server is gone every request fails; that is the point.
            if sent.is_err() || tx.send(index).is_err() {
                return;
            }
        }
    });

    let mut acked = Vec::new();
    while acked.len() < E2E_ACKS {
        let next = tokio::time::timeout(DEADLINE, rx.recv())
            .await
            .expect("waiting for an acknowledged PUT")
            .expect("the uploader stopped early");
        acked.push(next);
    }
    server.proc.kill_9();
    writer.abort();
    let acked_note = format!("e2e acked {acked:?}");
    record(&scratch.path().join("acked.log"), &acked_note);

    let mut restarted = ServerProc::start(&root);
    let c = client(restarted.addr);
    for index in &acked {
        let key = key_for(*index);
        let got = c.get_object().bucket(BUCKET).key(&key).send().await;
        let Ok(got) = got else {
            fail_e2e(
                scratch.path(),
                seed,
                &format!("{key} was acknowledged before the kill and is gone after the restart"),
            );
        };
        let bytes = got
            .body
            .collect()
            .await
            .expect("reading the object")
            .into_bytes();
        if bytes[..] != content_for(&key, seed, E2E_BODY)[..] {
            fail_e2e(
                scratch.path(),
                seed,
                &format!("{key} read back {} bytes that do not match", bytes.len()),
            );
        }
    }

    still_serving(&c, scratch.path(), seed).await;
    restarted.proc.kill_9();
}

/// Healthy, not merely alive: the restarted server takes a write into the
/// recovered store and serves it back.
///
/// Failures here go through [`fail_e2e`] rather than `expect`, because the store
/// this ran against is the evidence and an unwind past the caller's `TempDir`
/// would delete it.
async fn still_serving(c: &aws_sdk_s3::Client, scratch: &Path, seed: u64) {
    let key = "after-the-crash";
    let body = content_for(key, seed, 4096);
    let wrote = c
        .put_object()
        .bucket(BUCKET)
        .key(key)
        .body(body.clone().into())
        .send()
        .await;
    let read = match wrote {
        Ok(_) => c.get_object().bucket(BUCKET).key(key).send().await,
        Err(e) => fail_e2e(
            scratch,
            seed,
            &format!("the recovered store refused a fresh write: {e}"),
        ),
    };
    let streamed = match read {
        Ok(got) => got.body.collect().await,
        Err(e) => fail_e2e(
            scratch,
            seed,
            &format!("the recovered store would not serve a fresh write back: {e}"),
        ),
    };
    let fresh = match streamed {
        Ok(bytes) => bytes.into_bytes(),
        Err(e) => fail_e2e(
            scratch,
            seed,
            &format!("the fresh write's body stopped arriving: {e}"),
        ),
    };
    if fresh[..] != body[..] {
        fail_e2e(scratch, seed, "the recovered store lost a fresh write");
    }
}
