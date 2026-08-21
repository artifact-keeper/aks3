// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! A single-directory [`ObjectLayer`]: one host directory holds every bucket.
//!
//! # Layout
//!
//! ```text
//! <root>/
//!   .aks3/
//!     format.json          layout version of this data directory
//!     tmp/                 staging area for every atomic write
//!   buckets/
//!     <bucket>/
//!       .bucket.meta       { "created_epoch_ms": u64 }
//!       objects/           the object tree (Tasks 6 and 7)
//! ```
//!
//! Buckets live under `buckets/` rather than directly under `<root>` so that a
//! bucket name can never collide with aks3's own bookkeeping: `.aks3` is not a
//! legal bucket name today, but the separation means a future rule change
//! cannot make one, and it keeps `list_buckets` a plain directory read with no
//! names to exclude.
//!
//! A bucket's own files are named with a leading `.`, which
//! [`crate::paths::key_to_rel_path`] never produces, and they sit beside
//! `objects/` rather than inside it, so no object can shadow them.
//!
//! Staging in `<root>/.aks3/tmp` puts temp files on the same filesystem as
//! their destinations, which is what makes [`crate::atomic`]'s rename atomic.
//!
//! # Durability
//!
//! Creating a bucket makes a directory, and a new directory entry is only
//! durable once its *parent* is fsynced. [`crate::atomic::write_json_atomic`]
//! syncs the bucket directory (the meta file's parent) but not `buckets/`,
//! since that already existed, so bucket create and delete fsync `buckets/`
//! themselves. Without it a crash could take away a bucket whose creation was
//! already acknowledged, or bring back one that was deleted.
//!
//! # Crash recovery
//!
//! [`FsEngine::open`] sweeps `.aks3/tmp`. A [`crate::atomic::StagedFile`]
//! cleans up after itself on drop, so the only files that survive there are
//! from a process that died mid-write; their destinations were never published
//! and the bytes are garbage. Sweeping at startup is the only thing that keeps
//! a crash loop from filling the disk.
//!
//! `open` therefore assumes no other process is writing to the same `<root>`:
//! it is a startup step, and a second live engine would have its in-flight
//! staged files deleted underneath it. aks3 runs one engine per data directory.

use std::io;
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::{Deserialize, Serialize};
use tokio::fs;

use crate::atomic::write_json_atomic;
use crate::error::EngineError;
use crate::layer::{
    BoxByteStream, BucketInfo, ByteRange, ListParams, ListResult, ObjectInfo, ObjectLayer, PutOpts,
};

/// Layout version stamped on `<root>/.aks3/format.json`.
///
/// Read the same way [`crate::meta::MANIFEST_FORMAT`] is: a data directory
/// written by a newer build is refused rather than half-understood.
pub const DISK_FORMAT: u32 = 1;

/// Directory holding aks3's own state, as opposed to the user's buckets.
const AKS3_DIR: &str = ".aks3";
/// Staging directory for atomic writes, inside [`AKS3_DIR`].
const TMP_DIR: &str = "tmp";
/// Layout version file, inside [`AKS3_DIR`].
const FORMAT_FILE: &str = "format.json";
/// Directory holding one subdirectory per bucket.
const BUCKETS_DIR: &str = "buckets";
/// Per-bucket metadata file, inside a bucket directory.
const BUCKET_META_FILE: &str = ".bucket.meta";
/// Object tree, inside a bucket directory.
const OBJECTS_DIR: &str = "objects";

/// Contents of `<root>/.aks3/format.json`.
#[derive(Debug, Serialize, Deserialize)]
struct DiskFormat {
    format: u32,
}

/// Contents of a bucket's `.bucket.meta`.
#[derive(Debug, Serialize, Deserialize)]
struct BucketMeta {
    created_epoch_ms: u64,
}

/// An object store backed by one directory on one filesystem.
#[derive(Debug)]
pub struct FsEngine {
    root: PathBuf,
}

impl FsEngine {
    /// Open, and if necessary initialise, the data directory at `root`.
    ///
    /// Creates the layout, stamps [`DISK_FORMAT`] if the directory is new, and
    /// sweeps temp files left by a previous crash. Safe to call on an existing
    /// data directory: nothing already there is rewritten.
    ///
    /// # Errors
    ///
    /// [`EngineError::Io`] if the layout cannot be created or read, or if the
    /// directory was written by a build with a newer [`DISK_FORMAT`].
    pub async fn open(root: impl Into<PathBuf>) -> Result<Self, EngineError> {
        let engine = Self { root: root.into() };
        // The temp directory first: write_json_atomic stages through it, so it
        // has to exist before anything below writes a file.
        fs::create_dir_all(engine.tmp_dir()).await?;
        fs::create_dir_all(engine.buckets_dir()).await?;
        engine.sweep_tmp().await?;
        engine.init_format().await?;
        Ok(engine)
    }

    /// `<root>/buckets/<b>`.
    ///
    /// Only ever called with a name [`is_valid_bucket_name`] accepted, which is
    /// what keeps `b` a single path component: a valid name holds no `/` and is
    /// neither `.` nor `..`, so the join cannot escape `buckets/`.
    fn bucket_dir(&self, b: &str) -> PathBuf {
        debug_assert!(is_valid_bucket_name(b), "unvalidated bucket name: {b:?}");
        self.buckets_dir().join(b)
    }

    /// `<root>/.aks3/tmp`.
    fn tmp_dir(&self) -> PathBuf {
        self.root.join(AKS3_DIR).join(TMP_DIR)
    }

    /// `<root>/buckets`.
    fn buckets_dir(&self) -> PathBuf {
        self.root.join(BUCKETS_DIR)
    }

    /// [`Self::bucket_dir`] for a name that has been validated, so callers
    /// cannot forget the check.
    fn checked_bucket_dir(&self, b: &str) -> Result<PathBuf, EngineError> {
        if is_valid_bucket_name(b) {
            Ok(self.bucket_dir(b))
        } else {
            Err(EngineError::InvalidBucketName)
        }
    }

    /// Delete everything in the staging directory.
    ///
    /// A failure to remove one entry is logged and skipped: a leftover temp
    /// file costs disk space, and refusing to start the server over one would
    /// cost the service. Failing to *read* the directory is different, and
    /// propagates: we just created it, so that means the data directory is not
    /// usable.
    async fn sweep_tmp(&self) -> io::Result<()> {
        let tmp = self.tmp_dir();
        let mut entries = fs::read_dir(&tmp).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            // Nothing creates directories in here, but a stray one must not
            // survive every sweep forever.
            let removed = if entry.file_type().await.is_ok_and(|t| t.is_dir()) {
                fs::remove_dir_all(&path).await
            } else {
                fs::remove_file(&path).await
            };
            if let Err(e) = removed {
                tracing::warn!(path = %path.display(), error = %e, "could not sweep temp file");
            }
        }
        Ok(())
    }

    /// Check the on-disk layout version, stamping it if the directory is new.
    ///
    /// An existing file is read and never rewritten, so fields a later build
    /// adds (the design's deployment id, for one) survive being opened by this
    /// one instead of being parsed away and written back without them.
    async fn init_format(&self) -> io::Result<()> {
        let path = self.root.join(AKS3_DIR).join(FORMAT_FILE);
        let bytes = match fs::read(&path).await {
            Ok(bytes) => bytes,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                let format = DiskFormat {
                    format: DISK_FORMAT,
                };
                return write_json_atomic(&self.tmp_dir(), &path, &format).await;
            }
            Err(e) => return Err(e),
        };
        let on_disk: DiskFormat = serde_json::from_slice(&bytes)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if on_disk.format > DISK_FORMAT {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "data directory format {} at {} is newer than the supported format {DISK_FORMAT}",
                    on_disk.format,
                    path.display(),
                ),
            ));
        }
        Ok(())
    }
}

#[async_trait::async_trait]
impl ObjectLayer for FsEngine {
    async fn create_bucket(&self, bucket: &str) -> Result<(), EngineError> {
        let dir = self.checked_bucket_dir(bucket)?;
        // create_dir, not create_dir_all: it fails when the name is taken, which
        // makes "does it exist" and "claim it" one step. Checking first and
        // creating after would let two concurrent creates both succeed.
        match fs::create_dir(&dir).await {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::AlreadyExists => {
                return Err(EngineError::BucketAlreadyExists);
            }
            Err(e) => return Err(e.into()),
        }

        let meta = BucketMeta {
            created_epoch_ms: now_epoch_ms(),
        };
        if let Err(e) = write_json_atomic(&self.tmp_dir(), &dir.join(BUCKET_META_FILE), &meta).await
        {
            // Give the name back, so a reported failure means no bucket. Best
            // effort: if this fails too the directory stands, and list_buckets
            // dates it from the directory itself rather than hiding it.
            let _ = fs::remove_dir_all(&dir).await;
            return Err(e.into());
        }
        fsync_dir(&self.buckets_dir()).await?;
        Ok(())
    }

    async fn delete_bucket(&self, bucket: &str) -> Result<(), EngineError> {
        let dir = self.checked_bucket_dir(bucket)?;
        if !fs::try_exists(&dir).await? {
            return Err(EngineError::NoSuchBucket);
        }
        // S3 deletes only empty buckets. `objects/` is created lazily by the
        // first PUT, so its absence is emptiness. Any entry under it counts,
        // which errs towards refusing: a key directory that DELETE could not
        // prune keeps the bucket alive until a sweep removes it, rather than
        // taking a tree away that might still hold an object.
        if has_any_entry(&dir.join(OBJECTS_DIR)).await? {
            return Err(EngineError::BucketNotEmpty);
        }
        match fs::remove_dir_all(&dir).await {
            Ok(()) => {}
            // Lost a race with another delete of the same bucket; the caller's
            // bucket is gone either way, but only one of them deleted it.
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                return Err(EngineError::NoSuchBucket);
            }
            Err(e) => return Err(e.into()),
        }
        fsync_dir(&self.buckets_dir()).await?;
        Ok(())
    }

    async fn bucket_exists(&self, bucket: &str) -> Result<bool, EngineError> {
        let dir = self.checked_bucket_dir(bucket)?;
        match fs::metadata(&dir).await {
            Ok(m) => Ok(m.is_dir()),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    async fn list_buckets(&self) -> Result<Vec<BucketInfo>, EngineError> {
        let mut out = Vec::new();
        let mut entries = fs::read_dir(self.buckets_dir()).await?;
        while let Some(entry) = entries.next_entry().await? {
            // A name that is not valid UTF-8, or not a legal bucket name, was
            // not put there by create_bucket. Skipping is what keeps a stray
            // file from being served as a bucket that nothing can delete.
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            if !is_valid_bucket_name(&name) {
                continue;
            }
            // An entry that vanished between readdir and stat was deleted
            // concurrently; report the listing as of before it existed rather
            // than failing the whole call.
            let Ok(file_type) = entry.file_type().await else {
                continue;
            };
            if !file_type.is_dir() {
                continue;
            }
            out.push(BucketInfo {
                created_epoch_ms: bucket_created_ms(&entry.path()).await,
                name,
            });
        }
        // Directory order is whatever the filesystem feels like; S3 reports
        // buckets by name.
        out.sort_by(|a, b| a.name.cmp(&b.name));
        Ok(out)
    }

    async fn put_object(
        &self,
        _bucket: &str,
        _key: &str,
        _body: BoxByteStream,
        _opts: PutOpts,
    ) -> Result<ObjectInfo, EngineError> {
        Err(not_yet_implemented())
    }

    async fn get_object(
        &self,
        _bucket: &str,
        _key: &str,
        _range: Option<ByteRange>,
    ) -> Result<(ObjectInfo, u64, u64, BoxByteStream), EngineError> {
        Err(not_yet_implemented())
    }

    async fn head_object(&self, _bucket: &str, _key: &str) -> Result<ObjectInfo, EngineError> {
        Err(not_yet_implemented())
    }

    async fn delete_object(&self, _bucket: &str, _key: &str) -> Result<(), EngineError> {
        Err(not_yet_implemented())
    }

    async fn list_objects_v2(
        &self,
        _bucket: &str,
        _p: &ListParams,
    ) -> Result<ListResult, EngineError> {
        Err(not_yet_implemented())
    }
}

/// Whether `name` is a legal S3 bucket name.
///
/// A simplified port of `MinIO`'s `internal/s3utils` strict check, which is
/// itself AWS's rule: 3 to 63 characters from `[a-z0-9.-]`, starting and ending
/// alphanumeric, with no `..`, `.-` or `-.` and no IPv4-looking name. The
/// adjacency rules exist because such names break virtual-host-style addressing
/// and TLS certificate matching, not because of anything on disk.
///
/// Two AWS rules are deliberately left out, as `MinIO` leaves them out: the
/// `xn--` prefix and the `-s3alias` suffix, which only matter to services aks3
/// does not implement.
///
/// The rule is also what makes a bucket name safe to join onto a path. It is
/// checked on the way in, not on the way out: names already on disk were
/// checked when they were created.
fn is_valid_bucket_name(name: &str) -> bool {
    if !(3..=63).contains(&name.len()) {
        return false;
    }
    if name.contains("..") || name.contains(".-") || name.contains("-.") {
        return false;
    }
    // The charset below admits nothing but IPv4 among address forms, and Rust's
    // parser is strict about what an IPv4 address is (four decimal octets, no
    // leading zeros), which is the shape AWS rejects.
    if name.parse::<Ipv4Addr>().is_ok() {
        return false;
    }
    let bytes = name.as_bytes();
    // Indexing is safe: the length check above proved the name is not empty.
    if !is_bucket_alnum(bytes[0]) || !is_bucket_alnum(bytes[bytes.len() - 1]) {
        return false;
    }
    bytes
        .iter()
        .all(|&b| is_bucket_alnum(b) || b == b'.' || b == b'-')
}

/// The characters a bucket name may start and end with.
fn is_bucket_alnum(b: u8) -> bool {
    b.is_ascii_lowercase() || b.is_ascii_digit()
}

/// When the bucket at `dir` was created.
///
/// Falls back to the directory's own timestamp when `.bucket.meta` is missing
/// or unreadable, which is what a crash between the mkdir and the meta write
/// leaves behind. Such a bucket is real: `create_bucket` reports the name as
/// taken, so hiding it from a listing would be the inconsistency, not showing
/// it with an approximate date.
async fn bucket_created_ms(dir: &Path) -> u64 {
    if let Ok(bytes) = fs::read(dir.join(BUCKET_META_FILE)).await {
        if let Ok(meta) = serde_json::from_slice::<BucketMeta>(&bytes) {
            return meta.created_epoch_ms;
        }
    }
    fs::metadata(dir)
        .await
        .ok()
        .and_then(|m| m.modified().ok())
        .map_or(0, epoch_ms)
}

/// Now, in milliseconds since the Unix epoch.
fn now_epoch_ms() -> u64 {
    epoch_ms(SystemTime::now())
}

/// `t` in milliseconds since the Unix epoch, or 0 if it predates the epoch or
/// does not fit. Both mean a clock aks3 cannot report a timestamp from, and
/// neither is worth failing a request over.
fn epoch_ms(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .ok()
        .and_then(|d| u64::try_from(d.as_millis()).ok())
        .unwrap_or(0)
}

/// Whether `dir` holds anything. A directory that is not there holds nothing.
async fn has_any_entry(dir: &Path) -> io::Result<bool> {
    match fs::read_dir(dir).await {
        Ok(mut entries) => Ok(entries.next_entry().await?.is_some()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(e) => Err(e),
    }
}

/// Make `dir`'s entries durable. See the module's durability note.
async fn fsync_dir(dir: &Path) -> io::Result<()> {
    fs::File::open(dir).await?.sync_all().await
}

/// The five object methods land in Tasks 6 and 7. Until then they fail loudly
/// rather than pretending an empty store.
fn not_yet_implemented() -> EngineError {
    EngineError::Io(io::Error::new(
        io::ErrorKind::Unsupported,
        "not yet implemented",
    ))
}

#[cfg(test)]
mod tests {
    use futures::StreamExt;

    use super::*;

    async fn eng() -> (tempfile::TempDir, FsEngine) {
        let d = tempfile::tempdir().unwrap();
        let e = FsEngine::open(d.path()).await.unwrap();
        (d, e)
    }

    #[tokio::test]
    async fn bucket_lifecycle() {
        let (_d, e) = eng().await;
        assert!(!e.bucket_exists("photos").await.unwrap());
        e.create_bucket("photos").await.unwrap();
        assert!(e.bucket_exists("photos").await.unwrap());
        assert!(matches!(
            e.create_bucket("photos").await,
            Err(EngineError::BucketAlreadyExists)
        ));
        let names: Vec<_> = e
            .list_buckets()
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.name)
            .collect();
        assert_eq!(names, vec!["photos"]);
        e.delete_bucket("photos").await.unwrap();
        assert!(matches!(
            e.delete_bucket("photos").await,
            Err(EngineError::NoSuchBucket)
        ));
    }

    #[tokio::test]
    async fn bucket_name_validation() {
        let (_d, e) = eng().await;
        for bad in [
            "ab",
            "UPPER",
            "has_underscore",
            "-lead",
            "trail-",
            "a..b",
            "192.168.1.1",
        ] {
            assert!(
                matches!(
                    e.create_bucket(bad).await,
                    Err(EngineError::InvalidBucketName)
                ),
                "{bad}"
            );
        }
        for good in ["abc", "my-bucket.v2", "a1b2c3"] {
            e.create_bucket(good).await.unwrap();
        }
    }

    #[tokio::test]
    async fn open_sweeps_tmp() {
        let d = tempfile::tempdir().unwrap();
        let tmp = d.path().join(".aks3/tmp");
        std::fs::create_dir_all(&tmp).unwrap();
        std::fs::write(tmp.join("leftover"), b"x").unwrap();
        let _e = FsEngine::open(d.path()).await.unwrap();
        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
    }

    // --- additional cases beyond the brief ---

    #[tokio::test]
    async fn open_creates_the_layout_and_stamps_the_format() {
        let (d, e) = eng().await;
        assert!(e.tmp_dir().is_dir());
        assert!(e.buckets_dir().is_dir());
        let format: DiskFormat =
            serde_json::from_slice(&std::fs::read(d.path().join(".aks3/format.json")).unwrap())
                .unwrap();
        assert_eq!(format.format, DISK_FORMAT);
    }

    #[tokio::test]
    async fn reopening_keeps_buckets_and_does_not_restamp() {
        let (d, e) = eng().await;
        e.create_bucket("photos").await.unwrap();
        let created = e.list_buckets().await.unwrap()[0].created_epoch_ms;
        drop(e);

        // A hand-edited format file survives the reopen, proving the stamp is
        // written only when it is missing.
        let format_path = d.path().join(".aks3/format.json");
        std::fs::write(&format_path, br#"{"format":1,"marker":true}"#).unwrap();

        let e = FsEngine::open(d.path()).await.unwrap();
        let buckets = e.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 1);
        assert_eq!(buckets[0].name, "photos");
        assert_eq!(buckets[0].created_epoch_ms, created);
        assert!(std::fs::read_to_string(&format_path)
            .unwrap()
            .contains("marker"));
    }

    #[tokio::test]
    async fn open_refuses_a_newer_on_disk_format() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join(".aks3")).unwrap();
        std::fs::write(
            d.path().join(".aks3/format.json"),
            format!(r#"{{"format":{}}}"#, DISK_FORMAT + 1),
        )
        .unwrap();

        let err = FsEngine::open(d.path()).await.unwrap_err();
        let EngineError::Io(err) = err else {
            panic!("expected an io error, got {err:?}");
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert!(
            err.to_string()
                .contains(&format!("format {}", DISK_FORMAT + 1)),
            "{err}"
        );
    }

    #[tokio::test]
    async fn open_refuses_an_unparsable_format_file() {
        let d = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(d.path().join(".aks3")).unwrap();
        std::fs::write(d.path().join(".aks3/format.json"), b"not json").unwrap();

        let err = FsEngine::open(d.path()).await.unwrap_err();
        let EngineError::Io(err) = err else {
            panic!("expected an io error, got {err:?}");
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn open_sweeps_only_the_temp_directory() {
        let d = tempfile::tempdir().unwrap();
        let e = FsEngine::open(d.path()).await.unwrap();
        e.create_bucket("photos").await.unwrap();

        let tmp = e.tmp_dir();
        std::fs::write(tmp.join("aborted.tmp"), b"partial upload").unwrap();
        std::fs::create_dir_all(tmp.join("stray-dir/inner")).unwrap();
        drop(e);

        let e = FsEngine::open(d.path()).await.unwrap();
        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
        // The sweep must not reach outside tmp: the bucket and its meta stand.
        assert!(e.bucket_exists("photos").await.unwrap());
        assert!(e.bucket_dir("photos").join(".bucket.meta").is_file());
    }

    #[tokio::test]
    async fn create_bucket_records_a_creation_time() {
        let before = now_epoch_ms();
        let (_d, e) = eng().await;
        e.create_bucket("photos").await.unwrap();
        let after = now_epoch_ms();

        let created = e.list_buckets().await.unwrap()[0].created_epoch_ms;
        assert!(
            (before..=after).contains(&created),
            "{created} not in {before}..={after}"
        );
    }

    #[tokio::test]
    async fn a_failed_create_leaves_no_bucket() {
        let (_d, e) = eng().await;
        // With the staging directory replaced by a file, the meta write fails
        // after the bucket directory has already been made.
        let tmp = e.tmp_dir();
        std::fs::remove_dir_all(&tmp).unwrap();
        std::fs::write(&tmp, b"not a directory").unwrap();

        assert!(matches!(
            e.create_bucket("photos").await,
            Err(EngineError::Io(_))
        ));
        assert!(!e.bucket_exists("photos").await.unwrap());
        assert!(e.list_buckets().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn delete_bucket_refuses_a_non_empty_bucket() {
        let (_d, e) = eng().await;
        e.create_bucket("photos").await.unwrap();
        let objects = e.bucket_dir("photos").join("objects");

        // An empty objects/ directory is still an empty bucket: the first PUT
        // creates it and the last DELETE need not take it away.
        std::fs::create_dir_all(&objects).unwrap();
        assert!(!has_any_entry(&objects).await.unwrap());

        std::fs::create_dir_all(objects.join("k")).unwrap();
        std::fs::write(objects.join("k/__aks3.meta.json"), b"{}").unwrap();
        assert!(matches!(
            e.delete_bucket("photos").await,
            Err(EngineError::BucketNotEmpty)
        ));
        assert!(e.bucket_exists("photos").await.unwrap());

        std::fs::remove_dir_all(objects.join("k")).unwrap();
        e.delete_bucket("photos").await.unwrap();
        assert!(!e.bucket_dir("photos").exists());
    }

    #[tokio::test]
    async fn bucket_calls_reject_an_invalid_name_without_touching_the_disk() {
        let (d, e) = eng().await;
        // Names that would escape buckets/ if they were ever joined onto it.
        for bad in ["../escape", "a/b", ".aks3", "..", "/etc/passwd", ""] {
            assert!(
                matches!(
                    e.create_bucket(bad).await,
                    Err(EngineError::InvalidBucketName)
                ),
                "create {bad:?}"
            );
            assert!(
                matches!(
                    e.delete_bucket(bad).await,
                    Err(EngineError::InvalidBucketName)
                ),
                "delete {bad:?}"
            );
            assert!(
                matches!(
                    e.bucket_exists(bad).await,
                    Err(EngineError::InvalidBucketName)
                ),
                "exists {bad:?}"
            );
        }
        assert_eq!(
            std::fs::read_dir(d.path().join("buckets")).unwrap().count(),
            0
        );
    }

    #[test]
    fn bucket_name_rules() {
        let longest = "a".repeat(63);
        let too_long = "a".repeat(64);
        for good in [
            "abc",
            "a-b",
            "a.b",
            "a1b2c3",
            "my-bucket.v2",
            "010.1.1.1", // not an IPv4 address: octets do not take leading zeros
            "1.2.3.4.5",
            longest.as_str(),
        ] {
            assert!(is_valid_bucket_name(good), "{good} should be valid");
        }
        for bad in [
            "",
            "ab",
            too_long.as_str(),
            "UPPER",
            "has_underscore",
            "-lead",
            "trail-",
            ".lead",
            "trail.",
            "a..b",
            "a.-b",
            "a-.b",
            "a b",
            "a/b",
            "..",
            "192.168.1.1",
            "1.2.3.4",
            "255.255.255.255",
            "caf\u{e9}s",
        ] {
            assert!(!is_valid_bucket_name(bad), "{bad} should be invalid");
        }
    }

    #[tokio::test]
    async fn list_buckets_is_sorted_and_ignores_strays() {
        let (d, e) = eng().await;
        for b in ["zulu", "alpha", "mike"] {
            e.create_bucket(b).await.unwrap();
        }
        // Things create_bucket never made: a loose file and a directory whose
        // name is not a legal bucket name.
        std::fs::write(d.path().join("buckets/loose-file"), b"x").unwrap();
        std::fs::create_dir_all(d.path().join("buckets/Not_A_Bucket")).unwrap();

        let names: Vec<_> = e
            .list_buckets()
            .await
            .unwrap()
            .into_iter()
            .map(|b| b.name)
            .collect();
        assert_eq!(names, vec!["alpha", "mike", "zulu"]);
    }

    #[tokio::test]
    async fn a_bucket_whose_meta_never_landed_is_still_a_bucket() {
        let (_d, e) = eng().await;
        e.create_bucket("photos").await.unwrap();
        // What a crash between the mkdir and the meta write leaves behind.
        std::fs::remove_file(e.bucket_dir("photos").join(".bucket.meta")).unwrap();

        assert!(e.bucket_exists("photos").await.unwrap());
        let buckets = e.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 1);
        assert!(
            buckets[0].created_epoch_ms > 0,
            "expected the directory's own timestamp"
        );
        e.delete_bucket("photos").await.unwrap();
    }

    #[tokio::test]
    async fn a_corrupt_bucket_meta_does_not_fail_the_listing() {
        let (_d, e) = eng().await;
        e.create_bucket("photos").await.unwrap();
        std::fs::write(e.bucket_dir("photos").join(".bucket.meta"), b"{").unwrap();

        let buckets = e.list_buckets().await.unwrap();
        assert_eq!(buckets.len(), 1);
        assert!(buckets[0].created_epoch_ms > 0);
    }

    #[tokio::test]
    async fn a_file_where_a_bucket_would_go_is_not_a_bucket() {
        let (d, e) = eng().await;
        std::fs::write(d.path().join("buckets/photos"), b"x").unwrap();

        assert!(!e.bucket_exists("photos").await.unwrap());
        assert!(e.list_buckets().await.unwrap().is_empty());
        assert!(matches!(
            e.create_bucket("photos").await,
            Err(EngineError::BucketAlreadyExists)
        ));
    }

    /// Delete this once Tasks 6 and 7 have replaced the stubs.
    #[tokio::test]
    async fn the_object_methods_are_not_implemented_yet() {
        let (_d, e) = eng().await;
        e.create_bucket("photos").await.unwrap();

        let body = futures::stream::empty().boxed();
        let calls: Vec<Result<(), EngineError>> = vec![
            e.put_object("photos", "k", body, PutOpts::default())
                .await
                .map(|_| ()),
            e.get_object("photos", "k", None).await.map(|_| ()),
            e.head_object("photos", "k").await.map(|_| ()),
            e.delete_object("photos", "k").await,
            e.list_objects_v2("photos", &ListParams::default())
                .await
                .map(|_| ()),
        ];
        for call in calls {
            let Err(EngineError::Io(err)) = call else {
                panic!("expected an unimplemented error");
            };
            assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        }
    }
}
