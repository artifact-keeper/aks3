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
//!       objects/
//!         <encoded-key>/
//!           __aks3.meta.json     the key's version manifest
//!           __aks3.v.null.data   the bytes of one version
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
//!
//! # Concurrency
//!
//! The filesystem gives atomicity for a single rename and nothing above it. Two
//! in-process locks supply the rest, and they are always taken in this order:
//!
//! 1. **The bucket lock**, an `RwLock` per bucket name. Every write to an
//!    object takes it *shared*; [`FsEngine::delete_bucket`] and
//!    [`FsEngine::create_bucket`] take it *exclusively*. Without this,
//!    `delete_bucket`'s emptiness check and its `remove_dir_all` are two steps
//!    with a gap, and an object published in that gap is destroyed by a call
//!    that reported the bucket as empty.
//! 2. **The key lock**, a `Mutex` per (bucket, key). It serialises the
//!    *publication* of a write: the data commit and the manifest update, which
//!    are two renames that must not interleave with another writer's. Streaming
//!    a body into the staging directory happens before the lock is taken, so a
//!    slow upload never blocks another writer, and `delete_bucket` waits only
//!    for publications, not for transfers.
//!
//! Both tables drop an entry once nothing holds it, so they stay the size of
//! the concurrent work rather than growing with every key ever written.
//!
//! Reads of one key take neither lock. A `GET` racing a `DELETE` of the same
//! key reports whichever state it observed, which is what S3 promises; holding
//! a lock for the life of a response stream would let one slow reader block
//! every writer.
//!
//! A listing does take the bucket lock, shared. It reads the whole tree rather
//! than one key, and `delete_bucket`'s `remove_dir_all` would otherwise pull
//! that tree out from under the walk, turning "the bucket was deleted" into I/O
//! errors. Shared is enough: writers hold it shared too, so a listing still
//! runs alongside every `PUT` and `DELETE`, and only sees each key as of
//! whenever it reached it.
//!
//! The locks live in this process, so they order the requests one server
//! handles. Two servers over one data directory are not supported, as
//! [`FsEngine::open`]'s temp sweep already assumes.

use std::hash::Hash;
use std::io::{self, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use dashmap::DashMap;
use futures::StreamExt;
use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use tokio::fs;
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::{Mutex, OwnedMutexGuard, OwnedRwLockReadGuard, OwnedRwLockWriteGuard, RwLock};

use crate::atomic::{write_json_atomic, StagedFile};
use crate::error::EngineError;
use crate::layer::{
    BoxByteStream, BucketInfo, ByteRange, ListParams, ListResult, ObjectInfo, ObjectLayer, PutOpts,
};
use crate::meta::{
    load_manifest, store_manifest, VersionEntry, VersionManifest, MANIFEST_FORMAT, NULL_VERSION_ID,
};
use crate::paths::{data_file_name, key_to_rel_path, META_FILE};
use crate::walk;

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
/// Content type recorded for a `PUT` that did not declare one.
const DEFAULT_CONTENT_TYPE: &str = "application/octet-stream";
/// Bytes read per chunk of a `GET` body stream.
const READ_CHUNK: usize = 64 * 1024;

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
    /// See the module's concurrency note. Keyed by bucket name.
    bucket_locks: DashMap<String, Arc<RwLock<()>>>,
    /// See the module's concurrency note. Keyed by (bucket, key).
    key_locks: DashMap<(String, String), Arc<Mutex<()>>>,
}

/// A held lock from one of [`FsEngine`]'s lock tables, which takes its own
/// entry out of the table on release.
///
/// A table entry is only useful while some task is holding or waiting for it;
/// keeping it afterwards would grow the table by one `Arc` for every key the
/// server ever writes. Removal is conditional on the entry's strong count
/// being 1, meaning the table is its only holder, and `DashMap` evaluates that
/// predicate while holding the shard lock that any other task would need to
/// clone the `Arc`. So an entry is only ever dropped when no task can be about
/// to lock it, and two tasks can never end up holding different locks for the
/// same key.
struct LockEntry<'a, K, L, G>
where
    K: Eq + Hash,
{
    table: &'a DashMap<K, Arc<L>>,
    key: K,
    /// `Option` only so that [`Drop`] can drop the guard before the entry it
    /// belongs to; always `Some` until then.
    guard: Option<G>,
}

/// A held shared bucket lock. See the module's concurrency note.
type BucketReadLock<'a> = LockEntry<'a, String, RwLock<()>, OwnedRwLockReadGuard<()>>;
/// A held exclusive bucket lock.
type BucketWriteLock<'a> = LockEntry<'a, String, RwLock<()>, OwnedRwLockWriteGuard<()>>;
/// A held per-key lock.
type KeyLock<'a> = LockEntry<'a, (String, String), Mutex<()>, OwnedMutexGuard<()>>;

impl<'a, K, L, G> LockEntry<'a, K, L, G>
where
    K: Eq + Hash,
{
    fn new(table: &'a DashMap<K, Arc<L>>, key: K, guard: G) -> Self {
        Self {
            table,
            key,
            guard: Some(guard),
        }
    }
}

impl<K, L, G> Drop for LockEntry<'_, K, L, G>
where
    K: Eq + Hash,
{
    fn drop(&mut self) {
        // Releasing first is what lets the count reach 1: the guard owns the
        // `Arc` clone this lock was taken through.
        drop(self.guard.take());
        self.table
            .remove_if(&self.key, |_, lock| Arc::strong_count(lock) == 1);
    }
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
        let engine = Self {
            root: root.into(),
            bucket_locks: DashMap::new(),
            key_locks: DashMap::new(),
        };
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

    /// [`Self::checked_bucket_dir`] for a bucket that must already exist.
    ///
    /// The object methods all start here: S3 distinguishes "no such bucket"
    /// from "no such key", and on disk both are just a missing directory.
    async fn existing_bucket_dir(&self, b: &str) -> Result<PathBuf, EngineError> {
        let dir = self.checked_bucket_dir(b)?;
        match fs::metadata(&dir).await {
            Ok(m) if m.is_dir() => Ok(dir),
            // A non-directory here was not made by create_bucket, and
            // bucket_exists already reports it as absent.
            Ok(_) => Err(EngineError::NoSuchBucket),
            Err(e) if e.kind() == io::ErrorKind::NotFound => Err(EngineError::NoSuchBucket),
            Err(e) => Err(e.into()),
        }
    }

    /// `<root>/buckets/<b>/objects/<encoded key>`, the directory holding one
    /// key's manifest and data files.
    fn object_dir(bucket_dir: &Path, key: &str) -> PathBuf {
        bucket_dir.join(OBJECTS_DIR).join(key_to_rel_path(key))
    }

    /// Take the bucket lock shared, for a write that must not have the bucket
    /// removed underneath it. See the module's concurrency note.
    async fn lock_bucket_shared(&self, bucket: &str) -> BucketReadLock<'_> {
        let lock = Arc::clone(&self.bucket_locks.entry(bucket.to_owned()).or_default());
        let guard = lock.read_owned().await;
        LockEntry::new(&self.bucket_locks, bucket.to_owned(), guard)
    }

    /// Take the bucket lock exclusively, excluding every object write to it.
    async fn lock_bucket_exclusive(&self, bucket: &str) -> BucketWriteLock<'_> {
        let lock = Arc::clone(&self.bucket_locks.entry(bucket.to_owned()).or_default());
        let guard = lock.write_owned().await;
        LockEntry::new(&self.bucket_locks, bucket.to_owned(), guard)
    }

    /// Take the key lock, serialising publication of writes to one key.
    ///
    /// Always taken after [`Self::lock_bucket_shared`], never before: one
    /// order, so two writers cannot each hold what the other is waiting for.
    async fn lock_key(&self, bucket: &str, key: &str) -> KeyLock<'_> {
        let id = (bucket.to_owned(), key.to_owned());
        let lock = Arc::clone(&self.key_locks.entry(id.clone()).or_default());
        let guard = lock.lock_owned().await;
        LockEntry::new(&self.key_locks, id, guard)
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
        // Excludes delete_bucket, so a name is never reported as taken by a
        // create that a concurrent delete then takes away underneath it.
        let _bucket_lock = self.lock_bucket_exclusive(bucket).await;
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
        // Held across the emptiness check and the removal below, which are
        // otherwise two steps with a gap: a PUT that published in that gap
        // would be destroyed by a call that had just found the bucket empty.
        // Every object write takes this lock shared, so none is in flight here.
        let _bucket_lock = self.lock_bucket_exclusive(bucket).await;
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

    /// Stage the body, then publish it under the key's locks.
    ///
    /// The bytes are streamed into `.aks3/tmp` before any lock is taken, so an
    /// upload the size of a disk does not hold up another writer. Only the two
    /// renames that publish it, the data file and then the manifest, happen
    /// under the locks.
    ///
    /// Data lands before the manifest that points at it. A crash between the
    /// two leaves bytes nothing refers to, which the next `PUT` replaces; the
    /// other order would leave a manifest promising a version whose data never
    /// arrived.
    ///
    /// # A known Phase 0 window
    ///
    /// Unversioned writes reuse one data file name, so an overwrite replaces
    /// the previous object's bytes in place. Between that rename and the
    /// manifest write, a concurrent reader can see the new bytes described by
    /// the old entry: a shorter object reads as the [`io::ErrorKind`]
    /// `UnexpectedEof` [`file_stream`] reports, a longer one as a prefix of the
    /// new object under the old etag. Writers do not see it, since publication
    /// is serialised by the key lock, and it closes as soon as versioning mints
    /// a version id per `PUT` and leaves the previous data file where it is.
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        mut body: BoxByteStream,
        opts: PutOpts,
    ) -> Result<ObjectInfo, EngineError> {
        // Checked up front so a PUT to a bucket that is not there fails before
        // reading the body, and again under the lock below, which is the check
        // that actually holds.
        let bucket_dir = self.existing_bucket_dir(bucket).await?;

        let mut staged = StagedFile::create(&self.tmp_dir()).await?;
        let mut hasher = Md5::new();
        let mut size: u64 = 0;
        while let Some(chunk) = body.next().await {
            let chunk = chunk?;
            hasher.update(&chunk);
            size += chunk.len() as u64;
            staged.write_all(&chunk).await?;
        }

        let entry = VersionEntry {
            version_id: NULL_VERSION_ID.to_owned(),
            etag: hex_lower(&hasher.finalize()),
            size,
            content_type: opts
                .content_type
                .filter(|c| !c.is_empty())
                .unwrap_or_else(|| DEFAULT_CONTENT_TYPE.to_owned()),
            user_metadata: opts.user_metadata,
            mtime_epoch_ms: now_epoch_ms(),
            delete_marker: false,
        };

        let _bucket_lock = self.lock_bucket_shared(bucket).await;
        let _key_lock = self.lock_key(bucket, key).await;
        // The bucket lock is held now, so delete_bucket cannot be between its
        // emptiness check and its remove_dir_all. Re-checking here is what
        // stops the commit below from re-creating a tree under a bucket that
        // was deleted while the body was still arriving.
        self.existing_bucket_dir(bucket).await?;

        let dir = Self::object_dir(&bucket_dir, key);
        staged
            .commit(&dir.join(data_file_name(NULL_VERSION_ID)))
            .await?;

        let mut manifest = load_manifest(&dir.join(META_FILE))
            .await?
            .unwrap_or_else(new_manifest);
        manifest.upsert(entry.clone());
        store_manifest(&self.tmp_dir(), &dir.join(META_FILE), &manifest).await?;

        Ok(object_info(key.to_owned(), &entry))
    }

    /// Read the object's manifest, then open the version it names.
    ///
    /// Those are two steps with an await between them, and no lock spans them:
    /// see the module's concurrency note for why a read holds none. A `DELETE`
    /// landing in the gap removes the manifest and then the data, so the state
    /// this observes is a version it just read whose file is already gone. That
    /// is the object being deleted, not the store being broken, so it reports
    /// [`EngineError::NoSuchKey`] and the API layer answers 404, which is what
    /// S3 does with the same race.
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<(ObjectInfo, u64, u64, BoxByteStream), EngineError> {
        let dir = Self::object_dir(&self.existing_bucket_dir(bucket).await?, key);
        let entry = live_version(&dir).await?;
        let (offset, len) = resolve_range(range, entry.size)?;

        let mut file = match fs::File::open(dir.join(data_file_name(&entry.version_id))).await {
            Ok(file) => file,
            // Only this one open is read as an absent object. Everywhere else a
            // missing file is still an error, since nothing else has a
            // concurrent delete as its likely explanation.
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Err(EngineError::NoSuchKey),
            Err(e) => return Err(e.into()),
        };
        if offset > 0 {
            file.seek(SeekFrom::Start(offset)).await?;
        }
        let info = object_info(key.to_owned(), &entry);
        Ok((info, offset, len, file_stream(file, len)))
    }

    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo, EngineError> {
        let dir = Self::object_dir(&self.existing_bucket_dir(bucket).await?, key);
        let entry = live_version(&dir).await?;
        Ok(object_info(key.to_owned(), &entry))
    }

    /// Remove the key's manifest and data, then prune the directories that
    /// held them.
    ///
    /// The manifest goes first, because it is what makes the object visible:
    /// after that one rename the key reads as gone and everything below is
    /// cleanup. A crash in between leaves data files nothing points at, which
    /// the next write to the key replaces. The other order would leave a
    /// manifest naming a version whose bytes are already gone; [`Self::get_object`]
    /// reads that as an absent object too, but a crash would make it permanent,
    /// leaving a key directory that holds its bucket open and a manifest entry
    /// a versioned build would try to serve.
    ///
    /// Deleting a key that is not there succeeds, as it does in S3.
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), EngineError> {
        let bucket_dir = self.existing_bucket_dir(bucket).await?;
        let _bucket_lock = self.lock_bucket_shared(bucket).await;
        let _key_lock = self.lock_key(bucket, key).await;

        let dir = Self::object_dir(&bucket_dir, key);
        remove_if_present(&dir.join(META_FILE)).await?;
        // Phase 0 writes exactly one version, and reading the manifest to find
        // the others would make a corrupt manifest an object that cannot be
        // deleted. Versioned deletes arrive with versioning itself.
        remove_if_present(&dir.join(data_file_name(NULL_VERSION_ID))).await?;

        let objects_dir = bucket_dir.join(OBJECTS_DIR);
        prune_empty_dirs(&dir, &objects_dir).await;
        // Whichever directory the removals last changed. Without this fsync a
        // crash can bring a deleted object back.
        if let Some(deepest) = deepest_existing(&dir, &objects_dir).await {
            if let Err(e) = fsync_dir(&deepest).await {
                if e.kind() != io::ErrorKind::NotFound {
                    return Err(e.into());
                }
            }
        }
        Ok(())
    }

    /// Walk the bucket's object tree, then cut one page out of the keys.
    ///
    /// # Ordering and paging
    ///
    /// Keys come back in ascending UTF-8 order, and the page is the run of
    /// items strictly after the start bound. The bound is the later of
    /// `continuation_token` and `start_after`, and it is compared against the
    /// *item* a key produces rather than the key itself: with a delimiter that
    /// item is the common prefix the key folds into, and comparing the key
    /// instead would make a token that names a common prefix (`dir/`) re-emit
    /// every key under it (`dir/one` is above `dir/`), which folds back to the
    /// same common prefix and never advances. The cost is that `start_after`
    /// naming a key inside a folded group skips the whole group, which is what
    /// `MinIO` does with the equivalent marker.
    ///
    /// # Phase 0 simplification
    ///
    /// The whole keyspace is materialised and sorted before the page is taken
    /// from it, so listing ten keys of a million walks all million directories,
    /// and does it again for the next page. Correctness first: a lazy walk that
    /// descends only into the subtrees a prefix can reach, and stops once the
    /// page is full, is a later optimisation that changes no behaviour visible
    /// here.
    async fn list_objects_v2(
        &self,
        bucket: &str,
        p: &ListParams,
    ) -> Result<ListResult, EngineError> {
        // Rejects an illegal name before it reaches the lock table.
        self.checked_bucket_dir(bucket)?;
        // Shared, so it holds off delete_bucket without holding off writers.
        // Without it a remove_dir_all could take the tree away mid-walk, and a
        // listing would report I/O errors for a bucket that is simply gone.
        let _bucket_lock = self.lock_bucket_shared(bucket).await;
        let bucket_dir = self.existing_bucket_dir(bucket).await?;

        // A legal request for nothing, and the only case where a full budget is
        // not a truncated listing. Answered before the walk, since walking a
        // tree to report none of it is pure cost.
        let mut out = ListResult::default();
        if p.max_keys == 0 {
            return Ok(out);
        }

        let objects_dir = bucket_dir.join(OBJECTS_DIR);
        // A blocking walk of the tree; see `walk` for why it is not async. A
        // join error means it panicked, since nothing here cancels it.
        let keys = tokio::task::spawn_blocking(move || walk::sorted_keys(&objects_dir))
            .await
            .map_err(io::Error::other)??;

        let prefix = p.prefix.as_deref().unwrap_or_default();
        // An empty delimiter folds nothing: every key would "contain" it at
        // offset zero and the whole listing would collapse to one prefix.
        let delimiter = p.delimiter.as_deref().filter(|d| !d.is_empty());
        let start = start_bound(p);

        for key in &keys {
            if !key.starts_with(prefix) {
                continue;
            }
            let folded = delimiter.and_then(|d| common_prefix_of(key, prefix.len(), d));
            let item = folded.as_deref().unwrap_or(key);
            if start.is_some_and(|start| item <= start) {
                continue;
            }
            // Keys of one group are contiguous in sorted order, so the group
            // already counted is always the last one emitted. Checking before
            // reading the manifest is what keeps a folded listing from paying
            // for every key it hides.
            if folded.is_some()
                && out.common_prefixes.last().map(String::as_str) == folded.as_deref()
            {
                continue;
            }
            // Deleted between the walk and here, or a delete-marker head: not
            // an item, so it neither fills the budget nor opens a group.
            let Some(entry) = listable_version(&Self::object_dir(&bucket_dir, key)).await? else {
                continue;
            };
            // Found one more item than fits, which is what "truncated" means.
            if out.objects.len() + out.common_prefixes.len() == p.max_keys {
                out.is_truncated = true;
                break;
            }
            // The token is the last item emitted, whichever kind it was.
            if let Some(common) = folded {
                out.next_continuation_token = Some(common.clone());
                out.common_prefixes.push(common);
            } else {
                out.next_continuation_token = Some(key.clone());
                out.objects.push(object_info(key.clone(), &entry));
            }
        }
        if !out.is_truncated {
            out.next_continuation_token = None;
        }
        Ok(out)
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
    if looks_like_ipv4(name) {
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

/// Whether `name` has the shape of a dotted-decimal IPv4 address: exactly four
/// groups of ASCII digits separated by dots.
///
/// This is a shape test, not an address parse, which is what `MinIO`'s
/// `^(\d+\.){3}\d+$` does. `999.999.999.999`, `010.1.1.1` and `1.2.3.400` are
/// not addresses any parser would accept, but they still read as one to a
/// person or to a client building a virtual-host-style URL, and that is the
/// confusion the rule exists to prevent. Five groups (`1.2.3.4.5`) do not match
/// the shape and stay legal, as they do in `MinIO`.
fn looks_like_ipv4(name: &str) -> bool {
    let mut groups = name.split('.');
    let four_decimal_groups = (0..4).all(|_| groups.next().is_some_and(is_decimal_group));
    four_decimal_groups && groups.next().is_none()
}

/// A non-empty run of ASCII digits, the `\d+` of the shape above.
fn is_decimal_group(g: &str) -> bool {
    !g.is_empty() && g.bytes().all(|b| b.is_ascii_digit())
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

/// An empty manifest for a key that has never been written.
fn new_manifest() -> VersionManifest {
    VersionManifest {
        format: MANIFEST_FORMAT,
        versions: Vec::new(),
    }
}

/// `entry` as the API layer sees it.
fn object_info(key: String, entry: &VersionEntry) -> ObjectInfo {
    ObjectInfo {
        key,
        size: entry.size,
        etag: entry.etag.clone(),
        content_type: entry.content_type.clone(),
        mtime_epoch_ms: entry.mtime_epoch_ms,
        user_metadata: entry.user_metadata.clone(),
    }
}

/// The version a `GET` or `HEAD` of the object at `dir` resolves to.
///
/// A key with no manifest and a key whose latest version is a delete marker
/// are the same thing to a client: the object is not there.
///
/// # Errors
///
/// [`EngineError::NoSuchKey`] for either of those. A manifest that exists but
/// cannot be read propagates as [`EngineError::Io`], since presenting a
/// corrupt history as an absent object would invite the next write to
/// overwrite it.
async fn live_version(dir: &Path) -> Result<VersionEntry, EngineError> {
    let manifest = load_manifest(&dir.join(META_FILE))
        .await?
        .ok_or(EngineError::NoSuchKey)?;
    match manifest.latest() {
        Some(e) if !e.delete_marker => Ok(e.clone()),
        _ => Err(EngineError::NoSuchKey),
    }
}

/// The version a listing reports for the object at `dir`, or `None` if there is
/// nothing to report.
///
/// Unlike a `GET`, an absent key here is ordinary: the walk names keys it saw,
/// and one can be deleted before its manifest is read. A delete-marker head is
/// the same absence recorded in the manifest rather than by removing it.
async fn listable_version(dir: &Path) -> Result<Option<VersionEntry>, EngineError> {
    match live_version(dir).await {
        Ok(entry) => Ok(Some(entry)),
        Err(EngineError::NoSuchKey) => Ok(None),
        Err(e) => Err(e),
    }
}

/// The key a listing starts strictly after, or `None` to start at the
/// beginning.
///
/// Both bounds are exclusive and both may be given, in which case the later one
/// wins: each says "not before here", so the answer has to satisfy both.
fn start_bound(p: &ListParams) -> Option<&str> {
    match (p.continuation_token.as_deref(), p.start_after.as_deref()) {
        (Some(token), Some(after)) => Some(token.max(after)),
        (token, after) => token.or(after),
    }
}

/// The common prefix `key` folds into, or `None` if it holds no `delimiter`
/// after the listing prefix.
///
/// The result runs from the start of the key through the first delimiter past
/// `prefix_len`, delimiter included, which is the form S3 reports.
///
/// `prefix_len` is the length of a prefix the caller has already matched, so it
/// is a character boundary, and so is the end of a delimiter that was found by
/// searching from there.
fn common_prefix_of(key: &str, prefix_len: usize, delimiter: &str) -> Option<String> {
    let at = key[prefix_len..].find(delimiter)?;
    Some(key[..prefix_len + at + delimiter.len()].to_owned())
}

/// Resolve `range` against an object of `size` bytes, yielding the offset to
/// read from and the number of bytes to send.
///
/// The bounds in [`ByteRange`] are inclusive, as HTTP's are. A range that
/// starts past the last byte is unsatisfiable; one that merely *ends* past it
/// is clamped, which is what makes `bytes=0-` work on any object.
///
/// # Errors
///
/// [`EngineError::InvalidRange`], which the API layer reports as a 416.
fn resolve_range(range: Option<ByteRange>, size: u64) -> Result<(u64, u64), EngineError> {
    let Some(range) = range else {
        return Ok((0, size));
    };
    // No byte of an empty object can be in range, including `bytes=-0`, which
    // is unsatisfiable at any size.
    if size == 0 {
        return Err(EngineError::InvalidRange);
    }
    match range {
        ByteRange::FromTo(first, last) => {
            if first > last || first >= size {
                return Err(EngineError::InvalidRange);
            }
            let last = last.min(size - 1);
            Ok((first, last - first + 1))
        }
        ByteRange::From(first) => {
            if first >= size {
                return Err(EngineError::InvalidRange);
            }
            Ok((first, size - first))
        }
        ByteRange::Suffix(n) => {
            if n == 0 {
                return Err(EngineError::InvalidRange);
            }
            let n = n.min(size);
            Ok((size - n, n))
        }
    }
}

/// Stream `len` bytes from `file`, starting wherever it is positioned.
///
/// Chunked rather than read whole: an object is as large as a client cares to
/// make it, and a `GET` must not need it all in memory.
///
/// A file that runs out early is an error, not a short body. It means the data
/// file no longer matches the manifest that described it, and reporting the
/// bytes as complete would hand the client a silently truncated object.
fn file_stream(file: fs::File, len: u64) -> BoxByteStream {
    futures::stream::unfold((file, len), |(mut file, remaining)| async move {
        if remaining == 0 {
            return None;
        }
        let want = usize::try_from(remaining)
            .unwrap_or(READ_CHUNK)
            .min(READ_CHUNK);
        let mut buf = vec![0_u8; want];
        let mut filled = 0;
        while filled < want {
            match file.read(&mut buf[filled..]).await {
                Ok(0) => break,
                Ok(n) => filled += n,
                // `remaining` of 0 ends the stream after this item: a reader
                // that polls again gets `None`, not a retry of a failed read.
                Err(e) => return Some((Err(e), (file, 0))),
            }
        }
        if filled == 0 {
            let e = io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "object data file is shorter than its manifest records",
            );
            return Some((Err(e), (file, 0)));
        }
        buf.truncate(filled);
        Some((
            Ok(bytes::Bytes::from(buf)),
            (file, remaining - filled as u64),
        ))
    })
    .boxed()
}

/// Remove `path`, treating "it was not there" as success.
async fn remove_if_present(path: &Path) -> io::Result<()> {
    match fs::remove_file(path).await {
        Ok(()) => Ok(()),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(e) => Err(e),
    }
}

/// Remove `dir` and each parent up to but excluding `stop`, stopping at the
/// first one that is not empty.
///
/// Best effort. A directory that could not be pruned costs an empty directory
/// and keeps its bucket from being deleted until something removes it; failing
/// the `DELETE` over it would be worse, since the object itself is already
/// gone.
async fn prune_empty_dirs(dir: &Path, stop: &Path) {
    let mut current = dir;
    while current != stop && current.starts_with(stop) {
        if let Err(e) = fs::remove_dir(current).await {
            if e.kind() != io::ErrorKind::NotFound && e.kind() != io::ErrorKind::DirectoryNotEmpty {
                tracing::warn!(path = %current.display(), error = %e, "could not prune directory");
            }
            return;
        }
        let Some(parent) = current.parent() else {
            return;
        };
        current = parent;
    }
}

/// The deepest of `dir` and its parents up to `stop` that still exists.
///
/// After a delete that is the directory whose entries changed last, and so the
/// one whose removal has to be made durable.
async fn deepest_existing(dir: &Path, stop: &Path) -> Option<PathBuf> {
    let mut current = dir;
    loop {
        if fs::try_exists(current).await.unwrap_or(false) {
            return Some(current.to_path_buf());
        }
        if current == stop {
            return None;
        }
        current = current.parent()?;
        if !current.starts_with(stop) {
            return None;
        }
    }
}

/// `bytes` as lowercase hex, the form an S3 etag takes.
fn hex_lower(bytes: &[u8]) -> String {
    const HEX: [char; 16] = [
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f',
    ];
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize]);
        out.push(HEX[(b & 0x0F) as usize]);
    }
    out
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

    fn body(b: &'static [u8]) -> BoxByteStream {
        futures::stream::iter(vec![Ok(bytes::Bytes::from_static(b))]).boxed()
    }

    async fn read_all(mut s: BoxByteStream) -> Vec<u8> {
        let mut out = vec![];
        while let Some(c) = s.next().await {
            out.extend_from_slice(&c.unwrap());
        }
        out
    }

    #[tokio::test]
    async fn put_get_roundtrip() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let info = e
            .put_object(
                "buk",
                "dir/hello.txt",
                body(b"hello world"),
                PutOpts::default(),
            )
            .await
            .unwrap();
        assert_eq!(info.size, 11);
        assert_eq!(info.etag, "5eb63bbbe01eeed093cb22bb8f5acdc3"); // md5("hello world")
        let (gi, off, len, s) = e.get_object("buk", "dir/hello.txt", None).await.unwrap();
        assert_eq!((gi.size, off, len), (11, 0, 11));
        assert_eq!(read_all(s).await, b"hello world");
    }

    #[tokio::test]
    async fn ranged_get() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "k", body(b"0123456789"), PutOpts::default())
            .await
            .unwrap();
        let (_, off, len, s) = e
            .get_object("buk", "k", Some(ByteRange::FromTo(2, 5)))
            .await
            .unwrap();
        assert_eq!((off, len), (2, 4));
        assert_eq!(read_all(s).await, b"2345");
        let (_, off, len, s) = e
            .get_object("buk", "k", Some(ByteRange::Suffix(3)))
            .await
            .unwrap();
        assert_eq!((off, len), (7, 3));
        assert_eq!(read_all(s).await, b"789");
        assert!(matches!(
            e.get_object("buk", "k", Some(ByteRange::From(10))).await,
            Err(EngineError::InvalidRange)
        ));
    }

    #[tokio::test]
    async fn overwrite_replaces() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "k", body(b"one"), PutOpts::default())
            .await
            .unwrap();
        e.put_object("buk", "k", body(b"two!"), PutOpts::default())
            .await
            .unwrap();
        let (i, _, _, s) = e.get_object("buk", "k", None).await.unwrap();
        assert_eq!(i.size, 4);
        assert_eq!(read_all(s).await, b"two!");
    }

    #[tokio::test]
    async fn delete_and_missing() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "x/y", body(b"z"), PutOpts::default())
            .await
            .unwrap();
        e.delete_object("buk", "x/y").await.unwrap();
        assert!(matches!(
            e.head_object("buk", "x/y").await,
            Err(EngineError::NoSuchKey)
        ));
        e.delete_object("buk", "x/y").await.unwrap(); // idempotent
        assert!(matches!(
            e.get_object("nope", "k", None).await,
            Err(EngineError::NoSuchBucket)
        ));
    }

    /// A key component longer than the filesystem's name limit is a legal S3
    /// key that this engine cannot store, and it has to fail as a client error
    /// rather than as an I/O fault the API layer would report as a 500 and a
    /// client would retry forever.
    ///
    /// Every verb is checked, not just the `PUT`: they build the same path from
    /// the same key, and a `GET` answering `NoSuchKey` would tell a client the
    /// key is merely empty right after the `PUT` said it was unusable.
    #[tokio::test]
    async fn a_key_component_over_the_name_limit_is_a_client_error() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let key = "n".repeat(300);
        assert!(matches!(
            e.put_object("buk", &key, body(b"v"), PutOpts::default())
                .await,
            Err(EngineError::KeyTooLong)
        ));
        assert!(matches!(
            e.get_object("buk", &key, None).await,
            Err(EngineError::KeyTooLong)
        ));
        assert!(matches!(
            e.head_object("buk", &key).await,
            Err(EngineError::KeyTooLong)
        ));
        assert!(matches!(
            e.delete_object("buk", &key).await,
            Err(EngineError::KeyTooLong)
        ));
    }

    /// The other side of the limit: a component right at `NAME_MAX` is stored
    /// and read back, so the rejection above is the filesystem's boundary and
    /// not a rule aks3 applies too early.
    #[tokio::test]
    async fn a_key_component_at_the_name_limit_still_works() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let key = "n".repeat(255);
        e.put_object("buk", &key, body(b"v"), PutOpts::default())
            .await
            .unwrap();
        let (_, _, _, s) = e.get_object("buk", &key, None).await.unwrap();
        assert_eq!(read_all(s).await, b"v");
    }

    #[tokio::test]
    async fn metadata_persisted() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let mut opts = PutOpts {
            content_type: Some("text/plain".into()),
            ..Default::default()
        };
        opts.user_metadata.insert("owner".into(), "khan".into());
        e.put_object("buk", "k", body(b"v"), opts).await.unwrap();
        let h = e.head_object("buk", "k").await.unwrap();
        assert_eq!(h.content_type, "text/plain");
        assert_eq!(h.user_metadata.get("owner").unwrap(), "khan");
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
            "1.2.3.4.5", // five groups is not the IPv4 shape
            "1.2.3.4x",  // nor is a group that is not all digits
            "12.34.56",  // nor are three groups
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
            // IPv4-shaped without being addresses. The rule is about the shape:
            // MinIO refuses these too.
            "010.1.1.1",
            "999.999.999.999",
            "1.2.3.400",
            "0.0.0.0",
            "caf\u{e9}s",
        ] {
            assert!(!is_valid_bucket_name(bad), "{bad} should be invalid");
        }
    }

    #[test]
    fn ipv4_shape_is_four_groups_of_digits() {
        // Exactly what `^(\d+\.){3}\d+$` matches, addresses or not.
        for shaped in [
            "1.2.3.4",
            "192.168.1.1",
            "0.0.0.0",
            "010.1.1.1",
            "999.999.999.999",
            "1.2.3.400",
            "00000.0.0.00000",
        ] {
            assert!(looks_like_ipv4(shaped), "{shaped} has the IPv4 shape");
        }
        for unshaped in [
            "",
            "1.2.3",
            "1.2.3.4.5",
            "1.2.3.",
            ".1.2.3",
            "1..2.3",
            "1.2.3.4x",
            "1.2.3.-4",
            "a.b.c.d",
            "1234",
        ] {
            assert!(
                !looks_like_ipv4(unshaped),
                "{unshaped} does not have the IPv4 shape"
            );
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

    // --- object cases beyond the brief ---

    fn body_vec(v: Vec<u8>) -> BoxByteStream {
        futures::stream::iter(vec![Ok(bytes::Bytes::from(v))]).boxed()
    }

    fn body_chunks(chunks: &[&'static [u8]]) -> BoxByteStream {
        let items: Vec<_> = chunks
            .iter()
            .map(|c| Ok(bytes::Bytes::from_static(c)))
            .collect();
        futures::stream::iter(items).boxed()
    }

    /// The directory holding one key's files, for tests that reach past the
    /// API to check or corrupt the layout.
    fn obj_dir(e: &FsEngine, bucket: &str, key: &str) -> PathBuf {
        FsEngine::object_dir(&e.bucket_dir(bucket), key)
    }

    #[test]
    fn range_resolution() {
        use ByteRange::{From, FromTo, Suffix};

        // No range is the whole object, empty or not.
        assert_eq!(resolve_range(None, 10).unwrap(), (0, 10));
        assert_eq!(resolve_range(None, 0).unwrap(), (0, 0));

        // An end past the last byte clamps; that is what makes `bytes=0-` work.
        assert_eq!(resolve_range(Some(FromTo(0, 9)), 10).unwrap(), (0, 10));
        assert_eq!(resolve_range(Some(FromTo(0, 100)), 10).unwrap(), (0, 10));
        assert_eq!(resolve_range(Some(FromTo(9, 9)), 10).unwrap(), (9, 1));
        assert_eq!(resolve_range(Some(From(0)), 10).unwrap(), (0, 10));
        assert_eq!(resolve_range(Some(From(9)), 10).unwrap(), (9, 1));
        assert_eq!(resolve_range(Some(Suffix(1)), 10).unwrap(), (9, 1));
        assert_eq!(resolve_range(Some(Suffix(10)), 10).unwrap(), (0, 10));
        assert_eq!(resolve_range(Some(Suffix(99)), 10).unwrap(), (0, 10));

        // A start past the last byte is unsatisfiable, and so is a backwards
        // range or a zero-length suffix.
        for bad in [FromTo(10, 12), FromTo(5, 2), From(10), From(99), Suffix(0)] {
            assert!(
                matches!(resolve_range(Some(bad), 10), Err(EngineError::InvalidRange)),
                "{bad:?} should be unsatisfiable"
            );
        }
        // No byte of an empty object is in range, whatever was asked for.
        for bad in [FromTo(0, 0), From(0), Suffix(1), Suffix(0)] {
            assert!(
                matches!(resolve_range(Some(bad), 0), Err(EngineError::InvalidRange)),
                "{bad:?} should be unsatisfiable on an empty object"
            );
        }
    }

    #[test]
    fn hex_is_lowercase_and_zero_padded() {
        assert_eq!(hex_lower(&[0x00, 0x0f, 0xff, 0xa5]), "000fffa5");
        assert_eq!(hex_lower(&[]), "");
        // The etag of the empty object, which every S3 client knows by sight.
        assert_eq!(
            hex_lower(&Md5::digest(b"")),
            "d41d8cd98f00b204e9800998ecf8427e"
        );
    }

    #[tokio::test]
    async fn an_empty_object_roundtrips_and_has_no_satisfiable_range() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let info = e
            .put_object("buk", "empty", body(b""), PutOpts::default())
            .await
            .unwrap();
        assert_eq!(info.size, 0);
        assert_eq!(info.etag, "d41d8cd98f00b204e9800998ecf8427e");

        let (gi, off, len, s) = e.get_object("buk", "empty", None).await.unwrap();
        assert_eq!((gi.size, off, len), (0, 0, 0));
        assert!(read_all(s).await.is_empty());
        assert!(matches!(
            e.get_object("buk", "empty", Some(ByteRange::From(0))).await,
            Err(EngineError::InvalidRange)
        ));
    }

    #[tokio::test]
    async fn a_body_arriving_in_chunks_is_hashed_and_stored_whole() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let info = e
            .put_object(
                "buk",
                "k",
                body_chunks(&[b"hello", b"", b" ", b"world"]),
                PutOpts::default(),
            )
            .await
            .unwrap();
        assert_eq!(info.size, 11);
        assert_eq!(info.etag, "5eb63bbbe01eeed093cb22bb8f5acdc3");
        let (_, _, _, s) = e.get_object("buk", "k", None).await.unwrap();
        assert_eq!(read_all(s).await, b"hello world");
    }

    #[tokio::test]
    async fn an_object_larger_than_one_read_chunk_streams_whole() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        // Not a multiple of the chunk size, so the last read is a short one.
        let data: Vec<u8> = (0..READ_CHUNK * 2 + 1234)
            .map(|i| u8::try_from(i % 251).unwrap())
            .collect();
        let info = e
            .put_object("buk", "big", body_vec(data.clone()), PutOpts::default())
            .await
            .unwrap();
        assert_eq!(info.size, data.len() as u64);
        assert_eq!(info.etag, hex_lower(&Md5::digest(&data)));

        let (_, _, len, s) = e.get_object("buk", "big", None).await.unwrap();
        assert_eq!(len, data.len() as u64);
        assert_eq!(read_all(s).await, data);

        // A range that starts and ends inside different chunks.
        let (first, last) = (READ_CHUNK - 5, READ_CHUNK + 5);
        let (_, off, len, s) = e
            .get_object(
                "buk",
                "big",
                Some(ByteRange::FromTo(first as u64, last as u64)),
            )
            .await
            .unwrap();
        assert_eq!((off, len), (first as u64, 11));
        assert_eq!(read_all(s).await, data[first..=last]);
    }

    #[tokio::test]
    async fn put_defaults_the_content_type() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "silent", body(b"v"), PutOpts::default())
            .await
            .unwrap();
        assert_eq!(
            e.head_object("buk", "silent").await.unwrap().content_type,
            "application/octet-stream"
        );

        // An empty header is no more a content type than a missing one.
        let opts = PutOpts {
            content_type: Some(String::new()),
            ..Default::default()
        };
        e.put_object("buk", "blank", body(b"v"), opts)
            .await
            .unwrap();
        assert_eq!(
            e.head_object("buk", "blank").await.unwrap().content_type,
            "application/octet-stream"
        );
    }

    #[tokio::test]
    async fn object_calls_report_a_missing_or_invalid_bucket() {
        let (_d, e) = eng().await;
        assert!(matches!(
            e.put_object("absent", "k", body(b"v"), PutOpts::default())
                .await,
            Err(EngineError::NoSuchBucket)
        ));
        assert!(matches!(
            e.head_object("absent", "k").await,
            Err(EngineError::NoSuchBucket)
        ));
        assert!(matches!(
            e.delete_object("absent", "k").await,
            Err(EngineError::NoSuchBucket)
        ));

        // A name that would escape buckets/ never reaches the disk at all.
        for bad in ["../escape", "a/b", "..", ""] {
            assert!(matches!(
                e.put_object(bad, "k", body(b"v"), PutOpts::default()).await,
                Err(EngineError::InvalidBucketName)
            ));
            assert!(matches!(
                e.get_object(bad, "k", None).await,
                Err(EngineError::InvalidBucketName)
            ));
            assert!(matches!(
                e.head_object(bad, "k").await,
                Err(EngineError::InvalidBucketName)
            ));
            assert!(matches!(
                e.delete_object(bad, "k").await,
                Err(EngineError::InvalidBucketName)
            ));
        }
    }

    #[tokio::test]
    async fn an_unwritten_key_is_absent_and_deleting_it_succeeds() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        assert!(matches!(
            e.head_object("buk", "never").await,
            Err(EngineError::NoSuchKey)
        ));
        assert!(matches!(
            e.get_object("buk", "never", None).await,
            Err(EngineError::NoSuchKey)
        ));
        // S3 deletes are idempotent, so this is not an error.
        e.delete_object("buk", "never").await.unwrap();
    }

    #[tokio::test]
    async fn keys_the_filesystem_would_choke_on_roundtrip_and_stay_separate() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let keys = [
            "",
            "..",
            ".",
            "a/../b",
            "a//b",
            "trailing/",
            META_FILE,
            "__aks3.v.null.data",
            "dir/\u{1f600}/emoji",
            "back\\slash",
            "pct%25",
        ];
        for (i, k) in keys.iter().enumerate() {
            let value = format!("value-{i}");
            e.put_object("buk", k, body_vec(value.into_bytes()), PutOpts::default())
                .await
                .unwrap();
        }
        // Every key must have kept its own bytes: no two encoded to one path.
        for (i, k) in keys.iter().enumerate() {
            let (_, _, _, s) = e.get_object("buk", k, None).await.unwrap();
            assert_eq!(
                read_all(s).await,
                format!("value-{i}").into_bytes(),
                "{k:?}"
            );
        }
        for k in keys {
            e.delete_object("buk", k).await.unwrap();
            assert!(
                matches!(e.head_object("buk", k).await, Err(EngineError::NoSuchKey)),
                "{k:?}"
            );
        }
        // Every key pruned itself away, so the bucket is empty again.
        e.delete_bucket("buk").await.unwrap();
    }

    #[tokio::test]
    async fn the_on_disk_layout_is_what_the_module_documents() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "dir/hello.txt", body(b"hi"), PutOpts::default())
            .await
            .unwrap();

        let dir = e.bucket_dir("buk").join("objects/dir/hello.txt");
        assert!(dir.join("__aks3.meta.json").is_file());
        assert!(dir.join("__aks3.v.null.data").is_file());
        assert_eq!(
            std::fs::read(dir.join("__aks3.v.null.data")).unwrap(),
            b"hi"
        );
        // The staging directory is left as it was found.
        assert_eq!(std::fs::read_dir(e.tmp_dir()).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn delete_prunes_its_own_directories_and_leaves_siblings_alone() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "a/b/c", body(b"c"), PutOpts::default())
            .await
            .unwrap();
        e.put_object("buk", "a/b/d", body(b"d"), PutOpts::default())
            .await
            .unwrap();

        e.delete_object("buk", "a/b/c").await.unwrap();
        assert!(!obj_dir(&e, "buk", "a/b/c").exists());
        // The shared parent still holds a sibling, so pruning stopped there.
        assert!(obj_dir(&e, "buk", "a/b/d").is_dir());
        let (_, _, _, s) = e.get_object("buk", "a/b/d", None).await.unwrap();
        assert_eq!(read_all(s).await, b"d");
        assert!(matches!(
            e.delete_bucket("buk").await,
            Err(EngineError::BucketNotEmpty)
        ));

        e.delete_object("buk", "a/b/d").await.unwrap();
        let objects = e.bucket_dir("buk").join(OBJECTS_DIR);
        assert!(
            objects.is_dir(),
            "objects/ is pruned by nothing but its bucket"
        );
        assert!(!has_any_entry(&objects).await.unwrap());
        e.delete_bucket("buk").await.unwrap();
    }

    #[tokio::test]
    async fn objects_survive_reopening_the_engine() {
        let d = tempfile::tempdir().unwrap();
        let e = FsEngine::open(d.path()).await.unwrap();
        e.create_bucket("buk").await.unwrap();
        let put = e
            .put_object("buk", "dir/k", body(b"durable"), PutOpts::default())
            .await
            .unwrap();
        drop(e);

        let e = FsEngine::open(d.path()).await.unwrap();
        let head = e.head_object("buk", "dir/k").await.unwrap();
        assert_eq!((head.size, head.etag), (put.size, put.etag));
        assert_eq!(head.key, "dir/k");
        let (_, _, _, s) = e.get_object("buk", "dir/k", None).await.unwrap();
        assert_eq!(read_all(s).await, b"durable");
    }

    #[tokio::test]
    async fn a_corrupt_manifest_is_an_error_not_an_absent_object() {
        // Reporting NoSuchKey here would present a damaged history as an empty
        // one and invite the next PUT to overwrite it.
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "k", body(b"v"), PutOpts::default())
            .await
            .unwrap();
        std::fs::write(obj_dir(&e, "buk", "k").join(META_FILE), b"{ not json").unwrap();

        for err in [
            e.head_object("buk", "k").await.map(|_| ()),
            e.get_object("buk", "k", None).await.map(|_| ()),
        ] {
            let Err(EngineError::Io(err)) = err else {
                panic!("expected the parse failure to surface");
            };
            assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        }
        // A key whose manifest cannot be parsed must still be deletable.
        e.delete_object("buk", "k").await.unwrap();
        assert!(matches!(
            e.head_object("buk", "k").await,
            Err(EngineError::NoSuchKey)
        ));
    }

    #[tokio::test]
    async fn a_delete_marker_at_the_head_reads_as_absent() {
        // Phase 0 never writes one, but the manifest format carries them and
        // reads must already honour them.
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "k", body(b"v"), PutOpts::default())
            .await
            .unwrap();

        let path = obj_dir(&e, "buk", "k").join(META_FILE);
        let mut manifest = load_manifest(&path).await.unwrap().unwrap();
        manifest.versions[0].delete_marker = true;
        store_manifest(&e.tmp_dir(), &path, &manifest)
            .await
            .unwrap();

        assert!(matches!(
            e.head_object("buk", "k").await,
            Err(EngineError::NoSuchKey)
        ));
        assert!(matches!(
            e.get_object("buk", "k", None).await,
            Err(EngineError::NoSuchKey)
        ));
    }

    #[tokio::test]
    async fn an_empty_manifest_reads_as_absent() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let dir = obj_dir(&e, "buk", "k");
        store_manifest(&e.tmp_dir(), &dir.join(META_FILE), &new_manifest())
            .await
            .unwrap();
        assert!(matches!(
            e.head_object("buk", "k").await,
            Err(EngineError::NoSuchKey)
        ));
    }

    #[tokio::test]
    async fn a_delete_landing_mid_read_reads_as_absent_not_as_a_broken_store() {
        // delete_object removes the manifest first and the data second, so a
        // GET that read the manifest just before it opens a path that is
        // already gone. That is the object being deleted, and S3 answers the
        // race with a 404, never an internal error.
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "k", body(b"v"), PutOpts::default())
            .await
            .unwrap();
        std::fs::remove_file(obj_dir(&e, "buk", "k").join("__aks3.v.null.data")).unwrap();

        assert!(matches!(
            e.get_object("buk", "k", None).await,
            Err(EngineError::NoSuchKey)
        ));
        // HEAD never opens the data file, so it still answers from the manifest
        // it read, which is the same "whichever state it observed" promise.
        assert_eq!(e.head_object("buk", "k").await.unwrap().size, 1);
    }

    #[tokio::test]
    async fn a_data_file_shorter_than_its_manifest_fails_the_read() {
        // Handing the client a short body under a full Content-Length would be
        // a silently truncated object.
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "k", body(b"0123456789"), PutOpts::default())
            .await
            .unwrap();
        std::fs::write(obj_dir(&e, "buk", "k").join("__aks3.v.null.data"), b"012").unwrap();

        let (_, _, len, mut s) = e.get_object("buk", "k", None).await.unwrap();
        assert_eq!(len, 10);
        assert_eq!(s.next().await.unwrap().unwrap(), &b"012"[..]);
        let err = s.next().await.unwrap().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::UnexpectedEof);
        assert!(
            s.next().await.is_none(),
            "the stream retried after an error"
        );
    }

    #[tokio::test]
    async fn a_failing_body_publishes_nothing_and_leaves_no_temp_file() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let body: BoxByteStream = futures::stream::iter(vec![
            Ok(bytes::Bytes::from_static(b"partial")),
            Err(io::Error::other("the client hung up")),
        ])
        .boxed();

        assert!(matches!(
            e.put_object("buk", "k", body, PutOpts::default()).await,
            Err(EngineError::Io(_))
        ));
        assert!(matches!(
            e.head_object("buk", "k").await,
            Err(EngineError::NoSuchKey)
        ));
        assert!(!obj_dir(&e, "buk", "k").exists());
        assert_eq!(std::fs::read_dir(e.tmp_dir()).unwrap().count(), 0);
        // Nothing was published, so the bucket is still empty.
        e.delete_bucket("buk").await.unwrap();
    }

    #[tokio::test]
    async fn overwriting_does_not_grow_the_version_history() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        for body_bytes in [&b"one"[..], b"two", b"three"] {
            e.put_object(
                "buk",
                "k",
                body_vec(body_bytes.to_vec()),
                PutOpts::default(),
            )
            .await
            .unwrap();
        }
        let manifest = load_manifest(&obj_dir(&e, "buk", "k").join(META_FILE))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(manifest.versions.len(), 1);
        assert_eq!(manifest.versions[0].version_id, "null");
        // One data file, not one per overwrite.
        let files = std::fs::read_dir(obj_dir(&e, "buk", "k")).unwrap().count();
        assert_eq!(files, 2, "expected just the manifest and one data file");
    }

    #[tokio::test]
    async fn a_put_after_delete_bucket_finds_no_bucket_and_recreates_cleanly() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        e.delete_bucket("buk").await.unwrap();

        // The bucket is gone, so the PUT must not re-create its directory tree
        // as a side effect of committing into it.
        assert!(matches!(
            e.put_object("buk", "k", body(b"v"), PutOpts::default())
                .await,
            Err(EngineError::NoSuchBucket)
        ));
        assert!(!e.bucket_exists("buk").await.unwrap());
        assert!(e.list_buckets().await.unwrap().is_empty());

        e.create_bucket("buk").await.unwrap();
        e.put_object("buk", "k", body(b"v"), PutOpts::default())
            .await
            .unwrap();
        let (_, _, _, s) = e.get_object("buk", "k", None).await.unwrap();
        assert_eq!(read_all(s).await, b"v");
        // The recreated bucket has its own meta, not a leftover tree.
        assert!(e.bucket_dir("buk").join(BUCKET_META_FILE).is_file());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_delete_bucket_racing_puts_never_destroys_a_stored_object() {
        // The check-then-remove in delete_bucket is two steps; the bucket lock
        // is what stops a PUT from landing between them and being deleted by a
        // call that had just found the bucket empty.
        const PUTS: usize = 8;

        let d = tempfile::tempdir().unwrap();
        let e = Arc::new(FsEngine::open(d.path()).await.unwrap());

        // The delete is started at a different point in the run of PUTs each
        // round, so the two calls meet in more than one order.
        for round in 0..=PUTS {
            let bucket = format!("race-{round}");
            e.create_bucket(&bucket).await.unwrap();

            let mut puts = Vec::new();
            let mut deleted = None;
            for i in 0..=PUTS {
                if i == round {
                    let (e, bucket) = (Arc::clone(&e), bucket.clone());
                    deleted = Some(tokio::spawn(async move { e.delete_bucket(&bucket).await }));
                }
                if i == PUTS {
                    break;
                }
                let (e, bucket) = (Arc::clone(&e), bucket.clone());
                puts.push(tokio::spawn(async move {
                    e.put_object(&bucket, &format!("k{i}"), body(b"v"), PutOpts::default())
                        .await
                        .map(|_| ())
                }));
            }

            let puts: Vec<_> = futures::future::join_all(puts)
                .await
                .into_iter()
                .map(|r| r.expect("put task panicked"))
                .collect();
            let deleted = deleted.unwrap().await.expect("delete task panicked");

            let stored = puts.iter().filter(|r| r.is_ok()).count();
            for (i, put) in puts.iter().enumerate() {
                match put {
                    // Anything reported as stored must be readable afterwards.
                    Ok(()) => {
                        e.head_object(&bucket, &format!("k{i}")).await.unwrap();
                    }
                    // The only legal failure is the bucket having gone first.
                    Err(EngineError::NoSuchBucket) => {}
                    Err(other) => panic!("round {round}: unexpected put failure: {other:?}"),
                }
            }
            match deleted {
                // The bucket only goes away empty, so nothing was destroyed.
                Ok(()) => {
                    assert_eq!(
                        stored, 0,
                        "round {round}: {stored} stored objects destroyed"
                    );
                    assert!(!e.bucket_exists(&bucket).await.unwrap());
                }
                // Otherwise the objects held the bucket open, which is the point.
                Err(EngineError::BucketNotEmpty) => {
                    assert!(stored > 0, "round {round}: bucket held open by nothing");
                    assert!(e.bucket_exists(&bucket).await.unwrap());
                }
                Err(other) => panic!("round {round}: unexpected delete failure: {other:?}"),
            }
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_puts_to_one_key_leave_exactly_one_whole_version() {
        let d = tempfile::tempdir().unwrap();
        let e = Arc::new(FsEngine::open(d.path()).await.unwrap());
        e.create_bucket("race").await.unwrap();

        // Distinct lengths, so a manifest describing one body and a data file
        // holding another cannot pass the check below.
        let bodies: Vec<Vec<u8>> = (1..=8_u8)
            .map(|n| vec![b'a' + n; usize::from(n) * 3])
            .collect();
        let mut puts = Vec::new();
        for want in bodies.clone() {
            let e = Arc::clone(&e);
            puts.push(tokio::spawn(async move {
                e.put_object("race", "one-key", body_vec(want), PutOpts::default())
                    .await
            }));
        }
        for put in futures::future::join_all(puts).await {
            put.expect("put task panicked").expect("put failed");
        }

        let (info, _, len, s) = e.get_object("race", "one-key", None).await.unwrap();
        let got = read_all(s).await;
        assert!(
            bodies.contains(&got),
            "the object is not any body that was put"
        );
        assert_eq!(info.size, got.len() as u64);
        assert_eq!(len, got.len() as u64);
        assert_eq!(info.etag, hex_lower(&Md5::digest(&got)));
    }

    #[tokio::test]
    async fn the_lock_tables_do_not_outlive_the_work() {
        // One entry per key ever written would be a leak the size of the
        // keyspace; entries go away once nothing holds them.
        let (_d, e) = eng().await;
        e.create_bucket("locks").await.unwrap();
        for i in 0..32 {
            let key = format!("k{i}");
            e.put_object("locks", &key, body(b"v"), PutOpts::default())
                .await
                .unwrap();
            e.head_object("locks", &key).await.unwrap();
            e.delete_object("locks", &key).await.unwrap();
        }
        e.delete_bucket("locks").await.unwrap();

        assert!(
            e.key_locks.is_empty(),
            "{} key locks left",
            e.key_locks.len()
        );
        assert!(
            e.bucket_locks.is_empty(),
            "{} bucket locks left",
            e.bucket_locks.len()
        );
    }

    // --- listing ---

    async fn seed(e: &FsEngine) {
        e.create_bucket("buk").await.unwrap();
        for k in ["a.txt", "dir/one", "dir/two", "dir/sub/deep", "z.txt"] {
            e.put_object("buk", k, body(b"x"), PutOpts::default())
                .await
                .unwrap();
        }
    }

    fn listed(r: &ListResult) -> Vec<String> {
        r.objects.iter().map(|o| o.key.clone()).collect()
    }

    #[tokio::test]
    async fn plain_listing_sorted() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let r = e
            .list_objects_v2(
                "buk",
                &ListParams {
                    max_keys: 1000,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            listed(&r),
            vec!["a.txt", "dir/one", "dir/sub/deep", "dir/two", "z.txt"]
        );
        assert!(!r.is_truncated);
    }

    #[tokio::test]
    async fn delimiter_folds_common_prefixes() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let r = e
            .list_objects_v2(
                "buk",
                &ListParams {
                    delimiter: Some("/".into()),
                    max_keys: 1000,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(listed(&r), vec!["a.txt", "z.txt"]);
        assert_eq!(r.common_prefixes, vec!["dir/"]);
    }

    #[tokio::test]
    async fn prefix_and_delimiter() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let r = e
            .list_objects_v2(
                "buk",
                &ListParams {
                    prefix: Some("dir/".into()),
                    delimiter: Some("/".into()),
                    max_keys: 1000,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(listed(&r), vec!["dir/one", "dir/two"]);
        assert_eq!(r.common_prefixes, vec!["dir/sub/"]);
    }

    #[tokio::test]
    async fn pagination_with_continuation() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let p1 = e
            .list_objects_v2(
                "buk",
                &ListParams {
                    max_keys: 2,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(p1.objects.len(), 2);
        assert!(p1.is_truncated);
        let p2 = e
            .list_objects_v2(
                "buk",
                &ListParams {
                    max_keys: 2,
                    continuation_token: p1.next_continuation_token.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(p2.objects[0].key, "dir/sub/deep");
        let p3 = e
            .list_objects_v2(
                "buk",
                &ListParams {
                    max_keys: 100,
                    continuation_token: p2.next_continuation_token.clone(),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(p3.objects.len(), 1);
        assert!(!p3.is_truncated);
        assert!(p3.next_continuation_token.is_none());
    }

    // --- listing cases beyond the brief ---

    /// A listing of `bucket` with the defaults and a budget nothing here
    /// reaches.
    async fn list(e: &FsEngine, bucket: &str, p: ListParams) -> ListResult {
        e.list_objects_v2(
            bucket,
            &ListParams {
                max_keys: 1000,
                ..p
            },
        )
        .await
        .unwrap()
    }

    /// Make the head of `key`'s manifest a delete marker. Phase 0 never writes
    /// one, but a listing has to skip it before versioning starts to.
    async fn mark_deleted(e: &FsEngine, bucket: &str, key: &str) {
        let path = obj_dir(e, bucket, key).join(META_FILE);
        let mut m = load_manifest(&path).await.unwrap().unwrap();
        m.versions[0].delete_marker = true;
        store_manifest(&e.tmp_dir(), &path, &m).await.unwrap();
    }

    #[tokio::test]
    async fn listing_reports_the_bucket_it_was_asked_for() {
        let (_d, e) = eng().await;
        seed(&e).await;
        assert!(matches!(
            e.list_objects_v2("other", &ListParams::default()).await,
            Err(EngineError::NoSuchBucket)
        ));
        assert!(matches!(
            e.list_objects_v2("../escape", &ListParams::default()).await,
            Err(EngineError::InvalidBucketName)
        ));
    }

    #[tokio::test]
    async fn a_bucket_with_no_object_tree_lists_empty() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        // Nothing has been written, so `objects/` does not exist yet.
        assert!(!e.bucket_dir("buk").join("objects").exists());

        let r = list(&e, "buk", ListParams::default()).await;
        assert!(r.objects.is_empty());
        assert!(r.common_prefixes.is_empty());
        assert!(!r.is_truncated);
    }

    #[tokio::test]
    async fn max_keys_zero_is_an_empty_page_that_is_not_truncated() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let r = e
            .list_objects_v2("buk", &ListParams::default())
            .await
            .unwrap();
        assert!(r.objects.is_empty());
        assert!(!r.is_truncated);
        assert!(r.next_continuation_token.is_none());
    }

    #[tokio::test]
    async fn a_page_that_exactly_holds_the_listing_is_not_truncated() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let r = e
            .list_objects_v2(
                "buk",
                &ListParams {
                    max_keys: 5,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(r.objects.len(), 5);
        assert!(!r.is_truncated);
        assert!(r.next_continuation_token.is_none());
    }

    #[tokio::test]
    async fn paging_a_folded_listing_advances_past_the_common_prefix() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let page = |token: Option<String>| async {
            e.list_objects_v2(
                "buk",
                &ListParams {
                    delimiter: Some("/".into()),
                    continuation_token: token,
                    max_keys: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        };

        let p1 = page(None).await;
        assert_eq!(listed(&p1), vec!["a.txt"]);
        assert!(p1.is_truncated);

        // The token names a common prefix, and every key under it is above it.
        // Resuming has to skip the whole group rather than fold it again.
        let p2 = page(p1.next_continuation_token.clone()).await;
        assert!(p2.objects.is_empty());
        assert_eq!(p2.common_prefixes, vec!["dir/"]);
        assert_eq!(p2.next_continuation_token.as_deref(), Some("dir/"));
        assert!(p2.is_truncated);

        let p3 = page(p2.next_continuation_token.clone()).await;
        assert_eq!(listed(&p3), vec!["z.txt"]);
        assert!(!p3.is_truncated);
    }

    #[tokio::test]
    async fn the_later_of_the_token_and_start_after_wins() {
        let (_d, e) = eng().await;
        seed(&e).await;
        for (token, after) in [("a.txt", "dir/two"), ("dir/two", "a.txt")] {
            let r = list(
                &e,
                "buk",
                ListParams {
                    continuation_token: Some(token.into()),
                    start_after: Some(after.into()),
                    ..Default::default()
                },
            )
            .await;
            assert_eq!(listed(&r), vec!["z.txt"], "{token} / {after}");
        }
    }

    #[tokio::test]
    async fn start_after_alone_is_exclusive() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let r = list(
            &e,
            "buk",
            ListParams {
                start_after: Some("dir/one".into()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(listed(&r), vec!["dir/sub/deep", "dir/two", "z.txt"]);
    }

    #[tokio::test]
    async fn a_prefix_can_cut_a_key_component_in_half() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let r = list(
            &e,
            "buk",
            ListParams {
                prefix: Some("di".into()),
                delimiter: Some("/".into()),
                ..Default::default()
            },
        )
        .await;
        // The fold starts looking for the delimiter after the prefix, so the
        // group is the whole first component, not `di`.
        assert!(r.objects.is_empty());
        assert_eq!(r.common_prefixes, vec!["dir/"]);
    }

    #[tokio::test]
    async fn a_delimiter_need_not_be_a_slash() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        for k in ["aXXb", "aXXc", "plain"] {
            e.put_object("buk", k, body(b"x"), PutOpts::default())
                .await
                .unwrap();
        }
        let r = list(
            &e,
            "buk",
            ListParams {
                delimiter: Some("XX".into()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(listed(&r), vec!["plain"]);
        assert_eq!(r.common_prefixes, vec!["aXX"]);
    }

    #[tokio::test]
    async fn an_empty_delimiter_folds_nothing() {
        let (_d, e) = eng().await;
        seed(&e).await;
        let r = list(
            &e,
            "buk",
            ListParams {
                delimiter: Some(String::new()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(r.objects.len(), 5);
        assert!(r.common_prefixes.is_empty());
    }

    #[tokio::test]
    async fn a_deleted_key_leaves_the_listing() {
        let (_d, e) = eng().await;
        seed(&e).await;
        e.delete_object("buk", "dir/sub/deep").await.unwrap();
        e.delete_object("buk", "a.txt").await.unwrap();

        let r = list(&e, "buk", ListParams::default()).await;
        assert_eq!(listed(&r), vec!["dir/one", "dir/two", "z.txt"]);
    }

    #[tokio::test]
    async fn delete_marker_heads_are_skipped() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        for k in ["gone/a", "gone/b", "live/x", "z"] {
            e.put_object("buk", k, body(b"x"), PutOpts::default())
                .await
                .unwrap();
        }
        for k in ["gone/a", "gone/b"] {
            mark_deleted(&e, "buk", k).await;
        }

        let flat = list(&e, "buk", ListParams::default()).await;
        assert_eq!(listed(&flat), vec!["live/x", "z"]);

        // A group whose every key is a tombstone is not a common prefix, and
        // neither the keys nor the group it would have opened take a slot.
        let folded = e
            .list_objects_v2(
                "buk",
                &ListParams {
                    delimiter: Some("/".into()),
                    max_keys: 1,
                    ..Default::default()
                },
            )
            .await
            .unwrap();
        assert!(folded.objects.is_empty());
        assert_eq!(folded.common_prefixes, vec!["live/"]);
        assert!(folded.is_truncated);
    }

    #[tokio::test]
    async fn a_listing_entry_carries_the_whole_object() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let mut opts = PutOpts {
            content_type: Some("text/plain".into()),
            ..Default::default()
        };
        opts.user_metadata.insert("owner".into(), "khan".into());
        let put = e
            .put_object("buk", "k", body(b"hello"), opts)
            .await
            .unwrap();

        let r = list(&e, "buk", ListParams::default()).await;
        let [got] = &r.objects[..] else {
            panic!("expected one object, got {:?}", r.objects);
        };
        assert_eq!(got.key, "k");
        assert_eq!(got.size, 5);
        assert_eq!(got.etag, put.etag);
        assert_eq!(got.content_type, "text/plain");
        assert_eq!(got.user_metadata.get("owner").unwrap(), "khan");
        assert_eq!(got.mtime_epoch_ms, put.mtime_epoch_ms);
    }

    #[tokio::test]
    async fn keys_the_filesystem_would_choke_on_come_back_intact_and_in_order() {
        let (_d, e) = eng().await;
        e.create_bucket("buk").await.unwrap();
        let keys = [
            "",
            "..",
            META_FILE,
            "a/",
            "a/b",
            "caf\u{e9}",
            "pct%",
            "tab\tx",
            "\u{1f600}/emoji",
        ];
        for k in keys {
            e.put_object("buk", k, body(b"x"), PutOpts::default())
                .await
                .unwrap();
        }
        let mut want: Vec<String> = keys.iter().map(|k| (*k).to_owned()).collect();
        want.sort_unstable();

        let r = list(&e, "buk", ListParams::default()).await;
        assert_eq!(listed(&r), want);
    }

    #[test]
    fn the_start_bound_is_the_later_of_the_two() {
        let params = |token: Option<&str>, after: Option<&str>| ListParams {
            continuation_token: token.map(str::to_owned),
            start_after: after.map(str::to_owned),
            ..Default::default()
        };
        assert_eq!(start_bound(&params(None, None)), None);
        assert_eq!(start_bound(&params(Some("a"), None)), Some("a"));
        assert_eq!(start_bound(&params(None, Some("b"))), Some("b"));
        assert_eq!(start_bound(&params(Some("a"), Some("b"))), Some("b"));
        assert_eq!(start_bound(&params(Some("b"), Some("a"))), Some("b"));
    }

    #[test]
    fn folding_takes_the_first_delimiter_past_the_prefix() {
        assert_eq!(common_prefix_of("a/b/c", 0, "/").unwrap(), "a/");
        assert_eq!(common_prefix_of("a/b/c", 2, "/").unwrap(), "a/b/");
        assert_eq!(common_prefix_of("a/b/c", 4, "/"), None);
        assert_eq!(common_prefix_of("aXXbXXc", 0, "XX").unwrap(), "aXX");
        // A key ending in the delimiter folds into itself.
        assert_eq!(common_prefix_of("a/", 0, "/").unwrap(), "a/");
        // Multi-byte characters either side of the delimiter.
        assert_eq!(
            common_prefix_of("\u{1f600}/x", 0, "/").unwrap(),
            "\u{1f600}/"
        );
    }
}
