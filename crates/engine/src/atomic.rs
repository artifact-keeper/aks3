// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Crash-safe file writes: stage, fsync, rename, fsync the parent directory.
//!
//! Nothing outside this module writes to a final path directly. A writer stages
//! its bytes in a temp file ([`StagedFile`]) and publishes them with a single
//! [`StagedFile::commit`], so a reader (or a crash) never observes a half-written
//! object or manifest: the destination either does not exist or holds the whole
//! value.
//!
//! # Durability
//!
//! `commit` performs, in order:
//!
//! 1. `fsync` on the temp file, so its bytes are on disk before anything points
//!    at them. A rename is atomic with respect to other readers, but not with
//!    respect to power loss: without this step the directory entry can reach disk
//!    ahead of the data and the committed name can come back empty or truncated.
//! 2. `create_dir_all` on the destination's parent.
//! 3. `rename` onto the destination, replacing any previous file.
//! 4. `fsync` on the destination's *parent directory*, so the new directory entry
//!    itself is durable. Without it the rename can be lost even though the data
//!    survived.
//!
//! Steps 1 and 4 are correctness requirements, not tuning knobs.
//!
//! Directory `fsync` is a Unix behaviour; this module assumes a Unix-like
//! platform, as the rest of the storage engine does.
//!
//! # Cleanup
//!
//! A `StagedFile` dropped without `commit` deletes its temp file on a best-effort
//! basis, so an aborted upload or an error path does not leak. Only a crash
//! between `create` and `commit` leaves a stray temp file behind, which is why
//! staging happens in a dedicated temp directory that startup can sweep.

use std::io;
use std::path::{Path, PathBuf};

use serde::Serialize;
use tokio::fs::{self, File, OpenOptions};
use tokio::io::AsyncWriteExt;

/// A file being written to a temp directory, to be published with [`Self::commit`].
///
/// Dropping without committing removes the temp file.
#[derive(Debug)]
pub struct StagedFile {
    file: File,
    path: PathBuf,
    /// Set once the rename succeeds, so [`Drop`] does not delete the file we
    /// just published under its final name.
    committed: bool,
}

impl StagedFile {
    /// Create a uniquely named temp file inside `tmp_dir`.
    ///
    /// `tmp_dir` must already exist. The name is a fresh v4 UUID, so concurrent
    /// writers never collide.
    ///
    /// # Errors
    ///
    /// Propagates the underlying create error (missing or unwritable `tmp_dir`).
    pub async fn create(tmp_dir: &Path) -> io::Result<Self> {
        let path = tmp_dir.join(format!("{}.tmp", uuid::Uuid::new_v4()));
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .await?;
        Ok(Self {
            file,
            path,
            committed: false,
        })
    }

    /// Append `buf` to the staged file.
    ///
    /// # Errors
    ///
    /// Propagates the underlying write error.
    pub async fn write_all(&mut self, buf: &[u8]) -> io::Result<()> {
        self.file.write_all(buf).await
    }

    /// Path of the temp file. Valid until [`Self::commit`] moves it.
    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Publish the staged bytes at `dest`, replacing anything already there.
    ///
    /// Creates the destination's parent directories if needed. See the module
    /// docs for the fsync ordering this relies on.
    ///
    /// # Errors
    ///
    /// Propagates flush, fsync, `create_dir_all` and rename errors. If the error
    /// comes before the rename the temp file is cleaned up; after a successful
    /// rename the data is published even if the final directory fsync reports an
    /// error, and the temp file is gone either way.
    pub async fn commit(mut self, dest: &Path) -> io::Result<()> {
        self.file.flush().await?;
        self.file.sync_all().await?;

        let parent = parent_dir(dest);
        fs::create_dir_all(parent).await?;
        fs::rename(&self.path, dest).await?;
        // The temp name no longer exists; deleting it in Drop would either fail
        // harmlessly or, worse, race a re-created name. Nothing below may unset
        // this: the object is published from here on.
        self.committed = true;

        File::open(parent).await?.sync_all().await
    }
}

impl Drop for StagedFile {
    fn drop(&mut self) {
        if !self.committed {
            // Best effort: Drop cannot be async and cannot report failure. A
            // leftover temp file is swept by startup, not by a retry here.
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

/// Serialize `value` as JSON and write it atomically to `dest`.
///
/// Staging happens in `tmp_dir`; see [`StagedFile`] for the guarantees.
///
/// # Errors
///
/// A serialization failure surfaces as an [`io::Error`] with
/// [`io::ErrorKind::Other`]; everything else propagates from the staged write.
pub async fn write_json_atomic(
    tmp_dir: &Path,
    dest: &Path,
    value: &impl Serialize,
) -> io::Result<()> {
    let bytes = serde_json::to_vec(value).map_err(io::Error::other)?;
    let mut staged = StagedFile::create(tmp_dir).await?;
    staged.write_all(&bytes).await?;
    staged.commit(dest).await
}

/// Directory that will hold `dest`.
///
/// `Path::parent` yields an empty path for a bare file name, which names no
/// directory that can be created or opened; the current directory is what that
/// case means.
fn parent_dir(dest: &Path) -> &Path {
    match dest.parent() {
        Some(p) if !p.as_os_str().is_empty() => p,
        _ => Path::new("."),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn commit_moves_file_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("tmp");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let dest = dir.path().join("deep/nested/out.bin");
        let mut f = StagedFile::create(&tmp).await.unwrap();
        f.write_all(b"hello").await.unwrap();
        f.commit(&dest).await.unwrap();
        assert_eq!(tokio::fs::read(&dest).await.unwrap(), b"hello");
        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn drop_without_commit_cleans_up() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("tmp");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        {
            let mut f = StagedFile::create(&tmp).await.unwrap();
            f.write_all(b"junk").await.unwrap();
        }
        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn json_atomic_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("tmp");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let dest = dir.path().join("m.json");
        write_json_atomic(&tmp, &dest, &vec![1, 2, 3])
            .await
            .unwrap();
        let got: Vec<u32> = serde_json::from_slice(&tokio::fs::read(&dest).await.unwrap()).unwrap();
        assert_eq!(got, vec![1, 2, 3]);
    }

    // --- additional cases beyond the brief ---

    /// Fresh temp dir under a tempdir, returned with the guard that owns it.
    async fn tmp_dir() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("tmp");
        fs::create_dir_all(&tmp).await.unwrap();
        (dir, tmp)
    }

    #[tokio::test]
    async fn staged_path_is_a_fresh_file_in_tmp_dir() {
        let (_guard, tmp) = tmp_dir().await;
        let a = StagedFile::create(&tmp).await.unwrap();
        let b = StagedFile::create(&tmp).await.unwrap();
        assert_ne!(a.path(), b.path(), "two staged files share a name");
        for f in [&a, &b] {
            assert_eq!(f.path().parent().unwrap(), tmp);
            assert!(f.path().is_file(), "{:?} was not created", f.path());
        }
        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 2);
    }

    #[tokio::test]
    async fn writes_concatenate_in_order() {
        let (guard, tmp) = tmp_dir().await;
        let dest = guard.path().join("out.bin");
        let mut f = StagedFile::create(&tmp).await.unwrap();
        f.write_all(b"chunk-1;").await.unwrap();
        f.write_all(b"").await.unwrap();
        f.write_all(b"chunk-2").await.unwrap();
        f.commit(&dest).await.unwrap();
        assert_eq!(fs::read(&dest).await.unwrap(), b"chunk-1;chunk-2");
    }

    #[tokio::test]
    async fn commit_without_writing_yields_an_empty_file() {
        let (guard, tmp) = tmp_dir().await;
        let dest = guard.path().join("empty.bin");
        StagedFile::create(&tmp)
            .await
            .unwrap()
            .commit(&dest)
            .await
            .unwrap();
        assert_eq!(fs::read(&dest).await.unwrap(), b"");
        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn commit_replaces_an_existing_destination() {
        let (guard, tmp) = tmp_dir().await;
        let dest = guard.path().join("out.bin");
        fs::write(&dest, b"old contents, longer").await.unwrap();

        let mut f = StagedFile::create(&tmp).await.unwrap();
        f.write_all(b"new").await.unwrap();
        f.commit(&dest).await.unwrap();
        assert_eq!(fs::read(&dest).await.unwrap(), b"new");
    }

    #[test]
    fn parent_dir_maps_an_empty_parent_to_the_current_directory() {
        // `Path::parent` of a bare "x.bin" is "", which names no directory that
        // create_dir_all or File::open can act on.
        assert_eq!(parent_dir(Path::new("x.bin")), Path::new("."));
        assert_eq!(parent_dir(Path::new("/")), Path::new("."));
        assert_eq!(parent_dir(Path::new("a/x.bin")), Path::new("a"));
    }

    #[tokio::test]
    async fn failed_commit_reports_the_error_and_leaves_no_temp_file() {
        let (guard, tmp) = tmp_dir().await;
        // A regular file where the destination's parent directory would go, so
        // create_dir_all fails and the rename never happens.
        let blocker = guard.path().join("blocker");
        fs::write(&blocker, b"not a directory").await.unwrap();
        let dest = blocker.join("nested/out.bin");

        let mut f = StagedFile::create(&tmp).await.unwrap();
        f.write_all(b"doomed").await.unwrap();
        assert!(f.commit(&dest).await.is_err());

        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
        assert_eq!(fs::read(&blocker).await.unwrap(), b"not a directory");
    }

    #[tokio::test]
    async fn create_fails_when_tmp_dir_is_missing() {
        let guard = tempfile::tempdir().unwrap();
        let err = StagedFile::create(&guard.path().join("absent"))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn json_atomic_replaces_and_creates_parents() {
        let (guard, tmp) = tmp_dir().await;
        let dest = guard.path().join("deep/nested/m.json");
        write_json_atomic(&tmp, &dest, &"first").await.unwrap();
        write_json_atomic(&tmp, &dest, &[1_u32, 2]).await.unwrap();

        let got: Vec<u32> = serde_json::from_slice(&fs::read(&dest).await.unwrap()).unwrap();
        assert_eq!(got, vec![1, 2]);
        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn json_atomic_surfaces_serialization_failure() {
        use std::collections::BTreeMap;

        let (guard, tmp) = tmp_dir().await;
        let dest = guard.path().join("m.json");
        // Non-string map keys are not representable in JSON.
        let bad: BTreeMap<(u8, u8), u8> = [((1, 2), 3)].into_iter().collect();

        let err = write_json_atomic(&tmp, &dest, &bad).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Other);
        assert!(!dest.exists(), "destination was created despite the error");
        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
    }
}
