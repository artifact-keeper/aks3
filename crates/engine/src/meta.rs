// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The per-object version manifest: what aks3 knows about a key.
//!
//! Every key has exactly one manifest, holding one [`VersionEntry`] per version,
//! newest first. Storage is versioning-native: an unversioned bucket is not a
//! separate code path, it is a manifest whose single entry carries the reserved
//! id [`NULL_VERSION_ID`], and overwriting that object replaces the entry in
//! place rather than growing history. Turning versioning on later therefore
//! needs no migration.
//!
//! A deletion is also an entry, with [`VersionEntry::delete_marker`] set, so the
//! object's tombstone participates in ordering like any other version. That is
//! what lets `GET` of a deleted key report "no such key" while a `GET` naming an
//! older version id still succeeds.
//!
//! Manifests reach disk as JSON through [`crate::atomic::write_json_atomic`], so
//! a reader or a crash never sees a partly written history.
//!
//! # Compatibility
//!
//! [`VersionManifest::format`] is stamped on every manifest written, so a future
//! layout change can be recognised rather than guessed at. Fields that arrived
//! after the first release are `serde(default)`, letting a newer binary read an
//! older manifest untouched.

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::atomic::write_json_atomic;

/// Version id of an object stored in a bucket that has never had versioning on.
///
/// S3 reports this literal to clients, and it is reserved: a generated version
/// id never collides with it.
pub const NULL_VERSION_ID: &str = "null";

/// Layout version stamped on manifests this build writes.
pub const MANIFEST_FORMAT: u32 = 1;

/// One version of one object.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VersionEntry {
    /// [`NULL_VERSION_ID`] when the object was written without versioning.
    pub version_id: String,
    /// Lowercase hex MD5 of the object bytes, without the quotes S3 wraps it in
    /// on the wire.
    pub etag: String,
    pub size: u64,
    pub content_type: String,
    /// User metadata, without the `x-amz-meta-` prefix. Sorted, so a manifest
    /// serializes byte-for-byte the same way twice.
    #[serde(default)]
    pub user_metadata: BTreeMap<String, String>,
    pub mtime_epoch_ms: u64,
    /// A tombstone: the version exists in history but the object reads as gone.
    #[serde(default)]
    pub delete_marker: bool,
}

/// Every version of one object, newest first.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct VersionManifest {
    /// See [`MANIFEST_FORMAT`].
    pub format: u32,
    /// Index 0 is the latest version, the one an unqualified `GET` resolves to.
    pub versions: Vec<VersionEntry>,
}

impl VersionManifest {
    /// The version an unqualified read resolves to, or `None` for an empty
    /// manifest.
    ///
    /// A delete marker at the head is still the latest version; the caller
    /// decides what that means for the request.
    #[must_use]
    pub fn latest(&self) -> Option<&VersionEntry> {
        self.versions.first()
    }

    /// Insert `e`, or replace the entry that already carries its version id.
    ///
    /// A new id becomes the latest version. Replacement happens in place and
    /// keeps the entry's position, which is what makes an overwrite in an
    /// unversioned bucket a no-growth operation on [`NULL_VERSION_ID`], and
    /// keeps history ordered when an existing version's metadata is rewritten.
    pub fn upsert(&mut self, e: VersionEntry) {
        if let Some(slot) = self
            .versions
            .iter_mut()
            .find(|v| v.version_id == e.version_id)
        {
            *slot = e;
        } else {
            self.versions.insert(0, e);
        }
    }

    /// Drop the entry with `version_id`, returning it. `None` if no such version.
    pub fn remove(&mut self, version_id: &str) -> Option<VersionEntry> {
        let at = self
            .versions
            .iter()
            .position(|v| v.version_id == version_id)?;
        Some(self.versions.remove(at))
    }
}

/// Read the manifest at `path`, or `None` if the key has no manifest yet.
///
/// Only a missing file reads as `None`. A manifest that exists but cannot be
/// parsed is an error, not an absent object: reporting `None` would present a
/// corrupt file as an empty history and invite the next write to overwrite it.
///
/// # Errors
///
/// Propagates read errors other than [`io::ErrorKind::NotFound`], and reports a
/// parse failure as [`io::ErrorKind::InvalidData`].
pub async fn load_manifest(path: &Path) -> io::Result<Option<VersionManifest>> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    serde_json::from_slice(&bytes)
        .map(Some)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

/// Write `m` to `path` atomically, staging through `tmp`.
///
/// `tmp` must already exist. See [`crate::atomic`] for the durability the write
/// relies on, including the case where an error surfaces after the manifest is
/// already published.
///
/// # Errors
///
/// Propagates the staged write.
pub async fn store_manifest(tmp: &Path, path: &Path, m: &VersionManifest) -> io::Result<()> {
    write_json_atomic(tmp, path, m).await
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;
    use std::io;
    use std::path::PathBuf;

    fn entry(v: &str, etag: &str) -> VersionEntry {
        VersionEntry {
            version_id: v.into(),
            etag: etag.into(),
            size: 1,
            content_type: "application/octet-stream".into(),
            user_metadata: BTreeMap::new(),
            mtime_epoch_ms: 0,
            delete_marker: false,
        }
    }

    fn manifest(versions: Vec<VersionEntry>) -> VersionManifest {
        VersionManifest {
            format: MANIFEST_FORMAT,
            versions,
        }
    }

    /// Fresh temp dir for staging, plus the tempdir guard that owns it.
    async fn tmp_dir() -> (tempfile::TempDir, PathBuf) {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("tmp");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        (dir, tmp)
    }

    #[test]
    fn upsert_null_replaces_in_place() {
        let mut m = VersionManifest {
            format: MANIFEST_FORMAT,
            versions: vec![],
        };
        m.upsert(entry(NULL_VERSION_ID, "aa"));
        m.upsert(entry(NULL_VERSION_ID, "bb"));
        assert_eq!(m.versions.len(), 1);
        assert_eq!(m.latest().unwrap().etag, "bb");
    }

    #[test]
    fn upsert_new_version_prepends() {
        let mut m = VersionManifest {
            format: MANIFEST_FORMAT,
            versions: vec![entry("v1", "aa")],
        };
        m.upsert(entry("v2", "bb"));
        assert_eq!(m.latest().unwrap().version_id, "v2");
        assert_eq!(m.versions.len(), 2);
    }

    #[tokio::test]
    async fn manifest_disk_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let tmp = dir.path().join("tmp");
        tokio::fs::create_dir_all(&tmp).await.unwrap();
        let p = dir.path().join("m.json");
        assert!(load_manifest(&p).await.unwrap().is_none());
        let m = VersionManifest {
            format: MANIFEST_FORMAT,
            versions: vec![entry("null", "aa")],
        };
        store_manifest(&tmp, &p, &m).await.unwrap();
        assert_eq!(load_manifest(&p).await.unwrap().unwrap(), m);
    }

    // --- additional cases beyond the brief ---

    #[test]
    fn latest_is_the_head_and_none_when_empty() {
        let mut m = manifest(vec![entry("v2", "bb"), entry("v1", "aa")]);
        assert_eq!(m.latest().unwrap().version_id, "v2");
        m.versions.clear();
        assert!(m.latest().is_none());
    }

    #[test]
    fn upsert_replaces_a_non_head_version_without_reordering() {
        // Replacement is in place, so history order (newest first) survives an
        // overwrite of an older version.
        let mut m = manifest(vec![entry("v2", "bb"), entry("v1", "aa")]);
        m.upsert(entry("v1", "cc"));
        let ids: Vec<&str> = m.versions.iter().map(|e| e.version_id.as_str()).collect();
        assert_eq!(ids, ["v2", "v1"]);
        assert_eq!(m.versions[1].etag, "cc");
    }

    #[test]
    fn remove_takes_the_named_version_and_leaves_the_rest() {
        let mut m = manifest(vec![entry("v2", "bb"), entry("v1", "aa")]);
        assert_eq!(m.remove("v1").unwrap().etag, "aa");
        let ids: Vec<&str> = m.versions.iter().map(|e| e.version_id.as_str()).collect();
        assert_eq!(ids, ["v2"]);
        assert!(m.remove("v1").is_none(), "removed twice");
        assert!(m.remove("nope").is_none());
    }

    #[test]
    fn entry_deserializes_without_the_optional_fields() {
        // Manifests written before user metadata or delete markers existed must
        // still load; both fields are `serde(default)`.
        let json = r#"{"version_id":"null","etag":"aa","size":7,
            "content_type":"text/plain","mtime_epoch_ms":5}"#;
        let e: VersionEntry = serde_json::from_str(json).unwrap();
        assert!(e.user_metadata.is_empty());
        assert!(!e.delete_marker);
        assert_eq!(e.size, 7);
    }

    #[tokio::test]
    async fn roundtrip_preserves_metadata_and_delete_markers() {
        let (guard, tmp) = tmp_dir().await;
        let p = guard.path().join("nested/m.json");

        let mut deleted = entry("v2", "");
        deleted.delete_marker = true;
        deleted.size = 0;
        let mut live = entry("v1", "9a0364b9e99bb480dd25e1f0284c8555");
        live.size = 42;
        live.mtime_epoch_ms = 1_700_000_000_000;
        live.user_metadata.insert("a".into(), "1".into());
        live.user_metadata.insert("b".into(), "2".into());

        let m = manifest(vec![deleted, live]);
        store_manifest(&tmp, &p, &m).await.unwrap();
        assert_eq!(load_manifest(&p).await.unwrap().unwrap(), m);
        assert_eq!(std::fs::read_dir(&tmp).unwrap().count(), 0);
    }

    #[tokio::test]
    async fn store_manifest_replaces_an_earlier_manifest() {
        let (guard, tmp) = tmp_dir().await;
        let p = guard.path().join("m.json");
        store_manifest(&tmp, &p, &manifest(vec![entry("v1", "aa")]))
            .await
            .unwrap();

        let second = manifest(vec![entry("v2", "bb"), entry("v1", "aa")]);
        store_manifest(&tmp, &p, &second).await.unwrap();
        assert_eq!(load_manifest(&p).await.unwrap().unwrap(), second);
    }

    #[tokio::test]
    async fn load_manifest_rejects_a_corrupt_file() {
        // A truncated or garbled manifest is not an absent one: reporting `None`
        // here would silently drop every version of the object.
        let guard = tempfile::tempdir().unwrap();
        let p = guard.path().join("m.json");
        tokio::fs::write(&p, b"{\"format\": 1, \"versions\": [")
            .await
            .unwrap();
        let err = load_manifest(&p).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    #[tokio::test]
    async fn load_manifest_reports_absence_only_for_a_missing_file() {
        let guard = tempfile::tempdir().unwrap();
        assert!(load_manifest(&guard.path().join("absent.json"))
            .await
            .unwrap()
            .is_none());
        assert!(load_manifest(&guard.path().join("no/such/dir/m.json"))
            .await
            .unwrap()
            .is_none());
    }

    #[tokio::test]
    async fn store_manifest_propagates_a_missing_tmp_dir() {
        let guard = tempfile::tempdir().unwrap();
        let p = guard.path().join("m.json");
        let err = store_manifest(&guard.path().join("absent"), &p, &manifest(vec![]))
            .await
            .unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::NotFound);
        assert!(!p.exists(), "manifest published despite the error");
    }
}
