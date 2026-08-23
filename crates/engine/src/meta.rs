// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The per-object version manifest: what aks3 knows about a key.
//!
//! Every key has exactly one manifest, holding one [`VersionEntry`] per version,
//! newest first. Storage is versioning-native: an unversioned bucket is not a
//! separate code path, it is a manifest whose single entry carries the reserved
//! id [`NULL_VERSION_ID`], and overwriting that object replaces that entry
//! rather than growing history. Turning versioning on later therefore needs no
//! migration.
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
//! older manifest untouched. In the other direction [`load_manifest`] refuses a
//! manifest newer than this build understands, rather than parsing away the
//! fields it does not know and writing the remains back.

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

    /// Insert `e` as the latest version, replacing any entry that already
    /// carries its version id.
    ///
    /// The entry always ends up at index 0, whether or not it replaced one:
    /// in S3, an overwrite *becomes* the current version. That matters when
    /// versioning is suspended and a `PUT` rewrites [`NULL_VERSION_ID`] on a
    /// key that already has newer versioned entries above it. Replacing the
    /// null entry where it sat would leave [`Self::latest`] pointing at an
    /// older version that the client just overwrote.
    ///
    /// The unversioned case still never grows history: the old null entry is
    /// removed as the new one goes in.
    pub fn upsert(&mut self, e: VersionEntry) {
        self.versions.retain(|v| v.version_id != e.version_id);
        self.versions.insert(0, e);
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
/// A manifest stamped with a format newer than [`MANIFEST_FORMAT`] is refused
/// for the same reason. Deserialization ignores fields this build does not know
/// about, so parsing one and writing it back would silently drop them while
/// leaving the newer format number in place, hiding the loss from the binary
/// that wrote it. Older formats are still read; only newer ones are refused.
///
/// # Errors
///
/// Propagates read errors other than [`io::ErrorKind::NotFound`]. Reports a
/// parse failure or an unsupported format as [`io::ErrorKind::InvalidData`].
pub async fn load_manifest(path: &Path) -> io::Result<Option<VersionManifest>> {
    let bytes = match tokio::fs::read(path).await {
        Ok(bytes) => bytes,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let m: VersionManifest = serde_json::from_slice(&bytes)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    if m.format > MANIFEST_FORMAT {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "manifest format {} at {} is newer than the supported format {MANIFEST_FORMAT}",
                m.format,
                path.display(),
            ),
        ));
    }
    Ok(Some(m))
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
    fn upsert_moves_a_replaced_non_head_version_to_the_front() {
        // Versioning suspended on a key that already has versioned entries: the
        // PUT overwrites the null version *and* that null version becomes
        // current. Leaving it in place would make `latest` report v3, a version
        // the client just overwrote.
        let mut m = manifest(vec![
            entry("v3", "cc"),
            entry("v2", "bb"),
            entry("null", "aa"),
        ]);
        m.upsert(entry(NULL_VERSION_ID, "dd"));

        let ids: Vec<&str> = m.versions.iter().map(|e| e.version_id.as_str()).collect();
        assert_eq!(ids, [NULL_VERSION_ID, "v3", "v2"]);
        assert_eq!(m.latest().unwrap().etag, "dd");
        assert_eq!(m.versions.len(), 3, "history grew on an overwrite");
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
    async fn load_manifest_refuses_a_newer_format() {
        // Unknown fields deserialize away silently, so parsing a newer manifest
        // and writing it back would drop them while keeping its format stamp.
        // Refusing to read it is what stops that from happening quietly.
        let guard = tempfile::tempdir().unwrap();
        let p = guard.path().join("m.json");
        tokio::fs::write(
            &p,
            br#"{"format":2,"versions":[],"retention":{"mode":"COMPLIANCE"}}"#,
        )
        .await
        .unwrap();

        let err = load_manifest(&p).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let msg = err.to_string();
        assert!(
            msg.contains('2') && msg.contains(&MANIFEST_FORMAT.to_string()),
            "{msg}"
        );

        // Reject-newer only: an older stamp still reads.
        tokio::fs::write(&p, br#"{"format":0,"versions":[]}"#)
            .await
            .unwrap();
        assert_eq!(load_manifest(&p).await.unwrap().unwrap().format, 0);
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

#[cfg(test)]
mod proptests {
    //! Manifest invariants over arbitrary operation sequences.
    //!
    //! The examples above pin the three cases that were reasoned about while
    //! writing `upsert`. These say the invariants hold whatever order the
    //! operations arrive in, which is what a key being written, overwritten,
    //! versioned and deleted in some interleaving actually produces.
    //!
    //! A failure prints its minimal input together with a `cc <seed>` line for
    //! `crates/engine/proptest-regressions/meta.txt`; see the README there.

    use std::collections::BTreeSet;

    use proptest::prelude::*;

    use super::*;

    /// One step of a manifest's life.
    #[derive(Debug, Clone)]
    enum Op {
        /// A write: the entry becomes the current version. A `PUT` when the
        /// flag is clear, the tombstone a `DELETE` leaves when it is set, since
        /// both reach the manifest the same way.
        Upsert(String, String, bool),
        /// Dropping one named version from history.
        Remove(String),
    }

    /// A small id alphabet on purpose: sequences only get interesting where ids
    /// repeat, and repeats are what distinguish an upsert from an insert.
    /// [`NULL_VERSION_ID`] is in it because the unversioned case is the one
    /// that must never grow history.
    fn version_id() -> impl Strategy<Value = String> {
        prop::sample::select(vec![NULL_VERSION_ID, "v1", "v2", "v3"])
            .prop_map(std::borrow::ToOwned::to_owned)
    }

    fn op() -> impl Strategy<Value = Op> {
        prop_oneof![
            3 => (version_id(), "[0-9a-f]{4}", any::<bool>())
                .prop_map(|(id, etag, marker)| Op::Upsert(id, etag, marker)),
            1 => version_id().prop_map(Op::Remove),
        ]
    }

    fn entry(version_id: &str, etag: &str, delete_marker: bool) -> VersionEntry {
        VersionEntry {
            version_id: version_id.to_owned(),
            etag: etag.to_owned(),
            size: 1,
            content_type: "application/octet-stream".to_owned(),
            user_metadata: BTreeMap::new(),
            mtime_epoch_ms: 0,
            delete_marker,
        }
    }

    /// Manifests carrying the awkward halves of every field: hostile strings,
    /// extreme numbers, tombstones, user metadata.
    fn arbitrary_manifest() -> impl Strategy<Value = VersionManifest> {
        let text = prop_oneof![
            2 => "[ -~]{0,8}",
            1 => any::<String>(),
        ];
        let entry = (
            any::<String>(),
            "[0-9a-f]{32}",
            any::<u64>(),
            text.clone(),
            prop::collection::btree_map(text.clone(), text, 0..3),
            any::<u64>(),
            any::<bool>(),
        )
            .prop_map(|(version_id, etag, size, ctype, meta, mtime, marker)| {
                VersionEntry {
                    version_id,
                    etag,
                    size,
                    content_type: ctype,
                    user_metadata: meta,
                    mtime_epoch_ms: mtime,
                    delete_marker: marker,
                }
            });
        (0_u32..=MANIFEST_FORMAT, prop::collection::vec(entry, 0..4))
            .prop_map(|(format, versions)| VersionManifest { format, versions })
    }

    proptest! {
        /// Whatever sequence of writes and deletes a key sees, the manifest
        /// keeps its promises: the entry just written is the one an unqualified
        /// read resolves to, an id appears at most once, an overwrite does not
        /// grow history, and removing an id that is not there changes nothing.
        #[test]
        fn a_manifest_holds_its_invariants_through_any_sequence(
            ops in prop::collection::vec(op(), 0..24),
        ) {
            let mut m = VersionManifest { format: MANIFEST_FORMAT, versions: vec![] };
            // The model records when each surviving id was last written, and
            // nothing about how `upsert` arranges the vector. "Newest first" is
            // the whole ordering claim the manifest makes, so a model built
            // from write order tests that claim; a model built by retaining and
            // inserting would only be a copy of the implementation, and would
            // agree with it however wrong both were.
            let mut written: BTreeMap<String, u64> = BTreeMap::new();
            let mut clock: u64 = 0;

            for step in ops {
                let before = m.versions.len();
                match step {
                    Op::Upsert(id, etag, marker) => {
                        let known = written.contains_key(&id);
                        m.upsert(entry(&id, &etag, marker));
                        clock += 1;
                        written.insert(id.clone(), clock);

                        // The whole entry, not just its id: a head carrying the
                        // right id and a previous write's bytes is the version
                        // loss this property exists to catch.
                        let head = m.latest().expect("a manifest with an entry has a head");
                        prop_assert_eq!(
                            head, &entry(&id, &etag, marker),
                            "the head is not the entry just written"
                        );
                        if known {
                            prop_assert_eq!(m.versions.len(), before, "an overwrite grew history");
                        } else {
                            prop_assert_eq!(m.versions.len(), before + 1);
                        }
                    }
                    Op::Remove(id) => {
                        let known = written.contains_key(&id);
                        if let Some(removed) = m.remove(&id) {
                            prop_assert!(known, "removed {:?}, which was not there", id);
                            prop_assert_eq!(removed.version_id, id.clone());
                            prop_assert_eq!(m.versions.len(), before - 1);
                            written.remove(&id);
                        } else {
                            prop_assert!(!known, "did not remove {:?}, which was there", id);
                            prop_assert_eq!(
                                m.versions.len(),
                                before,
                                "removing an absent id changed the manifest"
                            );
                        }
                    }
                }

                let ids: Vec<&str> = m.versions.iter().map(|e| e.version_id.as_str()).collect();
                let unique: BTreeSet<&str> = ids.iter().copied().collect();
                prop_assert_eq!(unique.len(), ids.len(), "duplicate version ids in {:?}", ids);

                // Exactly the ids written and not since removed, most recently
                // written first. Write numbers are distinct, so the order is
                // total and the comparison is exact.
                let mut want: Vec<&str> = written.keys().map(String::as_str).collect();
                want.sort_by_key(|id| std::cmp::Reverse(written[*id]));
                prop_assert_eq!(&ids, &want);
            }
        }

        /// A manifest reaches disk as JSON, so anything it can hold has to
        /// survive that trip unchanged: a field that serialized lossily would
        /// lose object metadata on the next read.
        ///
        /// Byte stability is the second half, and the one the crash-safety
        /// story leans on. Reading a manifest and writing it back has to
        /// produce the same file, or a no-op rewrite would republish different
        /// bytes and every claim about what a reader can observe mid-write
        /// would be about a different file each time.
        #[test]
        fn a_manifest_survives_the_json_it_is_stored_as(m in arbitrary_manifest()) {
            let json = serde_json::to_vec(&m).expect("a manifest serializes");
            let back: VersionManifest =
                serde_json::from_slice(&json).expect("a manifest we wrote parses");
            prop_assert_eq!(&back, &m);
            let again = serde_json::to_vec(&back).expect("a manifest we read serializes");
            prop_assert_eq!(again, json, "rewriting a manifest changed its bytes");
        }
    }
}
