// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Walking a bucket's object tree back into S3 keys.
//!
//! The tree under `objects/` is the keyspace, encoded: one directory per key
//! component ([`crate::paths::key_to_rel_path`]), and a directory holding a
//! [`META_FILE`] *is* an object rather than merely being on the way to one.
//! Both can be true of the same directory: `a/b` and `a` can each be an object,
//! and `a`'s manifest sits beside the subdirectory `b`.
//!
//! # Blocking
//!
//! [`sorted_keys`] uses `std::fs` and returns only when the whole tree has been
//! read, so it must be called from `tokio::task::spawn_blocking` and never
//! straight from an async task. A per-entry async `read_dir` would await once
//! per directory to no purpose: the walk has nothing else to do between
//! entries, and a listing of a large bucket is a long run of small metadata
//! reads, which is exactly what the blocking pool is for.
//!
//! # What is skipped
//!
//! Only names this engine could have written are followed. A name that is not
//! valid UTF-8, does not decode ([`crate::paths::decode_component`]), or is not
//! a directory is left alone, as is anything starting with
//! [`RESERVED_PREFIX`]: those are aks3's own files, and a user key can never
//! encode to one. So a stray file dropped into the tree is ignored rather than
//! served as an object with a corrupt name, and a symlink is never followed,
//! which also means the walk cannot loop or leave the bucket.
//!
//! Directories that vanish mid-walk are not an error. A listing runs under the
//! shared bucket lock, which keeps `delete_bucket` out but not `delete_object`,
//! so the tree is pruned underneath it; a key removed while the walk was
//! elsewhere is simply a key the listing does not report.

use std::io;
use std::path::{Path, PathBuf};

use crate::paths::{decode_component, rel_path_to_key, META_FILE, RESERVED_PREFIX};

/// Every key under `objects_root`, in ascending UTF-8 order.
///
/// `objects_root` need not exist: a bucket that has never been written to has
/// no object tree, and holds no keys.
///
/// # Errors
///
/// [`io::Error`] if a directory that exists cannot be read. An entry that
/// disappears while the walk is running is not an error; see the module note.
pub fn sorted_keys(objects_root: &Path) -> io::Result<Vec<String>> {
    let mut keys = Vec::new();
    // An explicit stack rather than recursion: the depth is the number of
    // components in a key, which the client chooses, and a key deep enough to
    // exhaust the stack must not be able to take the server down with it.
    let mut stack = vec![(objects_root.to_path_buf(), PathBuf::new())];
    while let Some((dir, rel)) = stack.pop() {
        let entries = read_object_dir(&dir)?;
        // The root is the bucket's object tree, not an object in it, and its
        // empty relative path is not a key.
        if entries.has_manifest && !rel.as_os_str().is_empty() {
            match rel_path_to_key(&rel) {
                Ok(key) => keys.push(key),
                // Every component was decoded on the way down, so this is only
                // reachable if the path holds something encoding never emits.
                Err(e) => {
                    tracing::warn!(path = %rel.display(), error = %e, "skipping undecodable key");
                }
            }
        }
        // Reversed, because a stack pops last-pushed first: the walk descends
        // into the first child before its siblings.
        for name in entries.children.into_iter().rev() {
            stack.push((dir.join(&name), rel.join(&name)));
        }
    }
    // The per-directory ordering above is by component, and component order is
    // not key order: `dir-x` sorts before `dir/one` because `-` is below `/`,
    // while the directory `dir` sorts before the directory `dir-x`. S3 orders
    // by the whole key, and the paging below this depends on it, so the
    // assembled list is sorted once more as keys.
    keys.sort_unstable();
    Ok(keys)
}

/// What one directory contributes to the walk.
#[derive(Default)]
struct DirEntries {
    /// The directory holds a manifest, so it is an object as well as, possibly,
    /// a step towards deeper ones.
    has_manifest: bool,
    /// Encoded names of the subdirectories to descend into, ordered by their
    /// decoded form.
    children: Vec<String>,
}

/// Read one directory of the object tree.
///
/// A directory that is not there yields nothing: that is a bucket with no
/// object tree yet, or a key pruned by a concurrent delete.
fn read_object_dir(dir: &Path) -> io::Result<DirEntries> {
    let iter = match std::fs::read_dir(dir) {
        Ok(iter) => iter,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(DirEntries::default()),
        Err(e) => return Err(e),
    };

    let mut has_manifest = false;
    // Decoded name first, so the sort below orders by what the key says rather
    // than by how it was escaped.
    let mut children: Vec<(String, String)> = Vec::new();
    for entry in iter {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) if e.kind() == io::ErrorKind::NotFound => continue,
            Err(e) => return Err(e),
        };
        let Ok(name) = entry.file_name().into_string() else {
            continue;
        };
        if name == META_FILE {
            has_manifest = true;
            continue;
        }
        // Data files, and whatever bookkeeping a later phase adds beside them.
        if name.starts_with(RESERVED_PREFIX) {
            continue;
        }
        let Ok(decoded) = decode_component(&name) else {
            continue;
        };
        // is_dir is false for a symlink as well as for a file, which is what
        // keeps the walk inside the bucket.
        if !entry.file_type().is_ok_and(|t| t.is_dir()) {
            continue;
        }
        children.push((decoded, name));
    }

    children.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(DirEntries {
        has_manifest,
        children: children.into_iter().map(|(_, name)| name).collect(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paths::key_to_rel_path;

    /// A tree holding exactly `keys`, as `put_object` would leave it.
    fn tree(keys: &[&str]) -> tempfile::TempDir {
        let root = tempfile::tempdir().unwrap();
        for key in keys {
            let dir = root.path().join(key_to_rel_path(key));
            std::fs::create_dir_all(&dir).unwrap();
            std::fs::write(dir.join(META_FILE), b"{}").unwrap();
        }
        root
    }

    #[test]
    fn an_absent_tree_holds_no_keys() {
        let root = tempfile::tempdir().unwrap();
        assert!(sorted_keys(&root.path().join("never-written"))
            .unwrap()
            .is_empty());
    }

    #[test]
    fn an_empty_tree_holds_no_keys() {
        assert!(sorted_keys(tree(&[]).path()).unwrap().is_empty());
    }

    #[test]
    fn keys_come_back_sorted() {
        let root = tree(&["z", "a/b", "a", "m/n/o"]);
        assert_eq!(
            sorted_keys(root.path()).unwrap(),
            vec!["a", "a/b", "m/n/o", "z"]
        );
    }

    #[test]
    fn ordering_is_by_key_not_by_component() {
        // `-` is below `/`, so the whole key `dir-x` sorts before `dir/one`
        // while the component `dir` sorts before the component `dir-x`.
        let root = tree(&["dir/one", "dir-x"]);
        assert_eq!(sorted_keys(root.path()).unwrap(), vec!["dir-x", "dir/one"]);
    }

    #[test]
    fn encoded_names_decode_back_to_their_keys() {
        let keys = [
            "",
            "..",
            META_FILE,
            "a/",
            "caf\u{e9}",
            "pct%",
            "tab\tx",
            "\u{1f600}/emoji",
        ];
        let root = tree(&keys);
        let mut want: Vec<String> = keys.iter().map(|k| (*k).to_owned()).collect();
        want.sort_unstable();
        assert_eq!(sorted_keys(root.path()).unwrap(), want);
    }

    #[test]
    fn a_directory_without_a_manifest_is_only_a_step_on_the_way() {
        let root = tree(&["a/b"]);
        assert_eq!(sorted_keys(root.path()).unwrap(), vec!["a/b"]);
    }

    #[test]
    fn strays_and_reserved_names_are_left_alone() {
        let root = tree(&["real"]);
        // Not a directory, so not a key.
        std::fs::write(root.path().join("loose-file"), b"x").unwrap();
        // A directory whose name is not something encoding ever produced.
        std::fs::create_dir_all(root.path().join("%ZZ/deeper")).unwrap();
        std::fs::write(root.path().join("%ZZ/deeper").join(META_FILE), b"{}").unwrap();
        // Reserved names are aks3's own, at any depth.
        std::fs::create_dir_all(root.path().join("__aks3.something")).unwrap();
        std::fs::write(root.path().join("real/__aks3.v.null.data"), b"x").unwrap();

        assert_eq!(sorted_keys(root.path()).unwrap(), vec!["real"]);
    }

    #[test]
    fn a_deeply_nested_key_is_walked_to_the_bottom() {
        // As deep as the filesystem's own path limit allows, which is the
        // deepest key a client can ever get stored.
        let root = tempfile::tempdir().unwrap();
        let mut at = root.path().to_path_buf();
        loop {
            let next = at.join("a");
            if std::fs::create_dir(&next).is_err() {
                break;
            }
            at = next;
        }
        // The manifest's own name needs room too, so back off until it fits.
        while std::fs::write(at.join(META_FILE), b"{}").is_err() {
            assert!(at.pop() && at.starts_with(root.path()), "no room for a key");
        }

        let keys = sorted_keys(root.path()).unwrap();
        assert_eq!(keys.len(), 1);
        assert!(
            keys[0].split('/').count() > 100,
            "expected a deep key, got {} components",
            keys[0].split('/').count()
        );
    }
}
