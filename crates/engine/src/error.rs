// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! What the storage engine can fail with.
//!
//! The variants are deliberately few and deliberately named after S3 error
//! codes: the API layer turns an [`EngineError`] into a wire error with a flat
//! table and no guessing. Anything the engine cannot express as one of those
//! conditions is [`EngineError::Io`], which the API layer reports as
//! `InternalError` without leaking the underlying message, since it usually
//! carries a host path.
//!
//! Nothing here carries the bucket or key it happened to; the caller knows what
//! it asked for, and keeping the payload empty makes the variants cheap to
//! match on and impossible to accidentally log a path with.
//!
//! # Classifying an I/O error
//!
//! Two filesystem failures are sorted out here rather than at each call site: a
//! name the filesystem refuses as too long, which is the client's fault, and the
//! disk running out from under a write, which is the deployment's. See
//! [`EngineError::KeyTooLong`], [`EngineError::StorageFull`], and the [`From`]
//! impl below.

/// A storage engine failure.
#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// The named bucket does not exist.
    #[error("bucket not found")]
    NoSuchBucket,
    /// A bucket with that name already exists.
    #[error("bucket already exists")]
    BucketAlreadyExists,
    /// The bucket still holds objects, and S3 only deletes empty buckets.
    #[error("bucket not empty")]
    BucketNotEmpty,
    /// The name is not a legal S3 bucket name; see the engine's validation.
    #[error("invalid bucket name")]
    InvalidBucketName,
    /// The object does not exist, or its latest version is a delete marker.
    #[error("object not found")]
    NoSuchKey,
    /// The requested byte range does not overlap the object.
    #[error("invalid range")]
    InvalidRange,
    /// The key encodes to a path the filesystem will not accept, because one
    /// of its components is longer than the name length the filesystem allows
    /// (255 bytes on APFS and ext4).
    ///
    /// S3 itself allows a key of up to 1024 bytes with no per-component limit,
    /// so this is a Phase 0 shortfall of storing keys as paths rather than
    /// something the client got wrong. It is still reported to the client, as
    /// `KeyTooLongError`, because it is a property of the key: no retry and no
    /// wait makes the same key work.
    ///
    /// Every operation on such a key reports this, not just the `PUT`. A `GET`
    /// or a `HEAD` could just as truthfully say `NoSuchKey`, since a key that
    /// cannot be written is certainly not there, and a `DELETE` of an absent
    /// key is a success in S3. Saying so would tell a client that the key is
    /// merely empty and invite it to keep trying, so instead one rule holds for
    /// the key across every verb: aks3 cannot represent it, and says which key
    /// property is at fault.
    #[error("key too long for the filesystem")]
    KeyTooLong,
    /// The filesystem is out of space (`ENOSPC`): a write could not be committed
    /// because the disk backing the store is full.
    ///
    /// This is about the deployment rather than the request. The same key and the
    /// same bytes would store fine on a disk with room, so it is not the client's
    /// fault, but it is also not a bug the way a bare I/O failure is: a client, a
    /// load balancer, or a retry policy should be able to tell "this server is out
    /// of space, back off and let an operator grow the disk" apart from "this
    /// server hit something it did not expect". The API layer maps it to a
    /// retryable 5xx rather than to `InternalError`; see `map_engine_err`.
    ///
    /// Carried as its own variant, and not folded into [`EngineError::Io`], so the
    /// message it is built from never reaches a client: like every other variant
    /// here it holds nothing, so there is no host path to leak.
    #[error("storage full")]
    StorageFull,
    /// Anything the filesystem reported. Not a client-visible condition.
    #[error("io: {0}")]
    Io(std::io::Error),
}

/// Sort a filesystem failure into the one client-visible condition it can be,
/// or [`EngineError::Io`].
///
/// Written out rather than derived with `#[from]` so that every `?` on an
/// `io::Error` anywhere in the engine classifies the same way. `put_object` is
/// the operation that provokes it, but a `GET`, a `HEAD` and a `DELETE` all
/// build the same path from the same key and hit the same refusal, and each of
/// them reaches this impl through a different call.
///
/// [`std::io::ErrorKind::InvalidFilename`] is what the standard library decodes
/// `ENAMETOOLONG` to (raw OS error 63 on macOS, 36 on Linux); it has been
/// stable since Rust 1.83, below this workspace's MSRV, so there is no need to
/// match the raw numbers and no need for a `libc` dependency to name them. The
/// test below pins the decoding by provoking a real one.
///
/// On Linux the same errno also covers a whole path longer than `PATH_MAX`
/// rather than one over-long component, which a key with very many components
/// can reach. That is still the key being too long to store, so the same
/// answer is the right one. A `data_dir` so deeply nested that ordinary keys
/// overflow `PATH_MAX` would be misreported as the client's fault, which is
/// the one case this classification gets wrong.
/// [`std::io::ErrorKind::StorageFull`] is what the standard library decodes
/// `ENOSPC` to (raw OS error 28 on both macOS and Linux). It is a stable
/// `ErrorKind`, so the classification names the kind rather than the raw errno,
/// the same way the too-long case names `InvalidFilename`. A full disk is a
/// property of the deployment, not the request, so it becomes
/// [`EngineError::StorageFull`] rather than being buried in the catch-all where a
/// client could not tell it from a bug.
impl From<std::io::Error> for EngineError {
    fn from(err: std::io::Error) -> Self {
        match err.kind() {
            std::io::ErrorKind::InvalidFilename => Self::KeyTooLong,
            std::io::ErrorKind::StorageFull => Self::StorageFull,
            _ => Self::Io(err),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::EngineError;

    /// The classification rests on the standard library decoding
    /// `ENAMETOOLONG` to `InvalidFilename`. That is a platform detail, so it is
    /// checked against a real filesystem rather than assumed: a component
    /// longer than `NAME_MAX` cannot be created anywhere aks3 runs.
    #[test]
    fn a_name_the_filesystem_refuses_as_too_long_is_not_an_io_error() {
        let dir = tempfile::tempdir().expect("temp dir");
        let too_long = dir.path().join("n".repeat(300));
        let err = std::fs::write(&too_long, b"x").expect_err("300-byte name is refused");
        assert!(
            matches!(EngineError::from(err), EngineError::KeyTooLong),
            "ENAMETOOLONG did not decode to ErrorKind::InvalidFilename on this platform"
        );
    }

    /// A full disk (`ENOSPC`, which the standard library decodes to
    /// `ErrorKind::StorageFull`) is classified as its own condition rather than
    /// swept into the catch-all, so the API layer can answer it with a retryable
    /// 5xx instead of a bare 500.
    #[test]
    fn a_full_disk_is_storage_full_not_an_io_error() {
        let err = EngineError::from(std::io::Error::from(std::io::ErrorKind::StorageFull));
        assert!(matches!(err, EngineError::StorageFull));
    }

    /// Everything else stays an I/O error, so the classification cannot swallow
    /// a genuine fault.
    #[test]
    fn any_other_io_error_stays_an_io_error() {
        let err = EngineError::from(std::io::Error::other("disk on fire"));
        assert!(matches!(err, EngineError::Io(_)));
    }
}
