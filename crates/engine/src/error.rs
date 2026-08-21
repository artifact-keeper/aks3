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
    /// Anything the filesystem reported. Not a client-visible condition.
    #[error("io: {0}")]
    Io(#[from] std::io::Error),
}
