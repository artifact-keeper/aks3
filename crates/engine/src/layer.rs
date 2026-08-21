// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The storage contract: what an object store must be able to do.
//!
//! [`ObjectLayer`] is the only thing the S3 front end knows about the storage
//! side. It is deliberately narrower than S3 itself and speaks in plain types:
//! no `s3s` request or response objects reach an implementation, and no
//! filesystem detail reaches the API layer. That boundary is what lets the
//! single-disk [`crate::fs_engine::FsEngine`] be swapped for an erasure-coded
//! backend later without touching a handler, and what lets the handlers be
//! tested against a real engine over a temp directory.
//!
//! # Streaming
//!
//! Object bodies cross the boundary as [`BoxByteStream`], never as a `Vec<u8>`:
//! an object is as large as a client cares to make it, so neither a `PUT` nor a
//! `GET` may require the whole body in memory at once. The cost of boxing is
//! one allocation per request, which is noise next to the transfer itself.

use std::collections::BTreeMap;

use crate::error::EngineError;

/// An object body in motion: chunks of bytes, or the I/O error that stopped
/// them arriving.
pub type BoxByteStream = futures::stream::BoxStream<'static, Result<bytes::Bytes, std::io::Error>>;

/// A bucket, as `ListBuckets` reports it.
#[derive(Debug, Clone)]
pub struct BucketInfo {
    pub name: String,
    /// Milliseconds since the Unix epoch.
    pub created_epoch_ms: u64,
}

/// Everything a `HEAD`, a `GET` or a listing entry says about one object.
#[derive(Debug, Clone)]
pub struct ObjectInfo {
    pub key: String,
    pub size: u64,
    /// Lowercase hex MD5 of the object bytes, without the quotes S3 puts around
    /// it on the wire.
    pub etag: String,
    pub content_type: String,
    /// Milliseconds since the Unix epoch.
    pub mtime_epoch_ms: u64,
    /// User metadata, without the `x-amz-meta-` prefix. Sorted, so a response
    /// built from it is byte-for-byte reproducible.
    pub user_metadata: BTreeMap<String, String>,
}

/// The parts of a `PUT` that are not the body.
///
/// `Default` means "no declared content type, no user metadata"; the engine
/// picks `application/octet-stream` when the client did not say.
#[derive(Debug, Clone, Default)]
pub struct PutOpts {
    pub content_type: Option<String>,
    pub user_metadata: BTreeMap<String, String>,
}

/// A resolved HTTP `Range` header, with **inclusive** bounds.
///
/// The variants mirror the three forms S3 accepts. None of them is validated
/// against the object here: `bytes=5-2` and a range past the end are only
/// wrong once a size is known, so the engine resolves them and reports
/// [`EngineError::InvalidRange`].
#[derive(Debug, Clone, Copy)]
pub enum ByteRange {
    /// `bytes=first-last`, both ends given and both included.
    FromTo(u64, u64),
    /// `bytes=first-`: from that offset to the end of the object.
    From(u64),
    /// `bytes=-n`: the last `n` bytes.
    Suffix(u64),
}

/// A `ListObjectsV2` request, minus the parts the API layer handles itself.
///
/// `Default` lists nothing useful on purpose: `max_keys` of 0 is a legal S3
/// request that returns an empty page, so a caller that forgets to set a budget
/// gets an obviously empty answer rather than an unbounded scan.
#[derive(Debug, Clone, Default)]
pub struct ListParams {
    /// Only keys starting with this are returned.
    pub prefix: Option<String>,
    /// Folds keys that contain it after the prefix into common prefixes.
    pub delimiter: Option<String>,
    /// Resume point from a previous truncated page. Opaque to clients.
    pub continuation_token: Option<String>,
    /// Start strictly after this key. Applied together with the token; the
    /// later of the two wins.
    pub start_after: Option<String>,
    /// Maximum number of keys plus common prefixes on this page.
    pub max_keys: usize,
}

/// One page of a listing.
#[derive(Debug, Default)]
pub struct ListResult {
    pub objects: Vec<ObjectInfo>,
    /// Directory-like groupings produced by [`ListParams::delimiter`], each
    /// ending with the delimiter.
    pub common_prefixes: Vec<String>,
    /// More items remain past this page.
    pub is_truncated: bool,
    /// Token that resumes the listing, set only when truncated.
    pub next_continuation_token: Option<String>,
}

/// A store that S3 operations can be served from.
///
/// Implementations must be safe to call concurrently from many request tasks:
/// the API layer holds one instance in an `Arc` for the life of the process.
#[async_trait::async_trait]
pub trait ObjectLayer: Send + Sync + 'static {
    /// Create `bucket`.
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidBucketName`] if the name is not legal,
    /// [`EngineError::BucketAlreadyExists`] if it is taken.
    async fn create_bucket(&self, bucket: &str) -> Result<(), EngineError>;

    /// Delete `bucket`, which must be empty.
    ///
    /// # Errors
    ///
    /// [`EngineError::NoSuchBucket`] if it does not exist,
    /// [`EngineError::BucketNotEmpty`] if it still holds objects.
    async fn delete_bucket(&self, bucket: &str) -> Result<(), EngineError>;

    /// Whether `bucket` exists.
    ///
    /// # Errors
    ///
    /// [`EngineError::InvalidBucketName`] if the name is not legal. A legal
    /// name that is simply absent is `Ok(false)`, not an error.
    async fn bucket_exists(&self, bucket: &str) -> Result<bool, EngineError>;

    /// Every bucket, ordered by name.
    ///
    /// # Errors
    ///
    /// [`EngineError::Io`] if the store cannot be read.
    async fn list_buckets(&self) -> Result<Vec<BucketInfo>, EngineError>;

    /// Store `body` at `key`, replacing whatever was there.
    ///
    /// # Errors
    ///
    /// [`EngineError::NoSuchBucket`], or [`EngineError::Io`] including any
    /// error the body stream itself yields.
    async fn put_object(
        &self,
        bucket: &str,
        key: &str,
        body: BoxByteStream,
        opts: PutOpts,
    ) -> Result<ObjectInfo, EngineError>;

    /// Read `key`, optionally only `range` of it.
    ///
    /// Returns the object's full metadata together with the offset and length
    /// actually being sent, which is what a `206` response has to report, and
    /// the body stream for exactly that span.
    ///
    /// # Errors
    ///
    /// [`EngineError::NoSuchBucket`], [`EngineError::NoSuchKey`], or
    /// [`EngineError::InvalidRange`] if `range` does not overlap the object.
    async fn get_object(
        &self,
        bucket: &str,
        key: &str,
        range: Option<ByteRange>,
    ) -> Result<(ObjectInfo, u64, u64, BoxByteStream), EngineError>;

    /// Metadata for `key`, without its body.
    ///
    /// # Errors
    ///
    /// [`EngineError::NoSuchBucket`] or [`EngineError::NoSuchKey`].
    async fn head_object(&self, bucket: &str, key: &str) -> Result<ObjectInfo, EngineError>;

    /// Delete `key`.
    ///
    /// Deleting a key that is not there succeeds, as it does in S3.
    ///
    /// # Errors
    ///
    /// [`EngineError::NoSuchBucket`], or [`EngineError::Io`].
    async fn delete_object(&self, bucket: &str, key: &str) -> Result<(), EngineError>;

    /// One page of the bucket's keys.
    ///
    /// # Errors
    ///
    /// [`EngineError::NoSuchBucket`], or [`EngineError::Io`].
    async fn list_objects_v2(
        &self,
        bucket: &str,
        p: &ListParams,
    ) -> Result<ListResult, EngineError>;
}
