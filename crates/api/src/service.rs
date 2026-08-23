// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The `s3s` service that fronts the storage engine.
//!
//! [`Aks3`] is the whole S3 API surface: `s3s` routes a parsed request to one of
//! the [`S3`](s3s::S3) trait methods, and this type answers it by calling the
//! [`ObjectLayer`] it holds. It is generic over the layer rather than taking a
//! `dyn ObjectLayer` so the calls stay static, and it holds an [`Arc`] because
//! `s3s` shares one service across every connection.
//!
//! Every method in [`S3`](s3s::S3) has a default body that returns
//! `NotImplemented`, so only the operations implemented here are answered; the
//! rest are rejected until they are filled in. The bucket and object
//! operations are in place, as is `ListObjectsV2`; the multipart family is not.
//!
//! A handler does three things and nothing else: unwrap the input, call the
//! engine, and shape the answer. It never decides what an error means (that is
//! [`map_engine_err`]'s job) and never touches the disk, which keeps the whole
//! S3 vocabulary of locations, owners and wire timestamps on this side of the
//! boundary and out of the engine.
//!
//! # Bodies
//!
//! An object body is never collected here. A `PUT` hands the engine the request
//! stream and a `GET` hands `s3s` the engine's, so the largest thing this layer
//! holds at once is one chunk, whatever the object's size.

use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aks3_engine::{BoxByteStream, ByteRange, ListParams, ObjectInfo, ObjectLayer, PutOpts};
use futures::{Stream, StreamExt as _, TryStreamExt as _};
use s3s::dto::{
    Bucket, CommonPrefix, ContentLength, CreateBucketInput, CreateBucketOutput, DeleteBucketInput,
    DeleteBucketOutput, DeleteObjectInput, DeleteObjectOutput, ETag, GetObjectInput,
    GetObjectOutput, HeadBucketInput, HeadBucketOutput, HeadObjectInput, HeadObjectOutput,
    KeyCount, ListBucketsInput, ListBucketsOutput, ListObjectsV2Input, ListObjectsV2Output,
    MaxKeys, Object, Owner, PutObjectInput, PutObjectOutput, Range as DtoRange, StreamingBlob,
    Timestamp,
};
use s3s::header::{CONTENT_ENCODING, X_AMZ_CONTENT_SHA256, X_AMZ_DECODED_CONTENT_LENGTH};
use s3s::{s3_error, S3Request, S3Response, S3Result};

use crate::error::map_engine_err;

/// The single owner every bucket in an aks3 store belongs to.
///
/// S3 reports an owner on `ListBuckets`, and clients key off the id. aks3 has
/// no notion of accounts yet, so the store answers with one fixed identity
/// rather than omitting the field and leaving clients to guess.
const OWNER_ID: &str = "aks3-root";
const OWNER_DISPLAY_NAME: &str = "aks3";

/// What `Accept-Ranges` says on a read: the engine serves byte ranges, and
/// clients that download in parallel decide whether to try from this header.
const ACCEPT_RANGES: &str = "bytes";

/// The S3 service: one storage engine, dressed as the S3 API.
pub struct Aks3<L: ObjectLayer> {
    engine: Arc<L>,
}

impl<L: ObjectLayer> Aks3<L> {
    /// Wrap `engine` in a service `s3s` can serve.
    #[must_use]
    pub fn new(engine: Arc<L>) -> Self {
        Self { engine }
    }
}

/// The last instant an S3 timestamp can express: 9999-12-31T23:59:59Z, in
/// milliseconds since the epoch.
const MAX_TIMESTAMP_MS: u64 = 253_402_300_799_000;

/// The engine's epoch milliseconds as the `SystemTime` the wire format wants,
/// saturating at [`MAX_TIMESTAMP_MS`].
///
/// The clamp is not decoration. A bucket's creation time is read back from a
/// manifest on disk, so it is an input, and converting a `SystemTime` past year
/// 9999 into a wire [`Timestamp`] panics. A nonsense number in a manifest has to
/// produce a nonsense date, not a dead request handler.
fn epoch_ms_to_systemtime(ms: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(ms.min(MAX_TIMESTAMP_MS)))
        .unwrap_or(UNIX_EPOCH)
}

/// The client's `Range` as the engine's, form for form.
///
/// Neither side interprets it here: which bytes `bytes=5-` names depends on a
/// size only the engine knows, so an unsatisfiable range comes back as
/// [`EngineError::InvalidRange`](aks3_engine::EngineError::InvalidRange)
/// rather than being caught on the way in.
fn to_byte_range(range: &DtoRange) -> ByteRange {
    match *range {
        DtoRange::Int {
            first,
            last: Some(last),
        } => ByteRange::FromTo(first, last),
        DtoRange::Int { first, last: None } => ByteRange::From(first),
        DtoRange::Suffix { length } => ByteRange::Suffix(length),
    }
}

/// The `Content-Range` of a partial read: the span actually being sent, then
/// the size of the whole object.
///
/// A resolved range covers at least one byte, so the last offset never
/// underflows; the saturating form keeps that true whatever an engine returns.
fn content_range(offset: u64, len: u64, size: u64) -> String {
    let last = offset.saturating_add(len).saturating_sub(1);
    format!("bytes {offset}-{last}/{size}")
}

/// The most keys one page of a listing carries, which is also the number S3
/// uses when the client names none.
const MAX_KEYS_PER_PAGE: MaxKeys = 1000;

/// The page budget a listing request asks for, made usable.
///
/// An absent `max-keys` is S3's default, and a larger one is capped: the number
/// decides how much of a bucket a single request makes the engine hold, so it
/// is not the client's to raise. A negative number is not a legal request and
/// becomes a request for nothing, which the engine answers with an empty page.
fn page_size(requested: Option<MaxKeys>) -> MaxKeys {
    requested
        .unwrap_or(MAX_KEYS_PER_PAGE)
        .clamp(0, MAX_KEYS_PER_PAGE)
}

/// A byte count as the wire's signed one.
///
/// The clamp is unreachable in practice: no object is 8 exabytes, and one that
/// was could not be stored. It is here so the conversion is total.
fn wire_length(len: u64) -> ContentLength {
    ContentLength::try_from(len).unwrap_or(ContentLength::MAX)
}

/// An engine body stream, made `Sync` so `s3s` can carry it.
///
/// [`BoxByteStream`] is `Send` but not `Sync`, and [`StreamingBlob`] demands
/// both. Polling a stream needs `&mut` and so happens on one task at a time
/// regardless; the mutex is what says that in the type system, and costs an
/// uncontended lock per chunk to do it without `unsafe`.
struct SyncStream(Mutex<BoxByteStream>);

impl Stream for SyncStream {
    type Item = Result<bytes::Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // A poll that panicked poisons the lock. The stream behind it is no
        // less usable for that, and refusing to read it would turn one failed
        // request into a permanently stuck body, so the poison is ignored.
        let mut inner = self
            .get_mut()
            .0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        inner.poll_next_unpin(cx)
    }
}

/// The `x-amz-content-sha256` values that put a request body in aws-chunked
/// framing.
///
/// `s3s` decides whether a body is chunked from this header alone: it builds an
/// `AwsChunkedStream` and decodes the framing only when the value is one of
/// these, and passes the body through untouched for anything else.
///
/// This list must mirror `s3s`'s `AmzContentSha256::is_streaming()` (all five
/// variants it returns `true` for): the two ECDSA (`SigV4A`) sentinels are
/// inert today because `s3s` 0.14 answers them `NotImplemented` before a
/// handler runs, but listing them means that if a later `s3s` starts decoding
/// them this guard keeps passing those legitimate uploads rather than turning
/// them into false-positive `400`s.
const STREAMING_SENTINELS: [&str; 5] = [
    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD",
    "STREAMING-AWS4-HMAC-SHA256-PAYLOAD-TRAILER",
    "STREAMING-UNSIGNED-PAYLOAD-TRAILER",
    "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD",
    "STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER",
];

/// Whether a `Content-Encoding` names `aws-chunked` among its tokens.
///
/// The header is a comma-separated list, so `gzip, aws-chunked` announces the
/// framing as much as `aws-chunked` on its own does. The comparison ignores
/// case and surrounding space because header tokens carry neither meaning.
fn declares_aws_chunked(value: &str) -> bool {
    value
        .split(',')
        .any(|token| token.trim().eq_ignore_ascii_case("aws-chunked"))
}

/// Refuse a `PUT` that announces aws-chunked framing without a sentinel that
/// tells `s3s` to decode it.
///
/// The framing of an aws-chunked body is a contract three headers carry:
/// `Content-Encoding: aws-chunked` and `x-amz-decoded-content-length` announce
/// it, and `x-amz-content-sha256` says how it was signed. `s3s` reads only the
/// last, decoding the body when it is a streaming sentinel and treating it as
/// opaque otherwise. When a request announces the encoding but signs a
/// non-streaming sentinel (for instance `UNSIGNED-PAYLOAD`) the two disagree,
/// and `s3s` hands the raw chunk envelope (`3f\r\n...0\r\n\r\n`) to the engine
/// as the object's bytes: the object is stored longer than the body, under an
/// etag over the framing, and the request is answered `200`. The corruption
/// surfaces only when the object is read back.
///
/// Rejecting the request is the safe answer. Decoding it instead would mean
/// guessing which of the two disagreeing halves the client meant, where
/// refusing needs no guess.
fn reject_contradictory_aws_chunked(headers: &http::HeaderMap) -> S3Result<()> {
    let announces_chunked = headers
        .get(CONTENT_ENCODING)
        .and_then(|value| value.to_str().ok())
        .is_some_and(declares_aws_chunked);
    let has_decoded_length = headers.contains_key(X_AMZ_DECODED_CONTENT_LENGTH);

    // Nothing announced the framing, so there is no disagreement to catch and
    // the ordinary paths (a plain body, a signed single-chunk hash) fall
    // straight through.
    if !announces_chunked && !has_decoded_length {
        return Ok(());
    }

    let is_streaming = headers
        .get(X_AMZ_CONTENT_SHA256)
        .and_then(|value| value.to_str().ok())
        .is_some_and(|sentinel| {
            STREAMING_SENTINELS
                .iter()
                .any(|streaming| sentinel.eq_ignore_ascii_case(streaming))
        });

    // The legitimate chunked paths land here: the encoding is announced and the
    // sentinel agrees, so `s3s` will decode the framing and the body reaches the
    // engine as the object it is.
    if is_streaming {
        return Ok(());
    }

    Err(s3_error!(
        InvalidRequest,
        "Content-Encoding: aws-chunked or x-amz-decoded-content-length declares \
         aws-chunked framing, but x-amz-content-sha256 is not a streaming payload \
         sentinel, so the framing would be stored as the object's bytes"
    ))
}

#[async_trait::async_trait]
impl<L: ObjectLayer> s3s::S3 for Aks3<L> {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = req.input.bucket;
        self.engine
            .create_bucket(&bucket)
            .await
            .map_err(map_engine_err)?;
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(format!("/{bucket}")),
        }))
    }

    /// A `HEAD` has no body to carry an error in, so the answer is carried
    /// entirely by the status: the `NoSuchBucket` error below becomes the 404.
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        if self
            .engine
            .bucket_exists(&req.input.bucket)
            .await
            .map_err(map_engine_err)?
        {
            Ok(S3Response::new(HeadBucketOutput::default()))
        } else {
            Err(s3_error!(NoSuchBucket))
        }
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.engine
            .delete_bucket(&req.input.bucket)
            .await
            .map_err(map_engine_err)?;
        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    /// Lists every bucket in the store. The engine orders them by name, and
    /// that order is passed through: S3 clients display the list as given.
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.engine.list_buckets().await.map_err(map_engine_err)?;
        let buckets = buckets
            .into_iter()
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(Timestamp::from(epoch_ms_to_systemtime(b.created_epoch_ms))),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(Owner {
                display_name: Some(OWNER_DISPLAY_NAME.to_owned()),
                id: Some(OWNER_ID.to_owned()),
            }),
            ..Default::default()
        }))
    }

    /// Stores the request body under the key, streaming it straight through to
    /// the engine.
    ///
    /// The etag goes back bare: [`ETag`] is a validator, not a string, and
    /// `s3s` writes the quotes S3 spells it with when it renders the header.
    /// Quoting it here too would put two pairs on the wire.
    async fn put_object(
        &self,
        req: S3Request<PutObjectInput>,
    ) -> S3Result<S3Response<PutObjectOutput>> {
        // Caught before the body is touched: a request that declares aws-chunked
        // framing without a sentinel `s3s` decodes would otherwise store the
        // chunk envelope as the object's bytes under a 200.
        reject_contradictory_aws_chunked(&req.headers)?;

        let input = req.input;
        // `s3s` makes the body optional because a malformed request can arrive
        // without one. An empty object is a body of zero bytes, not this.
        let Some(body) = input.body else {
            return Err(s3_error!(IncompleteBody));
        };
        let opts = PutOpts {
            content_type: input.content_type,
            user_metadata: input.metadata.unwrap_or_default().into_iter().collect(),
        };

        let info = self
            .engine
            .put_object(
                &input.bucket,
                &input.key,
                body.map_err(std::io::Error::other).boxed(),
                opts,
            )
            .await
            .map_err(map_engine_err)?;

        Ok(S3Response::new(PutObjectOutput {
            e_tag: Some(ETag::Strong(info.etag)),
            ..Default::default()
        }))
    }

    /// Reads the key, or the range of it the client asked for.
    ///
    /// A `Content-Range` is set only when the request carried a range, because
    /// setting it is what makes `s3s` answer `206` instead of `200`: a whole
    /// object reported as a partial read would have clients reassembling a
    /// file they already have.
    async fn get_object(
        &self,
        req: S3Request<GetObjectInput>,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let input = req.input;
        let range = input.range.as_ref().map(to_byte_range);

        let (info, offset, len, body) = self
            .engine
            .get_object(&input.bucket, &input.key, range)
            .await
            .map_err(map_engine_err)?;
        let ObjectInfo {
            size,
            etag,
            content_type,
            mtime_epoch_ms,
            user_metadata,
            ..
        } = info;

        Ok(S3Response::new(GetObjectOutput {
            body: Some(StreamingBlob::wrap(SyncStream(Mutex::new(body)))),
            accept_ranges: Some(ACCEPT_RANGES.to_owned()),
            // The length of what is being sent, which is the span for a ranged
            // read and the whole object otherwise.
            content_length: Some(wire_length(len)),
            content_range: range.map(|_| content_range(offset, len, size)),
            content_type: Some(content_type),
            e_tag: Some(ETag::Strong(etag)),
            last_modified: Some(Timestamp::from(epoch_ms_to_systemtime(mtime_epoch_ms))),
            metadata: Some(user_metadata.into_iter().collect()),
            ..Default::default()
        }))
    }

    /// Everything a `GET` would report except the bytes.
    ///
    /// A `HEAD` has no body to carry an error in, so an absent key is answered
    /// by the `NoSuchKey` the engine reports becoming a 404.
    async fn head_object(
        &self,
        req: S3Request<HeadObjectInput>,
    ) -> S3Result<S3Response<HeadObjectOutput>> {
        let input = req.input;
        let info = self
            .engine
            .head_object(&input.bucket, &input.key)
            .await
            .map_err(map_engine_err)?;
        let ObjectInfo {
            size,
            etag,
            content_type,
            mtime_epoch_ms,
            user_metadata,
            ..
        } = info;

        Ok(S3Response::new(HeadObjectOutput {
            accept_ranges: Some(ACCEPT_RANGES.to_owned()),
            content_length: Some(wire_length(size)),
            content_type: Some(content_type),
            e_tag: Some(ETag::Strong(etag)),
            last_modified: Some(Timestamp::from(epoch_ms_to_systemtime(mtime_epoch_ms))),
            metadata: Some(user_metadata.into_iter().collect()),
            ..Default::default()
        }))
    }

    /// Removes the key. Deleting one that is not there succeeds, as it does in
    /// S3: the request names an end state, and that state already holds.
    async fn delete_object(
        &self,
        req: S3Request<DeleteObjectInput>,
    ) -> S3Result<S3Response<DeleteObjectOutput>> {
        self.engine
            .delete_object(&req.input.bucket, &req.input.key)
            .await
            .map_err(map_engine_err)?;
        Ok(S3Response::new(DeleteObjectOutput::default()))
    }

    /// One page of the bucket's keys, in ascending order.
    ///
    /// The request is repeated back beside the page. S3 clients match a
    /// response to the query that produced it, and a paging client sends the
    /// prefix and delimiter it was given back on the next request, so a listing
    /// that dropped them would have the client paging over a different query
    /// than it started.
    ///
    /// The count reported is objects plus common prefixes, not objects alone:
    /// a folded prefix fills a slot of the budget in the engine, and reporting
    /// only the objects would let the count exceed the `max-keys` that produced
    /// it, which is the one thing S3 promises about it.
    async fn list_objects_v2(
        &self,
        req: S3Request<ListObjectsV2Input>,
    ) -> S3Result<S3Response<ListObjectsV2Output>> {
        let input = req.input;
        let max_keys = page_size(input.max_keys);
        // The query moves into the params and is echoed back out of them
        // afterwards, so nothing here is cloned to be said twice.
        let params = ListParams {
            prefix: input.prefix,
            delimiter: input.delimiter,
            continuation_token: input.continuation_token,
            start_after: input.start_after,
            // Non-negative after the clamp, so the fallback never runs.
            max_keys: usize::try_from(max_keys).unwrap_or(0),
        };

        let page = self
            .engine
            .list_objects_v2(&input.bucket, &params)
            .await
            .map_err(map_engine_err)?;

        let contents: Vec<Object> = page
            .objects
            .into_iter()
            .map(|o| Object {
                key: Some(o.key),
                size: Some(wire_length(o.size)),
                // Bare, as everywhere else: `s3s` writes the quotes S3 spells
                // an etag with when it renders the listing.
                e_tag: Some(ETag::Strong(o.etag)),
                last_modified: Some(Timestamp::from(epoch_ms_to_systemtime(o.mtime_epoch_ms))),
                ..Default::default()
            })
            .collect();
        let common_prefixes: Vec<CommonPrefix> = page
            .common_prefixes
            .into_iter()
            .map(|prefix| CommonPrefix {
                prefix: Some(prefix),
            })
            .collect();
        // Bounded by the budget, which is bounded by MAX_KEYS_PER_PAGE, so the
        // fallback never runs; it is here so the conversion is total.
        let key_count =
            KeyCount::try_from(contents.len() + common_prefixes.len()).unwrap_or(MAX_KEYS_PER_PAGE);

        Ok(S3Response::new(ListObjectsV2Output {
            name: Some(input.bucket),
            prefix: params.prefix,
            delimiter: params.delimiter,
            max_keys: Some(max_keys),
            key_count: Some(key_count),
            continuation_token: params.continuation_token,
            start_after: params.start_after,
            is_truncated: Some(page.is_truncated),
            next_continuation_token: page.next_continuation_token,
            // Both lists are always present, empty when nothing matched: an
            // absent list and an empty one mean the same thing here, and only
            // one of them makes a client handle two cases.
            contents: Some(contents),
            common_prefixes: Some(common_prefixes),
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{
        declares_aws_chunked, epoch_ms_to_systemtime, page_size, reject_contradictory_aws_chunked,
        Aks3, STREAMING_SENTINELS,
    };
    use aks3_engine::FsEngine;
    use http::header::{HeaderName, HeaderValue};
    use s3s::dto::{
        CreateBucketInput, DeleteBucketInput, DeleteObjectInput, ETag, GetObjectInput,
        HeadBucketInput, HeadObjectInput, ListBucketsInput, ListObjectsV2Input,
        ListObjectsV2Output, Metadata, PutObjectInput, Range, StreamingBlob, Timestamp,
    };
    use s3s::header::{CONTENT_ENCODING, X_AMZ_CONTENT_SHA256, X_AMZ_DECODED_CONTENT_LENGTH};
    use s3s::{S3ErrorCode, S3Request, S3};
    use std::sync::Arc;

    /// A service over a fresh engine, plus the temp dir it lives in. The dir is
    /// returned because dropping it deletes the store out from under the engine.
    async fn svc() -> (tempfile::TempDir, Aks3<FsEngine>) {
        let dir = tempfile::tempdir().expect("temp dir");
        let engine = FsEngine::open(dir.path()).await.expect("open engine");
        (dir, Aks3::new(Arc::new(engine)))
    }

    /// `s3s` builds an [`S3Request`] from a parsed HTTP request; calling the
    /// trait directly means building one by hand. `s3s` 0.14 exposes no
    /// constructor, so this fills the public fields: everything but the input is
    /// request context that a bucket handler never reads.
    fn req<T>(input: T) -> S3Request<T> {
        S3Request {
            input,
            method: http::Method::default(),
            uri: http::Uri::default(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn create(bucket: &str) -> S3Request<CreateBucketInput> {
        req(CreateBucketInput {
            bucket: bucket.to_owned(),
            ..Default::default()
        })
    }

    fn head(bucket: &str) -> S3Request<HeadBucketInput> {
        req(HeadBucketInput {
            bucket: bucket.to_owned(),
            ..Default::default()
        })
    }

    fn delete(bucket: &str) -> S3Request<DeleteBucketInput> {
        req(DeleteBucketInput {
            bucket: bucket.to_owned(),
            ..Default::default()
        })
    }

    /// A one-chunk request body.
    fn blob(bytes: &'static [u8]) -> StreamingBlob {
        StreamingBlob::wrap(futures::stream::iter([Ok::<_, std::io::Error>(
            bytes::Bytes::from_static(bytes),
        )]))
    }

    /// `PUT buk/<key>` carrying `bytes`, with no declared type or metadata.
    fn put(key: &str, bytes: &'static [u8]) -> S3Request<PutObjectInput> {
        req(PutObjectInput {
            bucket: "buk".to_owned(),
            key: key.to_owned(),
            body: Some(blob(bytes)),
            ..Default::default()
        })
    }

    /// A header map built from name/value pairs, for the aws-chunked checks.
    fn header_map(pairs: &[(HeaderName, &str)]) -> http::HeaderMap {
        let mut map = http::HeaderMap::new();
        for (name, value) in pairs {
            map.insert(
                name.clone(),
                HeaderValue::from_str(value).expect("header value"),
            );
        }
        map
    }

    /// `PUT buk/<key>` carrying `bytes`, with the given request headers set.
    fn put_with_headers(
        key: &str,
        bytes: &'static [u8],
        pairs: &[(HeaderName, &str)],
    ) -> S3Request<PutObjectInput> {
        let mut request = put(key, bytes);
        request.headers = header_map(pairs);
        request
    }

    fn get(key: &str) -> S3Request<GetObjectInput> {
        req(GetObjectInput {
            bucket: "buk".to_owned(),
            key: key.to_owned(),
            ..Default::default()
        })
    }

    fn get_range(key: &str, range: Range) -> S3Request<GetObjectInput> {
        req(GetObjectInput {
            bucket: "buk".to_owned(),
            key: key.to_owned(),
            range: Some(range),
            ..Default::default()
        })
    }

    fn head_object_req(key: &str) -> S3Request<HeadObjectInput> {
        req(HeadObjectInput {
            bucket: "buk".to_owned(),
            key: key.to_owned(),
            ..Default::default()
        })
    }

    fn delete_object_req(key: &str) -> S3Request<DeleteObjectInput> {
        req(DeleteObjectInput {
            bucket: "buk".to_owned(),
            key: key.to_owned(),
            ..Default::default()
        })
    }

    /// A `ListObjectsV2` over `buk`, with `params` supplying everything else.
    fn list(params: ListObjectsV2Input) -> S3Request<ListObjectsV2Input> {
        req(ListObjectsV2Input {
            bucket: "buk".to_owned(),
            ..params
        })
    }

    /// A bucket named `buk` holding a one-byte object at each of `keys`.
    async fn seeded(keys: &[&str]) -> (tempfile::TempDir, Aks3<FsEngine>) {
        let (dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        for key in keys {
            s.put_object(put(key, b"1")).await.expect("put");
        }
        (dir, s)
    }

    /// The keys a listing reported, in the order it reported them.
    fn keys_of(out: &ListObjectsV2Output) -> Vec<String> {
        out.contents
            .as_ref()
            .expect("contents")
            .iter()
            .map(|o| o.key.clone().expect("key"))
            .collect()
    }

    /// The common prefixes a listing reported, in order.
    fn prefixes_of(out: &ListObjectsV2Output) -> Vec<String> {
        out.common_prefixes
            .as_ref()
            .expect("common prefixes")
            .iter()
            .map(|p| p.prefix.clone().expect("prefix"))
            .collect()
    }

    /// The etag of `hello world`, as the engine spells it: bare lowercase hex.
    fn hello_world_etag() -> ETag {
        ETag::Strong("5eb63bbbe01eeed093cb22bb8f5acdc3".to_owned())
    }

    /// Drain a response body to the bytes it carried.
    async fn drain(body: Option<StreamingBlob>) -> Vec<u8> {
        use futures::StreamExt as _;

        let mut body = body.expect("a body");
        let mut out = Vec::new();
        while let Some(chunk) = body.next().await {
            out.extend_from_slice(&chunk.expect("chunk"));
        }
        out
    }

    /// The service has to satisfy `s3s`'s bounds to be servable at all: `S3`
    /// itself, and the `Send + Sync + 'static` it requires.
    #[test]
    fn service_is_a_servable_s3_impl() {
        const fn assert_s3<T: s3s::S3 + Send + Sync + 'static>() {}
        assert_s3::<Aks3<FsEngine>>();
    }

    /// The whole bucket lifecycle over the trait: create, head, list, delete,
    /// and a head that now fails.
    #[tokio::test]
    async fn bucket_ops_via_s3_trait() {
        let (_dir, s) = svc().await;

        s.create_bucket(create("buk")).await.expect("create");
        s.head_bucket(head("buk")).await.expect("head");

        let out = s
            .list_buckets(req(ListBucketsInput::default()))
            .await
            .expect("list");
        assert_eq!(out.output.buckets.expect("buckets").len(), 1);

        s.delete_bucket(delete("buk")).await.expect("delete");
        assert!(s.head_bucket(head("buk")).await.is_err());
    }

    /// The service must act on the engine it was handed, not on one of its own:
    /// a bucket created through the trait is visible on the caller's handle.
    #[tokio::test]
    async fn new_keeps_the_engine_it_was_given() {
        use aks3_engine::ObjectLayer;

        let dir = tempfile::tempdir().expect("temp dir");
        let engine = Arc::new(FsEngine::open(dir.path()).await.expect("open engine"));
        let service = Aks3::new(Arc::clone(&engine));

        service.create_bucket(create("buk")).await.expect("create");
        assert!(engine.bucket_exists("buk").await.expect("exists"));
    }

    /// `CreateBucket` answers with the bucket's location, which is the name
    /// after a slash.
    #[tokio::test]
    async fn create_reports_the_location() {
        let (_dir, s) = svc().await;
        let out = s.create_bucket(create("buk")).await.expect("create");
        assert_eq!(out.output.location.as_deref(), Some("/buk"));
    }

    /// Heading a bucket that was never created is `NoSuchBucket`, not a generic
    /// failure: clients use the code to tell "absent" from "broken".
    #[tokio::test]
    async fn head_of_a_missing_bucket_is_no_such_bucket() {
        let (_dir, s) = svc().await;
        let err = s.head_bucket(head("buk")).await.expect_err("missing");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchBucket);
    }

    /// Re-creating your own bucket is `BucketAlreadyOwnedByYou`, the code the
    /// engine's `BucketAlreadyExists` maps to.
    #[tokio::test]
    async fn creating_a_bucket_twice_reports_already_owned() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        let err = s.create_bucket(create("buk")).await.expect_err("duplicate");
        assert_eq!(*err.code(), S3ErrorCode::BucketAlreadyOwnedByYou);
    }

    /// Deleting a bucket that is not there is an error, unlike deleting an
    /// absent key.
    #[tokio::test]
    async fn deleting_a_missing_bucket_is_no_such_bucket() {
        let (_dir, s) = svc().await;
        let err = s.delete_bucket(delete("buk")).await.expect_err("missing");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchBucket);
    }

    /// A name the engine rejects reaches the client as `InvalidBucketName`
    /// rather than as an internal error.
    #[tokio::test]
    async fn an_illegal_name_is_rejected_by_code() {
        let (_dir, s) = svc().await;
        let err = s.create_bucket(create("A")).await.expect_err("illegal");
        assert_eq!(*err.code(), S3ErrorCode::InvalidBucketName);
    }

    /// Every listed bucket carries its name and a creation date, and the
    /// listing names an owner: some clients index the response by owner id.
    #[tokio::test]
    async fn list_buckets_reports_names_dates_and_owner() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("alpha")).await.expect("create");
        s.create_bucket(create("bravo")).await.expect("create");

        let out = s
            .list_buckets(req(ListBucketsInput::default()))
            .await
            .expect("list")
            .output;

        let buckets = out.buckets.expect("buckets");
        let names: Vec<_> = buckets
            .iter()
            .map(|b| b.name.clone().expect("name"))
            .collect();
        assert_eq!(names, ["alpha", "bravo"]);
        for b in &buckets {
            let created = b.creation_date.clone().expect("creation date");
            assert!(created > Timestamp::from(std::time::UNIX_EPOCH));
        }

        let owner = out.owner.expect("owner");
        assert_eq!(owner.id.as_deref(), Some("aks3-root"));
        assert_eq!(owner.display_name.as_deref(), Some("aks3"));
    }

    /// Milliseconds are milliseconds: the conversion keeps sub-second
    /// precision rather than truncating to whole seconds.
    #[test]
    fn epoch_ms_converts_including_the_fraction() {
        let t = epoch_ms_to_systemtime(1_500);
        let d = t
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch");
        assert_eq!(d, std::time::Duration::from_millis(1_500));
        assert_eq!(epoch_ms_to_systemtime(0), std::time::UNIX_EPOCH);
    }

    /// The conversion a listing performs must survive any number a manifest
    /// can hold: `Timestamp::from` panics past year 9999, so the clamp is what
    /// keeps a corrupt creation time from killing the request.
    #[test]
    fn an_out_of_range_timestamp_is_clamped_rather_than_panicking() {
        let clamped = Timestamp::from(epoch_ms_to_systemtime(u64::MAX));
        assert_eq!(
            clamped,
            Timestamp::from(epoch_ms_to_systemtime(super::MAX_TIMESTAMP_MS))
        );
        assert!(clamped > Timestamp::from(epoch_ms_to_systemtime(0)));
    }

    /// The whole object lifecycle over the trait: put, get, delete, and a head
    /// that now fails.
    #[tokio::test]
    async fn object_roundtrip_via_s3_trait() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");

        let put = s
            .put_object(req(PutObjectInput {
                bucket: "buk".to_owned(),
                key: "greet/hi.txt".to_owned(),
                body: Some(blob(b"hello world")),
                content_type: Some("text/plain".to_owned()),
                ..Default::default()
            }))
            .await
            .expect("put")
            .output;
        assert_eq!(put.e_tag, Some(hello_world_etag()));

        let got = s.get_object(get("greet/hi.txt")).await.expect("get").output;
        assert_eq!(got.content_length, Some(11));
        assert_eq!(got.content_type.as_deref(), Some("text/plain"));
        assert_eq!(drain(got.body).await, b"hello world");

        s.delete_object(delete_object_req("greet/hi.txt"))
            .await
            .expect("delete");
        assert!(s
            .head_object(head_object_req("greet/hi.txt"))
            .await
            .is_err());
    }

    /// The etag reaches the wire quoted, as S3 spells it, on every operation
    /// that reports one: clients compare the two against each other.
    #[tokio::test]
    async fn every_operation_quotes_the_etag() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        s.put_object(put("k", b"hello world")).await.expect("put");

        let head = s.head_object(head_object_req("k")).await.expect("head");
        assert_eq!(head.output.e_tag, Some(hello_world_etag()));
        let got = s.get_object(get("k")).await.expect("get");
        assert_eq!(got.output.e_tag, Some(hello_world_etag()));

        // `s3s` writes the quotes, so the etag must reach it bare: quoting it
        // here as well would put two pairs on the wire.
        let header = hello_world_etag().to_http_header().expect("etag header");
        assert_eq!(header.as_bytes(), b"\"5eb63bbbe01eeed093cb22bb8f5acdc3\"");
    }

    /// A ranged `GET` sends only the requested span and says which span it is:
    /// `s3s` turns the content range into the `206` the client is waiting for.
    #[tokio::test]
    async fn a_ranged_get_reports_and_sends_only_that_span() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        s.put_object(put("k", b"hello world")).await.expect("put");

        let got = s
            .get_object(get_range(
                "k",
                Range::Int {
                    first: 6,
                    last: Some(10),
                },
            ))
            .await
            .expect("get")
            .output;
        assert_eq!(got.content_range.as_deref(), Some("bytes 6-10/11"));
        assert_eq!(got.content_length, Some(5));
        assert_eq!(drain(got.body).await, b"world");
    }

    /// `bytes=6-` runs to the end of the object, and `bytes=-5` counts back
    /// from it: both forms have to reach the engine, not just the closed one.
    #[tokio::test]
    async fn open_ended_and_suffix_ranges_both_resolve() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        s.put_object(put("k", b"hello world")).await.expect("put");

        let open = s
            .get_object(get_range(
                "k",
                Range::Int {
                    first: 6,
                    last: None,
                },
            ))
            .await
            .expect("get")
            .output;
        assert_eq!(open.content_range.as_deref(), Some("bytes 6-10/11"));
        assert_eq!(drain(open.body).await, b"world");

        let suffix = s
            .get_object(get_range("k", Range::Suffix { length: 5 }))
            .await
            .expect("get")
            .output;
        assert_eq!(suffix.content_range.as_deref(), Some("bytes 6-10/11"));
        assert_eq!(drain(suffix.body).await, b"world");
    }

    /// A `GET` with no range is a plain `200`: leaving a content range out is
    /// what keeps `s3s` from answering a whole-object read with a `206`.
    #[tokio::test]
    async fn an_unranged_get_reports_no_content_range() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        s.put_object(put("k", b"hello world")).await.expect("put");

        let got = s.get_object(get("k")).await.expect("get").output;
        assert!(got.content_range.is_none());
        assert_eq!(got.content_length, Some(11));
    }

    /// A range the object cannot satisfy is `InvalidRange`, the engine's
    /// verdict passed through rather than an internal error.
    #[tokio::test]
    async fn an_unsatisfiable_range_is_invalid_range() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        s.put_object(put("k", b"hello world")).await.expect("put");

        let err = s
            .get_object(get_range(
                "k",
                Range::Int {
                    first: 5,
                    last: Some(2),
                },
            ))
            .await
            .expect_err("backwards range");
        assert_eq!(*err.code(), S3ErrorCode::InvalidRange);
    }

    /// Reading a key that is not there is `NoSuchKey`, not a generic failure.
    #[tokio::test]
    async fn getting_a_missing_key_is_no_such_key() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        let err = s.get_object(get("k")).await.expect_err("missing");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    }

    /// A `PUT` whose bucket does not exist fails before anything is stored.
    #[tokio::test]
    async fn putting_into_a_missing_bucket_is_no_such_bucket() {
        let (_dir, s) = svc().await;
        let err = s.put_object(put("k", b"x")).await.expect_err("missing");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchBucket);
    }

    /// A key too long for the filesystem underneath is a 400 with a code that
    /// names the problem, not a 500 the client would keep retrying. The `GET`
    /// is checked alongside it because answering that one with `NoSuchKey`
    /// would contradict the `PUT` that just refused the same key.
    #[tokio::test]
    async fn a_key_over_the_filesystem_name_limit_is_key_too_long() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");

        let key = "n".repeat(300);
        let err = s
            .put_object(put(&key, b"x"))
            .await
            .expect_err("300-byte key component");
        assert_eq!(*err.code(), S3ErrorCode::KeyTooLongError);
        assert!(
            err.status_code().expect("a status").is_client_error(),
            "{:?}",
            err.status_code()
        );

        let err = s.get_object(get(&key)).await.expect_err("same key");
        assert_eq!(*err.code(), S3ErrorCode::KeyTooLongError);
    }

    /// `s3s` hands the handler an optional body, so a `PUT` can arrive with
    /// none. That is a malformed request, not an empty object.
    #[tokio::test]
    async fn a_put_without_a_body_is_incomplete_body() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");

        let err = s
            .put_object(req(PutObjectInput {
                bucket: "buk".to_owned(),
                key: "k".to_owned(),
                body: None,
                ..Default::default()
            }))
            .await
            .expect_err("no body");
        assert_eq!(*err.code(), S3ErrorCode::IncompleteBody);
    }

    /// `aws-chunked` is a token in a list, so it is recognised on its own, next
    /// to other encodings, and whatever the case; an unrelated encoding is not.
    #[test]
    fn aws_chunked_is_recognised_as_a_content_encoding_token() {
        assert!(declares_aws_chunked("aws-chunked"));
        assert!(declares_aws_chunked("gzip, aws-chunked"));
        assert!(declares_aws_chunked("AWS-Chunked"));
        assert!(declares_aws_chunked("  aws-chunked  "));
        assert!(!declares_aws_chunked("gzip"));
        assert!(!declares_aws_chunked(""));
    }

    /// The guard leaves alone every request that does not announce the framing:
    /// no headers at all, and a signed single-chunk hash on a plain body.
    #[test]
    fn a_request_that_does_not_announce_chunking_is_left_alone() {
        reject_contradictory_aws_chunked(&header_map(&[])).expect("no chunk headers");
        reject_contradictory_aws_chunked(&header_map(&[(
            X_AMZ_CONTENT_SHA256,
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
        )]))
        .expect("a plain signed body");
    }

    /// Each streaming sentinel, with the encoding announced, is a legitimate
    /// chunked upload and must pass. This covers the HMAC and unsigned-trailer
    /// sentinels a current boto3 sends, and the two ECDSA (`SigV4A`) sentinels
    /// that are inert under s3s 0.14 but must not become false positives if a
    /// later s3s decodes them: the guard's allowlist mirrors s3s's
    /// `is_streaming()`, and this asserts the ECDSA pair is in it.
    #[test]
    fn the_streaming_sentinels_are_accepted() {
        assert!(STREAMING_SENTINELS.contains(&"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD"));
        assert!(STREAMING_SENTINELS.contains(&"STREAMING-AWS4-ECDSA-P256-SHA256-PAYLOAD-TRAILER"));

        for sentinel in STREAMING_SENTINELS {
            reject_contradictory_aws_chunked(&header_map(&[
                (CONTENT_ENCODING, "aws-chunked"),
                (X_AMZ_DECODED_CONTENT_LENGTH, "63"),
                (X_AMZ_CONTENT_SHA256, sentinel),
            ]))
            .unwrap_or_else(|_| panic!("{sentinel} is a legitimate chunked upload"));
        }
    }

    /// The bug this fixes: `Content-Encoding: aws-chunked` signed with a
    /// non-streaming sentinel is refused rather than stored as its own framing.
    #[test]
    fn aws_chunked_with_a_non_streaming_sentinel_is_rejected() {
        let err = reject_contradictory_aws_chunked(&header_map(&[
            (CONTENT_ENCODING, "aws-chunked"),
            (X_AMZ_DECODED_CONTENT_LENGTH, "63"),
            (X_AMZ_CONTENT_SHA256, "UNSIGNED-PAYLOAD"),
        ]))
        .expect_err("contradictory request");
        assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);
    }

    /// Either announcing header alone is enough to be held to the sentinel:
    /// `x-amz-decoded-content-length` with a non-streaming hash is refused, and
    /// so is the decoded length with no `x-amz-content-sha256` at all.
    #[test]
    fn either_announcing_header_alone_is_held_to_the_sentinel() {
        let err = reject_contradictory_aws_chunked(&header_map(&[
            (X_AMZ_DECODED_CONTENT_LENGTH, "63"),
            (X_AMZ_CONTENT_SHA256, "UNSIGNED-PAYLOAD"),
        ]))
        .expect_err("decoded length without a streaming sentinel");
        assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);

        let err =
            reject_contradictory_aws_chunked(&header_map(&[(X_AMZ_DECODED_CONTENT_LENGTH, "63")]))
                .expect_err("decoded length with no sentinel");
        assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);
    }

    /// End to end through the trait: a contradictory chunked `PUT` is a 400 and
    /// stores nothing, so the chunk framing never becomes an object.
    #[tokio::test]
    async fn a_contradictory_chunked_put_is_rejected_and_stores_nothing() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");

        // The issue's request: 63 bytes of body wrapped in chunk framing,
        // declared aws-chunked but signed UNSIGNED-PAYLOAD.
        let framed =
            b"3f\r\nintegrity checksums are computed by default since botocore 1.36\r\n0\r\n\r\n";
        let err = s
            .put_object(put_with_headers(
                "framed",
                framed,
                &[
                    (CONTENT_ENCODING, "aws-chunked"),
                    (X_AMZ_DECODED_CONTENT_LENGTH, "63"),
                    (X_AMZ_CONTENT_SHA256, "UNSIGNED-PAYLOAD"),
                ],
            ))
            .await
            .expect_err("contradictory request");
        assert_eq!(*err.code(), S3ErrorCode::InvalidRequest);

        let err = s
            .head_object(head_object_req("framed"))
            .await
            .expect_err("nothing stored");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    }

    /// The legitimate trailer path is not caught by the guard: the encoding is
    /// announced and the sentinel agrees, so the `PUT` goes through.
    #[tokio::test]
    async fn a_legitimate_chunked_put_is_not_rejected() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");

        s.put_object(put_with_headers(
            "trailered",
            b"hello world",
            &[
                (CONTENT_ENCODING, "aws-chunked"),
                (X_AMZ_DECODED_CONTENT_LENGTH, "11"),
                (X_AMZ_CONTENT_SHA256, "STREAMING-UNSIGNED-PAYLOAD-TRAILER"),
            ],
        ))
        .await
        .expect("legitimate chunked put");

        let head = s
            .head_object(head_object_req("trailered"))
            .await
            .expect("head");
        assert_eq!(head.output.content_length, Some(11));
    }

    /// What a `PUT` declares comes back on both a `HEAD` and a `GET`: the
    /// content type, the user metadata, the size and a modification time.
    #[tokio::test]
    async fn user_metadata_and_content_type_survive_a_roundtrip() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");

        let metadata: Metadata = [("colour".to_owned(), "blue".to_owned())]
            .into_iter()
            .collect();
        s.put_object(req(PutObjectInput {
            bucket: "buk".to_owned(),
            key: "k".to_owned(),
            body: Some(blob(b"hello world")),
            content_type: Some("text/plain".to_owned()),
            metadata: Some(metadata.clone()),
            ..Default::default()
        }))
        .await
        .expect("put");

        let head = s
            .head_object(head_object_req("k"))
            .await
            .expect("head")
            .output;
        assert_eq!(head.content_length, Some(11));
        assert_eq!(head.content_type.as_deref(), Some("text/plain"));
        assert_eq!(head.metadata, Some(metadata.clone()));
        assert!(
            head.last_modified.expect("last modified") > Timestamp::from(std::time::UNIX_EPOCH)
        );

        let got = s.get_object(get("k")).await.expect("get").output;
        assert_eq!(got.metadata, Some(metadata));
        assert!(got.last_modified.expect("last modified") > Timestamp::from(std::time::UNIX_EPOCH));
    }

    /// An object stored without a declared type is served as the engine's
    /// default rather than as no type at all.
    #[tokio::test]
    async fn an_undeclared_content_type_becomes_the_default() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        s.put_object(put("k", b"hello world")).await.expect("put");

        let head = s.head_object(head_object_req("k")).await.expect("head");
        assert_eq!(
            head.output.content_type.as_deref(),
            Some("application/octet-stream")
        );
    }

    /// Heading a key that is not there is `NoSuchKey`. A `HEAD` has no body to
    /// carry the code in, so the status is the whole answer.
    #[tokio::test]
    async fn heading_a_missing_key_is_no_such_key() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        let err = s
            .head_object(head_object_req("k"))
            .await
            .expect_err("missing");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    }

    /// Deleting a key that was never there succeeds, as it does in S3: the
    /// request states an end state, and that state already holds.
    #[tokio::test]
    async fn deleting_a_missing_key_succeeds() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        s.delete_object(delete_object_req("k"))
            .await
            .expect("delete of an absent key");
    }

    /// Deleting from a bucket that does not exist is still an error: there is
    /// no end state to have reached.
    #[tokio::test]
    async fn deleting_from_a_missing_bucket_is_no_such_bucket() {
        let (_dir, s) = svc().await;
        let err = s
            .delete_object(delete_object_req("k"))
            .await
            .expect_err("missing bucket");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchBucket);
    }

    /// A body larger than one chunk has to arrive whole and hash as one
    /// object: nothing in the handler may buffer or reorder it.
    #[tokio::test]
    async fn a_multi_chunk_body_is_stored_as_one_object() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");

        let chunks = futures::stream::iter([
            Ok::<_, std::io::Error>(bytes::Bytes::from_static(b"hello ")),
            Ok(bytes::Bytes::from_static(b"world")),
        ]);
        let put = s
            .put_object(req(PutObjectInput {
                bucket: "buk".to_owned(),
                key: "k".to_owned(),
                body: Some(StreamingBlob::wrap(chunks)),
                ..Default::default()
            }))
            .await
            .expect("put")
            .output;
        assert_eq!(put.e_tag, Some(hello_world_etag()));

        let got = s.get_object(get("k")).await.expect("get").output;
        assert_eq!(drain(got.body).await, b"hello world");
    }

    /// A body that fails mid-stream is the client's failure to deliver, and it
    /// must not be reported as an object that was stored.
    #[tokio::test]
    async fn a_body_that_fails_mid_stream_fails_the_put() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");

        let chunks = futures::stream::iter([
            Ok(bytes::Bytes::from_static(b"hello ")),
            Err(std::io::Error::other("connection reset")),
        ]);
        s.put_object(req(PutObjectInput {
            bucket: "buk".to_owned(),
            key: "k".to_owned(),
            body: Some(StreamingBlob::wrap(chunks)),
            ..Default::default()
        }))
        .await
        .expect_err("truncated body");

        let err = s
            .head_object(head_object_req("k"))
            .await
            .expect_err("absent");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchKey);
    }

    /// Why [`SyncStream`] exists at all: `s3s` will only carry a body that is
    /// `Send` and `Sync`, and the engine's stream is only `Send`. If this ever
    /// compiles without the wrapper, the wrapper can go.
    #[test]
    fn the_body_wrapper_is_send_and_sync() {
        const fn assert_send_sync<T: Send + Sync>() {}
        assert_send_sync::<super::SyncStream>();
    }

    /// The wrapper is a passthrough: it must hand on what the engine yields,
    /// chunks and failures alike, in order and unchanged.
    #[tokio::test]
    async fn the_body_wrapper_forwards_chunks_and_errors() {
        use futures::StreamExt as _;

        let inner = futures::stream::iter([
            Ok(bytes::Bytes::from_static(b"ab")),
            Err(std::io::Error::other("read failed")),
        ])
        .boxed();
        let mut wrapped = super::SyncStream(std::sync::Mutex::new(inner));

        assert_eq!(
            wrapped.next().await.expect("a chunk").expect("ok"),
            bytes::Bytes::from_static(b"ab")
        );
        let err = wrapped.next().await.expect("an item").expect_err("failure");
        assert_eq!(err.to_string(), "read failed");
        assert!(wrapped.next().await.is_none());
    }

    /// An empty store lists no buckets, and says so with an empty list rather
    /// than by omitting the field.
    #[tokio::test]
    async fn an_empty_store_lists_an_empty_bucket_list() {
        let (_dir, s) = svc().await;
        let out = s
            .list_buckets(req(ListBucketsInput::default()))
            .await
            .expect("list")
            .output;
        assert_eq!(out.buckets.expect("buckets").len(), 0);
    }

    /// The budget a request turns into: S3's default when it names none, the
    /// number it named when that is usable, and nothing outside the range the
    /// engine will be asked for.
    #[test]
    fn the_page_budget_defaults_and_clamps() {
        assert_eq!(page_size(None), 1000);
        assert_eq!(page_size(Some(7)), 7);
        assert_eq!(page_size(Some(0)), 0);
        assert_eq!(page_size(Some(1000)), 1000);
        assert_eq!(page_size(Some(5_000)), 1000);
        assert_eq!(page_size(Some(i32::MAX)), 1000);
        assert_eq!(page_size(Some(-1)), 0);
        assert_eq!(page_size(Some(i32::MIN)), 0);
    }

    /// A delimited listing over the trait: a top-level key is reported as
    /// itself, and everything below a folder collapses into one prefix.
    #[tokio::test]
    async fn list_v2_via_s3_trait() {
        let (_dir, s) = seeded(&["a", "d/x", "d/y"]).await;

        let out = s
            .list_objects_v2(list(ListObjectsV2Input {
                delimiter: Some("/".to_owned()),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;

        assert_eq!(keys_of(&out), ["a"]);
        assert_eq!(prefixes_of(&out), ["d/"]);
    }

    /// Every listed object carries what a client indexes on: its key, its
    /// size, its etag and when it last changed.
    #[tokio::test]
    async fn a_listed_object_carries_size_etag_and_time() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        s.put_object(put("k", b"hello world")).await.expect("put");

        let out = s
            .list_objects_v2(list(ListObjectsV2Input::default()))
            .await
            .expect("list")
            .output;

        let contents = out.contents.expect("contents");
        let object = contents.first().expect("one object");
        assert_eq!(object.key.as_deref(), Some("k"));
        assert_eq!(object.size, Some(11));
        assert_eq!(object.e_tag, Some(hello_world_etag()));
        assert!(
            object.last_modified.clone().expect("last modified")
                > Timestamp::from(std::time::UNIX_EPOCH)
        );
    }

    /// The response repeats the request back: clients match a page to the
    /// query that produced it by the echoed prefix, delimiter and budget, and
    /// read the bucket's name off the listing itself.
    #[tokio::test]
    async fn a_listing_echoes_the_request_back() {
        let (_dir, s) = seeded(&["d/x"]).await;

        let out = s
            .list_objects_v2(list(ListObjectsV2Input {
                prefix: Some("d/".to_owned()),
                delimiter: Some("/".to_owned()),
                max_keys: Some(7),
                start_after: Some("d/".to_owned()),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;

        assert_eq!(out.name.as_deref(), Some("buk"));
        assert_eq!(out.prefix.as_deref(), Some("d/"));
        assert_eq!(out.delimiter.as_deref(), Some("/"));
        assert_eq!(out.max_keys, Some(7));
        assert_eq!(out.start_after.as_deref(), Some("d/"));
        assert_eq!(keys_of(&out), ["d/x"]);
    }

    /// A request that names no budget gets S3's default of 1000, and the page
    /// says so rather than leaving the client to assume it.
    #[tokio::test]
    async fn an_unbounded_request_defaults_to_a_thousand_keys() {
        let (_dir, s) = seeded(&["a"]).await;

        let out = s
            .list_objects_v2(list(ListObjectsV2Input::default()))
            .await
            .expect("list")
            .output;

        assert_eq!(out.max_keys, Some(1000));
    }

    /// A budget larger than the ceiling is capped at it: how much work one
    /// request makes the engine do is not the client's to raise.
    #[tokio::test]
    async fn an_oversized_budget_is_capped_at_a_thousand() {
        let (_dir, s) = seeded(&["a"]).await;

        let out = s
            .list_objects_v2(list(ListObjectsV2Input {
                max_keys: Some(5_000),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;

        assert_eq!(out.max_keys, Some(1000));
        assert_eq!(keys_of(&out), ["a"]);
    }

    /// A budget of zero is a legal request for nothing: an empty page that is
    /// not truncated, since nothing was left out that a token could resume.
    #[tokio::test]
    async fn a_budget_of_zero_lists_nothing_and_is_not_truncated() {
        let (_dir, s) = seeded(&["a", "b"]).await;

        let out = s
            .list_objects_v2(list(ListObjectsV2Input {
                max_keys: Some(0),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;

        assert!(keys_of(&out).is_empty());
        assert_eq!(out.key_count, Some(0));
        assert_eq!(out.is_truncated, Some(false));
        assert!(out.next_continuation_token.is_none());
    }

    /// A negative budget is not a legal request, and it reaches the engine as
    /// an empty page rather than as a conversion that blows up on the way.
    #[tokio::test]
    async fn a_negative_budget_lists_nothing() {
        let (_dir, s) = seeded(&["a"]).await;

        let out = s
            .list_objects_v2(list(ListObjectsV2Input {
                max_keys: Some(-1),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;

        assert!(keys_of(&out).is_empty());
        assert_eq!(out.max_keys, Some(0));
    }

    /// The token a truncated page hands back resumes the listing where it
    /// stopped: the pages together are the bucket, in order, once each.
    #[tokio::test]
    async fn the_continuation_token_resumes_the_listing() {
        let (_dir, s) = seeded(&["a", "b", "c"]).await;

        let first = s
            .list_objects_v2(list(ListObjectsV2Input {
                max_keys: Some(2),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;
        assert_eq!(keys_of(&first), ["a", "b"]);
        assert_eq!(first.is_truncated, Some(true));
        let token = first.next_continuation_token.expect("a token");

        let second = s
            .list_objects_v2(list(ListObjectsV2Input {
                max_keys: Some(2),
                continuation_token: Some(token.clone()),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;
        assert_eq!(keys_of(&second), ["c"]);
        assert_eq!(second.is_truncated, Some(false));
        assert!(second.next_continuation_token.is_none());
        assert_eq!(second.continuation_token.as_deref(), Some(token.as_str()));
    }

    /// A folded prefix is a key as far as the count is concerned, which is
    /// what keeps the count from exceeding the budget that produced it.
    #[tokio::test]
    async fn key_count_counts_objects_and_folded_prefixes_together() {
        let (_dir, s) = seeded(&["a", "d/x", "d/y", "e/z"]).await;

        let out = s
            .list_objects_v2(list(ListObjectsV2Input {
                delimiter: Some("/".to_owned()),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;
        assert_eq!(keys_of(&out), ["a"]);
        assert_eq!(prefixes_of(&out), ["d/", "e/"]);
        assert_eq!(out.key_count, Some(3));

        let page = s
            .list_objects_v2(list(ListObjectsV2Input {
                delimiter: Some("/".to_owned()),
                max_keys: Some(2),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;
        assert_eq!(page.key_count, Some(2));
        assert_eq!(page.is_truncated, Some(true));
    }

    /// `start_after` reaches the engine: the page begins strictly past the key
    /// it names.
    #[tokio::test]
    async fn start_after_begins_the_page_past_that_key() {
        let (_dir, s) = seeded(&["a", "b", "c"]).await;

        let out = s
            .list_objects_v2(list(ListObjectsV2Input {
                start_after: Some("a".to_owned()),
                ..Default::default()
            }))
            .await
            .expect("list")
            .output;

        assert_eq!(keys_of(&out), ["b", "c"]);
    }

    /// An empty bucket lists an empty page: both lists are present and empty
    /// rather than left out, so a client need not treat absence as a case.
    #[tokio::test]
    async fn an_empty_bucket_lists_empty_contents_and_prefixes() {
        let (_dir, s) = seeded(&[]).await;

        let out = s
            .list_objects_v2(list(ListObjectsV2Input::default()))
            .await
            .expect("list")
            .output;

        assert!(keys_of(&out).is_empty());
        assert!(prefixes_of(&out).is_empty());
        assert_eq!(out.key_count, Some(0));
        assert_eq!(out.is_truncated, Some(false));
    }

    /// Listing a bucket that does not exist is `NoSuchBucket`, the engine's
    /// verdict passed through rather than an empty listing.
    #[tokio::test]
    async fn listing_a_missing_bucket_is_no_such_bucket() {
        let (_dir, s) = svc().await;
        let err = s
            .list_objects_v2(list(ListObjectsV2Input::default()))
            .await
            .expect_err("missing bucket");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchBucket);
    }
}
