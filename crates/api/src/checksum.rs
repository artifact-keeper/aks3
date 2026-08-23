// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Integrity checksum verification for `PutObject`, and the response fields.
//!
//! The engine computes and stores a checksum; this module is the half that
//! decides which one the client asked for, checks the client's value against the
//! body, and shapes what goes back on the wire.
//!
//! # Where the expected value comes from, and why verification lives here
//!
//! A checksum arrives one of two ways, and s3s hands aks3 both without checking
//! either against the body:
//!
//! - **header form** (`x-amz-checksum-crc32`): a signed request header. s3s
//!   parses it onto [`PutObjectInput`], so it is known before the body is read.
//!   This is what boto3 sends over plain HTTP.
//! - **trailer form**: the body is `Content-Encoding: aws-chunked` and the
//!   checksum is a trailing header after the last chunk, signed (or not) as
//!   `STREAMING-*-PAYLOAD-TRAILER`. s3s decodes the framing, verifies the
//!   trailer *signature* when there is one, and exposes the parsed trailer
//!   through [`S3Request::trailing_headers`](s3s::S3Request) once the body
//!   stream is consumed. It never compares the trailer's checksum *value* to the
//!   body. This is what boto3 sends over HTTPS, and on that path the trailer is
//!   the only body-integrity check there is.
//!
//! So s3s exposes the value but does not verify it, and this is the only layer
//! that can: the engine takes plain bytes and knows nothing of headers or
//! trailers. Verification has to happen as the body streams and before anything
//! is committed, or a mismatch would be caught only after the object was already
//! stored. [`VerifyingStream`] does exactly that: it folds each chunk into a
//! running checksum, and on the final poll compares the result against the
//! expected value, failing the stream on a mismatch. The engine's staging loop
//! sees that failure, returns before its commit step, and drops the staged
//! temp file, so a rejected upload stores nothing. The handler recognises the
//! failure and answers `400 BadDigest`.

use std::pin::Pin;
use std::task::{Context, Poll};

use aks3_engine::{ChecksumAlgorithm, Checksummer, EngineError, StoredChecksum};
use bytes::Bytes;
use futures::Stream;
use http::{HeaderMap, HeaderName};
use s3s::dto::{ChecksumType, PutObjectInput};
use s3s::{s3_error, S3Error, TrailingHeaders};

use crate::error::map_engine_err;

/// The request header that names which trailing header carries the checksum.
/// s3s 0.14 exposes no constant for it, so it is spelled here.
const X_AMZ_TRAILER: HeaderName = HeaderName::from_static("x-amz-trailer");

/// The prefix an `x-amz-checksum-<algorithm>` header or trailer carries.
const CHECKSUM_TRAILER_PREFIX: &str = "x-amz-checksum-";

/// A checksum a `PutObject` asked aks3 to verify: which algorithm, and where its
/// expected value comes from.
pub(crate) struct PutChecksum {
    pub algorithm: ChecksumAlgorithm,
    expected: Expected,
}

/// Where the expected value is read from.
enum Expected {
    /// A signed request header, known before the body is read.
    Immediate(String),
    /// An aws-chunked trailing header, readable only once the body has been
    /// consumed. The handle is s3s's; the name is the lower-cased trailer key.
    Trailer {
        handle: TrailingHeaders,
        name: HeaderName,
    },
}

impl Expected {
    /// Resolve to the base64 value the client sent, or `None` if it never
    /// arrived (a trailer that was announced but absent). Consumes the handle's
    /// one-shot read without taking the trailers, so nothing else that inspects
    /// them is disturbed.
    fn resolve(self) -> Option<String> {
        match self {
            Self::Immediate(value) => Some(value),
            Self::Trailer { handle, name } => handle
                .read(|map| {
                    map.get(&name)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_owned)
                })
                .flatten(),
        }
    }
}

/// Work out which checksum, if any, a `PutObject` wants verified.
///
/// Header form wins if present. Otherwise the trailer form is recognised from
/// `x-amz-trailer` naming an `x-amz-checksum-<algorithm>` trailer for an
/// algorithm this build computes. A checksum for an algorithm aks3 does not
/// compute (CRC32C, CRC64NVME) returns `None`, which leaves it on the
/// pre-existing pass-through: accepted, neither verified nor stored.
pub(crate) fn detect(
    input: &PutObjectInput,
    headers: &HeaderMap,
    trailing: Option<&TrailingHeaders>,
) -> Option<PutChecksum> {
    if let Some(value) = input.checksum_crc32.clone() {
        return Some(PutChecksum {
            algorithm: ChecksumAlgorithm::Crc32,
            expected: Expected::Immediate(value),
        });
    }
    if let Some(value) = input.checksum_sha1.clone() {
        return Some(PutChecksum {
            algorithm: ChecksumAlgorithm::Sha1,
            expected: Expected::Immediate(value),
        });
    }
    if let Some(value) = input.checksum_sha256.clone() {
        return Some(PutChecksum {
            algorithm: ChecksumAlgorithm::Sha256,
            expected: Expected::Immediate(value),
        });
    }

    // Trailer form: the value is not a header, so it can only be read once the
    // body stream has run, and only if s3s gave us a handle to read it from.
    let handle = trailing?;
    let declared = headers.get(&X_AMZ_TRAILER)?.to_str().ok()?;
    // `x-amz-trailer` is a comma-separated list in principle; take the first
    // entry that is a checksum trailer for an algorithm aks3 computes.
    for token in declared.split(',') {
        let token = token.trim();
        let Some(algo_name) = token.strip_prefix(CHECKSUM_TRAILER_PREFIX) else {
            continue;
        };
        let Some(algorithm) = ChecksumAlgorithm::from_name(algo_name) else {
            continue;
        };
        let Ok(name) = HeaderName::from_bytes(token.as_bytes()) else {
            continue;
        };
        return Some(PutChecksum {
            algorithm,
            expected: Expected::Trailer {
                handle: handle.clone(),
                name,
            },
        });
    }
    None
}

/// A body corrupted or misdescribed on upload: the computed checksum did not
/// match the one the client supplied. Carried inside an [`std::io::Error`] so it
/// can travel out through the engine's stream-error path and be recognised by
/// [`put_error`].
#[derive(Debug)]
struct ChecksumMismatch {
    algorithm: ChecksumAlgorithm,
}

impl std::fmt::Display for ChecksumMismatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} checksum mismatch", self.algorithm.as_str())
    }
}

impl std::error::Error for ChecksumMismatch {}

/// A body stream that verifies the client's checksum as it passes.
///
/// Each chunk is folded into a running checksum on its way to the engine. When
/// the inner stream ends, the running value is compared with the expected one;
/// on a mismatch the stream yields an error carrying [`ChecksumMismatch`], which
/// aborts the engine before it commits. When they match (or no expected value
/// arrived) the stream simply ends, and the engine stores the object.
pub(crate) struct VerifyingStream {
    inner: aks3_engine::BoxByteStream,
    algorithm: ChecksumAlgorithm,
    /// `Some` until the body ends and it is finalized.
    checksummer: Option<Checksummer>,
    /// `Some` until the body ends and it is resolved.
    expected: Option<Expected>,
    /// Set once the terminal item (a mismatch error, or the end) has been
    /// produced, so a further poll returns `None` rather than repeating it.
    finished: bool,
}

impl VerifyingStream {
    pub(crate) fn new(inner: aks3_engine::BoxByteStream, checksum: PutChecksum) -> Self {
        Self {
            inner,
            algorithm: checksum.algorithm,
            checksummer: Some(checksum.algorithm.hasher()),
            expected: Some(checksum.expected),
            finished: false,
        }
    }
}

impl Stream for VerifyingStream {
    type Item = Result<Bytes, std::io::Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        if this.finished {
            return Poll::Ready(None);
        }
        match this.inner.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(Ok(chunk))) => {
                if let Some(c) = this.checksummer.as_mut() {
                    c.update(&chunk);
                }
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(e))) => {
                this.finished = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                this.finished = true;
                let computed = this.checksummer.take().map(Checksummer::finalize_base64);
                let expected = this.expected.take().and_then(Expected::resolve);
                match (computed, expected) {
                    (Some(computed), Some(expected)) if computed != expected => {
                        Poll::Ready(Some(Err(std::io::Error::other(ChecksumMismatch {
                            algorithm: this.algorithm,
                        }))))
                    }
                    _ => Poll::Ready(None),
                }
            }
        }
    }
}

/// Turn a failed `PutObject` into the S3 error a client should see.
///
/// A checksum mismatch surfaces as an [`EngineError::Io`] wrapping
/// [`ChecksumMismatch`], because [`VerifyingStream`] fails the body stream to
/// abort the upload. That one case becomes `400 BadDigest`; everything else is
/// an ordinary engine failure and goes through [`map_engine_err`].
pub(crate) fn put_error(err: EngineError) -> S3Error {
    if let EngineError::Io(ref io) = err {
        if let Some(mismatch) = io
            .get_ref()
            .and_then(|source| source.downcast_ref::<ChecksumMismatch>())
        {
            return s3_error!(
                BadDigest,
                "The {} checksum you specified did not match the checksum of the object received.",
                mismatch.algorithm.as_str()
            );
        }
    }
    map_engine_err(err)
}

/// The response checksum headers for a stored checksum: exactly one of the
/// value fields is set, alongside the type.
///
/// Every single-object `PUT` stores a whole-object checksum, so the type is
/// always `FULL_OBJECT`. Composite (multipart) checksums do not arise: aks3 has
/// no multipart upload.
pub(crate) struct ChecksumFields {
    pub crc32: Option<String>,
    pub sha1: Option<String>,
    pub sha256: Option<String>,
    pub checksum_type: Option<ChecksumType>,
}

/// The response fields for `stored`, or all-absent when there is nothing to
/// report.
pub(crate) fn fields_for(stored: Option<&StoredChecksum>) -> ChecksumFields {
    let mut fields = ChecksumFields {
        crc32: None,
        sha1: None,
        sha256: None,
        checksum_type: None,
    };
    if let Some(stored) = stored {
        match stored.algorithm {
            ChecksumAlgorithm::Crc32 => fields.crc32 = Some(stored.value.clone()),
            ChecksumAlgorithm::Sha1 => fields.sha1 = Some(stored.value.clone()),
            ChecksumAlgorithm::Sha256 => fields.sha256 = Some(stored.value.clone()),
        }
        fields.checksum_type = Some(ChecksumType::from_static(ChecksumType::FULL_OBJECT));
    }
    fields
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use s3s::S3ErrorCode;

    /// The base64 checksum of `bytes`, computed the same way the engine does, so
    /// tests can name the value they expect without hard-coding a digest.
    fn checksum_of(algorithm: ChecksumAlgorithm, bytes: &[u8]) -> String {
        let mut hasher = algorithm.hasher();
        hasher.update(bytes);
        hasher.finalize_base64()
    }

    fn body(chunks: &[&'static [u8]]) -> aks3_engine::BoxByteStream {
        let items: Vec<_> = chunks
            .iter()
            .map(|c| Ok::<_, std::io::Error>(Bytes::from_static(c)))
            .collect();
        futures::stream::iter(items).boxed()
    }

    fn immediate(algorithm: ChecksumAlgorithm, expected: &str) -> PutChecksum {
        PutChecksum {
            algorithm,
            expected: Expected::Immediate(expected.to_owned()),
        }
    }

    /// Drive a `VerifyingStream` to the end, returning the bytes it passed
    /// through or the first error it produced.
    async fn drive(stream: VerifyingStream) -> Result<Vec<u8>, std::io::Error> {
        let mut stream = stream;
        let mut out = Vec::new();
        while let Some(item) = stream.next().await {
            out.extend_from_slice(&item?);
        }
        Ok(out)
    }

    #[tokio::test]
    async fn a_matching_checksum_passes_the_body_through_unchanged() {
        let expected = checksum_of(ChecksumAlgorithm::Crc32, b"hello world");
        let stream = VerifyingStream::new(
            body(&[b"hello ", b"world"]),
            immediate(ChecksumAlgorithm::Crc32, &expected),
        );
        assert_eq!(drive(stream).await.unwrap(), b"hello world");
    }

    #[tokio::test]
    async fn a_mismatching_checksum_fails_the_stream() {
        // The value of a different body, so the stream must reject at the end.
        let wrong = checksum_of(ChecksumAlgorithm::Crc32, b"not this body");
        let stream = VerifyingStream::new(
            body(&[b"hello world"]),
            immediate(ChecksumAlgorithm::Crc32, &wrong),
        );
        let err = drive(stream)
            .await
            .expect_err("mismatch must fail the stream");
        assert!(
            err.get_ref()
                .and_then(|s| s.downcast_ref::<ChecksumMismatch>())
                .is_some(),
            "the failure must carry a ChecksumMismatch so put_error can map it"
        );
    }

    #[tokio::test]
    async fn every_algorithm_verifies() {
        for algorithm in [
            ChecksumAlgorithm::Crc32,
            ChecksumAlgorithm::Sha1,
            ChecksumAlgorithm::Sha256,
        ] {
            let good = checksum_of(algorithm, b"payload");
            let ok = VerifyingStream::new(body(&[b"payload"]), immediate(algorithm, &good));
            assert!(
                drive(ok).await.is_ok(),
                "{} good value rejected",
                algorithm.as_str()
            );

            let bad = VerifyingStream::new(
                body(&[b"payload"]),
                immediate(algorithm, "AAAAAAAAAAAAAAAAAAAAAA=="),
            );
            assert!(
                drive(bad).await.is_err(),
                "{} bad value accepted",
                algorithm.as_str()
            );
        }
    }

    /// An inner I/O error is propagated as itself, not masked as a checksum
    /// mismatch: the two must stay distinguishable so one becomes a 400 and the
    /// other a 500.
    #[tokio::test]
    async fn an_inner_error_is_not_a_checksum_mismatch() {
        let inner = futures::stream::iter(vec![Err(std::io::Error::other("disk gone"))]).boxed();
        let stream = VerifyingStream::new(inner, immediate(ChecksumAlgorithm::Crc32, "DUoRhQ=="));
        let err = drive(stream).await.expect_err("inner error propagates");
        assert!(err
            .get_ref()
            .and_then(|s| s.downcast_ref::<ChecksumMismatch>())
            .is_none());
    }

    #[test]
    fn put_error_maps_a_mismatch_to_bad_digest() {
        let io = std::io::Error::other(ChecksumMismatch {
            algorithm: ChecksumAlgorithm::Sha256,
        });
        let err = put_error(EngineError::Io(io));
        assert_eq!(*err.code(), S3ErrorCode::BadDigest);
        assert!(err.message().is_some_and(|m| m.contains("SHA256")));
    }

    #[test]
    fn put_error_leaves_an_ordinary_io_error_alone() {
        let err = put_error(EngineError::Io(std::io::Error::other("/secret path")));
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
    }

    #[test]
    fn put_error_passes_other_engine_errors_through() {
        assert_eq!(
            *put_error(EngineError::NoSuchBucket).code(),
            S3ErrorCode::NoSuchBucket
        );
    }

    #[test]
    fn detect_reads_the_header_form_from_the_input() {
        let input = PutObjectInput {
            checksum_sha256: Some("value".to_owned()),
            ..Default::default()
        };
        let found = detect(&input, &HeaderMap::new(), None).expect("a checksum");
        assert_eq!(found.algorithm, ChecksumAlgorithm::Sha256);
    }

    #[test]
    fn detect_leaves_crt_algorithms_on_the_pass_through() {
        // CRC32C is present on the input but aks3 does not compute it, so detect
        // returns None and the value is neither verified nor stored.
        let input = PutObjectInput {
            checksum_crc32c: Some("value".to_owned()),
            ..Default::default()
        };
        assert!(detect(&input, &HeaderMap::new(), None).is_none());
    }

    #[test]
    fn detect_finds_nothing_when_no_checksum_was_sent() {
        assert!(detect(&PutObjectInput::default(), &HeaderMap::new(), None).is_none());
    }

    #[test]
    fn fields_for_sets_exactly_the_named_algorithm_and_full_object() {
        let stored = StoredChecksum {
            algorithm: ChecksumAlgorithm::Crc32,
            value: "DUoRhQ==".to_owned(),
        };
        let fields = fields_for(Some(&stored));
        assert_eq!(fields.crc32.as_deref(), Some("DUoRhQ=="));
        assert!(fields.sha1.is_none() && fields.sha256.is_none());
        assert_eq!(
            fields
                .checksum_type
                .map(|t| t.as_str().to_owned())
                .as_deref(),
            Some(ChecksumType::FULL_OBJECT)
        );
    }

    #[test]
    fn fields_for_nothing_is_all_absent() {
        let fields = fields_for(None);
        assert!(
            fields.crc32.is_none()
                && fields.sha1.is_none()
                && fields.sha256.is_none()
                && fields.checksum_type.is_none()
        );
    }
}
