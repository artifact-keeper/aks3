// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Engine failures as S3 wire errors.
//!
//! The engine speaks in [`EngineError`], which has nine variants and no
//! knowledge of HTTP; the wire wants an S3 error code. [`map_engine_err`] is the
//! single place that translation happens, so a handler never invents a code and
//! the same engine condition always reaches a client as the same code.
//!
//! # Message, and what a client actually reads
//!
//! Every mapped variant carries a human-readable `Message` alongside its code.
//! A code is what client code branches on, but the message is the half a person
//! reads: boto3 renders a missing one as the literal string `Unknown`, so an
//! error with only a code lands in a traceback or a support ticket saying
//! nothing. The messages here match the wording AWS uses for the same codes.
//!
//! Two other halves of a real S3 error are not set here, for the same reason:
//! this function is handed an [`EngineError`] and nothing else. The resource
//! identifier (`Key` for `NoSuchKey`, `BucketName` for the bucket errors) is not
//! available, and s3s 0.14 does not serialize a `Resource` element in any case
//! (the field is present but commented out upstream), so there is nowhere to put
//! it even when the caller knows it. `RequestId` is a per-request value that a
//! middleware layer would stamp on the response, not something this table can
//! know. Both belong outside this function.
//!
//! # The variants that are about us, not the request
//!
//! [`EngineError::Io`] describes something that went wrong on our side: its
//! message routinely carries a host path (`/var/lib/aks3/...`), so it becomes a
//! bare `InternalError` with no message at all and the detail goes to the log
//! instead of the response body.
//!
//! [`EngineError::StorageFull`] is also about the deployment rather than the
//! request, but it is a condition an operator and a client should be able to see
//! rather than a bug: the disk is full. It maps to `ServiceUnavailable` (503),
//! the closest thing s3s 0.14 exposes to an "insufficient storage" signal (it
//! has no `InsufficientStorage`/507 code, and `SlowDown` would tell an SDK to
//! retry a persistently full disk on a tight backoff, which is the wrong
//! advice). A 503 reads as a capacity signal a load balancer or retry policy can
//! act on, rather than as the defect a 500 implies. Its message says the store is
//! out of space and names no path, so nothing internal leaks.

use aks3_engine::EngineError;
use s3s::{s3_error, S3Error};

/// Translate an engine failure into the S3 error a client should see.
///
/// Every client-visible variant carries a `Message` matching AWS's wording for
/// the code. [`EngineError::Io`] is the exception: it is logged at error level
/// and reported as a bare `InternalError` with no message, since its own message
/// routinely carries a host path that must not reach the response.
#[must_use]
pub fn map_engine_err(err: EngineError) -> S3Error {
    match err {
        EngineError::NoSuchBucket => {
            s3_error!(NoSuchBucket, "The specified bucket does not exist")
        }
        EngineError::BucketAlreadyExists => s3_error!(
            BucketAlreadyOwnedByYou,
            "Your previous request to create the named bucket succeeded and you already own it"
        ),
        EngineError::BucketNotEmpty => {
            s3_error!(
                BucketNotEmpty,
                "The bucket you tried to delete is not empty"
            )
        }
        EngineError::InvalidBucketName => {
            s3_error!(InvalidBucketName, "The specified bucket is not valid")
        }
        EngineError::NoSuchKey => s3_error!(NoSuchKey, "The specified key does not exist"),
        EngineError::InvalidRange => {
            s3_error!(InvalidRange, "The requested range is not satisfiable")
        }
        // A 400, not a 500: the key is longer than aks3 can store, and the
        // client is the only side that can do anything about that.
        EngineError::KeyTooLong => s3_error!(KeyTooLongError, "Your key is too long"),
        // A 503, not a 500: the disk is full. That is a capacity condition an
        // operator grows the disk for and a client backs off on, not the defect a
        // 500 pages someone about. ServiceUnavailable is the closest s3s 0.14
        // carries; see the module docs for why not SlowDown or 507.
        EngineError::StorageFull => s3_error!(
            ServiceUnavailable,
            "The server is out of storage capacity to complete the request; retry later"
        ),
        EngineError::Io(io) => {
            tracing::error!(error = %io, "engine i/o failure");
            s3_error!(InternalError)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::map_engine_err;
    use aks3_engine::EngineError;
    use s3s::S3ErrorCode;

    #[test]
    fn io_error_does_not_leak() {
        let e = map_engine_err(EngineError::Io(std::io::Error::other(
            "/secret/path denied",
        )));
        assert!(!format!("{e:?}").contains("/secret/path"));
    }

    /// `Debug` is what the test above pins, but the response body is built from
    /// the message and the source, so check both are empty for an i/o error.
    #[test]
    fn io_error_carries_no_message_or_source() {
        let e = map_engine_err(EngineError::Io(std::io::Error::other(
            "/secret/path denied",
        )));
        assert_eq!(*e.code(), S3ErrorCode::InternalError);
        assert!(e.message().is_none());
        assert!(e.source().is_none());
        assert!(!format!("{e}").contains("/secret/path"));
    }

    #[test]
    fn codes_map() {
        assert_eq!(
            *map_engine_err(EngineError::NoSuchKey).code(),
            S3ErrorCode::NoSuchKey
        );
        assert_eq!(
            *map_engine_err(EngineError::NoSuchBucket).code(),
            S3ErrorCode::NoSuchBucket
        );
        assert_eq!(
            *map_engine_err(EngineError::InvalidRange).code(),
            S3ErrorCode::InvalidRange
        );
    }

    /// The remaining client-visible variants, so all nine are pinned.
    #[test]
    fn every_variant_maps() {
        assert_eq!(
            *map_engine_err(EngineError::BucketAlreadyExists).code(),
            S3ErrorCode::BucketAlreadyOwnedByYou
        );
        assert_eq!(
            *map_engine_err(EngineError::BucketNotEmpty).code(),
            S3ErrorCode::BucketNotEmpty
        );
        assert_eq!(
            *map_engine_err(EngineError::InvalidBucketName).code(),
            S3ErrorCode::InvalidBucketName
        );
        assert_eq!(
            *map_engine_err(EngineError::KeyTooLong).code(),
            S3ErrorCode::KeyTooLongError
        );
        assert_eq!(
            *map_engine_err(EngineError::StorageFull).code(),
            S3ErrorCode::ServiceUnavailable
        );
    }

    /// Every client-visible variant carries a non-empty `Message`. A code is what
    /// client code branches on; the message is the half a person reads, and boto3
    /// renders a missing one as the literal string `Unknown`.
    #[test]
    fn client_visible_variants_carry_a_message() {
        for err in [
            EngineError::NoSuchBucket,
            EngineError::BucketAlreadyExists,
            EngineError::BucketNotEmpty,
            EngineError::InvalidBucketName,
            EngineError::NoSuchKey,
            EngineError::InvalidRange,
            EngineError::KeyTooLong,
            EngineError::StorageFull,
        ] {
            let mapped = map_engine_err(err);
            let message = mapped
                .message()
                .unwrap_or_else(|| panic!("{:?} has no message", mapped.code()));
            assert!(
                !message.trim().is_empty(),
                "{:?} has an empty message",
                mapped.code()
            );
        }
    }

    /// A full disk is a capacity signal, not a defect: it maps to a retryable 503
    /// `ServiceUnavailable`, and its message names no host path.
    #[test]
    fn storage_full_is_a_retryable_503() {
        let mapped = map_engine_err(EngineError::StorageFull);
        assert_eq!(*mapped.code(), S3ErrorCode::ServiceUnavailable);
        let status = mapped.status_code().expect("mapped code has a status");
        assert_eq!(status.as_u16(), 503);
        assert!(status.is_server_error(), "{status} is not 5xx");
        let message = mapped.message().expect("storage full carries a message");
        assert!(!message.contains('/'), "message leaks a path: {message}");
    }

    /// A client-visible code must not be reported as a server fault: every
    /// mapped variant except `Io` has to land in the 4xx range.
    #[test]
    fn client_errors_are_4xx() {
        for err in [
            EngineError::NoSuchBucket,
            EngineError::BucketAlreadyExists,
            EngineError::BucketNotEmpty,
            EngineError::InvalidBucketName,
            EngineError::NoSuchKey,
            EngineError::InvalidRange,
            EngineError::KeyTooLong,
        ] {
            let mapped = map_engine_err(err);
            let status = mapped.status_code().expect("mapped code has a status");
            assert!(status.is_client_error(), "{:?} is {status}", mapped.code());
        }
    }
}
