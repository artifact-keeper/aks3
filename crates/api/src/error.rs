// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Engine failures as S3 wire errors.
//!
//! The engine speaks in [`EngineError`], which has eight variants and no
//! knowledge of HTTP; the wire wants an S3 error code. [`map_engine_err`] is the
//! single place that translation happens, so a handler never invents a code and
//! the same engine condition always reaches a client as the same code.
//!
//! Seven of the eight variants describe something about the request, and map
//! straight across. The eighth, [`EngineError::Io`], describes something that
//! went wrong on our side: its message routinely carries a host path
//! (`/var/lib/aks3/...`), so it becomes a bare `InternalError` and the detail
//! goes to the log instead of the response body.

use aks3_engine::EngineError;
use s3s::{s3_error, S3Error};

/// Translate an engine failure into the S3 error a client should see.
///
/// [`EngineError::Io`] is logged at error level and reported as
/// `InternalError`; its message never reaches the response.
#[must_use]
pub fn map_engine_err(err: EngineError) -> S3Error {
    match err {
        EngineError::NoSuchBucket => s3_error!(NoSuchBucket),
        EngineError::BucketAlreadyExists => s3_error!(BucketAlreadyOwnedByYou),
        EngineError::BucketNotEmpty => s3_error!(BucketNotEmpty),
        EngineError::InvalidBucketName => s3_error!(InvalidBucketName),
        EngineError::NoSuchKey => s3_error!(NoSuchKey),
        EngineError::InvalidRange => s3_error!(InvalidRange),
        // A 400, not a 500: the key is longer than aks3 can store, and the
        // client is the only side that can do anything about that.
        EngineError::KeyTooLong => s3_error!(KeyTooLongError),
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

    /// The remaining four client-visible variants, so all eight are pinned.
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
