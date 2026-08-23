// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Object integrity checksums: the `x-amz-checksum-*` family.
//!
//! Since botocore 1.36 every AWS SDK computes one of these per upload and, when
//! the server returns it, validates it on the way back. This module is the
//! engine's half of honouring that: the algorithms aks3 can compute, a small
//! running hasher over each, and the `{algorithm, value}` pair stored beside an
//! object so a later `GET` or `HEAD` can return it.
//!
//! The value is always base64 of the raw digest, which is the form S3 puts on
//! the wire and the form boto3 compares against: CRC32 is the four big-endian
//! bytes of the checksum, SHA1 and SHA256 the raw digest.
//!
//! # Algorithm coverage
//!
//! CRC32, SHA1 and SHA256 are the algorithms a plain boto3 can produce: they
//! are computed, verified and stored. CRC32C and CRC64NVME are deliberately
//! absent. Both require boto3's `awscrt` extra, which the compliance lockfile
//! omits on purpose (see `test_crt_algorithms_are_out_of_reach`), so no test in
//! the suite can reach them, and adding a checksum path nothing exercises would
//! be asserting a behaviour rather than testing it. A request that carries one
//! of those two is left to the pre-existing pass-through: it is accepted and its
//! value is neither verified nor stored, exactly as before this module existed.
//! Verifying them is tracked as follow-on work; it is a matter of two more
//! crates and two more variants here, not a design change.
//!
//! Composite (multipart) checksums are out of scope: aks3 has no multipart
//! upload yet, so every stored checksum describes a whole object and its type is
//! always `FULL_OBJECT`. That constant lives at the API layer rather than on
//! disk, so nothing here records it.

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

/// A checksum algorithm aks3 computes over an object body.
///
/// Only the three a plain boto3 can produce are represented; see the module
/// docs for why CRC32C and CRC64NVME are not here. The `serde` spellings are the
/// uppercase names S3 uses (`CRC32`, `SHA1`, `SHA256`), so the on-disk manifest
/// reads the same as the wire and does not depend on the Rust variant names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    /// CRC-32 (IEEE), the botocore default. Same polynomial as `zlib.crc32`.
    #[serde(rename = "CRC32")]
    Crc32,
    /// SHA-1.
    #[serde(rename = "SHA1")]
    Sha1,
    /// SHA-256.
    #[serde(rename = "SHA256")]
    Sha256,
}

impl ChecksumAlgorithm {
    /// The uppercase S3 name of the algorithm (`CRC32`, `SHA1`, `SHA256`).
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
        }
    }

    /// The algorithm named by an S3 name, if it is one aks3 computes.
    ///
    /// The comparison is case-insensitive because the name arrives from headers
    /// (`x-amz-sdk-checksum-algorithm`, or the trailing-header name), which carry
    /// no canonical case. `CRC32C` and `CRC64NVME` return `None`, which is what
    /// keeps them on the pass-through path rather than being verified.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("CRC32") {
            Some(Self::Crc32)
        } else if name.eq_ignore_ascii_case("SHA1") {
            Some(Self::Sha1)
        } else if name.eq_ignore_ascii_case("SHA256") {
            Some(Self::Sha256)
        } else {
            None
        }
    }

    /// A fresh running hasher for this algorithm.
    #[must_use]
    pub fn hasher(self) -> Checksummer {
        match self {
            Self::Crc32 => Checksummer::Crc32(crc32fast::Hasher::new()),
            Self::Sha1 => Checksummer::Sha1(sha1::Sha1::new()),
            Self::Sha256 => Checksummer::Sha256(sha2::Sha256::new()),
        }
    }
}

/// A running checksum over a body, fed one chunk at a time.
///
/// Created from [`ChecksumAlgorithm::hasher`], updated as the body streams past,
/// and consumed by [`Self::finalize_base64`] once the last byte is in. It exists
/// so both the engine (which stores the result) and the API layer (which
/// verifies it) can compute a checksum in the same shape without either owning
/// the hashing crates.
pub enum Checksummer {
    /// CRC-32 accumulator.
    Crc32(crc32fast::Hasher),
    /// SHA-1 accumulator.
    Sha1(sha1::Sha1),
    /// SHA-256 accumulator.
    Sha256(sha2::Sha256),
}

impl Checksummer {
    /// Fold `data` into the running checksum.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(h) => h.update(data),
            Self::Sha1(h) => h.update(data),
            Self::Sha256(h) => h.update(data),
        }
    }

    /// Finish the checksum and return it base64-encoded, the form S3 sends it in.
    #[must_use]
    pub fn finalize_base64(self) -> String {
        match self {
            Self::Crc32(h) => STANDARD.encode(h.finalize().to_be_bytes()),
            Self::Sha1(h) => STANDARD.encode(h.finalize()),
            Self::Sha256(h) => STANDARD.encode(h.finalize()),
        }
    }
}

/// A checksum stored beside an object: which algorithm, and the base64 value.
///
/// This is what a `PUT` records and a `GET`/`HEAD` returns. The value is exactly
/// what was verified against the body on upload, so returning it lets a client's
/// own validation succeed rather than being a second, independent claim.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredChecksum {
    /// The algorithm the value was computed with.
    pub algorithm: ChecksumAlgorithm,
    /// The base64-encoded digest.
    pub value: String,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The empty-body vectors, which every S3 client and this crate must agree
    /// on. CRC32 of nothing is 0; SHA1 and SHA256 have their well-known digests.
    #[test]
    fn empty_body_vectors() {
        assert_eq!(
            ChecksumAlgorithm::Crc32.hasher().finalize_base64(),
            "AAAAAA=="
        );
        assert_eq!(
            ChecksumAlgorithm::Sha1.hasher().finalize_base64(),
            "2jmj7l5rSw0yVb/vlWAYkK/YBwk="
        );
        assert_eq!(
            ChecksumAlgorithm::Sha256.hasher().finalize_base64(),
            "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU="
        );
    }

    /// CRC32 matches what boto3 computes: base64 of the four big-endian bytes of
    /// the IEEE CRC. `zlib.crc32(b"hello world") == 0x0d4a1185`, so the value is
    /// `base64(0x0d, 0x4a, 0x11, 0x85)`.
    #[test]
    fn crc32_matches_zlib_and_boto3() {
        let mut h = ChecksumAlgorithm::Crc32.hasher();
        h.update(b"hello world");
        assert_eq!(h.finalize_base64(), "DUoRhQ==");
    }

    /// SHA256 of "hello world", the value a `ChecksumAlgorithm=SHA256` upload of
    /// that body would carry.
    #[test]
    fn sha256_matches_known_vector() {
        let mut h = ChecksumAlgorithm::Sha256.hasher();
        h.update(b"hello world");
        assert_eq!(
            h.finalize_base64(),
            "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek="
        );
    }

    /// Updating in several chunks is the same as updating in one: the body
    /// arrives split into arbitrary chunks, so this is the property that matters.
    #[test]
    fn chunked_updates_match_a_single_update() {
        let mut split = ChecksumAlgorithm::Sha1.hasher();
        split.update(b"hello ");
        split.update(b"world");
        let mut whole = ChecksumAlgorithm::Sha1.hasher();
        whole.update(b"hello world");
        assert_eq!(split.finalize_base64(), whole.finalize_base64());
    }

    #[test]
    fn from_name_is_case_insensitive_and_rejects_crt_algorithms() {
        assert_eq!(
            ChecksumAlgorithm::from_name("CRC32"),
            Some(ChecksumAlgorithm::Crc32)
        );
        assert_eq!(
            ChecksumAlgorithm::from_name("sha256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::from_name("Sha1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        // The two that need the awscrt extra stay on the pass-through path.
        assert_eq!(ChecksumAlgorithm::from_name("CRC32C"), None);
        assert_eq!(ChecksumAlgorithm::from_name("CRC64NVME"), None);
        assert_eq!(ChecksumAlgorithm::from_name("nonsense"), None);
    }

    /// The on-disk spelling of the algorithm is the uppercase S3 name, not the
    /// Rust variant, so a manifest does not change shape if the enum is renamed.
    #[test]
    fn algorithm_serializes_as_its_s3_name() {
        let json = serde_json::to_string(&ChecksumAlgorithm::Crc32).unwrap();
        assert_eq!(json, "\"CRC32\"");
        let back: ChecksumAlgorithm = serde_json::from_str("\"SHA256\"").unwrap();
        assert_eq!(back, ChecksumAlgorithm::Sha256);
    }
}
