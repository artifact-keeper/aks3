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
//! the wire and the form boto3 compares against: the CRC algorithms are their
//! big-endian bytes (four for the 32-bit CRCs, eight for CRC64NVME), SHA1 and
//! SHA256 the raw digest.
//!
//! # Algorithm coverage
//!
//! All five S3 algorithms are computed, verified and stored: CRC32, CRC32C,
//! SHA1, SHA256 and CRC64NVME. The two CRC variants a `RustCrypto`/`crc32fast`
//! crate does not readily give offline (CRC32C's Castagnoli polynomial and
//! CRC64NVME's NVME polynomial) are implemented here directly as small reflected
//! CRCs rather than pulled in as dependencies, so there is no crate to license
//! and their correctness is pinned by the published check vectors in the tests.
//!
//! CRC32C and CRC64NVME cannot be reached from the pinned compliance client:
//! boto3 needs its `awscrt` extra to send them and the lockfile omits it (see
//! `test_crt_algorithms_are_out_of_reach`, which pins that *client* limit).
//! Server-side verification is independent of that: any client that can produce
//! one of those checksums, or a future toolchain that adds the extra, is checked
//! rather than silently trusted. The invariant the API layer relies on is that
//! there is no "requested but unverified" algorithm left: a checksum for any of
//! the five is verified or the request is refused, never stored on faith.
//!
//! Composite (multipart) checksums are out of scope: aks3 has no multipart
//! upload yet, so every stored checksum describes a whole object and its type is
//! always `FULL_OBJECT`. That constant lives at the API layer rather than on
//! disk, so nothing here records it.

use base64::engine::general_purpose::STANDARD;
use base64::Engine as _;
use serde::{Deserialize, Serialize};
use sha2::Digest as _;

/// Reflected polynomial for CRC-32C (Castagnoli, the iSCSI CRC): the reverse of
/// `0x1EDC6F41`. Distinct from the CRC-32 `crc32fast` computes.
const CRC32C_POLY_REV: u32 = 0x82F6_3B78;

/// Reflected polynomial for CRC-64/NVME (the NVME command-set CRC): the reverse
/// of `0xAD93_D235_94C9_35A9`.
const CRC64NVME_POLY_REV: u64 = 0x9A6C_9329_AC4B_C9B5;

/// A checksum algorithm aks3 computes over an object body.
///
/// All five S3 algorithms are represented and all five are verified; see the
/// module docs. The `serde` spellings are the uppercase names S3 uses (`CRC32`,
/// `CRC32C`, `SHA1`, `SHA256`, `CRC64NVME`), so the on-disk manifest reads the
/// same as the wire and does not depend on the Rust variant names.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    /// CRC-32 (IEEE), the botocore default. Same polynomial as `zlib.crc32`.
    #[serde(rename = "CRC32")]
    Crc32,
    /// CRC-32C (Castagnoli), the CRC AWS's CRT uses by default.
    #[serde(rename = "CRC32C")]
    Crc32c,
    /// SHA-1.
    #[serde(rename = "SHA1")]
    Sha1,
    /// SHA-256.
    #[serde(rename = "SHA256")]
    Sha256,
    /// CRC-64/NVME.
    #[serde(rename = "CRC64NVME")]
    Crc64Nvme,
}

impl ChecksumAlgorithm {
    /// The uppercase S3 name of the algorithm.
    #[must_use]
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Crc32 => "CRC32",
            Self::Crc32c => "CRC32C",
            Self::Sha1 => "SHA1",
            Self::Sha256 => "SHA256",
            Self::Crc64Nvme => "CRC64NVME",
        }
    }

    /// The algorithm named by an S3 name, if it is one aks3 knows.
    ///
    /// The comparison is case-insensitive because the name arrives from headers
    /// (`x-amz-sdk-checksum-algorithm`, or the trailing-header name), which carry
    /// no canonical case. `CRC32C` is matched before `CRC32` so the longer name
    /// is not shadowed by the prefix test.
    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        if name.eq_ignore_ascii_case("CRC32C") {
            Some(Self::Crc32c)
        } else if name.eq_ignore_ascii_case("CRC32") {
            Some(Self::Crc32)
        } else if name.eq_ignore_ascii_case("SHA1") {
            Some(Self::Sha1)
        } else if name.eq_ignore_ascii_case("SHA256") {
            Some(Self::Sha256)
        } else if name.eq_ignore_ascii_case("CRC64NVME") {
            Some(Self::Crc64Nvme)
        } else {
            None
        }
    }

    /// A fresh running hasher for this algorithm.
    #[must_use]
    pub fn hasher(self) -> Checksummer {
        match self {
            Self::Crc32 => Checksummer::Crc32(crc32fast::Hasher::new()),
            // The reflected CRCs start with every register bit set, the standard
            // init the check vectors assume; `finalize_base64` applies the final
            // xor of all-ones.
            Self::Crc32c => Checksummer::Crc32c(u32::MAX),
            Self::Sha1 => Checksummer::Sha1(sha1::Sha1::new()),
            Self::Sha256 => Checksummer::Sha256(sha2::Sha256::new()),
            Self::Crc64Nvme => Checksummer::Crc64Nvme(u64::MAX),
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
///
/// The two CRC variants hold the running register of a bit-reflected CRC (init
/// all-ones, final xor all-ones), computed here rather than via a crate.
pub enum Checksummer {
    /// CRC-32 accumulator.
    Crc32(crc32fast::Hasher),
    /// CRC-32C running register.
    Crc32c(u32),
    /// SHA-1 accumulator.
    Sha1(sha1::Sha1),
    /// SHA-256 accumulator.
    Sha256(sha2::Sha256),
    /// CRC-64/NVME running register.
    Crc64Nvme(u64),
}

/// One byte folded into a reflected 32-bit CRC register.
fn crc32_reflected_step(crc: u32, byte: u8, poly_rev: u32) -> u32 {
    let mut crc = crc ^ u32::from(byte);
    for _ in 0..8 {
        crc = if crc & 1 != 0 {
            (crc >> 1) ^ poly_rev
        } else {
            crc >> 1
        };
    }
    crc
}

/// One byte folded into a reflected 64-bit CRC register.
fn crc64_reflected_step(crc: u64, byte: u8, poly_rev: u64) -> u64 {
    let mut crc = crc ^ u64::from(byte);
    for _ in 0..8 {
        crc = if crc & 1 != 0 {
            (crc >> 1) ^ poly_rev
        } else {
            crc >> 1
        };
    }
    crc
}

impl Checksummer {
    /// Fold `data` into the running checksum.
    pub fn update(&mut self, data: &[u8]) {
        match self {
            Self::Crc32(h) => h.update(data),
            Self::Crc32c(crc) => {
                for &byte in data {
                    *crc = crc32_reflected_step(*crc, byte, CRC32C_POLY_REV);
                }
            }
            Self::Sha1(h) => h.update(data),
            Self::Sha256(h) => h.update(data),
            Self::Crc64Nvme(crc) => {
                for &byte in data {
                    *crc = crc64_reflected_step(*crc, byte, CRC64NVME_POLY_REV);
                }
            }
        }
    }

    /// Finish the checksum and return it base64-encoded, the form S3 sends it in.
    #[must_use]
    pub fn finalize_base64(self) -> String {
        match self {
            Self::Crc32(h) => STANDARD.encode(h.finalize().to_be_bytes()),
            Self::Crc32c(crc) => STANDARD.encode((crc ^ u32::MAX).to_be_bytes()),
            Self::Sha1(h) => STANDARD.encode(h.finalize()),
            Self::Sha256(h) => STANDARD.encode(h.finalize()),
            Self::Crc64Nvme(crc) => STANDARD.encode((crc ^ u64::MAX).to_be_bytes()),
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
    /// on. Both CRCs of nothing are zero after the init/xor cancel; SHA1 and
    /// SHA256 have their well-known digests.
    #[test]
    fn empty_body_vectors() {
        assert_eq!(
            ChecksumAlgorithm::Crc32.hasher().finalize_base64(),
            "AAAAAA=="
        );
        assert_eq!(
            ChecksumAlgorithm::Crc32c.hasher().finalize_base64(),
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
        assert_eq!(
            ChecksumAlgorithm::Crc64Nvme.hasher().finalize_base64(),
            "AAAAAAAAAAA="
        );
    }

    /// The reflected CRCs match their published catalog check values, computed
    /// over the standard `"123456789"` string. These pin the polynomial and the
    /// init/xor so a wrong constant is caught here rather than as a silent
    /// disagreement with a real client.
    ///
    /// CRC-32C (Castagnoli) check = `0xE306_9283`.
    /// CRC-64/NVME check = `0xAE8B_1486_0A79_9888`.
    #[test]
    fn reflected_crc_check_values() {
        let mut crc32c = ChecksumAlgorithm::Crc32c.hasher();
        crc32c.update(b"123456789");
        assert_eq!(
            crc32c.finalize_base64(),
            STANDARD.encode(0xE306_9283_u32.to_be_bytes())
        );

        let mut crc64 = ChecksumAlgorithm::Crc64Nvme.hasher();
        crc64.update(b"123456789");
        assert_eq!(
            crc64.finalize_base64(),
            STANDARD.encode(0xAE8B_1486_0A79_9888_u64.to_be_bytes())
        );
    }

    /// A second, independent multi-byte vector for each hand-rolled CRC. The
    /// boto3 harness cannot cross-check these (no awscrt), so a single vector is
    /// thin insurance for a from-scratch primitive: a second one over a different
    /// input catches a wrong polynomial or byte order that the first happened to
    /// survive. Values computed over `"hello world"`.
    ///
    /// CRC-32C (Castagnoli) of "hello world" = `0xC994_65AA`.
    /// CRC-64/NVME of "hello world" = `0x8D29_D5C3_F6EA_8EBE`.
    #[test]
    fn reflected_crc_second_vectors() {
        // Asserted both as the literal base64 a client would see and as the
        // base64 of the raw check integer, so the two spellings cross-check.
        let mut crc32c = ChecksumAlgorithm::Crc32c.hasher();
        crc32c.update(b"hello world");
        assert_eq!(crc32c.finalize_base64(), "yZRlqg==");
        assert_eq!("yZRlqg==", STANDARD.encode(0xC994_65AA_u32.to_be_bytes()));

        let mut crc64 = ChecksumAlgorithm::Crc64Nvme.hasher();
        crc64.update(b"hello world");
        assert_eq!(crc64.finalize_base64(), "jSnVw/bqjr4=");
        assert_eq!(
            "jSnVw/bqjr4=",
            STANDARD.encode(0x8D29_D5C3_F6EA_8EBE_u64.to_be_bytes())
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
    fn from_name_is_case_insensitive_and_covers_every_algorithm() {
        assert_eq!(
            ChecksumAlgorithm::from_name("CRC32"),
            Some(ChecksumAlgorithm::Crc32)
        );
        // CRC32C must not be shadowed by the CRC32 prefix.
        assert_eq!(
            ChecksumAlgorithm::from_name("crc32c"),
            Some(ChecksumAlgorithm::Crc32c)
        );
        assert_eq!(
            ChecksumAlgorithm::from_name("sha256"),
            Some(ChecksumAlgorithm::Sha256)
        );
        assert_eq!(
            ChecksumAlgorithm::from_name("Sha1"),
            Some(ChecksumAlgorithm::Sha1)
        );
        assert_eq!(
            ChecksumAlgorithm::from_name("CRC64NVME"),
            Some(ChecksumAlgorithm::Crc64Nvme)
        );
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
