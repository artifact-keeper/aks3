// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The aks3 storage engine: the object store behind the S3 front end.
//!
//! [`ObjectLayer`] is the contract the API layer codes against, and
//! [`FsEngine`] is the single-disk implementation of it. Everything else here
//! is the machinery those two need: key-to-path encoding ([`paths`]), crash-safe
//! writes ([`atomic`]), the per-object version manifest ([`meta`]) and the walk
//! that turns an object tree back into keys ([`walk`]).

pub mod atomic;
pub mod checksum;
pub mod error;
pub mod fs_engine;
pub mod layer;
pub mod meta;
pub mod paths;
pub mod walk;

pub use checksum::{ChecksumAlgorithm, Checksummer, StoredChecksum};
pub use error::EngineError;
pub use fs_engine::FsEngine;
pub use layer::*;
