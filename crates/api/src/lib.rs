// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The S3 front end: the `s3s` service and the error mapping it answers with.

pub mod error;
pub mod service;

pub use service::Aks3;
