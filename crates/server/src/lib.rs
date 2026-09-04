// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The aks3 server: the settings it starts from and the loop that serves them.
//!
//! The binary in `main.rs` is a thin wrapper over this library, which keeps
//! [`config`] and [`serve`] reachable from integration tests that need to start
//! a server in-process.

pub mod config;
pub mod host;
pub mod serve;
