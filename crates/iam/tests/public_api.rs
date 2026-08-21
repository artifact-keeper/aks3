// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Pins the constructor shape the server wires up at startup.

use aks3_iam::{IamAuth, RootCredentials};

/// Stands in for `S3ServiceBuilder::set_auth`, which takes `impl S3Auth`.
fn set_auth(_: impl s3s::auth::S3Auth) {}

/// The server builds its auth provider from borrowed config strings.
#[test]
fn auth_provider_builds_from_borrowed_config_strings() -> Result<(), Box<dyn std::error::Error>> {
    let root_access_key: String = "admin".to_owned();
    let root_secret_key: String = "secretpassword".to_owned();

    let auth = IamAuth::new(RootCredentials::new(&root_access_key, &root_secret_key)?);
    set_auth(auth);

    Ok(())
}
