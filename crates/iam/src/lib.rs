// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Identity and access management for aks3.
//!
//! Phase 0 supports a single root credential, exposed to `s3s` through the
//! [`S3Auth`](s3s::auth::S3Auth) implementation on [`IamAuth`].

use std::fmt;

/// Shortest accepted root access key id.
const MIN_ACCESS_KEY_LEN: usize = 3;

/// Shortest accepted root secret key.
const MIN_SECRET_KEY_LEN: usize = 8;

/// Placeholder printed in place of a secret key.
const REDACTED: &str = "[REDACTED]";

/// Reasons a root credential pair can be rejected.
#[derive(Debug, thiserror::Error)]
pub enum CredentialsError {
    /// The access key id was shorter than [`MIN_ACCESS_KEY_LEN`] characters.
    #[error("access key too short")]
    AccessKeyTooShort,
    /// The secret key was shorter than [`MIN_SECRET_KEY_LEN`] characters.
    #[error("secret key too short")]
    SecretKeyTooShort,
}

/// The single root credential pair the server starts with.
#[derive(Clone)]
pub struct RootCredentials {
    /// Access key id. Not a secret; travels in the clear on every request.
    pub access_key: String,
    /// Secret key used to verify request signatures. Never log this.
    pub secret_key: String,
}

impl RootCredentials {
    /// Validates and stores a root credential pair.
    ///
    /// Errors if the access key is shorter than 3 characters or the secret key
    /// is shorter than 8 characters.
    pub fn new(
        access_key: impl Into<String>,
        secret_key: impl Into<String>,
    ) -> Result<Self, CredentialsError> {
        let access_key = access_key.into();
        let secret_key = secret_key.into();
        if access_key.chars().count() < MIN_ACCESS_KEY_LEN {
            return Err(CredentialsError::AccessKeyTooShort);
        }
        if secret_key.chars().count() < MIN_SECRET_KEY_LEN {
            return Err(CredentialsError::SecretKeyTooShort);
        }
        Ok(Self {
            access_key,
            secret_key,
        })
    }
}

impl fmt::Debug for RootCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RootCredentials")
            .field("access_key", &self.access_key)
            .field("secret_key", &REDACTED)
            .finish()
    }
}

/// `s3s` auth provider. Phase 0: root credential only.
#[derive(Debug, Clone)]
pub struct IamAuth {
    /// The one credential pair this provider knows about.
    root: RootCredentials,
}

impl IamAuth {
    /// Builds an auth provider backed by a single root credential.
    #[must_use]
    pub fn new(root: RootCredentials) -> Self {
        Self { root }
    }
}

#[async_trait::async_trait]
impl s3s::auth::S3Auth for IamAuth {
    /// Returns the root secret key when `access_key` is the root access key id.
    ///
    /// Access key ids are not secrets, so a plain comparison is fine here; the
    /// secret itself is only ever compared inside `s3s` signature verification.
    async fn get_secret_key(&self, access_key: &str) -> s3s::S3Result<s3s::auth::SecretKey> {
        if access_key == self.root.access_key {
            Ok(s3s::auth::SecretKey::from(self.root.secret_key.clone()))
        } else {
            Err(s3s::s3_error!(InvalidAccessKeyId))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{CredentialsError, IamAuth, RootCredentials};

    #[test]
    fn validation() {
        assert!(RootCredentials::new("ab", "longenough").is_err());
        assert!(RootCredentials::new("admin", "short").is_err());
        assert!(RootCredentials::new("admin", "secretpassword").is_ok());
    }

    #[test]
    fn validation_reports_which_key_was_rejected() {
        assert!(matches!(
            RootCredentials::new("ab", "longenough"),
            Err(CredentialsError::AccessKeyTooShort)
        ));
        assert!(matches!(
            RootCredentials::new("admin", "short"),
            Err(CredentialsError::SecretKeyTooShort)
        ));
    }

    #[test]
    fn minimum_lengths_are_accepted() {
        assert!(RootCredentials::new("abc", "12345678").is_ok());
    }

    #[tokio::test]
    async fn known_and_unknown_keys() {
        let auth = IamAuth::new(RootCredentials::new("admin", "secretpassword").unwrap());
        assert!(s3s::auth::S3Auth::get_secret_key(&auth, "admin")
            .await
            .is_ok());
        assert!(s3s::auth::S3Auth::get_secret_key(&auth, "intruder")
            .await
            .is_err());
    }

    #[tokio::test]
    async fn known_key_returns_the_root_secret() {
        let auth = IamAuth::new(RootCredentials::new("admin", "secretpassword").unwrap());
        let secret = s3s::auth::S3Auth::get_secret_key(&auth, "admin")
            .await
            .unwrap();
        assert_eq!(secret.expose(), "secretpassword");
    }

    #[tokio::test]
    async fn unknown_key_is_invalid_access_key_id() {
        let auth = IamAuth::new(RootCredentials::new("admin", "secretpassword").unwrap());
        let err = s3s::auth::S3Auth::get_secret_key(&auth, "intruder")
            .await
            .unwrap_err();
        assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidAccessKeyId);
    }

    #[test]
    fn debug_output_hides_the_secret_key() {
        let creds = RootCredentials::new("admin", "secretpassword").unwrap();
        let rendered = format!("{creds:?}");
        assert!(rendered.contains("admin"));
        assert!(!rendered.contains("secretpassword"));

        let rendered = format!("{:?}", IamAuth::new(creds));
        assert!(!rendered.contains("secretpassword"));
    }
}
