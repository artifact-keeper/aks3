// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! The `s3s` service that fronts the storage engine.
//!
//! [`Aks3`] is the whole S3 API surface: `s3s` routes a parsed request to one of
//! the [`S3`](s3s::S3) trait methods, and this type answers it by calling the
//! [`ObjectLayer`] it holds. It is generic over the layer rather than taking a
//! `dyn ObjectLayer` so the calls stay static, and it holds an [`Arc`] because
//! `s3s` shares one service across every connection.
//!
//! The trait impl is deliberately empty at this stage. Every method in
//! [`S3`](s3s::S3) has a default body that returns `NotImplemented`, so an empty
//! impl is a complete, compiling service that rejects every operation; the
//! operations get filled in one at a time on top of it.

use std::sync::Arc;

use aks3_engine::ObjectLayer;

/// The S3 service: one storage engine, dressed as the S3 API.
pub struct Aks3<L: ObjectLayer> {
    engine: Arc<L>,
}

impl<L: ObjectLayer> Aks3<L> {
    /// Wrap `engine` in a service `s3s` can serve.
    #[must_use]
    pub fn new(engine: Arc<L>) -> Self {
        Self { engine }
    }

    /// The engine behind this service.
    #[must_use]
    pub fn engine(&self) -> &L {
        &self.engine
    }
}

#[async_trait::async_trait]
impl<L: ObjectLayer> s3s::S3 for Aks3<L> {}

#[cfg(test)]
mod tests {
    use super::Aks3;
    use aks3_engine::FsEngine;
    use std::sync::Arc;

    /// The service has to satisfy `s3s`'s bounds to be servable at all: `S3`
    /// itself, and the `Send + Sync + 'static` it requires.
    #[test]
    fn service_is_a_servable_s3_impl() {
        const fn assert_s3<T: s3s::S3 + Send + Sync + 'static>() {}
        assert_s3::<Aks3<FsEngine>>();
    }

    #[tokio::test]
    async fn new_keeps_the_engine_it_was_given() {
        let dir = tempfile::tempdir().expect("temp dir");
        let engine = Arc::new(FsEngine::open(dir.path()).await.expect("open engine"));
        let service = Aks3::new(Arc::clone(&engine));
        assert!(std::ptr::eq(service.engine(), Arc::as_ptr(&engine)));
    }
}
