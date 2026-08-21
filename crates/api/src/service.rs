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
//! Every method in [`S3`](s3s::S3) has a default body that returns
//! `NotImplemented`, so only the operations implemented here are answered; the
//! rest are rejected until they are filled in. The bucket operations are the
//! first ones in place.
//!
//! A handler does three things and nothing else: unwrap the input, call the
//! engine, and shape the answer. It never decides what an error means (that is
//! [`map_engine_err`]'s job) and never touches the disk, which keeps the whole
//! S3 vocabulary of locations, owners and wire timestamps on this side of the
//! boundary and out of the engine.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use aks3_engine::ObjectLayer;
use s3s::dto::{
    Bucket, CreateBucketInput, CreateBucketOutput, DeleteBucketInput, DeleteBucketOutput,
    HeadBucketInput, HeadBucketOutput, ListBucketsInput, ListBucketsOutput, Owner, Timestamp,
};
use s3s::{s3_error, S3Request, S3Response, S3Result};

use crate::error::map_engine_err;

/// The single owner every bucket in an aks3 store belongs to.
///
/// S3 reports an owner on `ListBuckets`, and clients key off the id. aks3 has
/// no notion of accounts yet, so the store answers with one fixed identity
/// rather than omitting the field and leaving clients to guess.
const OWNER_ID: &str = "aks3-root";
const OWNER_DISPLAY_NAME: &str = "aks3";

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
}

/// The last instant an S3 timestamp can express: 9999-12-31T23:59:59Z, in
/// milliseconds since the epoch.
const MAX_TIMESTAMP_MS: u64 = 253_402_300_799_000;

/// The engine's epoch milliseconds as the `SystemTime` the wire format wants,
/// saturating at [`MAX_TIMESTAMP_MS`].
///
/// The clamp is not decoration. A bucket's creation time is read back from a
/// manifest on disk, so it is an input, and converting a `SystemTime` past year
/// 9999 into a wire [`Timestamp`] panics. A nonsense number in a manifest has to
/// produce a nonsense date, not a dead request handler.
fn epoch_ms_to_systemtime(ms: u64) -> SystemTime {
    UNIX_EPOCH
        .checked_add(Duration::from_millis(ms.min(MAX_TIMESTAMP_MS)))
        .unwrap_or(UNIX_EPOCH)
}

#[async_trait::async_trait]
impl<L: ObjectLayer> s3s::S3 for Aks3<L> {
    async fn create_bucket(
        &self,
        req: S3Request<CreateBucketInput>,
    ) -> S3Result<S3Response<CreateBucketOutput>> {
        let bucket = req.input.bucket;
        self.engine
            .create_bucket(&bucket)
            .await
            .map_err(map_engine_err)?;
        Ok(S3Response::new(CreateBucketOutput {
            location: Some(format!("/{bucket}")),
        }))
    }

    /// A `HEAD` has no body to carry an error in, so the answer is carried
    /// entirely by the status: the `NoSuchBucket` error below becomes the 404.
    async fn head_bucket(
        &self,
        req: S3Request<HeadBucketInput>,
    ) -> S3Result<S3Response<HeadBucketOutput>> {
        if self
            .engine
            .bucket_exists(&req.input.bucket)
            .await
            .map_err(map_engine_err)?
        {
            Ok(S3Response::new(HeadBucketOutput::default()))
        } else {
            Err(s3_error!(NoSuchBucket))
        }
    }

    async fn delete_bucket(
        &self,
        req: S3Request<DeleteBucketInput>,
    ) -> S3Result<S3Response<DeleteBucketOutput>> {
        self.engine
            .delete_bucket(&req.input.bucket)
            .await
            .map_err(map_engine_err)?;
        Ok(S3Response::new(DeleteBucketOutput {}))
    }

    /// Lists every bucket in the store. The engine orders them by name, and
    /// that order is passed through: S3 clients display the list as given.
    async fn list_buckets(
        &self,
        _req: S3Request<ListBucketsInput>,
    ) -> S3Result<S3Response<ListBucketsOutput>> {
        let buckets = self.engine.list_buckets().await.map_err(map_engine_err)?;
        let buckets = buckets
            .into_iter()
            .map(|b| Bucket {
                name: Some(b.name),
                creation_date: Some(Timestamp::from(epoch_ms_to_systemtime(b.created_epoch_ms))),
                ..Default::default()
            })
            .collect();
        Ok(S3Response::new(ListBucketsOutput {
            buckets: Some(buckets),
            owner: Some(Owner {
                display_name: Some(OWNER_DISPLAY_NAME.to_owned()),
                id: Some(OWNER_ID.to_owned()),
            }),
            ..Default::default()
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::{epoch_ms_to_systemtime, Aks3};
    use aks3_engine::FsEngine;
    use s3s::dto::{
        CreateBucketInput, DeleteBucketInput, HeadBucketInput, ListBucketsInput, Timestamp,
    };
    use s3s::{S3ErrorCode, S3Request, S3};
    use std::sync::Arc;

    /// A service over a fresh engine, plus the temp dir it lives in. The dir is
    /// returned because dropping it deletes the store out from under the engine.
    async fn svc() -> (tempfile::TempDir, Aks3<FsEngine>) {
        let dir = tempfile::tempdir().expect("temp dir");
        let engine = FsEngine::open(dir.path()).await.expect("open engine");
        (dir, Aks3::new(Arc::new(engine)))
    }

    /// `s3s` builds an [`S3Request`] from a parsed HTTP request; calling the
    /// trait directly means building one by hand. `s3s` 0.14 exposes no
    /// constructor, so this fills the public fields: everything but the input is
    /// request context that a bucket handler never reads.
    fn req<T>(input: T) -> S3Request<T> {
        S3Request {
            input,
            method: http::Method::default(),
            uri: http::Uri::default(),
            headers: http::HeaderMap::new(),
            extensions: http::Extensions::new(),
            credentials: None,
            region: None,
            service: None,
            trailing_headers: None,
        }
    }

    fn create(bucket: &str) -> S3Request<CreateBucketInput> {
        req(CreateBucketInput {
            bucket: bucket.to_owned(),
            ..Default::default()
        })
    }

    fn head(bucket: &str) -> S3Request<HeadBucketInput> {
        req(HeadBucketInput {
            bucket: bucket.to_owned(),
            ..Default::default()
        })
    }

    fn delete(bucket: &str) -> S3Request<DeleteBucketInput> {
        req(DeleteBucketInput {
            bucket: bucket.to_owned(),
            ..Default::default()
        })
    }

    /// The service has to satisfy `s3s`'s bounds to be servable at all: `S3`
    /// itself, and the `Send + Sync + 'static` it requires.
    #[test]
    fn service_is_a_servable_s3_impl() {
        const fn assert_s3<T: s3s::S3 + Send + Sync + 'static>() {}
        assert_s3::<Aks3<FsEngine>>();
    }

    /// The whole bucket lifecycle over the trait: create, head, list, delete,
    /// and a head that now fails.
    #[tokio::test]
    async fn bucket_ops_via_s3_trait() {
        let (_dir, s) = svc().await;

        s.create_bucket(create("buk")).await.expect("create");
        s.head_bucket(head("buk")).await.expect("head");

        let out = s
            .list_buckets(req(ListBucketsInput::default()))
            .await
            .expect("list");
        assert_eq!(out.output.buckets.expect("buckets").len(), 1);

        s.delete_bucket(delete("buk")).await.expect("delete");
        assert!(s.head_bucket(head("buk")).await.is_err());
    }

    /// The service must act on the engine it was handed, not on one of its own:
    /// a bucket created through the trait is visible on the caller's handle.
    #[tokio::test]
    async fn new_keeps_the_engine_it_was_given() {
        use aks3_engine::ObjectLayer;

        let dir = tempfile::tempdir().expect("temp dir");
        let engine = Arc::new(FsEngine::open(dir.path()).await.expect("open engine"));
        let service = Aks3::new(Arc::clone(&engine));

        service.create_bucket(create("buk")).await.expect("create");
        assert!(engine.bucket_exists("buk").await.expect("exists"));
    }

    /// `CreateBucket` answers with the bucket's location, which is the name
    /// after a slash.
    #[tokio::test]
    async fn create_reports_the_location() {
        let (_dir, s) = svc().await;
        let out = s.create_bucket(create("buk")).await.expect("create");
        assert_eq!(out.output.location.as_deref(), Some("/buk"));
    }

    /// Heading a bucket that was never created is `NoSuchBucket`, not a generic
    /// failure: clients use the code to tell "absent" from "broken".
    #[tokio::test]
    async fn head_of_a_missing_bucket_is_no_such_bucket() {
        let (_dir, s) = svc().await;
        let err = s.head_bucket(head("buk")).await.expect_err("missing");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchBucket);
    }

    /// Re-creating your own bucket is `BucketAlreadyOwnedByYou`, the code the
    /// engine's `BucketAlreadyExists` maps to.
    #[tokio::test]
    async fn creating_a_bucket_twice_reports_already_owned() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("buk")).await.expect("create");
        let err = s.create_bucket(create("buk")).await.expect_err("duplicate");
        assert_eq!(*err.code(), S3ErrorCode::BucketAlreadyOwnedByYou);
    }

    /// Deleting a bucket that is not there is an error, unlike deleting an
    /// absent key.
    #[tokio::test]
    async fn deleting_a_missing_bucket_is_no_such_bucket() {
        let (_dir, s) = svc().await;
        let err = s.delete_bucket(delete("buk")).await.expect_err("missing");
        assert_eq!(*err.code(), S3ErrorCode::NoSuchBucket);
    }

    /// A name the engine rejects reaches the client as `InvalidBucketName`
    /// rather than as an internal error.
    #[tokio::test]
    async fn an_illegal_name_is_rejected_by_code() {
        let (_dir, s) = svc().await;
        let err = s.create_bucket(create("A")).await.expect_err("illegal");
        assert_eq!(*err.code(), S3ErrorCode::InvalidBucketName);
    }

    /// Every listed bucket carries its name and a creation date, and the
    /// listing names an owner: some clients index the response by owner id.
    #[tokio::test]
    async fn list_buckets_reports_names_dates_and_owner() {
        let (_dir, s) = svc().await;
        s.create_bucket(create("alpha")).await.expect("create");
        s.create_bucket(create("bravo")).await.expect("create");

        let out = s
            .list_buckets(req(ListBucketsInput::default()))
            .await
            .expect("list")
            .output;

        let buckets = out.buckets.expect("buckets");
        let names: Vec<_> = buckets
            .iter()
            .map(|b| b.name.clone().expect("name"))
            .collect();
        assert_eq!(names, ["alpha", "bravo"]);
        for b in &buckets {
            let created = b.creation_date.clone().expect("creation date");
            assert!(created > Timestamp::from(std::time::UNIX_EPOCH));
        }

        let owner = out.owner.expect("owner");
        assert_eq!(owner.id.as_deref(), Some("aks3-root"));
        assert_eq!(owner.display_name.as_deref(), Some("aks3"));
    }

    /// Milliseconds are milliseconds: the conversion keeps sub-second
    /// precision rather than truncating to whole seconds.
    #[test]
    fn epoch_ms_converts_including_the_fraction() {
        let t = epoch_ms_to_systemtime(1_500);
        let d = t
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch");
        assert_eq!(d, std::time::Duration::from_millis(1_500));
        assert_eq!(epoch_ms_to_systemtime(0), std::time::UNIX_EPOCH);
    }

    /// The conversion a listing performs must survive any number a manifest
    /// can hold: `Timestamp::from` panics past year 9999, so the clamp is what
    /// keeps a corrupt creation time from killing the request.
    #[test]
    fn an_out_of_range_timestamp_is_clamped_rather_than_panicking() {
        let clamped = Timestamp::from(epoch_ms_to_systemtime(u64::MAX));
        assert_eq!(
            clamped,
            Timestamp::from(epoch_ms_to_systemtime(super::MAX_TIMESTAMP_MS))
        );
        assert!(clamped > Timestamp::from(epoch_ms_to_systemtime(0)));
    }

    /// An empty store lists no buckets, and says so with an empty list rather
    /// than by omitting the field.
    #[tokio::test]
    async fn an_empty_store_lists_an_empty_bucket_list() {
        let (_dir, s) = svc().await;
        let out = s
            .list_buckets(req(ListBucketsInput::default()))
            .await
            .expect("list")
            .output;
        assert_eq!(out.buckets.expect("buckets").len(), 0);
    }
}
