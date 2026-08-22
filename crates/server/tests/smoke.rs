// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! End-to-end checks against the real AWS SDK for Rust.
//!
//! Everything below this line is the actual `aws-sdk-s3` client talking to an
//! in-process aks3 over a real socket. That is the point: the unit tests in the
//! other crates check that each layer does what it says, and this checks that
//! what comes out the other end is S3 as a client that was never told about
//! aks3 understands it, signature included.

use std::net::SocketAddr;

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::primitives::ByteStream;

/// The root credentials the test server starts with.
const ACCESS_KEY: &str = "admin";
const SECRET_KEY: &str = "secretpassword";

/// The object every roundtrip test writes.
const BODY: &[u8] = b"hello world";

/// [`BODY`]'s length as S3 reports a content length.
fn body_len() -> i64 {
    i64::try_from(BODY.len()).unwrap()
}

/// Starts a server on an operating-system-chosen port over a fresh store.
///
/// The returned directory owns the store and must be kept alive for as long as
/// the server is wanted; the server task ends with the test.
///
/// The shutdown trigger is `pending`, so these servers stop by having their
/// task dropped and nothing here installs a signal handler. That matters
/// beyond tidiness: handlers are process-wide, and a test binary that
/// installed them would swallow the SIGINT a developer sends to stop a
/// `cargo test` that is taking too long.
async fn start() -> (tempfile::TempDir, SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = aks3_server::config::Config {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.path().to_path_buf(),
        root_access_key: ACCESS_KEY.into(),
        root_secret_key: SECRET_KEY.into(),
        shutdown_grace_seconds: 8,
        tls: None,
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        aks3_server::serve::run(cfg, tx, std::future::pending())
            .await
            .unwrap();
    });
    // `run` sends the address once it is bound, so there is no sleep here and
    // no race: the client cannot connect before this resolves.
    (dir, rx.await.unwrap())
}

/// An SDK client pointed at `addr`, signing with `secret`.
///
/// Path style is forced because the virtual-host form needs DNS for
/// `bucket.127.0.0.1`, which does not exist.
fn client(addr: SocketAddr, secret: &str) -> aws_sdk_s3::Client {
    let creds = Credentials::new(ACCESS_KEY, secret, None, None, "test");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

#[tokio::test]
async fn aws_sdk_roundtrip() {
    let (_dir, addr) = start().await;
    let c = client(addr, SECRET_KEY);

    c.create_bucket().bucket("smoke").send().await.unwrap();

    c.put_object()
        .bucket("smoke")
        .key("dir/hello.txt")
        .body(ByteStream::from_static(BODY))
        .content_type("text/plain")
        .send()
        .await
        .unwrap();

    let got = c
        .get_object()
        .bucket("smoke")
        .key("dir/hello.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_length(), Some(body_len()));
    assert_eq!(got.content_type(), Some("text/plain"));
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(&bytes[..], BODY);

    let ls = c.list_objects_v2().bucket("smoke").send().await.unwrap();
    assert_eq!(ls.key_count(), Some(1));
    assert_eq!(ls.contents()[0].key(), Some("dir/hello.txt"));

    c.delete_object()
        .bucket("smoke")
        .key("dir/hello.txt")
        .send()
        .await
        .unwrap();
    c.delete_bucket().bucket("smoke").send().await.unwrap();
}

#[tokio::test]
async fn wrong_secret_rejected() {
    let (_dir, addr) = start().await;
    let c = client(addr, "wrong-secret-key");
    let err = c.list_buckets().send().await.unwrap_err();
    let svc = err.into_service_error();
    assert!(format!("{svc:?}").contains("SignatureDoesNotMatch"));
}

/// A bucket the client just made shows up in the listing, and stops showing up
/// once it is gone. `ListBuckets` is the one operation with no path to key off,
/// so it is worth seeing it answer with real content.
#[tokio::test]
async fn buckets_appear_and_disappear_in_list_buckets() {
    let (_dir, addr) = start().await;
    let c = client(addr, SECRET_KEY);

    assert!(c.list_buckets().send().await.unwrap().buckets().is_empty());

    c.create_bucket().bucket("first").send().await.unwrap();
    c.create_bucket().bucket("second").send().await.unwrap();
    let listed = c.list_buckets().send().await.unwrap();
    let mut names: Vec<&str> = listed
        .buckets()
        .iter()
        .filter_map(aws_sdk_s3::types::Bucket::name)
        .collect();
    names.sort_unstable();
    assert_eq!(names, ["first", "second"]);

    c.delete_bucket().bucket("first").send().await.unwrap();
    let after = c.list_buckets().send().await.unwrap();
    assert_eq!(after.buckets().len(), 1);
}

/// A read of something that was never written has to be `NoSuchKey`, not a
/// connection error or a 500. Clients branch on this code.
#[tokio::test]
async fn missing_key_is_no_such_key() {
    let (_dir, addr) = start().await;
    let c = client(addr, SECRET_KEY);
    c.create_bucket().bucket("smoke").send().await.unwrap();

    let err = c
        .get_object()
        .bucket("smoke")
        .key("never-written")
        .send()
        .await
        .unwrap_err()
        .into_service_error();
    assert!(err.is_no_such_key(), "{err:?}");
}

/// A ranged read is what a client doing a parallel download issues, and it is
/// the one read where the status (206) and the headers matter as much as the
/// bytes.
#[tokio::test]
async fn ranged_read_returns_the_slice_and_the_whole_size() {
    let (_dir, addr) = start().await;
    let c = client(addr, SECRET_KEY);
    c.create_bucket().bucket("smoke").send().await.unwrap();
    c.put_object()
        .bucket("smoke")
        .key("hello.txt")
        .body(ByteStream::from_static(BODY))
        .send()
        .await
        .unwrap();

    let got = c
        .get_object()
        .bucket("smoke")
        .key("hello.txt")
        .range("bytes=6-10")
        .send()
        .await
        .unwrap();
    assert_eq!(got.content_length(), Some(5));
    assert_eq!(got.content_range(), Some("bytes 6-10/11"));
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(&bytes[..], b"world");
}

/// The `HEAD` an SDK issues before a download: same metadata as the `GET`, no
/// body.
#[tokio::test]
async fn head_object_reports_the_metadata() {
    let (_dir, addr) = start().await;
    let c = client(addr, SECRET_KEY);
    c.create_bucket().bucket("smoke").send().await.unwrap();
    let put = c
        .put_object()
        .bucket("smoke")
        .key("hello.txt")
        .body(ByteStream::from_static(BODY))
        .content_type("text/plain")
        .send()
        .await
        .unwrap();

    let head = c
        .head_object()
        .bucket("smoke")
        .key("hello.txt")
        .send()
        .await
        .unwrap();
    assert_eq!(head.content_length(), Some(body_len()));
    assert_eq!(head.content_type(), Some("text/plain"));
    // The ETag a write reported is the one a later read reports.
    assert_eq!(head.e_tag(), put.e_tag());
}

/// A prefix-and-delimiter listing is how every client renders a folder view,
/// and it is the one listing shape where the server has to do real work rather
/// than return everything.
#[tokio::test]
async fn listing_with_a_delimiter_rolls_up_prefixes() {
    let (_dir, addr) = start().await;
    let c = client(addr, SECRET_KEY);
    c.create_bucket().bucket("smoke").send().await.unwrap();
    for key in ["a.txt", "sub/b.txt", "sub/c.txt"] {
        c.put_object()
            .bucket("smoke")
            .key(key)
            .body(ByteStream::from_static(BODY))
            .send()
            .await
            .unwrap();
    }

    let ls = c
        .list_objects_v2()
        .bucket("smoke")
        .delimiter("/")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = ls.contents().iter().filter_map(|o| o.key()).collect();
    assert_eq!(keys, ["a.txt"]);
    let prefixes: Vec<&str> = ls
        .common_prefixes()
        .iter()
        .filter_map(|p| p.prefix())
        .collect();
    assert_eq!(prefixes, ["sub/"]);
}

/// An access key the server has never heard of is a different failure from a
/// bad signature, and clients tell users different things about the two.
#[tokio::test]
async fn unknown_access_key_is_rejected() {
    let (_dir, addr) = start().await;
    let creds = Credentials::new("nobody", SECRET_KEY, None, None, "test");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    let c = aws_sdk_s3::Client::from_conf(conf);

    let err = c.list_buckets().send().await.unwrap_err();
    assert!(format!("{:?}", err.into_service_error()).contains("InvalidAccessKeyId"));
}
