// aks3: S3-compatible object storage server
// Copyright (C) 2026 aks3 contributors
// Derived in part from MinIO (https://github.com/minio/minio), AGPL-3.0.
// SPDX-License-Identifier: AGPL-3.0-only

//! Virtual-hosted-style addressing, end to end, against a real SDK signature.
//!
//! The bucket in the hostname is not just a routing detail: the `Host` header
//! is part of what `SigV4` signs, so a store that reads a bucket out of it has
//! to read the same bytes the client signed. That is what this checks, and it
//! is why the requests below are built by the AWS SDK for Rust rather than by
//! hand.
//!
//! # Why a socket instead of the SDK's own client
//!
//! A virtual-hosted-style request is addressed to `bucket.s3.aks3.test`, which
//! resolves nowhere. Rather than teach the SDK's HTTP stack to dial 127.0.0.1
//! for a name that does not exist, or lean on a CI host file, each request is
//! *presigned* by the SDK and then written to the server's socket verbatim. The
//! signature and the `Host` header are the SDK's; only the delivery is ours.
//! Presigned `SigV4` puts the credential in the query string and signs `host`
//! alone, so nothing here has to reproduce a signing algorithm.

use std::net::SocketAddr;
use std::time::Duration;

use aws_sdk_s3::config::{Credentials, Region};
use aws_sdk_s3::presigning::PresigningConfig;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// The root credentials the test server starts with.
const ACCESS_KEY: &str = "admin";
const SECRET_KEY: &str = "secretpassword";

/// The domain the server is told is its own. `.test` is reserved for exactly
/// this (RFC 6761), so it can never resolve to a real host.
const DOMAIN: &str = "s3.aks3.test";

/// The bucket every request below addresses through the hostname.
const BUCKET: &str = "demo";

/// The object written and read back.
const KEY: &str = "greeting.txt";
const BODY: &[u8] = b"hello from a virtual host";

/// Starts a server whose `virtual_host_domains` is [`DOMAIN`].
async fn start() -> (tempfile::TempDir, SocketAddr) {
    let dir = tempfile::tempdir().unwrap();
    let cfg = aks3_server::config::Config {
        listen: "127.0.0.1:0".into(),
        data_dir: dir.path().to_path_buf(),
        root_access_key: ACCESS_KEY.into(),
        root_secret_key: SECRET_KEY.into(),
        shutdown_grace_seconds: 8,
        virtual_host_domains: vec![DOMAIN.to_owned()],
        tls: None,
    };
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        aks3_server::serve::run(cfg, tx, std::future::pending())
            .await
            .unwrap();
    });
    (dir, rx.await.unwrap())
}

/// A client that addresses buckets path-style at the socket, for the setup and
/// the regression check that path style still works while a domain is set.
fn path_style_client(addr: SocketAddr) -> aws_sdk_s3::Client {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{addr}"))
        .credentials_provider(creds)
        .force_path_style(true)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

/// A client that puts the bucket in the hostname, as the AWS SDKs do by
/// default. Its requests are only ever presigned: nothing dials `DOMAIN`.
fn virtual_host_client(addr: SocketAddr) -> aws_sdk_s3::Client {
    let creds = Credentials::new(ACCESS_KEY, SECRET_KEY, None, None, "test");
    let conf = aws_sdk_s3::Config::builder()
        .behavior_version_latest()
        .region(Region::new("us-east-1"))
        .endpoint_url(format!("http://{DOMAIN}:{}", addr.port()))
        .credentials_provider(creds)
        .force_path_style(false)
        .build();
    aws_sdk_s3::Client::from_conf(conf)
}

fn presigning() -> PresigningConfig {
    PresigningConfig::expires_in(Duration::from_secs(60)).unwrap()
}

/// One HTTP/1.1 request, written to `addr` exactly as `url` describes it.
///
/// `url` is a presigned URL, so its authority is the name the signature covers
/// and its path and query carry the rest. Returns the status line's code and
/// the body.
async fn send(addr: SocketAddr, method: &str, url: &str, body: &[u8]) -> (u16, Vec<u8>) {
    let rest = url.strip_prefix("http://").expect("presigned URL is http");
    let (authority, path_and_query) = match rest.split_once('/') {
        Some((authority, tail)) => (authority, format!("/{tail}")),
        None => (rest, "/".to_owned()),
    };

    // Presigned `SigV4` signs `host` and nothing else, so the two extra headers
    // do not disturb the signature. `close` is what lets the body be read to
    // EOF without parsing chunked framing.
    let head = format!(
        "{method} {path_and_query} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",
        body.len()
    );

    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(head.as_bytes()).await.unwrap();
    stream.write_all(body).await.unwrap();
    stream.flush().await.unwrap();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).await.unwrap();

    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("a response with headers");
    let head = String::from_utf8_lossy(&raw[..split]).to_string();
    let status: u16 = head
        .split_whitespace()
        .nth(1)
        .expect("a status line")
        .parse()
        .expect("a numeric status");
    (status, raw[split + 4..].to_vec())
}

/// The whole point: a bucket named in the hostname is the bucket the request
/// reaches, with the SDK's own signature over that hostname.
#[tokio::test]
async fn virtual_hosted_style_put_and_get() {
    let (_dir, addr) = start().await;
    path_style_client(addr)
        .create_bucket()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();

    let vh = virtual_host_client(addr);
    let put = vh
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .presigned(presigning())
        .await
        .unwrap();
    // The signed authority is where the bucket rides: `demo.s3.aks3.test`.
    assert!(
        put.uri().starts_with(&format!("http://{BUCKET}.{DOMAIN}:")),
        "the SDK addressed {} path-style",
        put.uri()
    );
    let (status, _) = send(addr, "PUT", put.uri(), BODY).await;
    assert_eq!(status, 200, "virtual-hosted-style PUT");

    let get = vh
        .get_object()
        .bucket(BUCKET)
        .key(KEY)
        .presigned(presigning())
        .await
        .unwrap();
    let (status, got) = send(addr, "GET", get.uri(), b"").await;
    assert_eq!(status, 200, "virtual-hosted-style GET");
    assert_eq!(got, BODY);

    // And the object really landed in the bucket the hostname named, which a
    // path-style read of the same bucket proves independently.
    let head = path_style_client(addr)
        .head_object()
        .bucket(BUCKET)
        .key(KEY)
        .send()
        .await
        .unwrap();
    assert_eq!(
        head.content_length(),
        Some(i64::try_from(BODY.len()).unwrap())
    );
}

/// A request through a hostname that is not under the configured domain stays
/// path style. Without this, setting a domain would break every client that
/// reaches the store by service name or IP — which is most of them.
#[tokio::test]
async fn path_style_still_works_while_a_domain_is_configured() {
    let (_dir, addr) = start().await;
    let client = path_style_client(addr);
    client.create_bucket().bucket(BUCKET).send().await.unwrap();
    client
        .put_object()
        .bucket(BUCKET)
        .key(KEY)
        .body(aws_sdk_s3::primitives::ByteStream::from_static(BODY))
        .send()
        .await
        .unwrap();
    let got = client
        .get_object()
        .bucket(BUCKET)
        .key(KEY)
        .send()
        .await
        .unwrap();
    let bytes = got.body.collect().await.unwrap().into_bytes();
    assert_eq!(&bytes[..], BODY);

    let listed = client
        .list_objects_v2()
        .bucket(BUCKET)
        .send()
        .await
        .unwrap();
    assert_eq!(listed.contents().len(), 1);
    assert_eq!(listed.contents()[0].key(), Some(KEY));
}

/// A subdomain of the configured domain that names no bucket is a request for
/// a bucket that does not exist, not a malformed request.
#[tokio::test]
async fn an_unknown_bucket_in_the_hostname_is_no_such_bucket() {
    let (_dir, addr) = start().await;
    let get = virtual_host_client(addr)
        .get_object()
        .bucket("nobody-created-this")
        .key(KEY)
        .presigned(presigning())
        .await
        .unwrap();
    let (status, body) = send(addr, "GET", get.uri(), b"").await;
    assert_eq!(status, 404, "{}", String::from_utf8_lossy(&body));
    assert!(
        String::from_utf8_lossy(&body).contains("NoSuchBucket"),
        "{}",
        String::from_utf8_lossy(&body)
    );
}
