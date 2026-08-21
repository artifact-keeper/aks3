# aks3 (working name)

A single-binary S3-compatible object store written in Rust, derived from MinIO's
engine design. What sets it apart from RustFS (Apache-2.0, young) and Garage
(AGPL, resilience-focused, eventually consistent) is behavioral fidelity to
MinIO, strong consistency, and a published, CI-enforced compliance matrix.

## Origin and license

aks3 is licensed under the GNU Affero General Public License v3.0 only. The full
text is in `LICENSE`.

Parts of the storage engine, IAM policy evaluation, and encryption design are
derived from MinIO. See `NOTICE` for the attribution, the pinned reference
commit, and the trademark statement.

## Status

Status: pre-alpha, Phase 0.

Phase 0 is a single node serving one data directory. Nine S3 operations are
implemented: `CreateBucket`, `HeadBucket`, `DeleteBucket`, `ListBuckets`,
`PutObject`, `GetObject` (including byte ranges), `HeadObject`, `DeleteObject`,
and `ListObjectsV2` with prefixes, delimiters, and pagination. Requests are
authenticated with SigV4 against a single root credential.

## Quickstart

Build the binary:

```
cargo build --release
```

`rust-toolchain.toml` pins the compiler to 1.98, so rustup picks it up without
being asked. The oldest compiler the workspace builds on is 1.94.1, recorded as
`rust-version` in the root `Cargo.toml`.

Write `aks3.toml`:

```toml
listen = "127.0.0.1:9000"
data_dir = "./data"
root_access_key = "admin"
root_secret_key = "secretpassword"
```

Run it:

```
./target/release/aks3 --config aks3.toml
```

The data directory is created if it is not there, and the server logs the
address it bound before it accepts anything.

### Configuration

`listen` defaults to `127.0.0.1:9000` and `data_dir` to `./data`. The root
credentials have no default and have to be set, so the shortest config that
works is those two lines on their own.

Four environment variables override the file, which is what lets an image ship
a config and still take its credentials at run time:

| Variable | Sets |
|----------|------|
| `AKS3_LISTEN` | `listen` |
| `AKS3_DATA_DIR` | `data_dir` |
| `AKS3_ROOT_USER` | `root_access_key` |
| `AKS3_ROOT_PASSWORD` | `root_secret_key` |

With the credentials in the environment there is no need for a file at all:

```
AKS3_ROOT_USER=admin AKS3_ROOT_PASSWORD=secretpassword ./target/release/aks3
```

For TLS, add a `[tls]` table naming a PEM certificate chain and its private key.
Without one the server speaks plain HTTP, which is the Phase 0 default.

```toml
[tls]
cert_pem = "/etc/aks3/cert.pem"
key_pem = "/etc/aks3/key.pem"
```

### Talking to it

```
export AWS_ACCESS_KEY_ID=admin
export AWS_SECRET_ACCESS_KEY=secretpassword
export AWS_REGION=us-east-1

aws --endpoint-url http://127.0.0.1:9000 s3 mb s3://demo
aws --endpoint-url http://127.0.0.1:9000 s3 cp README.md s3://demo/readme.md
aws --endpoint-url http://127.0.0.1:9000 s3 ls s3://demo/
aws --endpoint-url http://127.0.0.1:9000 s3 rm s3://demo/readme.md
aws --endpoint-url http://127.0.0.1:9000 s3 rb s3://demo
```

Path-style addressing is required, because aks3 does not parse virtual-host
style (`bucket.host`) requests yet. Nothing above configures it: the AWS CLI
already uses path style whenever `--endpoint-url` names a custom endpoint, so
the block works as it stands.

The one way to break that is to ask for the other style explicitly, with
`addressing_style = virtual` under an `s3` key in `~/.aws/config`. A client
configured that way puts the bucket in the hostname, aks3 does not recognise the
request, and the answer is `NotImplemented`. If some profile has done that, set
it back for this endpoint:

```
aws configure set default.s3.addressing_style path
```

That setting lives in the config file only. There is no environment variable for
it, so exporting one has no effect.

The same operations driven from the AWS SDK for Rust are what
`crates/server/tests/smoke.rs` runs on every `cargo test`.

### Known limitations

- One node and one data directory. No erasure coding, no replication, no
  distributed mode.
- No multipart upload, so an object is limited to what one `PutObject` can
  carry. No `CopyObject`, no batch `DeleteObjects`, and no versioning API.
- One root credential. No additional users, groups, or policies.
- Object keys become paths under `data_dir`, and letter case is kept as the
  client sent it. On a case-insensitive filesystem, which is the macOS default
  for APFS volumes, that makes `photo.jpg` and `Photo.JPG` one object where S3
  has two. Use a case-sensitive volume for anything that matters. Linux
  filesystems are already case-sensitive.
- Because keys become paths, a key whose components are longer than the
  filesystem's name limit (255 bytes on APFS and ext4) cannot be stored. S3
  allows any key up to 1024 bytes, so such a key is legal and aks3 rejects it,
  with `KeyTooLongError` and a 400 from every operation on it. Nesting the key
  with `/` separators keeps each component under the limit.
