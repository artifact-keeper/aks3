# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only

"""Fixtures for the boto3 golden-path suite.

The suite talks to an aks3 that is already running: run-s3-tests.sh spawns one
server for the whole compliance job and hands its address and credentials over
in the environment. Nothing here starts, stops or configures a server, so a
failure in this suite is always a statement about the server's behaviour rather
than about the harness's ability to produce one.

Why boto3 at all, next to a compliance suite that already drives boto3
underneath: ceph/s3-tests pins the wire format, and pins it as the AWS of a few
years ago. Since January 2025 the AWS SDKs compute an integrity checksum for
every upload by default, which is a wire change s3-tests does not exercise and
which broke a long list of third-party S3 implementations when it shipped. The
tests here are deliberately ordinary client code, run through a current SDK, so
that what breaks is what a user's script would hit.

The directory is called `boto3-tests` rather than `boto3` on purpose. A
directory named `boto3` under tests/compliance would be importable as a
namespace package the moment anything put tests/compliance on sys.path, which
run-s3-tests.sh does for the recon, and would then shadow the real boto3 for
every process that inherited it. The hyphen makes that impossible.
"""

import os
import urllib.request
import uuid

import boto3
import botocore
import pytest
from botocore.config import Config

# Bucket names all start here, so a run that dies without cleaning up leaves an
# obvious trail in a store the next run can be pointed at. The suite creates and
# deletes its own buckets; it never touches one it did not make.
BUCKET_PREFIX = "aks3-boto3-"


def _require_env(name):
    """Read a variable the harness is supposed to have set, or fail the run.

    Not a skip. A boto3 suite that silently skips because it could not find a
    server looks exactly like a boto3 suite that passed, which is the failure
    mode the compliance gate next door already spends a page of shell guarding
    against.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set; this suite runs against the server "
            "run-s3-tests.sh spawns, not one of its own"
        )
    return value


@pytest.fixture(scope="session")
def endpoint():
    return _require_env("AKS3_ENDPOINT")


@pytest.fixture(scope="session")
def credentials():
    return _require_env("AKS3_ACCESS_KEY"), _require_env("AKS3_SECRET_KEY")


@pytest.fixture(scope="session")
def client_factory(endpoint, credentials):
    """Builds clients that differ only in the knob under test.

    Two settings are fixed for every client the suite makes:

    `addressing_style="path"`, because aks3 is addressed by IP in CI and
    virtual-host addressing would need DNS that resolves bucket names.

    `retries={"max_attempts": 1}`, because a retry here would turn a server
    that answers one request in three into a green suite. The repo's standing
    position is that retrying masks the bugs these tests exist to find; the
    default of five attempts is a client-side courtesy that has no place in a
    correctness gate.

    `access_key` and `secret_key` override the harness's credentials, which is
    how the bad-credentials test gets a client that will be refused. Passing
    them to `session.client` is the supported way to do it; reaching into
    `client._request_signer` afterwards would work today and is exactly the
    kind of private-attribute grip that breaks on a botocore bump for reasons
    nobody remembers.
    """
    harness_access_key, harness_secret_key = credentials
    session = boto3.session.Session()

    def make(access_key=None, secret_key=None, **config_kwargs):
        s3_config = {"addressing_style": "path"}
        s3_config.update(config_kwargs.pop("s3", {}))
        return session.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key or harness_access_key,
            aws_secret_access_key=secret_key or harness_secret_key,
            region_name="us-east-1",
            config=Config(
                s3=s3_config,
                retries={"max_attempts": 1, "mode": "standard"},
                connect_timeout=10,
                read_timeout=60,
                **config_kwargs,
            ),
        )

    return make


@pytest.fixture(scope="session")
def http_get():
    """Fetch a URL with urllib, ignoring any proxy the environment configures.

    urllib's default opener honours http_proxy and HTTP_PROXY. The presigned
    tests fetch from a loopback address, and a proxy in the environment would
    either fail the connection or, worse, forward it somewhere that answered,
    which would make those tests a statement about the proxy. `ProxyHandler({})`
    is urllib's documented way to say "no proxies", and it costs nothing on the
    machines that have none.

    Returns a callable rather than an opener so callers cannot accidentally
    reach for `urlopen` and lose the handler.
    """
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def get(url, headers=None, timeout=30):
        return opener.open(
            urllib.request.Request(url, headers=headers or {}), timeout=timeout
        )

    return get


@pytest.fixture(scope="session")
def s3(client_factory):
    """The client almost every test uses: boto3 with nothing turned off."""
    return client_factory()


def _empty_bucket(client, bucket):
    """Delete every object in a bucket, one at a time.

    One at a time because Phase 0 aks3 has no DeleteObjects; when it lands this
    can become a batched delete, and the fact that it cannot yet is itself
    pinned by test_errors.py.
    """
    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            client.delete_object(Bucket=bucket, Key=obj["Key"])


@pytest.fixture
def bucket(s3):
    """A fresh empty bucket, removed with its contents when the test ends.

    Fresh per test rather than per session: listing and pagination assertions
    are statements about the whole bucket, and a bucket shared with the test
    that ran before it would make those statements depend on ordering.
    """
    name = f"{BUCKET_PREFIX}{uuid.uuid4().hex[:20]}"
    s3.create_bucket(Bucket=name)
    try:
        yield name
    finally:
        _empty_bucket(s3, name)
        s3.delete_bucket(Bucket=name)


@pytest.fixture(scope="session", autouse=True)
def report_versions():
    """Put the SDK version in the log.

    Which boto3 ran is the first question asked of any checksum-era failure,
    and the answer moves whenever Dependabot bumps the lockfile.
    """
    print(f"\nboto3 {boto3.__version__}, botocore {botocore.__version__}")


_skipped = []


def pytest_runtest_logreport(report):
    if report.skipped:
        _skipped.append(report.nodeid)


def pytest_sessionfinish(session, exitstatus):
    """A skip fails the run.

    Same discipline the compliance gate next door enforces by counting JUnit
    attributes, for the same reason: pytest exits 0 when every test skipped, and
    a skip is what a missing feature, an unreachable server or a fixture that
    quietly gave up all look like from the outside. Nothing in this suite has a
    legitimate reason to skip - there are no optional dependencies and no
    platform branches - so one appearing means something stopped being tested,
    and that should be loud.

    Written as a hook rather than a `-p no:skipping` flag so the reason is here,
    next to the reasoning, rather than in an argument list in a shell script.
    """
    if _skipped:
        print("\nthese tests skipped, which this suite treats as a failure:")
        for node in _skipped:
            print(f"  {node}")
        session.exitstatus = 1
