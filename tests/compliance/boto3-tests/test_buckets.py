# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only

"""Bucket lifecycle as boto3 sees it."""

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import BUCKET_PREFIX


def test_create_head_delete(s3):
    """The whole lifecycle, without the bucket fixture doing any of it.

    Every other test leans on that fixture, so this is the one place the
    create/head/delete path is asserted rather than assumed.
    """
    name = f"{BUCKET_PREFIX}{uuid.uuid4().hex[:20]}"

    created = s3.create_bucket(Bucket=name)
    assert created["ResponseMetadata"]["HTTPStatusCode"] == 200
    # AWS answers CreateBucket with a Location of "/<bucket>" for us-east-1;
    # boto3 surfaces it under this key, so a server that omitted the header
    # would show up here rather than in some later mystery.
    assert created["Location"] == f"/{name}"

    assert s3.head_bucket(Bucket=name)["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert name in [b["Name"] for b in s3.list_buckets()["Buckets"]]

    assert s3.delete_bucket(Bucket=name)["ResponseMetadata"]["HTTPStatusCode"] == 204

    with pytest.raises(ClientError) as err:
        s3.head_bucket(Bucket=name)
    # HeadBucket has no response body to carry an error code, so boto3 reports
    # the status as the code. Asserting the status is the honest form of this.
    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_creating_a_bucket_twice(s3, bucket):
    with pytest.raises(ClientError) as err:
        s3.create_bucket(Bucket=bucket)
    assert err.value.response["Error"]["Code"] == "BucketAlreadyOwnedByYou"
    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409


def test_list_buckets_shape(s3, bucket):
    """boto3 parses ListAllMyBucketsResult into typed fields; check they arrive.

    A bucket whose CreationDate the server omitted, or rendered in a format the
    parser could not read, would come back with the key missing rather than as
    an error, so this asserts on the parsed entry rather than on the call
    succeeding.
    """
    entry = next(b for b in s3.list_buckets()["Buckets"] if b["Name"] == bucket)
    assert entry["CreationDate"].tzinfo is not None
