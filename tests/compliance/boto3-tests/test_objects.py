# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only

"""Object roundtrips, ranges and metadata, byte-compared."""

import hashlib
import os

import pytest
from botocore.exceptions import ClientError

# A megabyte is the smallest size that is not a single read on either side: it
# crosses hyper's frame boundaries on the way in and the engine's copy buffer on
# the way out, which is where a body that is reassembled in the wrong order or
# truncated at a boundary would show up. Random rather than repeated bytes so a
# body that got duplicated or shifted cannot compare equal by accident.
LARGE = os.urandom(1024 * 1024)
SMALL = b"the quick brown fox jumps over the lazy dog"


def _md5(data):
    return '"%s"' % hashlib.md5(data).hexdigest()


@pytest.mark.parametrize(
    "body, label",
    [(b"", "empty"), (SMALL, "small"), (LARGE, "1MiB")],
    ids=["empty", "small", "1MiB"],
)
def test_put_get_head_delete_roundtrip(s3, bucket, body, label):
    key = f"roundtrip/{label}"

    put = s3.put_object(Bucket=bucket, Key=key, Body=body)
    assert put["ResponseMetadata"]["HTTPStatusCode"] == 200
    # Quoted, and the quotes are part of the value: an unquoted ETag is valid
    # XML that every SDK mis-parses, so it is asserted rather than stripped.
    assert put["ETag"] == _md5(body)

    got = s3.get_object(Bucket=bucket, Key=key)
    assert got["ContentLength"] == len(body)
    assert got["Body"].read() == body
    assert got["ETag"] == _md5(body)

    head = s3.head_object(Bucket=bucket, Key=key)
    assert head["ContentLength"] == len(body)
    assert head["ETag"] == _md5(body)
    assert head["LastModified"].tzinfo is not None

    assert s3.delete_object(Bucket=bucket, Key=key)["ResponseMetadata"]["HTTPStatusCode"] == 204
    with pytest.raises(ClientError) as err:
        s3.get_object(Bucket=bucket, Key=key)
    assert err.value.response["Error"]["Code"] == "NoSuchKey"


def test_overwrite_replaces_the_object(s3, bucket):
    """A second PUT is the whole object, not an append and not a merge."""
    key = "overwritten"
    s3.put_object(Bucket=bucket, Key=key, Body=b"x" * 200)
    s3.put_object(Bucket=bucket, Key=key, Body=b"y")

    got = s3.get_object(Bucket=bucket, Key=key)
    assert got["Body"].read() == b"y"
    assert got["ContentLength"] == 1


@pytest.mark.parametrize(
    "header, expected_bytes, expected_range",
    [
        ("bytes=2-5", b"2345", "bytes 2-5/10"),
        ("bytes=0-0", b"0", "bytes 0-0/10"),
        ("bytes=7-", b"789", "bytes 7-9/10"),
        ("bytes=-3", b"789", "bytes 7-9/10"),
        # Past the end on the high side is not an error: AWS clamps, and a
        # server that 416'd here would break every resumable downloader.
        ("bytes=8-99", b"89", "bytes 8-9/10"),
    ],
)
def test_ranged_get(s3, bucket, header, expected_bytes, expected_range):
    """Ranged reads, asserted through boto3's parse of Content-Range.

    The string form matters as much as the bytes: boto3 hands ContentRange
    through untouched, but s3transfer and every resumable client parse it, and
    the shape ("bytes first-last/total", one space, no unit suffix) is the part
    a hand-rolled formatter gets wrong.
    """
    s3.put_object(Bucket=bucket, Key="ranged", Body=b"0123456789")

    got = s3.get_object(Bucket=bucket, Key="ranged", Range=header)
    assert got["ResponseMetadata"]["HTTPStatusCode"] == 206
    assert got["Body"].read() == expected_bytes
    assert got["ContentLength"] == len(expected_bytes)
    assert got["ContentRange"] == expected_range


def test_unranged_get_reports_no_content_range(s3, bucket):
    s3.put_object(Bucket=bucket, Key="whole", Body=b"0123456789")
    got = s3.get_object(Bucket=bucket, Key="whole")
    assert got["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert "ContentRange" not in got


def test_unsatisfiable_range(s3, bucket):
    s3.put_object(Bucket=bucket, Key="ranged", Body=b"0123456789")
    with pytest.raises(ClientError) as err:
        s3.get_object(Bucket=bucket, Key="ranged", Range="bytes=100-200")
    assert err.value.response["Error"]["Code"] == "InvalidRange"
    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 416


def test_metadata_roundtrip(s3, bucket):
    """x-amz-meta-* and Content-Type survive a PUT, on both GET and HEAD.

    Keys come back lowercased because they are HTTP headers; that is AWS
    behaviour too, and asserting it here stops a future normalisation change
    from passing unnoticed.
    """
    metadata = {"Alpha": "one", "beta": "two", "with-dash": "three"}
    s3.put_object(
        Bucket=bucket,
        Key="described",
        Body=b"body",
        Metadata=metadata,
        ContentType="text/plain",
    )
    lowered = {k.lower(): v for k, v in metadata.items()}

    for response in (
        s3.head_object(Bucket=bucket, Key="described"),
        s3.get_object(Bucket=bucket, Key="described"),
    ):
        assert response["Metadata"] == lowered
        assert response["ContentType"] == "text/plain"


def test_content_type_defaults(s3, bucket):
    s3.put_object(Bucket=bucket, Key="plain", Body=b"body")
    assert s3.head_object(Bucket=bucket, Key="plain")["ContentType"] == "application/octet-stream"


def test_keys_that_need_encoding(s3, bucket):
    """Keys with characters the path layer has to encode, roundtripped.

    The engine's key-to-path encoding has its own property tests; this checks
    that the same keys survive the HTTP layer, where a double-encoded or
    un-encoded path segment is a different bug with the same symptom.
    """
    keys = [
        "spaces in the key",
        "plus+sign",
        "percent%25",
        "unicode/é中文",
        "question?mark",
        "hash#mark",
        "deep/a/b/c/d/e/f",
    ]
    for key in keys:
        s3.put_object(Bucket=bucket, Key=key, Body=key.encode())

    for key in keys:
        assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == key.encode()

    listed = {o["Key"] for o in s3.list_objects_v2(Bucket=bucket)["Contents"]}
    assert listed == set(keys)
