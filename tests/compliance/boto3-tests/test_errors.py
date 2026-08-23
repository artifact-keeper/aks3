# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only

"""Error surfaces, asserted the way client code branches on them.

Every assertion here goes through boto3's error parse rather than through the
raw XML, because that is what a caller sees: `e.response["Error"]["Code"]` is
the string every retry policy, every `if` in a migration script and every
alerting rule is written against. A server whose error XML is well-formed but
shaped slightly wrong produces a ClientError with an empty code, and code that
branches on it silently takes the wrong branch.
"""

import uuid

import pytest
from botocore.exceptions import ClientError

from conftest import BUCKET_PREFIX

MISSING_BUCKET = f"{BUCKET_PREFIX}absent-{uuid.uuid4().hex[:12]}"


def test_get_of_a_missing_key(s3, bucket):
    with pytest.raises(ClientError) as err:
        s3.get_object(Bucket=bucket, Key="not-here")

    response = err.value.response
    assert response["Error"]["Code"] == "NoSuchKey"
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_head_of_a_missing_key(s3, bucket):
    """HEAD has no body, so boto3 reports the status as the code.

    That is AWS behaviour too, not an aks3 quirk; pinned because a caller
    handling both GET and HEAD has to branch on two different strings and would
    notice if this one changed.
    """
    with pytest.raises(ClientError) as err:
        s3.head_object(Bucket=bucket, Key="not-here")

    assert err.value.response["Error"]["Code"] == "404"
    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


@pytest.mark.parametrize(
    "call",
    [
        lambda c, b: c.get_object(Bucket=b, Key="k"),
        lambda c, b: c.put_object(Bucket=b, Key="k", Body=b"x"),
        lambda c, b: c.delete_object(Bucket=b, Key="k"),
        lambda c, b: c.list_objects_v2(Bucket=b),
        lambda c, b: c.delete_bucket(Bucket=b),
    ],
    ids=["get", "put", "delete", "list", "delete-bucket"],
)
def test_operations_on_a_missing_bucket(s3, call):
    """NoSuchBucket, from every operation, and not NoSuchKey from any of them.

    The mix-up is a real one - the key genuinely is absent as well - and it
    sends the caller looking for a missing object instead of a missing bucket.
    """
    with pytest.raises(ClientError) as err:
        call(s3, MISSING_BUCKET)

    assert err.value.response["Error"]["Code"] == "NoSuchBucket"
    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 404


def test_deleting_a_non_empty_bucket(s3, bucket):
    s3.put_object(Bucket=bucket, Key="occupant", Body=b"x")

    with pytest.raises(ClientError) as err:
        s3.delete_bucket(Bucket=bucket)

    assert err.value.response["Error"]["Code"] == "BucketNotEmpty"
    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 409

    # And the bucket survived the attempt, contents intact.
    assert s3.get_object(Bucket=bucket, Key="occupant")["Body"].read() == b"x"


def test_deleting_a_missing_key_succeeds(s3, bucket):
    """DELETE of an absent key is a 204, as on AWS: delete is idempotent.

    Worth a test because "it was not there" and "it is not there any more" are
    the same outcome, and a server that reported NoSuchKey would break every
    cleanup loop that runs twice.
    """
    response = s3.delete_object(Bucket=bucket, Key="never-existed")
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 204


def test_an_illegal_bucket_name(s3):
    """Rejected by the server, with a code, not by an unhandled 500.

    boto3 does not validate bucket names client-side for a custom endpoint, so
    this really does reach the server.
    """
    with pytest.raises(ClientError) as err:
        s3.create_bucket(Bucket="A")

    assert err.value.response["Error"]["Code"] == "InvalidBucketName"
    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 400


def test_bad_credentials(client_factory, bucket):
    """A wrong secret is a signature error, not a 500 and not a success."""
    client = client_factory(access_key="wrong-key", secret_key="wrong-secret")

    with pytest.raises(ClientError) as err:
        client.list_objects_v2(Bucket=bucket)

    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403


def test_an_unknown_access_key(client_factory, bucket):
    """An access key the server has never heard of is refused the same way.

    Separate from the wrong-secret case because they fail at different points:
    one has no secret to look up, the other looks one up and computes a
    different signature with it. Both have to be a 403 and neither may leak
    which of the two it was.
    """
    client = client_factory(access_key="nobody-in-particular", secret_key="whatever-secret")

    with pytest.raises(ClientError) as err:
        client.list_objects_v2(Bucket=bucket)

    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 403


def test_unimplemented_operations_are_reported_as_such(s3, bucket):
    """Phase 0 has no DeleteObjects and no multipart; that must not be a 500.

    A client that probes for a feature and gets a 500 cannot tell "not
    supported" from "broken", and will usually retry. This pins the current
    answer so the day one of these does get implemented, the test that says it
    is missing is the thing that fails.

    FIXME: delete each case as its operation lands in Phase 1.
    """
    s3.put_object(Bucket=bucket, Key="k", Body=b"x")

    with pytest.raises(ClientError) as err:
        s3.delete_objects(Bucket=bucket, Delete={"Objects": [{"Key": "k"}]})
    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501

    with pytest.raises(ClientError) as err:
        s3.create_multipart_upload(Bucket=bucket, Key="k")
    assert err.value.response["ResponseMetadata"]["HTTPStatusCode"] == 501

    # And the server is still healthy afterwards, which is the half of "not
    # implemented" that a panic would fail.
    assert s3.get_object(Bucket=bucket, Key="k")["Body"].read() == b"x"
