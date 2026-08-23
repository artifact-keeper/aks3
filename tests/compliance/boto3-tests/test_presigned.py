# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only

"""Presigned URLs, fetched by something that is not an SDK.

The point of a presigned URL is that whoever follows it has no credentials and
no SDK: a browser, a curl in a runbook, a webhook consumer. So these fetch with
urllib rather than with boto3, which is the only way to find out whether the
URL works for the audience it exists for.
"""

import urllib.error
import urllib.request

import pytest

BODY = b"presigned bodies are fetched without credentials"


@pytest.fixture(params=["s3v4", "s3"], ids=["sigv4", "sigv2"])
def signing_client(request, client_factory):
    """A client per signature version aks3 accepts for presigned URLs.

    s3v4 is what every current SDK and the console produce. s3 is SigV2, which
    botocore still selects by default for presigning against a custom endpoint,
    so it is what a user of aks3 gets without asking - which makes it worth
    knowing it works rather than assuming it does not matter.
    """
    return client_factory(signature_version=request.param)


def test_presigned_get(signing_client, s3, bucket):
    s3.put_object(Bucket=bucket, Key="shared", Body=BODY)

    url = signing_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "shared"}, ExpiresIn=300
    )

    with urllib.request.urlopen(url, timeout=30) as response:
        assert response.status == 200
        assert response.read() == BODY
        assert response.headers["Content-Length"] == str(len(BODY))


def test_presigned_get_of_a_missing_key(signing_client, s3, bucket):
    """A valid signature over a key that is not there is a 404, not a 403.

    Worth separating: a server that checked existence before the signature, or
    that folded every failure into AccessDenied, would be indistinguishable
    from this one at the call site and very different to debug.
    """
    url = signing_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "absent"}, ExpiresIn=300
    )

    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(url, timeout=30)
    assert err.value.code == 404
    assert b"NoSuchKey" in err.value.read()


def test_presigned_get_with_a_tampered_signature(signing_client, s3, bucket):
    """Flipping one character of the signature has to break the URL.

    The cheapest possible check that the signature is actually being verified
    rather than parsed and discarded, which is a failure mode that looks
    perfect from every legitimate client.
    """
    s3.put_object(Bucket=bucket, Key="shared", Body=BODY)
    url = signing_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "shared"}, ExpiresIn=300
    )
    # The last character of the query string is inside the signature for both
    # signing schemes, and rotating it cannot collide with the real value.
    tampered = url[:-1] + ("0" if url[-1] != "0" else "1")

    with pytest.raises(urllib.error.HTTPError) as err:
        urllib.request.urlopen(tampered, timeout=30)
    assert err.value.code == 403


def test_presigned_get_of_a_key_that_needs_encoding(signing_client, s3, bucket):
    """The signature covers the path, so encoding has to agree on both sides.

    A key with a space and a plus sign is the classic disagreement: one side
    encodes the space as %20 and the other as +, and the signature stops
    matching for a reason that reads as a credentials problem.
    """
    key = "shared folder/file+name.txt"
    s3.put_object(Bucket=bucket, Key=key, Body=BODY)

    url = signing_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": key}, ExpiresIn=300
    )

    with urllib.request.urlopen(url, timeout=30) as response:
        assert response.read() == BODY


def test_presigned_ranged_get(signing_client, s3, bucket):
    """A presigned URL is still an ordinary GET, so Range still applies."""
    s3.put_object(Bucket=bucket, Key="shared", Body=BODY)
    url = signing_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "shared"}, ExpiresIn=300
    )

    request = urllib.request.Request(url, headers={"Range": "bytes=0-8"})
    with urllib.request.urlopen(request, timeout=30) as response:
        assert response.status == 206
        assert response.read() == BODY[:9]
