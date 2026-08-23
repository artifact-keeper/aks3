# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only

"""Presigned URLs, fetched by something that is not an SDK.

The point of a presigned URL is that whoever follows it has no credentials and
no SDK: a browser, a curl in a runbook, a webhook consumer. So these fetch with
urllib rather than with boto3, which is the only way to find out whether the
URL works for the audience it exists for. The `http_get` fixture is urllib with
proxies explicitly disabled; see conftest.
"""

import urllib.error
import urllib.parse

import pytest

BODY = b"presigned bodies are fetched without credentials"

# The query parameter each signing scheme puts its signature in. SigV4 spells
# it X-Amz-Signature; SigV2 spells it Signature.
SIGNATURE_PARAM = {"s3v4": "X-Amz-Signature", "s3": "Signature"}


@pytest.fixture(params=["s3v4", "s3"], ids=["sigv4", "sigv2"])
def signature_version(request):
    """The signature versions aks3 accepts for presigned URLs.

    s3v4 is what every current SDK and the console produce. s3 is SigV2, which
    botocore still selects by default for presigning against a custom endpoint,
    so it is what a user of aks3 gets without asking - which makes it worth
    knowing it works rather than assuming it does not matter.
    """
    return request.param


@pytest.fixture
def signing_client(signature_version, client_factory):
    return client_factory(signature_version=signature_version)


def test_presigned_get(signing_client, s3, bucket, http_get):
    s3.put_object(Bucket=bucket, Key="shared", Body=BODY)

    url = signing_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "shared"}, ExpiresIn=300
    )

    with http_get(url) as response:
        assert response.status == 200
        assert response.read() == BODY
        assert response.headers["Content-Length"] == str(len(BODY))


def test_presigned_get_of_a_missing_key(signing_client, s3, bucket, http_get):
    """A valid signature over a key that is not there is a 404, not a 403.

    Worth separating: a server that checked existence before the signature, or
    that folded every failure into AccessDenied, would be indistinguishable
    from this one at the call site and very different to debug.
    """
    url = signing_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "absent"}, ExpiresIn=300
    )

    with pytest.raises(urllib.error.HTTPError) as err:
        http_get(url)
    assert err.value.code == 404
    assert b"NoSuchKey" in err.value.read()


def test_presigned_get_with_a_tampered_signature(
    signing_client, signature_version, s3, bucket, http_get
):
    """Changing the signature has to break the URL.

    The cheapest possible check that the signature is actually being verified
    rather than parsed and discarded, which is a failure mode that looks
    perfect from every legitimate client.

    The signature parameter is found and rewritten by name. Mutating the last
    character of the URL would be shorter and would be testing the wrong thing:
    under SigV2 botocore puts Signature before Expires, so the last character
    belongs to the expiry, and the test would pass because the URL expired in
    1970 rather than because the signature was rejected.
    """
    s3.put_object(Bucket=bucket, Key="shared", Body=BODY)
    url = signing_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "shared"}, ExpiresIn=300
    )

    parts = urllib.parse.urlsplit(url)
    query = urllib.parse.parse_qsl(parts.query, keep_blank_values=True)
    param = SIGNATURE_PARAM[signature_version]
    assert param in dict(query), f"no {param} in {parts.query}"

    def flip(value):
        # Rotating the first character cannot collide with the real signature
        # and keeps the length and the alphabet intact, so the server rejects
        # it for the signature being wrong rather than for being malformed.
        return ("b" if value[0] == "a" else "a") + value[1:]

    tampered_query = [(k, flip(v) if k == param else v) for k, v in query]
    tampered = urllib.parse.urlunsplit(
        parts._replace(query=urllib.parse.urlencode(tampered_query))
    )

    with pytest.raises(urllib.error.HTTPError) as err:
        http_get(tampered)
    assert err.value.code == 403
    # The code, not just the status, because 403 is also what an expired URL
    # gets. Asserting the reason is what stops this from passing for a reason
    # that has nothing to do with the signature.
    assert b"SignatureDoesNotMatch" in err.value.read()


def test_presigned_get_of_a_key_that_needs_encoding(signing_client, s3, bucket, http_get):
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

    with http_get(url) as response:
        assert response.read() == BODY


def test_presigned_ranged_get(signing_client, s3, bucket, http_get):
    """A presigned URL is still an ordinary GET, so Range still applies."""
    s3.put_object(Bucket=bucket, Key="shared", Body=BODY)
    url = signing_client.generate_presigned_url(
        "get_object", Params={"Bucket": bucket, "Key": "shared"}, ExpiresIn=300
    )

    with http_get(url, headers={"Range": "bytes=0-8"}) as response:
        assert response.status == 206
        assert response.read() == BODY[:9]
