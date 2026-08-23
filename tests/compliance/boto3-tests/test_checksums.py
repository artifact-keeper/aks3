# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only

"""What a post-2025 boto3 puts on the wire, and what aks3 does with it.

Since botocore 1.36 (January 2025) `request_checksum_calculation` defaults to
`when_supported`, which means every PutObject carries an integrity checksum
whether the caller asked for one or not, and `response_checksum_validation`
defaults to `when_supported`, which means the SDK will check one on the way
back if the server sends it. That change broke a long list of third-party S3
implementations, and it is the single most likely thing to go wrong between
aks3 and a real client. This file is the record of exactly where aks3 stands.

**Phase 0 aks3 does not implement checksums.** s3s parses the headers and
verifies the aws-chunked framing and, on the signed path, its trailer
signature, but no x-amz-checksum-* value is ever compared against the body,
stored, or returned. Every test here that pins a divergence from AWS says so in
its own words and carries a FIXME naming issue #38, the Phase 1 checksum
matrix. They pass today because they describe today; when the matrix lands they
are the tests that fail and tell you what to change.

One divergence found alongside these has no SDK that produces it, and unlike
the rest it has been fixed rather than pinned: a PUT that declares
`Content-Encoding: aws-chunked` while signing a non-streaming payload sentinel.
s3s decides whether a body is aws-chunked from `x-amz-content-sha256` alone, so
such a request once stored the chunk framing as object bytes under a 200. aks3
now rejects the contradiction with a 400 before the body is touched (issue #37),
and `test_a_contradictory_aws_chunked_put_is_rejected` holds it there. Because
no SDK sends that combination the request is hand-built, the way the
wrong-checksum tests below are.

There are two wire forms and this file exercises both:

  header  - the checksum is an ordinary signed header, `x-amz-checksum-crc32`.
            This is what boto3 sends over plain HTTP, which is what CI runs.
  trailer - the body is sent `Content-Encoding: aws-chunked` with the checksum
            in a trailing header after the final chunk, and the payload is
            signed as STREAMING-UNSIGNED-PAYLOAD-TRAILER. This is what boto3
            sends over HTTPS, which is what production runs, and it is the form
            that historically broke stores: a server that does not decode the
            framing stores it verbatim and silently corrupts every upload.

CI has no TLS, so the trailer form is produced here by flipping botocore's own
checksum-location decision (see `force_trailer_checksum`) rather than by
mocking a wire format by hand. The bytes on the socket are botocore's, not the
test's.
"""

import binascii
import io
import zlib

import boto3
import botocore.auth
import botocore.awsrequest
import pytest
from botocore.credentials import Credentials

PAYLOAD = b"integrity checksums are computed by default since botocore 1.36"


def crc32_b64(data):
    """The value boto3 computes for a body, as it puts it on the wire."""
    return binascii.b2a_base64(zlib.crc32(data).to_bytes(4, "big"), newline=False).decode()


@pytest.fixture
def wire(s3):
    """Records the headers of the last request the client sent.

    A checksum test that only looked at the response would pass against a
    client that had quietly stopped sending checksums, which is exactly what a
    botocore default flipping back would look like. So the assertions start
    from what left the process.
    """
    captured = {}

    def record(request, **_kwargs):
        captured["headers"] = {k.lower(): _text(v) for k, v in request.headers.items()}
        captured["url"] = request.url

    s3.meta.events.register("before-send.s3.*", record)
    yield captured
    s3.meta.events.unregister("before-send.s3.*", record)


def _text(value):
    return value.decode() if isinstance(value, bytes) else value


def force_trailer_checksum(client, operation="PutObject"):
    """Make botocore send its checksum as an aws-chunked trailer over plain HTTP.

    botocore picks the trailer form in `_resolve_request_checksum_algorithm`
    when the operation has a streaming input and the URL scheme is https; it
    keeps the checksum in a header otherwise, because an unsigned trailer
    without TLS would leave the body unauthenticated. CI has no TLS, so the
    choice is overridden here instead of the condition being faked.

    The hook is `before-call`, which is the one event that fires between
    `resolve_checksum_context` deciding the location and `apply_request_checksum`
    acting on it (botocore/client.py). Everything downstream - the
    AwsChunkedWrapper, Content-Encoding, X-Amz-Trailer, X-Amz-Decoded-Content-Length
    and the STREAMING-UNSIGNED-PAYLOAD-TRAILER payload sentinel - is botocore's
    own code doing what it does against real S3 over HTTPS.
    """

    def flip(params=None, **_kwargs):
        algorithm = params.get("context", {}).get("checksum", {}).get("request_algorithm")
        if algorithm is not None:
            algorithm["in"] = "trailer"

    client.meta.events.register(f"before-call.s3.{operation}", flip)
    return lambda: client.meta.events.unregister(f"before-call.s3.{operation}", flip)


def test_default_put_sends_a_crc32_header(s3, bucket, wire):
    """The default upload path: no checksum was asked for, one is sent anyway.

    This is the request every unmodified boto3 script makes. It must succeed
    and it must roundtrip; a store that rejected it would be unusable by any
    current SDK.
    """
    response = s3.put_object(Bucket=bucket, Key="default", Body=PAYLOAD)

    assert wire["headers"]["x-amz-checksum-crc32"] == crc32_b64(PAYLOAD)
    assert wire["headers"]["x-amz-sdk-checksum-algorithm"] == "CRC32"
    # Signed, not merely present: a server that ignored the header would still
    # have to include it in the signature, so a signing bug looks like a 403.
    assert "x-amz-checksum-crc32" in wire["headers"]["authorization"]

    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert s3.get_object(Bucket=bucket, Key="default")["Body"].read() == PAYLOAD


def test_put_response_omits_the_checksum(s3, bucket):
    """DIVERGENCE FROM AWS, pinned deliberately.

    Real S3 echoes the checksum it stored in the PutObject response, and boto3
    surfaces it as `ChecksumCRC32` alongside `ChecksumType: FULL_OBJECT`. aks3
    returns neither, because it stores neither.

    Harmless to boto3 today, which does not validate a PutObject response
    checksum, and invisible to a caller who never looks. It costs the caller
    the end-to-end confirmation the header exists to provide.

    FIXME(#38): when the Phase 1 checksum matrix lands, this assertion inverts: the
    response should carry ChecksumCRC32 equal to crc32_b64(PAYLOAD).
    """
    response = s3.put_object(Bucket=bucket, Key="default", Body=PAYLOAD)

    assert "ChecksumCRC32" not in response
    assert "ChecksumType" not in response


@pytest.mark.parametrize("algorithm", ["CRC32", "SHA1", "SHA256"])
def test_explicitly_requested_algorithms_are_accepted(s3, bucket, wire, algorithm):
    """Every algorithm boto3 can compute without the CRT is accepted.

    Accepted, not verified: see test_a_wrong_checksum_is_not_rejected. What is
    being pinned is that none of them is a 400, because a caller who asks for
    SHA256 explicitly is usually a caller with a compliance reason to.
    """
    key = f"explicit-{algorithm.lower()}"
    response = s3.put_object(
        Bucket=bucket, Key=key, Body=PAYLOAD, ChecksumAlgorithm=algorithm
    )

    assert wire["headers"][f"x-amz-checksum-{algorithm.lower()}"]
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == PAYLOAD


@pytest.mark.parametrize("algorithm", ["CRC32C", "CRC64NVME"])
def test_crt_algorithms_are_out_of_reach(s3, bucket, algorithm):
    """Why this file tests CRC32 and not CRC64NVME.

    CRC64NVME is the algorithm most often named in the checksum-era
    post-mortems, but plain boto3 cannot compute it, and nor can it do CRC32C:
    both need the `awscrt` extra, a compiled wheel this harness deliberately
    does not pull in. botocore refuses the request client-side, so the server
    never sees it, and with no CRT the default stays CRC32.

    Pinned as a test rather than a comment so that adding awscrt to the
    lockfile fails here and forces the CRC64NVME coverage to be written in the
    same commit rather than assumed.
    """
    from botocore.exceptions import MissingDependencyException
    from botocore.httpchecksum import DEFAULT_CHECKSUM_ALGORITHM

    assert DEFAULT_CHECKSUM_ALGORITHM == "CRC32"
    with pytest.raises(MissingDependencyException):
        s3.put_object(
            Bucket=bucket, Key="crt", Body=PAYLOAD, ChecksumAlgorithm=algorithm
        )


def test_a_wrong_checksum_is_not_rejected(s3, bucket, endpoint, credentials):
    """DIVERGENCE FROM AWS, and the sharpest one in this file.

    A correctly signed PutObject whose x-amz-checksum-crc32 does not match the
    body is accepted with a 200. Real S3 answers 400 InvalidRequest, because the
    entire point of the header is to catch a body that changed in transit.

    The request is hand-built rather than made through the client because boto3
    computes the checksum itself and offers no way to lie in it. The signature
    is real and covers the wrong value, so the server is being asked precisely
    the question "do you check this?" and the answer today is no.

    In practice a body corrupted in transit would still fail SigV4 payload
    signing on this path, so this is not an open corruption hole so much as a
    guarantee that is not being provided. On the unsigned-payload paths
    (presigned PUT, streaming trailer) the checksum is the only integrity check
    there is, and it is not being made.

    FIXME(#38): when the Phase 1 checksum matrix lands this becomes a test that the
    server answers 400 InvalidRequest and stores nothing.
    """
    import http.client
    import urllib.parse

    access_key, secret_key = credentials
    key = "wrong-checksum"
    request = botocore.awsrequest.AWSRequest(
        method="PUT",
        url=f"{endpoint}/{bucket}/{key}",
        data=PAYLOAD,
        headers={
            "x-amz-checksum-crc32": crc32_b64(b"not this body at all"),
            "x-amz-sdk-checksum-algorithm": "CRC32",
            "Content-Length": str(len(PAYLOAD)),
        },
    )
    botocore.auth.S3SigV4Auth(
        Credentials(access_key, secret_key), "s3", "us-east-1"
    ).add_auth(request)
    prepared = request.prepare()

    parsed = urllib.parse.urlparse(prepared.url)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=30)
    connection.request("PUT", parsed.path, body=PAYLOAD, headers=dict(prepared.headers))
    response = connection.getresponse()
    body = response.read()
    connection.close()

    assert response.status == 200, body
    # And the bytes are there, under a checksum that describes something else.
    assert s3.get_object(Bucket=bucket, Key=key)["Body"].read() == PAYLOAD


def test_get_with_checksum_mode_enabled(s3, bucket):
    """DIVERGENCE FROM AWS: ChecksumMode=ENABLED has nothing to enable.

    boto3 asks for the stored checksum with `x-amz-checksum-mode: ENABLED` and
    validates whatever comes back. aks3 sends no checksum header, so the SDK's
    validation is a no-op that cannot fail. The GET succeeds and the bytes are
    right - it is the end-to-end integrity guarantee that is absent, not the
    data.

    Asserted as "no checksum field of any kind" rather than "no CRC32", so that
    a Phase 1 implementation that returns a different algorithm still trips it.

    FIXME(#38): when the Phase 1 checksum matrix lands, this inverts to
    response["ChecksumCRC32"] == crc32_b64(PAYLOAD).
    """
    s3.put_object(Bucket=bucket, Key="checked", Body=PAYLOAD)

    response = s3.get_object(Bucket=bucket, Key="checked", ChecksumMode="ENABLED")

    assert response["Body"].read() == PAYLOAD
    assert not [k for k in response if k.startswith("Checksum")]
    assert not [h for h in response["ResponseMetadata"]["HTTPHeaders"] if "checksum" in h]


def test_head_with_checksum_mode_enabled(s3, bucket):
    """Same divergence on the HEAD path, which s3transfer uses to plan reads.

    FIXME(#38): Phase 1 checksum matrix, as above.
    """
    s3.put_object(Bucket=bucket, Key="checked", Body=PAYLOAD)

    response = s3.head_object(Bucket=bucket, Key="checked", ChecksumMode="ENABLED")

    assert not [k for k in response if k.startswith("Checksum")]


def test_aws_chunked_trailer_upload_is_decoded(s3, bucket, wire):
    """The HTTPS wire form, and the one that has broken other stores.

    Over TLS a default boto3 sends the body aws-chunked with the checksum in a
    trailer. If the server treats that as an opaque body it stores the chunk
    framing and the trailer text as object bytes: the object is longer than it
    should be, its ETag is the MD5 of the framing, and nothing errors. That is
    silent corruption of every upload, and it is what several third-party
    stores shipped in early 2025.

    aks3 gets this right, via s3s, which decodes the framing and verifies the
    trailer signature. The assertion is a byte compare rather than a status
    check, because the broken behaviour is a 200.
    """
    undo = force_trailer_checksum(s3)
    try:
        response = s3.put_object(Bucket=bucket, Key="trailered", Body=PAYLOAD)
    finally:
        undo()

    assert wire["headers"]["content-encoding"] == "aws-chunked"
    assert wire["headers"]["transfer-encoding"] == "chunked"
    assert wire["headers"]["x-amz-trailer"] == "x-amz-checksum-crc32"
    assert wire["headers"]["x-amz-decoded-content-length"] == str(len(PAYLOAD))
    assert wire["headers"]["x-amz-content-sha256"] == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"
    assert response["ResponseMetadata"]["HTTPStatusCode"] == 200

    got = s3.get_object(Bucket=bucket, Key="trailered")
    assert got["ContentLength"] == len(PAYLOAD)
    assert got["Body"].read() == PAYLOAD


def test_aws_chunked_trailer_upload_of_a_large_body(s3, bucket, wire):
    """The same path with a body botocore splits into several chunks.

    AwsChunkedWrapper reads in 8 KiB chunks, so a megabyte is a hundred and
    twenty-eight of them plus the terminating zero chunk and the trailer. A
    decoder that handled one chunk and mishandled the boundary between two
    would pass the test above and fail this one.

    It carries the same four wire assertions as its sibling for the same
    reason: without them, a `force_trailer_checksum` that had quietly stopped
    flipping anything would leave this passing as an ordinary header-form PUT,
    which is a test that proves nothing while looking green.
    """
    import os

    payload = os.urandom(1024 * 1024)
    undo = force_trailer_checksum(s3)
    try:
        s3.put_object(Bucket=bucket, Key="trailered-large", Body=payload)
    finally:
        undo()

    assert wire["headers"]["content-encoding"] == "aws-chunked"
    assert wire["headers"]["transfer-encoding"] == "chunked"
    assert wire["headers"]["x-amz-trailer"] == "x-amz-checksum-crc32"
    assert wire["headers"]["x-amz-content-sha256"] == "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

    got = s3.get_object(Bucket=bucket, Key="trailered-large")
    assert got["ContentLength"] == len(payload)
    assert got["Body"].read() == payload


def test_a_wrong_trailer_checksum_is_not_rejected(s3, bucket, endpoint, credentials):
    """DIVERGENCE FROM AWS, and the one with no second line of defence.

    The header-form counterpart above is softened by SigV4 payload signing: a
    body corrupted in transit fails the signature there whether or not the
    checksum is checked. This path has no such cover. `x-amz-content-sha256:
    STREAMING-UNSIGNED-PAYLOAD-TRAILER` means exactly what it says - the seed
    signature covers the headers and not the body - and AWS relies on the
    trailing checksum as the body's integrity check. aks3 decodes the framing
    correctly and then does not check the trailer, so on the path a default
    boto3 takes over HTTPS there is currently no body-integrity check at all.

    Hand-built for the same reason as the header case: boto3 computes the
    trailer itself and will not lie in it. `payload()` is overridden rather than
    the header being set after signing, because the sentinel has to be the value
    the canonical request was built from or the signature would not verify.

    In a real deployment TLS covers the wire, so this is not a remotely
    exploitable substitution. What it means is that TLS is doing all of the
    integrity work and S3's own end-to-end mechanism none of it.

    FIXME(#38): when the Phase 1 checksum matrix lands this becomes a test that
    the server answers 400 and stores nothing.
    """
    import http.client
    import urllib.parse

    access_key, secret_key = credentials
    key = "wrong-trailer-checksum"
    lie = crc32_b64(b"not this body at all")
    framed = (
        b"%x\r\n" % len(PAYLOAD)
        + PAYLOAD
        + b"\r\n0\r\n"
        + f"x-amz-checksum-crc32:{lie}\r\n\r\n".encode()
    )

    class StreamingTrailer(botocore.auth.S3SigV4Auth):
        def payload(self, request):
            return "STREAMING-UNSIGNED-PAYLOAD-TRAILER"

    request = botocore.awsrequest.AWSRequest(
        method="PUT",
        url=f"{endpoint}/{bucket}/{key}",
        data=framed,
        headers={
            "Content-Encoding": "aws-chunked",
            "X-Amz-Trailer": "x-amz-checksum-crc32",
            "X-Amz-Decoded-Content-Length": str(len(PAYLOAD)),
            "Content-Length": str(len(framed)),
        },
    )
    StreamingTrailer(
        Credentials(access_key, secret_key), "s3", "us-east-1"
    ).add_auth(request)
    prepared = request.prepare()

    parsed = urllib.parse.urlparse(prepared.url)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=30)
    connection.request("PUT", parsed.path, body=framed, headers=dict(prepared.headers))
    response = connection.getresponse()
    body = response.read()
    connection.close()

    assert response.status == 200, body
    # The framing was decoded, so this really did take the trailer path and the
    # object is the body rather than the chunk envelope. It is stored under a
    # checksum describing something else entirely.
    stored = s3.get_object(Bucket=bucket, Key=key)
    assert stored["ContentLength"] == len(PAYLOAD)
    assert stored["Body"].read() == PAYLOAD


def test_a_contradictory_aws_chunked_put_is_rejected(s3, bucket, endpoint, credentials):
    """Issue #37: aws-chunked declared, but signed with a non-streaming sentinel.

    s3s keys the aws-chunked decode off `x-amz-content-sha256` alone and ignores
    `Content-Encoding` and `x-amz-decoded-content-length`. A request that
    declares the framing but signs `UNSIGNED-PAYLOAD` therefore disagreed with
    itself, and the raw chunk envelope (`3f\\r\\n...0\\r\\n\\r\\n`) was stored as
    the object's bytes: 63 bytes of body became 74 stored bytes, the ETag was the
    MD5 of the framing, and a 200 was returned. Silent corruption, discovered
    only when the object was read back.

    No SDK produces this combination, so the request is hand-built the way the
    wrong-checksum cases above are. `payload()` is overridden rather than the
    header set after signing, because `UNSIGNED-PAYLOAD` has to be the value the
    canonical request was signed from or the signature would not verify, and the
    framed body is put on the socket by hand.

    aks3 now refuses the contradiction with 400 InvalidRequest before the body is
    touched, and stores nothing. This is the request from the issue, verbatim.
    """
    import http.client
    import urllib.parse

    from botocore.exceptions import ClientError

    access_key, secret_key = credentials
    key = "contradictory-chunked"
    framed = (b"%x\r\n" % len(PAYLOAD)) + PAYLOAD + b"\r\n0\r\n\r\n"

    class Unsigned(botocore.auth.S3SigV4Auth):
        def payload(self, request):
            return "UNSIGNED-PAYLOAD"

    request = botocore.awsrequest.AWSRequest(
        method="PUT",
        url=f"{endpoint}/{bucket}/{key}",
        data=framed,
        headers={
            "Content-Encoding": "aws-chunked",
            "x-amz-decoded-content-length": str(len(PAYLOAD)),
            "Content-Length": str(len(framed)),
        },
    )
    Unsigned(Credentials(access_key, secret_key), "s3", "us-east-1").add_auth(request)
    prepared = request.prepare()

    parsed = urllib.parse.urlparse(prepared.url)
    connection = http.client.HTTPConnection(parsed.hostname, parsed.port, timeout=30)
    connection.request("PUT", parsed.path, body=framed, headers=dict(prepared.headers))
    response = connection.getresponse()
    body = response.read()
    connection.close()

    assert response.status == 400, body
    assert b"InvalidRequest" in body

    # The framing was never decoded into an object: the key is absent, not
    # storing the chunk envelope under an etag that describes it.
    with pytest.raises(ClientError) as excinfo:
        s3.get_object(Bucket=bucket, Key=key)
    assert excinfo.value.response["Error"]["Code"] == "NoSuchKey"


def test_checksum_calculation_can_be_turned_off(client_factory, bucket):
    """The escape hatch a user reaches for when a store rejects checksums.

    `request_checksum_calculation="when_required"` is the documented workaround
    that circulated when the default flipped. It has to keep working against
    aks3 too, and it is worth pinning that with it the header is genuinely gone
    rather than merely ignored.
    """
    client = client_factory(request_checksum_calculation="when_required")
    captured = {}
    client.meta.events.register(
        "before-send.s3.*",
        lambda request, **_kw: captured.update(
            headers={k.lower() for k in request.headers}
        ),
    )

    client.put_object(Bucket=bucket, Key="unchecksummed", Body=PAYLOAD)

    assert not [h for h in captured["headers"] if "checksum" in h]
    assert client.get_object(Bucket=bucket, Key="unchecksummed")["Body"].read() == PAYLOAD


def test_upload_fileobj_roundtrip(s3, bucket):
    """s3transfer's path, which is what upload_file and upload_fileobj use.

    Below the multipart threshold it is a plain PutObject, so this is really a
    check that the managed-transfer wrapper's own bookkeeping agrees with the
    server: a mismatch here is the shape of bug that only appears through the
    high-level API.
    """
    payload = b"managed transfer" * 1000
    s3.upload_fileobj(io.BytesIO(payload), bucket, "managed")

    downloaded = io.BytesIO()
    s3.download_fileobj(bucket, "managed", downloaded)
    assert downloaded.getvalue() == payload


def test_boto3_is_a_checksum_era_release():
    """Guards the premise of this whole file.

    Everything above describes botocore 1.36 and later. A lockfile pinned to
    something older would make the suite pass while testing nothing it claims
    to test.
    """
    major, minor, _ = (int(p) for p in boto3.__version__.split(".")[:3])
    assert (major, minor) >= (1, 36), boto3.__version__
