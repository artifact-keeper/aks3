# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only

"""ListObjectsV2: pagination, continuation tokens and delimiter folding."""

TREE = [
    "a.txt",
    "b.txt",
    "photos/2024/one.jpg",
    "photos/2024/two.jpg",
    "photos/2025/three.jpg",
    "photos/loose.jpg",
    "z.txt",
]


def _seed(s3, bucket, keys=TREE):
    for key in keys:
        s3.put_object(Bucket=bucket, Key=key, Body=key.encode())


def test_lists_everything_sorted(s3, bucket):
    _seed(s3, bucket)
    listing = s3.list_objects_v2(Bucket=bucket)

    assert [o["Key"] for o in listing["Contents"]] == sorted(TREE)
    assert listing["IsTruncated"] is False
    assert listing["KeyCount"] == len(TREE)
    assert listing["Name"] == bucket
    # Sizes come off the parsed XML, so a listing that reported the manifest's
    # size rather than the object's would be caught here and nowhere else.
    assert {o["Key"]: o["Size"] for o in listing["Contents"]} == {k: len(k) for k in TREE}


def test_delimiter_folds_prefixes(s3, bucket):
    _seed(s3, bucket)
    listing = s3.list_objects_v2(Bucket=bucket, Delimiter="/")

    assert [o["Key"] for o in listing["Contents"]] == ["a.txt", "b.txt", "z.txt"]
    assert [p["Prefix"] for p in listing["CommonPrefixes"]] == ["photos/"]
    # AWS counts a folded prefix as a key for KeyCount purposes. Three keys plus
    # one prefix is four, and a server that reported three would look right
    # until a client used KeyCount to decide whether to keep paging.
    assert listing["KeyCount"] == 4


def test_prefix_and_delimiter_together(s3, bucket):
    _seed(s3, bucket)
    listing = s3.list_objects_v2(Bucket=bucket, Prefix="photos/", Delimiter="/")

    assert [o["Key"] for o in listing["Contents"]] == ["photos/loose.jpg"]
    assert [p["Prefix"] for p in listing["CommonPrefixes"]] == ["photos/2024/", "photos/2025/"]
    assert listing["Prefix"] == "photos/"
    assert listing["Delimiter"] == "/"


def test_prefix_that_is_not_a_folder_boundary(s3, bucket):
    """A prefix may end mid-name; folding still starts after the prefix."""
    _seed(s3, bucket, ["photos/a", "photos/b", "photoshop"])
    listing = s3.list_objects_v2(Bucket=bucket, Prefix="photo")

    assert [o["Key"] for o in listing["Contents"]] == ["photos/a", "photos/b", "photoshop"]


def test_manual_pagination(s3, bucket):
    """Page by hand, the way a script that does not know about paginators does.

    The token is opaque by contract, so this asserts only what a client may
    rely on: that feeding NextContinuationToken back returns the rest, that
    ContinuationToken is echoed, and that the pages concatenate to the whole
    listing exactly once each.
    """
    _seed(s3, bucket)

    seen = []
    token = None
    pages = 0
    while True:
        kwargs = {"Bucket": bucket, "MaxKeys": 2}
        if token is not None:
            kwargs["ContinuationToken"] = token
        listing = s3.list_objects_v2(**kwargs)
        pages += 1

        assert listing["MaxKeys"] == 2
        assert len(listing.get("Contents", [])) <= 2
        if token is not None:
            assert listing["ContinuationToken"] == token
        seen.extend(o["Key"] for o in listing.get("Contents", []))

        if not listing["IsTruncated"]:
            assert "NextContinuationToken" not in listing
            break
        token = listing["NextContinuationToken"]
        # A truncated page that hands back nothing to continue from is an
        # infinite loop for every client that trusts IsTruncated.
        assert token

    assert seen == sorted(TREE)
    assert pages == 4


def test_paginator(s3, bucket):
    """The same walk through boto3's own paginator.

    Worth having next to the manual loop: the paginator decides for itself when
    to stop and what to send next, so it catches a server whose IsTruncated and
    token disagree in a way a hand-written loop happens to tolerate.
    """
    _seed(s3, bucket)
    pages = list(
        s3.get_paginator("list_objects_v2").paginate(
            Bucket=bucket, PaginationConfig={"PageSize": 3}
        )
    )

    assert [o["Key"] for page in pages for o in page.get("Contents", [])] == sorted(TREE)
    assert len(pages) == 3


def test_pagination_across_a_delimiter(s3, bucket):
    """Folded prefixes count against MaxKeys and survive being paged through."""
    _seed(s3, bucket)

    keys, prefixes = [], []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Delimiter": "/", "MaxKeys": 1}
        if token is not None:
            kwargs["ContinuationToken"] = token
        listing = s3.list_objects_v2(**kwargs)

        keys.extend(o["Key"] for o in listing.get("Contents", []))
        prefixes.extend(p["Prefix"] for p in listing.get("CommonPrefixes", []))
        if not listing["IsTruncated"]:
            break
        token = listing["NextContinuationToken"]

    assert keys == ["a.txt", "b.txt", "z.txt"]
    assert prefixes == ["photos/"]


def test_empty_bucket(s3, bucket):
    listing = s3.list_objects_v2(Bucket=bucket)
    assert "Contents" not in listing
    assert listing["KeyCount"] == 0
    assert listing["IsTruncated"] is False


def test_start_after(s3, bucket):
    _seed(s3, bucket)
    listing = s3.list_objects_v2(Bucket=bucket, StartAfter="b.txt")
    assert [o["Key"] for o in listing["Contents"]] == sorted(TREE)[2:]
