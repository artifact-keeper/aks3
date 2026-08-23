# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only

"""Gives every test of a full-suite recon run its own bucket prefix.

Why this exists
---------------

ceph/s3-tests draws one random bucket prefix per pytest process, and an autouse
fixture empties every bucket carrying that prefix both before and after every
test. Emptying one takes ``ListObjectVersions`` and ``DeleteObjects``, neither
of which Phase 0 aks3 implements, so the first test that ends with a bucket
still standing leaves it standing for the rest of the process, and every test
after it errors in *setup* over a bucket it never touched.

That is fine for the pull-request gate, which is why ``allowlist.txt`` is
written the way it is: every entry on it is a test that ends owning no bucket,
so the shared prefix is never poisoned. It is fatal for a full-suite run.
Measured against the pinned revision with a plain run: 1058 tests collected, one
passed, 1025 errored, and seventeen of the eighteen tests the gate proves green
on every pull request were among the errors. A recon that reports its own gate
as broken is a recon nobody will read twice.

Rotating the prefix before each test restores what the gate relies on: a test is
judged on the buckets it made itself. A test that leaves one behind still errors
in its own teardown, which is the honest answer, because such a test cannot go
on the allowlist until those two operations land.

What it costs
-------------

Nothing cleans up any more, so the store grows for the length of the run. That
is affordable against a temporary directory thrown away at the end of a nightly
job, and is why this plugin is loaded only by ``--full`` runs and never by the
gate.
"""

import configparser
import os

import pytest
import s3tests.functional


def _template():
    """The bucket prefix template from the same config file the suite reads."""
    cfg = configparser.RawConfigParser()
    cfg.read(os.environ["S3TEST_CONF"])
    return cfg.get("fixtures", "bucket prefix", fallback="test-{random}-")


# Before the fixtures, rather than as another autouse fixture: this has to
# happen ahead of s3-tests' own `setup_teardown`, and the relative order of two
# autouse fixtures of the same scope declared in different places is not
# something to rest a run on. `tryfirst` puts it ahead of the default
# implementation, which is what fills the fixtures in.
@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item):
    del item
    s3tests.functional.prefix = s3tests.functional.choose_bucket_prefix(
        template=_template()
    )
