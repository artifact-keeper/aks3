#!/usr/bin/env bash
# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only
#
# The gate a container image has to pass before it is pushed: it starts, it
# answers, it stores and returns an object byte for byte, and it refuses to
# start without root credentials.
#
# Usage: tests/container/smoke.sh <image-ref> [host-port]
#
# Runs the same way locally and in CI, so a failure on a pull request can be
# reproduced by hand with one command.

set -euo pipefail

IMAGE="${1:?usage: smoke.sh <image-ref> [host-port]}"
PORT="${2:-9000}"
ENDPOINT="http://127.0.0.1:${PORT}"

# Throwaway credentials for one container that lives for the length of this
# script. They are not secrets and are not reused anywhere.
ROOT_USER="smoketest"
ROOT_PASSWORD="smoketestsecret"

# How long the server gets to bind and answer before this is called a failure.
READY_TIMEOUT_SECONDS=60

CONTAINER=""
WORKDIR="$(mktemp -d)"

cleanup() {
    if [ -n "$CONTAINER" ]; then
        docker rm -f "$CONTAINER" >/dev/null 2>&1 || true
    fi
    rm -rf "$WORKDIR"
}
trap cleanup EXIT

# Prints everything the container said, for a failure that would otherwise be
# reported as a bare timeout.
dump_logs() {
    echo "--- container logs ---" >&2
    docker logs "$CONTAINER" >&2 2>&1 || true
    echo "--- end container logs ---" >&2
}

step() {
    echo
    echo "==> $*"
}

# ---------------------------------------------------------------------------
# Start.
# ---------------------------------------------------------------------------
step "starting $IMAGE on port $PORT"
CONTAINER="$(docker run -d \
    -e AKS3_ROOT_USER="$ROOT_USER" \
    -e AKS3_ROOT_PASSWORD="$ROOT_PASSWORD" \
    -p "127.0.0.1:${PORT}:9000" \
    "$IMAGE")"
echo "container: $CONTAINER"

# ---------------------------------------------------------------------------
# Readiness. Any HTTP status means the server is listening and answering; an
# unsigned GET of the service root is a 403, so the check is for a response at
# all rather than for a particular code.
# ---------------------------------------------------------------------------
step "waiting for a response from $ENDPOINT"
deadline=$((SECONDS + READY_TIMEOUT_SECONDS))
status=""
while [ "$SECONDS" -lt "$deadline" ]; do
    if ! docker inspect -f '{{.State.Running}}' "$CONTAINER" 2>/dev/null | grep -q true; then
        echo "container exited before it answered" >&2
        dump_logs
        exit 1
    fi
    status="$(curl -s -o /dev/null -w '%{http_code}' --max-time 2 "$ENDPOINT/" || true)"
    if [ -n "$status" ] && [ "$status" != "000" ]; then
        echo "answered with HTTP $status"
        break
    fi
    sleep 1
done
if [ -z "$status" ] || [ "$status" = "000" ]; then
    echo "no HTTP response within ${READY_TIMEOUT_SECONDS}s" >&2
    dump_logs
    exit 1
fi
if [ "$status" != "403" ]; then
    echo "warning: expected 403 from an unsigned request, got $status" >&2
fi

# ---------------------------------------------------------------------------
# A real put and get. The AWS CLI signs with SigV4, so this exercises auth, the
# storage engine and the read path rather than just the socket.
#
# Path-style addressing needs no configuration here: the endpoint is an IP
# address, and the CLI will not put a bucket name in front of one.
# ---------------------------------------------------------------------------
step "put/get roundtrip with the AWS CLI"
export AWS_ACCESS_KEY_ID="$ROOT_USER"
export AWS_SECRET_ACCESS_KEY="$ROOT_PASSWORD"
export AWS_DEFAULT_REGION="us-east-1"
# Point the CLI at config files that do not exist, so a developer profile that
# asked for virtual-host addressing (which aks3 answers with NotImplemented)
# cannot change what this test does.
export AWS_CONFIG_FILE="$WORKDIR/no-config"
export AWS_SHARED_CREDENTIALS_FILE="$WORKDIR/no-credentials"
export AWS_EC2_METADATA_DISABLED=true

BUCKET="smoke-$(date +%s)"
SENT="$WORKDIR/sent.bin"
RECEIVED="$WORKDIR/received.bin"

# Bigger than one buffer and not text, so a truncating or newline-mangling read
# path cannot pass this by accident.
head -c 1048576 /dev/urandom >"$SENT"

aws --endpoint-url "$ENDPOINT" s3 mb "s3://$BUCKET"
aws --endpoint-url "$ENDPOINT" s3 cp "$SENT" "s3://$BUCKET/object.bin"
aws --endpoint-url "$ENDPOINT" s3 ls "s3://$BUCKET/"
aws --endpoint-url "$ENDPOINT" s3 cp "s3://$BUCKET/object.bin" "$RECEIVED"

if ! cmp -s "$SENT" "$RECEIVED"; then
    echo "object came back different from what was sent" >&2
    dump_logs
    exit 1
fi
echo "1 MiB roundtripped byte for byte"

aws --endpoint-url "$ENDPOINT" s3 rm "s3://$BUCKET/object.bin"
aws --endpoint-url "$ENDPOINT" s3 rb "s3://$BUCKET"

step "stopping the container"
docker rm -f "$CONTAINER" >/dev/null
CONTAINER=""

# ---------------------------------------------------------------------------
# The refusal. Without root credentials the server must exit non-zero rather
# than come up on something guessable, and it must say which setting is
# missing.
# ---------------------------------------------------------------------------
step "refusing to start without credentials"
if output="$(docker run --rm "$IMAGE" 2>&1)"; then
    echo "started with no credentials; it must not" >&2
    echo "$output" >&2
    exit 1
fi
echo "$output"
if ! grep -q "AKS3_ROOT_USER" <<<"$output"; then
    echo "exited non-zero but did not name AKS3_ROOT_USER" >&2
    exit 1
fi

echo
echo "smoke test passed: $IMAGE"
