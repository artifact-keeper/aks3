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

# How long `docker stop` is told to wait before it gives up and sends SIGKILL.
# Passed explicitly rather than left to the default, so that what this measures
# is the server rather than whatever the local daemon happens to allow.
STOP_GRACE_SECONDS=10

# How long a healthy stop is allowed to take. A server with nothing in flight
# drains and exits in a fraction of a second, so this is loose enough that a
# busy runner cannot trip it and still far below both the server's own eight
# second drain window and STOP_GRACE_SECONDS, which is what lets a failure say
# which of the two went wrong.
STOP_MAX_SECONDS=3

# How long the credential-less container gets to prove it has given up. It
# should exit immediately; anything still alive at the end of this is a server
# that started without credentials, which is the failure this checks for.
REFUSAL_TIMEOUT_SECONDS=30

CONTAINER=""
# Named rather than left to `--rm`, so the exit status and the logs can be read
# back after it stops, and so cleanup can find it if it never does.
REFUSAL_CONTAINER="aks3-smoke-nocreds-$$"
WORKDIR="$(mktemp -d)"

# Prints everything a container said. Called from the exit trap, so a failure
# anywhere (including in the middle of the AWS CLI steps) comes with the log
# rather than just a non-zero status.
dump_logs() {
    local name="$1"
    [ -n "$name" ] || return 0
    docker inspect "$name" >/dev/null 2>&1 || return 0
    echo "--- logs: $name ---" >&2
    docker logs "$name" >&2 2>&1 || true
    echo "--- end logs: $name ---" >&2
}

cleanup() {
    local status=$?
    if [ "$status" -ne 0 ]; then
        dump_logs "$CONTAINER"
        dump_logs "$REFUSAL_CONTAINER"
    fi
    # -v as well as -f: without it every run leaves behind the anonymous volume
    # the image's `VOLUME /data` creates.
    for name in "$CONTAINER" "$REFUSAL_CONTAINER"; do
        [ -n "$name" ] && docker rm -fv "$name" >/dev/null 2>&1 || true
    done
    rm -rf "$WORKDIR"
    return "$status"
}
trap cleanup EXIT

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
    exit 1
fi
echo "1 MiB roundtripped byte for byte"

aws --endpoint-url "$ENDPOINT" s3 rm "s3://$BUCKET/object.bin"
aws --endpoint-url "$ENDPOINT" s3 rb "s3://$BUCKET"

# ---------------------------------------------------------------------------
# The stop. `docker stop`, a Kubernetes pod deletion and `systemctl stop` all
# ask with SIGTERM and only reach for SIGKILL once their grace period is up, so
# a server that does not handle SIGTERM burns the whole grace period on every
# stop, dies with 137, and cuts off whatever it was serving. As PID 1 it does
# not even get the kernel's default disposition to fall back on.
#
# Timed in whole seconds, which is all $SECONDS offers and all this needs: a
# healthy stop is a fraction of a second and the failures are seconds apart
# from it.
# ---------------------------------------------------------------------------
step "stopping with SIGTERM"
stop_started="$SECONDS"
docker stop -t "$STOP_GRACE_SECONDS" "$CONTAINER" >/dev/null
stop_elapsed=$((SECONDS - stop_started))
stop_status="$(docker inspect -f '{{.State.ExitCode}}' "$CONTAINER")"
# Captured rather than piped into `grep -q`, which exits at its first match:
# under `pipefail` the SIGPIPE that gives `docker logs` would fail the pipeline
# on exactly the runs that ought to pass. Same reason as the refusal check
# below.
stop_log="$(docker logs "$CONTAINER" 2>&1)"
echo "stopped in ${stop_elapsed}s with status $stop_status"

if [ "$stop_elapsed" -ge "$STOP_MAX_SECONDS" ]; then
    if [ "$stop_elapsed" -ge "$STOP_GRACE_SECONDS" ]; then
        echo "took the full ${STOP_GRACE_SECONDS}s grace period, so SIGTERM was ignored" >&2
    else
        echo "took ${stop_elapsed}s: SIGTERM was caught, but the drain did not finish" >&2
    fi
    exit 1
fi
if [ "$stop_status" -ne 0 ]; then
    echo "exited with $stop_status rather than 0 (137 means it was killed)" >&2
    exit 1
fi
# The exit status alone cannot tell a drain from a lucky race, so these check
# that the server said what stopped it and that the drain then ran to the end.
# The second is what separates a completed drain from one that gave up on
# connections still open when the window closed.
if ! grep -q "received SIGTERM" <<<"$stop_log"; then
    echo "stopped without logging that it received SIGTERM" >&2
    exit 1
fi
if ! grep -q "all connections closed" <<<"$stop_log"; then
    echo "stopped without finishing the drain" >&2
    exit 1
fi

# ---------------------------------------------------------------------------
# The refusal. Without root credentials the server must exit non-zero rather
# than come up on something guessable, and it must say which setting is
# missing.
#
# Bounded, because the failure this is looking for is a server that starts and
# keeps running: unbounded, that case would hang the job rather than fail it.
#
# Started detached and polled rather than run in the foreground under
# `timeout`, so that what is asserted on is the container's own exit status and
# its own logs, read back after it has stopped. Under `timeout` the status that
# reaches the script is the one the signalled client reports, which says
# nothing about whether the server refused or was stopped.
# ---------------------------------------------------------------------------
step "refusing to start without credentials"
docker run -d --name "$REFUSAL_CONTAINER" "$IMAGE" >/dev/null

deadline=$((SECONDS + REFUSAL_TIMEOUT_SECONDS))
running="true"
while [ "$SECONDS" -lt "$deadline" ]; do
    running="$(docker inspect -f '{{.State.Running}}' "$REFUSAL_CONTAINER")"
    [ "$running" = "false" ] && break
    sleep 1
done

if [ "$running" != "false" ]; then
    echo "still running after ${REFUSAL_TIMEOUT_SECONDS}s, so it started with no credentials" >&2
    exit 1
fi

refusal_status="$(docker inspect -f '{{.State.ExitCode}}' "$REFUSAL_CONTAINER")"
output="$(docker logs "$REFUSAL_CONTAINER" 2>&1)"

if [ "$refusal_status" -eq 0 ]; then
    echo "exited cleanly with no credentials; it must not" >&2
    echo "$output" >&2
    exit 1
fi

echo "exited with status $refusal_status"
echo "$output"
if ! grep -q "AKS3_ROOT_USER" <<<"$output"; then
    echo "exited non-zero but did not name AKS3_ROOT_USER" >&2
    exit 1
fi

echo
echo "smoke test passed: $IMAGE"
