#!/usr/bin/env bash
#
# aks3: S3-compatible object storage server
# Copyright (C) 2026 aks3 contributors
# SPDX-License-Identifier: AGPL-3.0-only
#
# Mounts a small filesystem for the crash harness's disk-full leg to fill, and
# unmounts it again with --teardown.
#
# The leg needs a filesystem it is allowed to fill completely, which no test may
# do to the filesystem the checkout is on. Making one needs mount(8), and
# mount(8) needs root, which is why this is a script run beside cargo rather
# than something the test does for itself: `cargo test --workspace` stays the
# single local gate and stays runnable without sudo, and the leg skips itself
# with a printed reason when AKS3_ENOSPC_DIR is not set. The Linux CI job runs
# this first and sets the variable, so the leg is enforced on every pull
# request; a Linux developer can do the same by hand.
#
# A file-backed ext4 image rather than a size-limited tmpfs: ext4 is what a
# single-node deployment actually runs on, and its ENOSPC behaviour (delayed
# allocation, a journal, 5% of the blocks reserved for root) is the behaviour
# worth pinning. The reserve is left at the mkfs default on purpose. A daemon
# runs unprivileged, so the state the leg drives the store into, an unprivileged
# writer seeing ENOSPC while a little space remains for root, is the state a
# real operator meets.
#
# Everything is parameterised through the environment so a developer can put the
# image somewhere other than /var/tmp:
#
#   AKS3_ENOSPC_DIR       mount point                 (default /mnt/aks3-enospc)
#   AKS3_ENOSPC_IMAGE     backing file                (default /var/tmp/aks3-enospc.img)
#   AKS3_ENOSPC_SIZE_MB   size of the filesystem      (default 32)
#
# Usage:
#   tests/enospc/setup-loopback.sh              # create, format and mount
#   tests/enospc/setup-loopback.sh --teardown   # unmount and delete the image

set -euo pipefail

MOUNT="${AKS3_ENOSPC_DIR:-/mnt/aks3-enospc}"
IMAGE="${AKS3_ENOSPC_IMAGE:-/var/tmp/aks3-enospc.img}"
SIZE_MB="${AKS3_ENOSPC_SIZE_MB:-32}"

# Root when we are not already root. Every privileged step goes through this,
# so the script works both under `sudo` and as an ordinary user on a host with
# passwordless sudo, which is what the GitHub runners give us.
as_root() {
  if [ "$(id -u)" -eq 0 ]; then
    "$@"
  else
    sudo "$@"
  fi
}

teardown() {
  # Idempotent: unmounting something that is not mounted, or deleting an image
  # that is not there, is how this looks when the job failed before the mount.
  if mountpoint -q "$MOUNT"; then
    as_root umount "$MOUNT"
  fi
  as_root rm -f "$IMAGE"
  echo "aks3 enospc: unmounted $MOUNT and removed $IMAGE"
}

case "${1-}" in
  --teardown)
    teardown
    exit 0
    ;;
  "") ;;
  *)
    echo "usage: $0 [--teardown]" >&2
    exit 2
    ;;
esac

if [ "$(uname -s)" != "Linux" ]; then
  echo "aks3 enospc: loopback mounts are Linux-only, this is $(uname -s)" >&2
  exit 1
fi

# A stale mount from an earlier run would leave the leg filling a filesystem
# that already holds someone else's data, so start from nothing.
teardown

mkdir -p "$(dirname "$IMAGE")"
truncate -s "${SIZE_MB}M" "$IMAGE"
# -F because the target is a file rather than a block device, -q because a
# mkfs banner in the CI log tells nobody anything.
mkfs.ext4 -F -q "$IMAGE"

as_root mkdir -p "$MOUNT"
as_root mount -o loop "$IMAGE" "$MOUNT"
# The tests run as whoever invoked this, not as root.
as_root chown "$(id -u):$(id -g)" "$MOUNT"

echo "aks3 enospc: mounted a ${SIZE_MB} MiB ext4 filesystem at $MOUNT"
df -h "$MOUNT"
