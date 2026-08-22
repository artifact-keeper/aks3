# syntax=docker/dockerfile:1

# The aks3 container image: a compiler-bearing builder on UBI 9, and a final
# stage on ubi-micro carrying the binary and nothing else of aks3's.
#
# What ubi-micro leaves out is the package manager: there is no dnf, no
# microdnf and no rpm, so nothing can be installed into a running container.
# What it keeps is bash and coreutils, so `docker exec` does get a shell, but
# one with no curl, no ps and no network tools to reach for. Real debugging is
# `docker debug` or a sidecar sharing the namespace; the store itself is meant
# to be reached over the S3 API on port 9000. See the README.

# ---------------------------------------------------------------------------
# Stage 1: build the binary.
# ---------------------------------------------------------------------------
FROM registry.access.redhat.com/ubi9/ubi AS builder

# gcc links the binary. cmake, make and perl are for aws-lc-sys, the C crypto
# backend rustls pulls in through tokio-rustls; it builds its own libcrypto
# with CMake and there is no pure-Rust path to it from here.
RUN dnf -y install gcc make cmake perl-core \
    && dnf clean all \
    && rm -rf /var/cache/dnf

# Toolchains live outside /root so the paths stay the same regardless of who
# the build runs as.
ENV RUSTUP_HOME=/usr/local/rustup \
    CARGO_HOME=/usr/local/cargo \
    PATH=/usr/local/cargo/bin:$PATH

# `--default-toolchain none` installs the rustup machinery without a compiler,
# because which compiler to install is not this line's decision to make.
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs \
    | sh -s -- -y --no-modify-path --profile minimal --default-toolchain none

WORKDIR /src

# The compiler version is single-sourced from rust-toolchain.toml: the file is
# copied in first, and `rustup toolchain install` with no toolchain named reads
# it and installs what it asks for. Nothing here repeats the version number, so
# a bump to the file is the whole change. Its own layer, so editing source does
# not reinstall the toolchain.
COPY rust-toolchain.toml ./
RUN rustup toolchain install

COPY Cargo.toml Cargo.lock ./
COPY crates ./crates

# The cargo registry and the target directory are cache mounts, which makes a
# rebuild after a source edit incremental. They are mounts rather than layers,
# so the binary has to be copied somewhere real before the RUN ends or it goes
# away with them.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked -p aks3-server \
    && cp target/release/aks3 /usr/local/bin/aks3

# The data directory is staged here because the final stage cannot make it:
# ubi-micro has no shell, so there is no RUN to mkdir and chown with.
#
# Group 0 and group-writable, rather than a named group and 0755, so the image
# still works when the runtime substitutes a uid of its own. OpenShift assigns
# an arbitrary high-numbered uid but always puts it in group 0, and a store
# that could not write its own data directory under that uid would fail on the
# first PutObject rather than at startup.
RUN mkdir -p /rootfs/data && chmod 0775 /rootfs/data

# ---------------------------------------------------------------------------
# Stage 2: the shipped image.
# ---------------------------------------------------------------------------
FROM registry.access.redhat.com/ubi9/ubi-micro

LABEL org.opencontainers.image.title="aks3" \
      org.opencontainers.image.description="Single-binary S3-compatible object store" \
      org.opencontainers.image.source="https://github.com/artifact-keeper/aks3" \
      org.opencontainers.image.licenses="AGPL-3.0-only"

COPY --from=builder /usr/local/bin/aks3 /usr/local/bin/aks3
# /rootfs holds the staged directory tree, copied over / so that `data` arrives
# as an entry with the mode it was given. Naming /data as the destination
# instead would copy the (empty) contents into a directory BuildKit creates
# fresh at 0755, losing the group-writable bit.
COPY --from=builder --chown=1001:0 /rootfs/ /

# The default listen address in the binary is loopback, which inside a
# container means nothing outside it can connect. An image is only ever reached
# through its published port, so it overrides that here.
ENV AKS3_LISTEN=0.0.0.0:9000 \
    AKS3_DATA_DIR=/data

# AKS3_ROOT_USER and AKS3_ROOT_PASSWORD are not set, and there is no default
# for them. A container started without both exits non-zero naming the one it
# is missing, which is the intended behaviour: an object store that came up on
# a guessable credential would be worse than one that did not come up.

USER 1001
EXPOSE 9000
VOLUME /data
ENTRYPOINT ["/usr/local/bin/aks3"]
