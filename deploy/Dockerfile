# Multi-stage build for the connector binary.
#
# Build:
#   docker build -t connector:latest -f deploy/Dockerfile .
#
# Run (single instance):
#   docker run --rm -p 9090:9090 \
#     -v /dev/shm/aeron:/dev/shm/aeron \
#     -e RUST_LOG=info \
#     connector:latest
#
# Run (multi-instance — set instance.id and instance.total in config, or via env):
#   docker run -d --name connector-0 -p 9090:9090 \
#     -e CONNECTOR__INSTANCE__ID=0 \
#     -e CONNECTOR__INSTANCE__TOTAL=4 \
#     -e RUST_LOG=info \
#     connector:latest

# ── Stage 1: build ───────────────────────────────────────────────────────────
FROM rust:1-slim-bookworm AS builder

# System deps for TLS (reqwest uses native-tls or rustls; install openssl for safety)
RUN apt-get update && apt-get install -y --no-install-recommends \
    pkg-config \
    libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build

# Copy manifests first so Cargo's layer cache survives source-only edits.
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY bin/  bin/
COPY crates/ crates/

# neoverse-n1 = Graviton2 (t4g).  Override with --build-arg RUSTFLAGS="..."
# for a different target, or "" for a fully portable image.
ARG RUSTFLAGS="-C target-cpu=neoverse-n1"
ENV RUSTFLAGS=$RUSTFLAGS

RUN cargo build --release --bin connector

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim

# Only ca-certificates is needed at runtime (TLS chain validation).
RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Dedicated non-root user / group.
RUN groupadd -r connector && useradd -r -g connector -s /sbin/nologin connector

COPY --from=builder /build/target/release/connector /usr/local/bin/connector
COPY config/ /etc/connector/

# /dev/shm/aeron is bind-mounted from the host at runtime (Aeron IPC).
# The connector user must be able to write there; the host mount handles it.

EXPOSE 9090

USER connector

ENTRYPOINT ["/usr/local/bin/connector", "--config", "/etc/connector/default.toml"]
CMD []
