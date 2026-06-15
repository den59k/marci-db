# syntax=docker/dockerfile:1
# Image for marcidb-server. Build from the workspace root: docker build -t marcidb-server .
#
# Two flavours, selected via build args:
#   full    (default) — vector + full-text index modules. The vector module needs nightly
#                       (portable_simd), hence the nightly base image.
#     docker build -t marcidb-server:full .
#   core              — no optional modules, builds on the stable toolchain (smaller, faster).
#     docker build -t marcidb-server:core \
#       --build-arg FEATURES="" --build-arg RUST_IMAGE="rust:bookworm" .

# ---- builder ----
ARG RUST_IMAGE=rustlang/rust:nightly-bookworm
FROM ${RUST_IMAGE} AS builder
WORKDIR /build
# Space-separated cargo features. Empty string = core build (no optional modules).
ARG FEATURES="vector fulltext"
COPY . .
# Only pass --features when non-empty, so the core build stays on stable.
RUN if [ -n "$FEATURES" ]; then \
        cargo build --release -p marcidb-server --features "$FEATURES"; \
    else \
        cargo build --release -p marcidb-server; \
    fi

# ---- runtime ----
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /build/target/release/marcidb-server /usr/local/bin/marcidb-server

ENV PORT=3000
EXPOSE 3000
VOLUME ["/app/data"]

CMD ["marcidb-server"]
