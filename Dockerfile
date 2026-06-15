# syntax=docker/dockerfile:1
# Image for marcidb-server. Build from the workspace root: docker build -t marcidb-server .
# Includes the vector + full-text index modules; the vector module needs nightly (portable_simd),
# hence the nightly base image and the --features flags.

# ---- builder ----
FROM rustlang/rust:nightly-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p marcidb-server --features "vector fulltext"

# ---- runtime ----
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /build/target/release/marcidb-server /usr/local/bin/marcidb-server

ENV PORT=3000
EXPOSE 3000
VOLUME ["/app/data"]

CMD ["marcidb-server"]
