# syntax=docker/dockerfile:1
# Образ для marcidb-server. Сборка из корня воркспейса: docker build -t marcidb-server .

# ---- builder ----
FROM rust:1-bookworm AS builder
WORKDIR /build
COPY . .
RUN cargo build --release -p marcidb-server

# ---- runtime ----
FROM debian:bookworm-slim
WORKDIR /app
COPY --from=builder /build/target/release/marcidb-server /usr/local/bin/marcidb-server

ENV PORT=3000
EXPOSE 3000
VOLUME ["/app/data"]

CMD ["marcidb-server"]
