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

# DockerHub показывает "Source repository" по этому лейблу
LABEL org.opencontainers.image.source="https://github.com/den59k/marci-db" \
      org.opencontainers.image.description="MarciDB — schema-first NoSQL database server" \
      org.opencontainers.image.licenses="MIT"

# Сервер слушает 0.0.0.0:$PORT и хранит БД в /app/data (схема приходит миграциями, schema.marci не нужен)
ENV PORT=3000
EXPOSE 3000
VOLUME ["/app/data"]

CMD ["marcidb-server"]
