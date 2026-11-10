FROM rust:1.96-bookworm AS builder
WORKDIR /build
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
RUN cargo build --locked --release --bin miniclick

FROM debian:bookworm-slim AS runtime
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /app /data \
    && chown -R 10001:10001 /app /data
WORKDIR /app
COPY --from=builder /build/target/release/miniclick /usr/local/bin/miniclick
USER 10001:10001
EXPOSE 8123
ENTRYPOINT ["miniclick"]
CMD ["--listen-addr", "0.0.0.0:8123", "--data-dir", "/data"]
