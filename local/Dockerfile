# syntax=docker/dockerfile:1
ARG RUST_VERSION=1.98.1
ARG CLICKHOUSE_VERSION=26.9.8.3

# Optional build/export target: produce Linux musl binaries for the selected
# Docker platform, even when building from macOS. No host Rust toolchain needed.
FROM rust:${RUST_VERSION}-alpine AS udf-build
RUN apk add --no-cache musl-dev
WORKDIR /build
COPY udf/count_tokens_rs/ count_tokens_rs/
COPY udf/count_token_chrs/ count_token_chrs/
RUN cargo build --locked --release --manifest-path count_tokens_rs/Cargo.toml \
    && cargo build --locked --release --manifest-path count_token_chrs/Cargo.toml \
    && mkdir /out \
    && install -m 0555 count_tokens_rs/target/release/count_tokens /out/count_tokens_main \
    && install -m 0555 count_token_chrs/target/release/count_token_chrs /out/count_token_chrs \
    && install -m 0444 count_tokens_rs/models.json /out/models.json

FROM scratch AS binaries
COPY --from=udf-build /out/ /

# Runtime contains configuration only. Compose bind-mounts the built artifacts.
FROM clickhouse/clickhouse-server:${CLICKHOUSE_VERSION} AS clickhouse
COPY local/docker-config.xml /etc/clickhouse-server/config.d/token-udfs.xml
COPY local/docker-functions.xml /etc/clickhouse-server/token-udfs.xml
