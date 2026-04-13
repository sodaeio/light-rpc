FROM rust:1.86-bookworm AS builder

RUN apt-get update && apt-get install -y \
    libclang-dev cmake pkg-config libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /build
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY crates/ crates/

RUN cargo build --release --bin light-indexer

FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/light-indexer /usr/local/bin/
COPY config.example.yml /etc/light-indexer/config.yml

EXPOSE 8876 9090

ENTRYPOINT ["light-indexer"]
CMD ["--config", "/etc/light-indexer/config.yml"]
