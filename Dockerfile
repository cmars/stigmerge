FROM rust:1.93-slim-bookworm AS base
ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && apt-get install -y git ca-certificates build-essential pkg-config libssl-dev
RUN cargo install sccache --version ^0.7
RUN cargo install cargo-chef --version ^0.1
ENV RUSTC_WRAPPER=sccache SCCACHE_DIR=/sccache

FROM base AS planner
WORKDIR /app
COPY . .
RUN --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo chef prepare --recipe-path recipe.json

FROM base AS builder
WORKDIR /app
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo chef cook --release --recipe-path recipe.json
COPY . .
RUN --mount=type=cache,target=$SCCACHE_DIR,sharing=locked \
    cargo build --release

### Dist image
FROM gcr.io/distroless/cc-debian12 AS dist
COPY --from=builder /app/target/release/stigmerge /usr/bin/stigmerge

ENV RUST_LOG="veilid_core=error,stigmerge=debug"
ENV NO_UI=true

VOLUME /state
ENV STATE_DIR=/state

VOLUME /data
WORKDIR /data

EXPOSE 5150
ENV NODE_ADDR=":5150"

ENTRYPOINT ["/usr/bin/stigmerge"]
