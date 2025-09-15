FROM rust:1.89-slim-trixie AS base

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && \
    apt-get install -y \
        git ca-certificates build-essential pkg-config libssl-dev \
        sudo \
        gnupg \
        vim

