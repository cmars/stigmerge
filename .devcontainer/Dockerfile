FROM rust:1.91-slim-trixie AS base

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && \
    apt-get install -y \
        curl git ca-certificates build-essential pkg-config libssl-dev \
        capnproto \
        sudo \
        gnupg \
        vim zsh
RUN rustup component add rustfmt clippy
RUN sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)"
RUN chsh root -s /usr/bin/zsh
