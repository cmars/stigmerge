FROM rust:1.89-slim-bookworm AS base

# Set user and group
ARG user=dev
ARG group=dev
ARG uid=1000
ARG gid=1000
RUN groupadd -g ${gid} ${group}
RUN useradd -u ${uid} -g ${group} -s /bin/sh -d /workspaces/stigmerge -m ${user} # <--- the '-m' create a user home directory

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update -qq && apt-get install -y git ca-certificates build-essential pkg-config libssl-dev

# Switch to user
USER ${uid}:${gid}

