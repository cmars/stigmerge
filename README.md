# stigmerge

[![crates.io/crates/stigmerge](https://img.shields.io/crates/v/stigmerge.svg)](https://crates.io/crates/stigmerge)
[![docs.rs/stigmerge-fileindex](https://img.shields.io/docsrs/stigmerge_fileindex)](https://docs.rs/stigmerge-fileindex)
[![docs.rs/stigmerge-peer](https://img.shields.io/docsrs/stigmerge_peer)](https://docs.rs/stigmerge-peer)
[![MPL-2.0](https://img.shields.io/crates/l/stigmerge.svg)](./LICENSE)

stigmerge (distribution and transfer) sends and receives file content anonymously over the [Veilid](https://veilid.com) network.

# Usage

`stigmerge seed <file>` indexes and seeds a file, displaying the dht key which can be used to fetch it.

[![asciicast](https://asciinema.org/a/663366.svg)](https://asciinema.org/a/663366)

`stigmerge fetch <dht key> [directory]` fetches a file while it's being seeded (defaults to current directory).

[![asciicast](https://asciinema.org/a/663367.svg)](https://asciinema.org/a/663367)

Similar to bittorrent, stigmerge cannot fetch a file unless it's being seeded by a peer.

See `stigmerge --help` for more options.

## Try it!

Try fetching a test file with `stigmerge fetch VLD0:yb7Mz4g-BaFzn2qDt-xCPzsbzlJz7iq1MOFFBaCXqTw`.

# Install

Install a [binary release](https://github.com/cmars/stigmerge/releases) on Linux, macOS or Windows.

## Rust crate

Build and install from crates.io with `cargo install --locked stigmerge`.

Crate dependencies may require system packages to be installed depending on the target platform.

Debian Bookworm needs `apt-get install build-essential libssl-dev pkg-config`.

Others may be similar.

## Nix flake

Run stigmerge on Nix with `nix run github:cmars/stigmerge`.

Add this flake (`github:cmars/stigmerge`) as an input to your home manager flake.

Or add the default package to a legacy `home.nix` with something like:

    (builtins.getFlake "github:cmars/stigmerge").packages.x86_64-linux.default

## Android (termux binary)

Stigmerge can be compiled from source and run on the command-line with [Termux](https://termux.dev) on Android.

You'll need to install Rust and Cargo to build it, and the following runtime dependencies:

```bash
pkg install which
pkg install openjdk-21
```

Other JDKs might work as well, YMMV. Comments in
[stigmerge/src/bin/stigmerge.rs](./stigmerge/src/bin/stigmerge.rs) explain why
this is currently necessary.

# Plans

What's on the roadmap for a 1.0 release.

## Trackers

Trackers will enable a swarm of stigmerge peers to operate more like bittorrent, where blocks may be simultaneously seeded and fetched.

## Multi-file shares

The stigmerge wire protocol and indexing structures support multi-file shares, but this hasn't been fully implemented yet.

# Troubleshooting

## Clock skew

stigmerge operates an embedded Veilid node, which requires a synchronized local clock. Clock skew can prevent stigmerge from connecting to the Veilid network.

## Debug logging

Logging can be configured with the [RUST_LOG environment variable](https://docs.rs/env_logger/latest/env_logger/#enabling-logging).

`RUST_LOG=debug` will enable all debug-level logging in stigmerge as well as veilid-core, which may be useful for troubleshooting low-level Veilid network problems and reporting issues.

## Issues

When opening an issue, note the OS type, OS release version, stigmerge version, and steps to reproduce. Any logs you can attach may help.

# Development

In a Nix environment, `nix develop github:cmars/stigmerge` (or `nix develop` in this directory) to get a devshell with the necessary tool dependencies.

On other platforms a [Veilid development environment](https://gitlab.com/veilid/veilid/-/blob/2ec00e18da999dd16b8c84444bb1e60f9503e752/DEVELOPMENT.md) will suffice.

`capnp` is only necessary when working on the protocol wire-format.

## CICD

Github is used for CICD and especially [release automation](https://blog.orhun.dev/automated-rust-releases/).

## Contributions

Branches and releases are regularly mirrored to [Codeberg](https://codeberg.org/cmars/stigmerge). Pull requests might be accepted from either, if they fit with the project plans and goals.

This project used to be hosted on Gitlab, but I've deleted my original account there, and will only use Gitlab for contributions when necessary. Why? [Gitlab is a collaborator](https://archive.is/okSlz).

Open an issue and ask before picking up a branch and proposing, for best results.
