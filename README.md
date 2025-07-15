# stigmerge

[![crates.io/crates/stigmerge](https://img.shields.io/crates/v/stigmerge.svg)](https://crates.io/crates/stigmerge)
[![docs.rs/stigmerge-fileindex](https://img.shields.io/docsrs/stigmerge_fileindex)](https://docs.rs/stigmerge-fileindex)
[![docs.rs/stigmerge-peer](https://img.shields.io/docsrs/stigmerge_peer)](https://docs.rs/stigmerge-peer)
[![MPL-2.0](https://img.shields.io/crates/l/stigmerge.svg)](./LICENSE)

stigmerge sends and receives file content anonymously over the [Veilid](https://veilid.com) network.

# Quick start

## Seeding a file

Similar to bittorrent, stigmerge users cannot fetch a file unless it's currently being seeded by a peer.

`stigmerge seed <file>` indexes and seeds a file, displaying the dht key which can be used to fetch it. For example (demo file obtained from the Internet Archive):

```
stigmerge seed ~/Videos/Max\ Headroom\ S1E1\ -\ Blipverts.mp4
```

![stigmerge seed command output, displaying the DHT key VLD0:0C9I9V_rcW17FQ_Zd2BB1wg9e-YmlEHh2vFySoat5Ls](media/seed.png)

## Fetching a file

`stigmerge fetch <dht key>` fetches a file from peers seeding it, storing it
in the current working directory by default. Note the DHT key
`VLD0:0C9I9V_rcW17FQ_Zd2BB1wg9e-YmlEHh2vFySoat5Ls` displayed above; the seeder
provides this key to others in order for them to fetch it.

```
stigmerge fetch VLD0:0C9I9V_rcW17FQ_Zd2BB1wg9e-YmlEHh2vFySoat5Ls
```

![stigmerge fetch command output, displaying a partial download in progress](media/fetch.png)

Note that this peer is also seeding the share to its own DHT key,
`VLD0:dJ4DHoakg8DZ_Kz9rhgMcY0eqlCP1AE_izZMU29EsL4`, even while it is still
fetching the content from other peers. There are no leechers in a stigmerge swarm; all peers are
seeders. Peers also support the underlying Veilid network as [Veilid nodes](https://veilid.com/how-it-works/).

## Gossip

stigmerge peers "gossip" with each other, announcing themselves to the peers
they fetch from, and exchanging other peers they have discovered in the swarm
with each other. These relationships are posted to the Veilid DHT, so that even
when a peer drops offline, its last-known neighbors can still be followed from
the share DHT to join the swarm.

A peer might start by fetching blocks of a payload from the initially provided
peer, but eventually it will attempt other peers, and gravitate toward the most
productive peers.

# Usage

See `stigmerge --help` for more options.

TODO: explain some of the highlights here:
- state dir
- output dir
- index digest auth

## Try it!

Try fetching a test file with `stigmerge fetch VLD0:Q0TGVjxV-LjDtpsj1wojsEBx8SU9egVwSlKo0bNjQoY`.

This is seeding from a secret location.

# Install

Install a [binary release](https://github.com/cmars/stigmerge/releases) on Linux, macOS or Windows.

## Podman & Docker

Substitute `docker` for `podman` if necessary.

Build the container with `podman build -t stigmerge .`

Mount the /data volume to seed or fetch files. The current working directory defaults to this volume in the container.

The following example seeds a file `linux.iso` located at `./data/linux.iso`.

```bash
podman run --rm \
    -v `pwd`/data:/data:rw,Z \
    -v `pwd`/state:/state:rw,Z \
    stigmerge seed linux.iso
```

Look for the log message containing the share record key, like this:

```
2025-06-28T00:54:30.118142Z  INFO stigmerge::app: announced share, key: VLD0:RDwOzUCVwY2EhL4z1C_od1J2JMS8oZbPgIuI6k5sS0I
```

Another peer could fetch this file to ~/Downloads/linux.iso with

```bash
podman run --rm \
    -v ~/Downloads:/data:rw,Z \
    -v `pwd`/state:/state:rw,Z \
    stigmerge fetch VLD0:RDwOzUCVwY2EhL4z1C_od1J2JMS8oZbPgIuI6k5sS0I
```

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

## Multi-file shares

The stigmerge wire protocol and indexing structures support multi-file shares, but this hasn't been fully implemented yet.

## Improving the network

There's a lot of opportunity to hint to peers which blocks are available.

There may be a need to shed load and throttle traffic.

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

Open an issue and ask before picking up a branch and proposing, for best results.
