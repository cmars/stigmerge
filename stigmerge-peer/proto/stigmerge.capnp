@0xc46f97c2b79df618;

###################################################################
# DHT record structures
###################################################################

struct Sha256 {
  p0 @0 :UInt64;
  p1 @1 :UInt64;
  p2 @2 :UInt64;
  p3 @3 :UInt64;
}

struct PublicKey {
  p0 @0 :UInt64;
  p1 @1 :UInt64;
  p2 @2 :UInt64;
  p3 @3 :UInt64;
}

const defaultPieceLength: UInt32 = 1048576;

struct Payload {
  # Metadata about the entire payload to be delivered.

  digest @0 :Sha256;
  length @1 :UInt64;
}

struct Piece {
  # Metadata about a piece of the payload.
  # Pieces are 1MB in size max.

  digest @0 :Sha256;
  length @1 :UInt32;
}

struct File {
  # Metadata about files which exist within the payload.

  contents @0 :Slice;  # Where the contents are located in the payload.
  path @1 :Text;       # A suggested name for the file.
}

struct Slice {
  # Describe a contiguous stream of bytes within a payload.
  # Pieces are consumed in consecutive order from the starting
  # piece until the length is reached.

  startingPiece @0 :UInt32;
  pieceOffset @1 :UInt32;
  length @2 :UInt64;
}

struct Index {
  pieces @0 :List(Piece);
  files @1 :List(File);
}

struct Header {
  # Describe the payload and how to get it.
  # This is the first subkey value of the main DHT key for a share.

  payload @0 :Payload;  # Identify the payload offered.
  subkeys @1 :UInt16;   # Number of DHT subkeys following this one; concatenate and decode to get the Index.
  route @2 :Data;       # Private route to request pieces from this peer.

  pieceSize @3 :UInt32 = .defaultPieceLength;  # Size in bytes of a piece. Defaults to 1MiB.

  haveMap @4 :HaveMap;  # Map of what this peer has.
  peerMap @5 :PeerMap;  # Map of other peers this peer knows.
}

struct HaveMap {
  # Describe a bitmap representing the pieces that this peer has.
  # The DHT key where the bitmap is published contains the raw bitmap
  # chunked into subkeys.

  key @0 :PublicKey;  # DHT key where the bitmap is published.
}

struct PeerMap {
  # Describe a bitmap representing the peers that this peer knows.
  # The DHT key contains PeerInfo records at each subkey.

  key @0 :PublicKey;   # DHT key where the peer map is published.
  subkeys @1 :UInt16;  # Max subkey in the peer DHT with a value.
}

struct PeerInfo {
  # Information published by a peer, about other peers it knows. These are
  # used as a means of localized peer discovery and gossip. Rated score should
  # always be taken as a hint and starting point for prioritization, rather than a
  # directive. A peer may defect, or may just have a different network
  # experience than others.
  #
  # Scores are not strictly defined here, but may be interpreted as thus:
  # - Strongly positive: This peer is recently exchanging valid pieces with me.
  # - Weakly positive: This peer is recently available and I was able to contact it with consistent responses.
  # - Weakly negative: This peer is not available to me, recently unable to connect. Typical route churn.
  # - More negative: This peer is advertising pieces it doesn't seem to have. Could be an outdated DHT.
  # - Strongly negative: This peer sends bad information: invalid pieces, bad peers as good, completely different index than expected.
  #
  # Scores may be used for a multi-tiered prioritization of peer selection, based on the relative distance to self,
  # the difference in self's direct experience and those advertised by others, and the recency of the information.

  key @0 :PublicKey;     # Peer's main DHT key for this share.
  updatedAt @1 :UInt64;  # Timestamp (epoch millis) when this peer info was last updated.
}

###################################################################
# app_call protocol structures
###################################################################

struct BlockRequest {
  # Request a block
  #
  # The block number uses optional extended fields, which provides for:
  # - Smaller message size for typical smaller payloads that use default piece size.
  # - Backwards compatibility with distrans that had a fixed piece size of 1MiB.

  piece @0 :UInt32;
  block @1 :UInt8;
  blockExt1 @2 :UInt8 = 0;   # Overflow of block number, if it doesn't fit in UInt8.
  blockExt2 @3 :UInt16 = 0;  # Overflow of block number, if it doesn't fit in UInt16.
}
