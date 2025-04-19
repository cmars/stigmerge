mod header;
mod index;
mod peerinfo;
mod request;

#[allow(dead_code)]
#[rustfmt::skip]
mod stigmerge_capnp;

use std::{array::TryFromSliceError, num::TryFromIntError, path::PathBuf, str::Utf8Error};

use veilid_core::ValueData;

pub use header::{HaveMapRef, Header, PeerMapRef};
pub use peerinfo::PeerInfo;
pub use request::{AnnouncePeerRequest, BlockRequest, Request};

const MAX_RECORD_DATA_SIZE: usize = 1_048_576;
const MAX_INDEX_BYTES: usize = MAX_RECORD_DATA_SIZE - ValueData::MAX_LEN;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Capnp(#[from] capnp::Error),
    #[error("{0}")]
    NotInSchema(#[from] capnp::NotInSchema),
    #[error("{0}")]
    InternalDigestSlice(#[from] TryFromSliceError),
    #[error("index too large: {0} bytes")]
    IndexTooLarge(usize),
    #[error("{0}")]
    EncodePath(PathBuf),
    #[error("{0}")]
    DecodePath(#[from] Utf8Error),
    #[error("{0}")]
    InternalUsize(#[from] TryFromIntError),
    #[error("{0}")]
    Other(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub trait Encoder {
    fn encode(&self) -> Result<Vec<u8>>;
}

pub trait Decoder: Sized {
    fn decode(buf: &[u8]) -> Result<Self>;
}

pub type Digest = [u8; 32];
pub type PublicKey = [u8; 32];
