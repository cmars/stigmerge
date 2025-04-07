use capnp::{
    message::{self, ReaderOptions},
    serialize,
};
use veilid_core::{Timestamp, TypedKey};

use super::{stigmerge_capnp::peer_info, Decoder, Encoder, PublicKey};

pub struct PeerInfo {
    key: TypedKey,
    updated_at: Timestamp,
}

impl PeerInfo {
    pub fn new(key: TypedKey) -> Self {
        Self {
            key,
            updated_at: Timestamp::now(),
        }
    }

    pub fn new_updated_at(key: TypedKey, updated_at: Timestamp) -> Self {
        Self { key, updated_at }
    }

    pub fn key(&self) -> &TypedKey {
        &self.key
    }

    pub fn updated_at(&self) -> Timestamp {
        self.updated_at
    }
}

impl Encoder for PeerInfo {
    fn encode(&self) -> super::Result<Vec<u8>> {
        let mut builder = message::Builder::new_default();
        let mut peer_info_builder = builder.get_root::<peer_info::Builder>()?;

        let mut typed_key_builder = peer_info_builder.reborrow().init_key();
        typed_key_builder.set_kind(self.key.kind.into());

        let mut key_builder = typed_key_builder.reborrow().init_key();
        key_builder.set_p0(u64::from_be_bytes(self.key.value[0..8].try_into()?));
        key_builder.set_p1(u64::from_be_bytes(self.key.value[8..16].try_into()?));
        key_builder.set_p2(u64::from_be_bytes(self.key.value[16..24].try_into()?));
        key_builder.set_p3(u64::from_be_bytes(self.key.value[24..32].try_into()?));

        let message = serialize::write_message_segments_to_words(&builder);
        Ok(message)
    }
}

impl Decoder for PeerInfo {
    fn decode(buf: &[u8]) -> super::Result<Self> {
        let reader = serialize::read_message(buf, ReaderOptions::new())?;
        let peer_info_reader = reader.get_root::<peer_info::Reader>()?;
        let typed_key_reader = peer_info_reader.get_key()?;
        let key_reader = typed_key_reader.get_key()?;

        let mut key = PublicKey::default();
        key[0..8].clone_from_slice(&key_reader.get_p0().to_be_bytes()[..]);
        key[8..16].clone_from_slice(&key_reader.get_p1().to_be_bytes()[..]);
        key[16..24].clone_from_slice(&key_reader.get_p2().to_be_bytes()[..]);
        key[24..32].clone_from_slice(&key_reader.get_p3().to_be_bytes()[..]);

        Ok(Self {
            key: TypedKey::new(typed_key_reader.get_kind().into(), key.into()),
            updated_at: Timestamp::new(peer_info_reader.get_updated_at()),
        })
    }
}
