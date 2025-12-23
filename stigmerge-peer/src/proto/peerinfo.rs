use capnp::{
    message::{self, ReaderOptions},
    serialize,
};
use veilid_core::{BareOpaqueRecordKey, BareRecordKey, RecordKey, Timestamp};

use super::{stigmerge_capnp::peer_info, Decoder, Encoder};

#[derive(Debug, PartialEq, Clone)]
pub struct PeerInfo {
    key: RecordKey,
    pub(crate) updated_at: Timestamp,
}

impl PeerInfo {
    pub fn new(key: RecordKey) -> Self {
        Self {
            key,
            updated_at: Timestamp::now(),
        }
    }

    pub fn new_updated_at(key: RecordKey, updated_at: Timestamp) -> Self {
        Self { key, updated_at }
    }

    pub fn key(&self) -> &RecordKey {
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
        typed_key_builder.set_kind(self.key.kind().into());

        let mut key_builder = typed_key_builder.reborrow().init_key();
        let key_bytes = self.key.ref_value().ref_key();
        key_builder.set_p0(u64::from_be_bytes(key_bytes[0..8].try_into()?));
        key_builder.set_p1(u64::from_be_bytes(key_bytes[8..16].try_into()?));
        key_builder.set_p2(u64::from_be_bytes(key_bytes[16..24].try_into()?));
        key_builder.set_p3(u64::from_be_bytes(key_bytes[24..32].try_into()?));
        if let Some(secret) = self.key.value().ref_encryption_key() {
            let secret_builder = typed_key_builder.reborrow().init_secret(32);
            secret_builder.copy_from_slice(secret);
        }

        peer_info_builder.set_updated_at(self.updated_at.into());

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

        let mut key_bytes = [0u8; 32];
        key_bytes[0..8].clone_from_slice(&key_reader.get_p0().to_be_bytes()[..]);
        key_bytes[8..16].clone_from_slice(&key_reader.get_p1().to_be_bytes()[..]);
        key_bytes[16..24].clone_from_slice(&key_reader.get_p2().to_be_bytes()[..]);
        key_bytes[24..32].clone_from_slice(&key_reader.get_p3().to_be_bytes()[..]);
        let secret_key = if typed_key_reader.has_secret() {
            let secret_reader = typed_key_reader.get_secret()?;
            Some(secret_reader.into())
        } else {
            None
        };

        Ok(Self {
            key: RecordKey::new(
                typed_key_reader.get_kind().into(),
                BareRecordKey::new(BareOpaqueRecordKey::new(&key_bytes), secret_key),
            ),
            updated_at: Timestamp::new(peer_info_reader.get_updated_at()),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use veilid_core::RecordKey;

    use crate::proto::{Decoder, Encoder, PeerInfo};

    #[test]
    fn test_encode_decode() {
        let key = RecordKey::from_str("VLD0:mluySonu8GKEd2AYTY0KB8y7upLHlXwm4movvucbfoQ").unwrap();
        let peer_info = PeerInfo::new(key.clone());
        let encoded = peer_info.encode().unwrap();
        let decoded = PeerInfo::decode(&encoded).unwrap();
        assert_eq!(peer_info, decoded);
        assert_eq!(key, decoded.key);
    }
}
