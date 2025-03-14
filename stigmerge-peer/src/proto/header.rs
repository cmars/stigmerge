use capnp::{
    message::{self, ReaderOptions},
    serialize,
};
use stigmerge_fileindex::Index;

use crate::peer::TypedKey;

use super::{
    stigmerge_capnp::{have_map, header, peer_map},
    Decoder, Digest, Encoder, Error, PublicKey, Result, MAX_INDEX_BYTES,
};

#[derive(Debug, PartialEq, Clone)]
pub struct Header {
    payload_digest: Digest,
    payload_length: usize,
    subkeys: u16,
    route_data: Vec<u8>,

    have_map_ref: Option<HaveMapRef>,
    peer_map_ref: Option<PeerMapRef>,
}

impl Encoder for Header {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut builder = message::Builder::new_default();
        let mut header_builder = builder.get_root::<header::Builder>()?;
        let mut payload_builder = header_builder.reborrow().init_payload();

        // Encode the payload
        let mut payload_digest_builder = payload_builder.reborrow().init_digest();
        payload_digest_builder.set_p0(u64::from_be_bytes(self.payload_digest[0..8].try_into()?));
        payload_digest_builder.set_p1(u64::from_be_bytes(self.payload_digest[8..16].try_into()?));
        payload_digest_builder.set_p2(u64::from_be_bytes(self.payload_digest[16..24].try_into()?));
        payload_digest_builder.set_p3(u64::from_be_bytes(self.payload_digest[24..32].try_into()?));
        payload_builder.set_length(self.payload_length as u64);

        header_builder.set_subkeys(self.subkeys);
        header_builder.set_route(&self.route_data);

        // TODO: write have and peer map refs

        let message = serialize::write_message_segments_to_words(&builder);
        if message.len() > MAX_INDEX_BYTES {
            return Err(Error::IndexTooLarge(message.len()));
        }
        Ok(message)
    }
}

impl Decoder for Header {
    fn decode(buf: &[u8]) -> Result<Self> {
        let reader = serialize::read_message(buf, ReaderOptions::new())?;
        let header_reader = reader.get_root::<header::Reader>()?;
        let payload_reader = header_reader.get_payload()?;

        let mut payload_digest = Digest::default();
        let payload_digest_reader = payload_reader.get_digest()?;
        payload_digest[0..8].clone_from_slice(&payload_digest_reader.get_p0().to_be_bytes()[..]);
        payload_digest[8..16].clone_from_slice(&payload_digest_reader.get_p1().to_be_bytes()[..]);
        payload_digest[16..24].clone_from_slice(&payload_digest_reader.get_p2().to_be_bytes()[..]);
        payload_digest[24..32].clone_from_slice(&payload_digest_reader.get_p3().to_be_bytes()[..]);

        let mut header = Header::new(
            payload_digest,
            payload_reader.get_length().try_into()?,
            header_reader.get_subkeys(),
            header_reader.get_route()?,
            None,
            None,
        );

        if header_reader.has_have_map() {
            let have_map_ref_reader = reader.get_root::<have_map::Reader>()?;
            if have_map_ref_reader.has_key() {
                let typed_key_reader = have_map_ref_reader.get_key()?;
                if typed_key_reader.has_key() {
                    let key_reader = typed_key_reader.get_key()?;
                    let mut key = PublicKey::default();
                    key[0..8].clone_from_slice(&key_reader.get_p0().to_be_bytes()[..]);
                    key[8..16].clone_from_slice(&key_reader.get_p1().to_be_bytes()[..]);
                    key[16..24].clone_from_slice(&key_reader.get_p2().to_be_bytes()[..]);
                    key[24..32].clone_from_slice(&key_reader.get_p3().to_be_bytes()[..]);
                    header = header.with_have_map(HaveMapRef {
                        key: TypedKey::new(typed_key_reader.get_kind().into(), key.into()),
                    });
                }
            }
        }

        if header_reader.has_peer_map() {
            let peer_map_ref_reader = reader.get_root::<peer_map::Reader>()?;
            if peer_map_ref_reader.has_key() {
                let typed_key_reader = peer_map_ref_reader.get_key()?;
                if typed_key_reader.has_key() {
                    let key_reader = typed_key_reader.get_key()?;
                    let mut key = PublicKey::default();
                    key[0..8].clone_from_slice(&key_reader.get_p0().to_be_bytes()[..]);
                    key[8..16].clone_from_slice(&key_reader.get_p1().to_be_bytes()[..]);
                    key[16..24].clone_from_slice(&key_reader.get_p2().to_be_bytes()[..]);
                    key[24..32].clone_from_slice(&key_reader.get_p3().to_be_bytes()[..]);
                    header = header.with_peer_map(PeerMapRef {
                        key: TypedKey::new(typed_key_reader.get_kind().into(), key.into()),
                        subkeys: peer_map_ref_reader.get_subkeys(),
                    });
                }
            }
        }

        Ok(header)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct HaveMapRef {
    key: TypedKey,
}

impl HaveMapRef {
    pub fn new(key: TypedKey) -> Self {
        Self { key }
    }

    pub fn key(&self) -> &TypedKey {
        &self.key
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct PeerMapRef {
    key: TypedKey,
    subkeys: u16,
}

impl PeerMapRef {
    pub fn new(key: TypedKey, subkeys: u16) -> Self {
        Self { key, subkeys }
    }

    pub fn key(&self) -> &TypedKey {
        &self.key
    }

    pub fn subkeys(&self) -> u16 {
        self.subkeys
    }
}

impl Header {
    pub fn new(
        payload_digest: Digest,
        payload_length: usize,
        subkeys: u16,
        route_data: &[u8],
        have_map: Option<HaveMapRef>,
        peer_map: Option<PeerMapRef>,
    ) -> Header {
        Header {
            payload_digest,
            payload_length,
            subkeys,
            route_data: route_data.to_vec(),
            have_map_ref: have_map,
            peer_map_ref: peer_map,
        }
    }

    pub fn from_index(index: &Index, index_bytes: &[u8], route_data: &[u8]) -> Header {
        Header::new(
            index.payload().digest().try_into().unwrap(),
            index.payload().length(),
            ((index_bytes.len() / 32768)
                + if (index_bytes.len() % 32768) > 0 {
                    1
                } else {
                    0
                })
            .try_into()
            .unwrap(),
            route_data,
            None,
            None,
        )
    }

    pub fn payload_digest(&self) -> Digest {
        self.payload_digest.clone()
    }

    pub fn payload_length(&self) -> usize {
        self.payload_length
    }

    pub fn subkeys(&self) -> u16 {
        self.subkeys
    }

    pub fn route_data(&self) -> &[u8] {
        &self.route_data.as_slice()
    }

    pub fn with_route_data(&self, route_data: Vec<u8>) -> Header {
        Header {
            payload_digest: self.payload_digest,
            payload_length: self.payload_length,
            subkeys: self.subkeys,
            route_data,
            have_map_ref: self.have_map_ref.clone(),
            peer_map_ref: self.peer_map_ref.clone(),
        }
    }

    pub fn with_have_map(mut self, have_map_ref: HaveMapRef) -> Self {
        self.have_map_ref = Some(have_map_ref);
        self
    }

    pub fn have_map(&self) -> Option<&HaveMapRef> {
        self.have_map_ref.as_ref()
    }

    pub fn with_peer_map(mut self, peer_map_ref: PeerMapRef) -> Self {
        self.peer_map_ref = Some(peer_map_ref);
        self
    }

    pub fn peer_map(&self) -> Option<&PeerMapRef> {
        self.peer_map_ref.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use stigmerge_fileindex::{Index, PayloadSpec};

    use crate::proto::{Decoder, Encoder, Header};

    #[test]
    fn round_trip_header() {
        let idx = Index::new(
            PathBuf::from(""),
            PayloadSpec::new([0xa5u8; 32], 42, vec![]),
            vec![],
        );
        let idx_bytes = idx.encode().expect("encode index");
        let expect_header = Header::from_index(&idx, idx_bytes.as_slice(), &[1u8, 2u8, 3u8, 4u8]);
        let message = expect_header.encode().expect("encode header");
        let actual_header = Header::decode(message.as_slice()).expect("decode header");
        assert_eq!(expect_header, actual_header);
    }
}
