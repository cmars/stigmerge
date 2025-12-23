use capnp::{
    message::{self, ReaderOptions},
    serialize,
};
use stigmerge_fileindex::Index;
use veilid_core::{BareOpaqueRecordKey, BareRecordKey, RecordKey, ValueData};

use super::{stigmerge_capnp::header, Decoder, Digest, Encoder, Error, Result, MAX_INDEX_BYTES};

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

        // Write the have map ref
        if let Some(have_map_ref) = self.have_map() {
            let mut have_map_builder = header_builder.reborrow().init_have_map();

            let mut typed_key_builder = have_map_builder.reborrow().init_key();
            typed_key_builder.set_kind(have_map_ref.key.kind().into());

            let mut key_builder = typed_key_builder.reborrow().init_key();
            let key_bytes = have_map_ref.key.ref_value().ref_key();
            key_builder.set_p0(u64::from_be_bytes(key_bytes[0..8].try_into()?));
            key_builder.set_p1(u64::from_be_bytes(key_bytes[8..16].try_into()?));
            key_builder.set_p2(u64::from_be_bytes(key_bytes[16..24].try_into()?));
            key_builder.set_p3(u64::from_be_bytes(key_bytes[24..32].try_into()?));

            if let Some(secret) = have_map_ref.key.ref_value().ref_encryption_key() {
                let secret_builder = typed_key_builder.reborrow().init_secret(secret.len().try_into().unwrap());
                secret_builder.copy_from_slice(secret);
            }

            have_map_builder.set_subkeys(have_map_ref.subkeys);
        }

        // Write the peer map ref
        if let Some(peer_map_ref) = self.peer_map() {
            let mut peer_map_builder = header_builder.reborrow().init_peer_map();

            let mut typed_key_builder = peer_map_builder.reborrow().init_key();
            typed_key_builder.set_kind(peer_map_ref.key.kind().into());

            let mut key_builder = typed_key_builder.reborrow().init_key();
            let key_bytes = peer_map_ref.key.ref_value().ref_key();
            key_builder.set_p0(u64::from_be_bytes(key_bytes[0..8].try_into()?));
            key_builder.set_p1(u64::from_be_bytes(key_bytes[8..16].try_into()?));
            key_builder.set_p2(u64::from_be_bytes(key_bytes[16..24].try_into()?));
            key_builder.set_p3(u64::from_be_bytes(key_bytes[24..32].try_into()?));

            if let Some(secret) = peer_map_ref.key.ref_value().ref_encryption_key() {
                let secret_builder = typed_key_builder.reborrow().init_secret(secret.len().try_into().unwrap());
                secret_builder.copy_from_slice(secret);
            }

            peer_map_builder.set_subkeys(peer_map_ref.subkeys);
        }

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
            let have_map_ref_reader = header_reader.get_have_map()?;
            if have_map_ref_reader.has_key() {
                let typed_key_reader = have_map_ref_reader.get_key()?;
                if typed_key_reader.has_key() {
                    let kind = typed_key_reader.get_kind().into();
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
                    header.set_have_map(HaveMapRef {
                        key: RecordKey::new(
                            kind,
                            BareRecordKey::new(BareOpaqueRecordKey::new(&key_bytes), secret_key),
                        ),
                        subkeys: have_map_ref_reader.get_subkeys(),
                    });
                }
            }
        }

        if header_reader.has_peer_map() {
            let peer_map_ref_reader = header_reader.get_peer_map()?;
            if peer_map_ref_reader.has_key() {
                let typed_key_reader = peer_map_ref_reader.get_key()?;
                let kind = typed_key_reader.get_kind().into();
                if typed_key_reader.has_key() {
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
                    header.set_peer_map(PeerMapRef {
                        key: RecordKey::new(
                            kind,
                            BareRecordKey::new(BareOpaqueRecordKey::new(&key_bytes), secret_key),
                        ),
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
    key: RecordKey,
    subkeys: u16,
}

impl HaveMapRef {
    pub fn new(key: RecordKey, subkeys: u16) -> Self {
        Self { key, subkeys }
    }

    pub fn key(&self) -> &RecordKey {
        &self.key
    }

    pub fn subkeys(&self) -> u16 {
        self.subkeys
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct PeerMapRef {
    key: RecordKey,
    subkeys: u16,
}

impl PeerMapRef {
    pub fn new(key: RecordKey, subkeys: u16) -> Self {
        Self { key, subkeys }
    }

    pub fn key(&self) -> &RecordKey {
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
            ((index_bytes.len() / ValueData::MAX_LEN)
                + if !index_bytes.len().is_multiple_of(ValueData::MAX_LEN) {
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
        self.payload_digest
    }

    pub fn payload_length(&self) -> usize {
        self.payload_length
    }

    pub fn subkeys(&self) -> u16 {
        self.subkeys
    }

    pub fn route_data(&self) -> &[u8] {
        self.route_data.as_slice()
    }

    pub fn set_route_data(&mut self, route_data: Vec<u8>) {
        self.route_data = route_data;
    }

    pub fn set_have_map(&mut self, have_map_ref: HaveMapRef) {
        self.have_map_ref = Some(have_map_ref);
    }

    pub fn have_map(&self) -> Option<&HaveMapRef> {
        self.have_map_ref.as_ref()
    }

    pub fn set_peer_map(&mut self, peer_map_ref: PeerMapRef) {
        self.peer_map_ref = Some(peer_map_ref);
    }

    pub fn peer_map(&self) -> Option<&PeerMapRef> {
        self.peer_map_ref.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;

    use stigmerge_fileindex::{Index, PayloadSpec};
    use veilid_core::{BareOpaqueRecordKey, BareRecordKey, BareSharedSecret, CryptoKind, RecordKey};

    use crate::proto::{Decoder, Encoder, HaveMapRef, Header, PeerMapRef};

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

    #[test]
    fn round_trip_header_with_peer_and_have_map() {
        // Create a basic header
        let payload_digest = [0xa5u8; 32];
        let payload_length = 42;
        let subkeys = 5;
        let route_data = vec![1u8, 2u8, 3u8, 4u8];

        // Create TypedKeys for peer map and have map
        let peer_key = RecordKey::new(
            CryptoKind::default(),
            BareRecordKey::new(BareOpaqueRecordKey::new(&[0xaa; 32]), Some(BareSharedSecret::new(&[0x55, 32]))),
        );
        let have_key = RecordKey::new(
            CryptoKind::default(),
            BareRecordKey::new(BareOpaqueRecordKey::new(&[0xbb; 32]), Some(BareSharedSecret::new(&[0x66, 32]))),
        );

        // Create the peer map ref and have map ref
        let peer_map_ref = PeerMapRef::new(peer_key, 10);
        let have_map_ref = HaveMapRef::new(have_key, 5);

        // Create the header with both refs
        let original_header = Header::new(
            payload_digest,
            payload_length,
            subkeys,
            &route_data,
            Some(have_map_ref),
            Some(peer_map_ref),
        );

        // Encode the header to bytes
        let encoded = original_header.encode().expect("encode header");

        // Decode the bytes back to a header
        let decoded_header = Header::decode(&encoded).expect("decode header");

        // Verify that the decoded header matches the original
        assert_eq!(original_header, decoded_header);

        // Verify that the peer map and have map refs are preserved
        assert!(decoded_header.peer_map().is_some());
        assert!(decoded_header.have_map().is_some());

        // Verify that the peer map and have map refs match the originals
        assert_eq!(original_header.peer_map(), decoded_header.peer_map());
        assert_eq!(original_header.have_map(), decoded_header.have_map());
    }
}
