use capnp::{
    message::{self, ReaderOptions},
    serialize,
};
use veilid_core::{BareOpaqueRecordKey, BareRecordKey, RecordKey};

use super::{stigmerge_capnp::request, Decoder, Encoder, Error, Result, MAX_INDEX_BYTES};

#[derive(Debug, PartialEq, Clone)]
pub enum Request {
    BlockRequest(BlockRequest),
    AdvertisePeer(AdvertisePeerRequest),
}

#[derive(Debug, PartialEq, Clone)]
pub struct BlockRequest {
    pub piece: u32,
    pub block: u8,
}

#[derive(Debug, PartialEq, Clone)]
pub struct AdvertisePeerRequest {
    pub key: RecordKey,
}

impl Encoder for Request {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut builder = message::Builder::new_default();
        let message_builder = builder.get_root::<request::Builder>()?;

        match self {
            Request::BlockRequest(block_req) => {
                let mut block_req_builder = message_builder.init_block_request();
                block_req_builder.set_piece(block_req.piece);
                block_req_builder.set_block(block_req.block);
            }
            Request::AdvertisePeer(advertise_req) => {
                let mut advertise_builder = message_builder.init_advertise_peer();

                let mut typed_key_builder = advertise_builder.reborrow().init_key();
                typed_key_builder.set_kind(advertise_req.key.kind().into());

                let mut key_builder = typed_key_builder.reborrow().init_key();
                let key_bytes = advertise_req.key.ref_value().ref_key();
                key_builder.set_p0(u64::from_be_bytes(key_bytes[0..8].try_into()?));
                key_builder.set_p1(u64::from_be_bytes(key_bytes[8..16].try_into()?));
                key_builder.set_p2(u64::from_be_bytes(key_bytes[16..24].try_into()?));
                key_builder.set_p3(u64::from_be_bytes(key_bytes[24..32].try_into()?));
                if let Some(secret) = advertise_req.key.value().ref_encryption_key() {
                    let secret_builder = typed_key_builder.reborrow().init_secret(32);
                    secret_builder.copy_from_slice(secret);
                }
            }
        }

        let message = serialize::write_message_segments_to_words(&builder);
        if message.len() > MAX_INDEX_BYTES {
            return Err(Error::IndexTooLarge(message.len()));
        }
        Ok(message)
    }
}

impl Decoder for Request {
    fn decode(buf: &[u8]) -> Result<Self> {
        let reader = serialize::read_message(buf, ReaderOptions::new())?;
        let message_reader = reader.get_root::<request::Reader>()?;

        match message_reader.which() {
            Ok(request::Which::BlockRequest(block_req)) => {
                let block_req = block_req?;
                Ok(Request::BlockRequest(BlockRequest {
                    piece: block_req.get_piece(),
                    block: block_req.get_block(),
                }))
            }
            Ok(request::Which::AdvertisePeer(advertise_req)) => {
                let advertise_req = advertise_req?;
                let typed_key_reader = advertise_req.get_key()?;

                let mut key_bytes = [0u8; 32];
                if typed_key_reader.has_key() {
                    let key_reader = typed_key_reader.get_key()?;
                    key_bytes[0..8].clone_from_slice(&key_reader.get_p0().to_be_bytes()[..]);
                    key_bytes[8..16].clone_from_slice(&key_reader.get_p1().to_be_bytes()[..]);
                    key_bytes[16..24].clone_from_slice(&key_reader.get_p2().to_be_bytes()[..]);
                    key_bytes[24..32].clone_from_slice(&key_reader.get_p3().to_be_bytes()[..]);
                } else {
                    return Err(Error::Other("empty advertise peer message".to_string()));
                }
                let secret_key = if typed_key_reader.has_secret() {
                    let secret_reader = typed_key_reader.get_secret()?;
                    Some(secret_reader.into())
                } else {
                    None
                };

                Ok(Request::AdvertisePeer(AdvertisePeerRequest {
                    key: RecordKey::new(
                        typed_key_reader.get_kind().into(),
                        BareRecordKey::new(BareOpaqueRecordKey::new(&key_bytes), secret_key),
                    ),
                }))
            }
            Err(e) => Err(e.into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use veilid_core::{CryptoKind, RecordKey};

    use super::*;

    #[test]
    fn test_encode_decode_block_request() {
        let message = Request::BlockRequest(BlockRequest { piece: 1, block: 2 });
        let encoded = message.encode().unwrap();
        let decoded = Request::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }

    #[test]
    fn test_encode_decode_advertise_peer() {
        let key = RecordKey::new(
            CryptoKind::default(),
            BareRecordKey::new(BareOpaqueRecordKey::new(&[0xaa; 32]), None),
        );
        let message = Request::AdvertisePeer(AdvertisePeerRequest { key });
        let encoded = message.encode().unwrap();
        let decoded = Request::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }
}
