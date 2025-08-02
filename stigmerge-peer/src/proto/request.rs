use capnp::{
    message::{self, ReaderOptions},
    serialize,
};
use veilid_core::{RecordKey, TypedRecordKey};

use super::{
    stigmerge_capnp::request, Decoder, Encoder, Error, PublicKey, Result, MAX_INDEX_BYTES,
};

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
    pub key: TypedRecordKey,
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
                typed_key_builder.set_kind(advertise_req.key.kind.into());

                let mut key_builder = typed_key_builder.reborrow().init_key();
                key_builder.set_p0(u64::from_be_bytes(
                    advertise_req.key.value.bytes[0..8].try_into()?,
                ));
                key_builder.set_p1(u64::from_be_bytes(
                    advertise_req.key.value.bytes[8..16].try_into()?,
                ));
                key_builder.set_p2(u64::from_be_bytes(
                    advertise_req.key.value.bytes[16..24].try_into()?,
                ));
                key_builder.set_p3(u64::from_be_bytes(
                    advertise_req.key.value.bytes[24..32].try_into()?,
                ));
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

                let mut key = PublicKey::default();
                if typed_key_reader.has_key() {
                    let key_reader = typed_key_reader.get_key()?;
                    key[0..8].clone_from_slice(&key_reader.get_p0().to_be_bytes()[..]);
                    key[8..16].clone_from_slice(&key_reader.get_p1().to_be_bytes()[..]);
                    key[16..24].clone_from_slice(&key_reader.get_p2().to_be_bytes()[..]);
                    key[24..32].clone_from_slice(&key_reader.get_p3().to_be_bytes()[..]);
                }

                Ok(Request::AdvertisePeer(AdvertisePeerRequest {
                    key: TypedRecordKey::new(
                        typed_key_reader.get_kind().into(),
                        RecordKey::new(key),
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
        let key = TypedRecordKey::new(CryptoKind::default(), RecordKey::new([0xaa; 32]));
        let message = Request::AdvertisePeer(AdvertisePeerRequest { key });
        let encoded = message.encode().unwrap();
        let decoded = Request::decode(&encoded).unwrap();
        assert_eq!(message, decoded);
    }
}
