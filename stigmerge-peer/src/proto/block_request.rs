use capnp::{
    message::{self, ReaderOptions},
    serialize,
};

use super::{stigmerge_capnp::block_request, Decoder, Encoder, Error, Result, MAX_INDEX_BYTES};

#[derive(Debug, PartialEq, Clone)]
pub struct BlockRequest {
    pub piece: u32,
    pub block: u8,
}

impl Encoder for BlockRequest {
    fn encode(&self) -> Result<Vec<u8>> {
        let mut builder = message::Builder::new_default();
        let mut request_builder = builder.get_root::<block_request::Builder>()?;
        request_builder.set_piece(self.piece);
        request_builder.set_block(self.block);

        let message = serialize::write_message_segments_to_words(&builder);
        if message.len() > MAX_INDEX_BYTES {
            return Err(Error::IndexTooLarge(message.len()));
        }
        Ok(message)
    }
}

impl Decoder for BlockRequest {
    fn decode(buf: &[u8]) -> Result<Self> {
        let reader = serialize::read_message(buf, ReaderOptions::new())?;
        let request_reader = reader.get_root::<block_request::Reader>()?;
        Ok(BlockRequest {
            piece: request_reader.get_piece(),
            block: request_reader.get_block(),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::proto::{BlockRequest, Decoder, Encoder};

    #[test]
    fn test_encode_decode() {
        let request = BlockRequest { piece: 1, block: 2 };
        let encoded = request.encode().unwrap();
        let decoded = BlockRequest::decode(&encoded).unwrap();
        assert_eq!(request, decoded);
    }
}
