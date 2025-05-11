use capnp::{
    message::{self, ReaderOptions},
    serialize,
};

use super::{
    stigmerge_capnp::{block_response, response},
    Decoder, Encoder, Error, MAX_INDEX_BYTES,
};

#[derive(Debug, PartialEq, Clone)]
pub enum Response {
    BadRequest,
    BlockResponse(Option<Vec<u8>>),
}

impl Decoder for Response {
    fn decode(buf: &[u8]) -> super::Result<Self> {
        let reader = serialize::read_message(buf, ReaderOptions::new())?;
        let message_reader = reader.get_root::<response::Reader>()?;

        match message_reader.which()? {
            response::Which::BadRequest(_) => Ok(Response::BadRequest),
            response::Which::BlockResponse(resp) => match resp?.which()? {
                block_response::Which::NotFound(_) => Ok(Response::BlockResponse(None)),
                block_response::Which::Contents(contents) => {
                    Ok(Response::BlockResponse(Some(contents?.to_vec())))
                }
            },
        }
    }
}

impl Encoder for Response {
    fn encode(&self) -> super::Result<Vec<u8>> {
        let mut builder = message::Builder::new_default();
        let mut message_builder = builder.get_root::<response::Builder>()?;

        match self {
            Response::BlockResponse(contents) => {
                let mut block_resp_builder = message_builder.init_block_response();
                match contents {
                    Some(contents) => block_resp_builder.set_contents(contents),
                    None => block_resp_builder.set_not_found(()),
                };
            }
            Response::BadRequest => {
                message_builder.set_bad_request(());
            }
        }

        let message = serialize::write_message_segments_to_words(&builder);
        if message.len() > MAX_INDEX_BYTES {
            return Err(Error::IndexTooLarge(message.len()));
        }
        Ok(message)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_bad_request_roundtrip() {
        let response = Response::BadRequest;
        let encoded = response.encode().unwrap();
        let decoded = Response::decode(&encoded).unwrap();
        assert_eq!(response, decoded);
    }

    #[test]
    fn test_response_block_not_found_roundtrip() {
        let response = Response::BlockResponse(None);
        let encoded = response.encode().unwrap();
        let decoded = Response::decode(&encoded).unwrap();
        assert_eq!(response, decoded);
    }

    #[test]
    fn test_response_block_with_contents_roundtrip() {
        let test_data = vec![1, 2, 3, 4, 5];
        let response = Response::BlockResponse(Some(test_data));
        let encoded = response.encode().unwrap();
        let decoded = Response::decode(&encoded).unwrap();
        assert_eq!(response, decoded);
    }
}
