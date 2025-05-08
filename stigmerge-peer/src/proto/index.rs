use std::{path::PathBuf, str::FromStr};

use capnp::{
    message::{self, ReaderOptions},
    serialize,
};
use stigmerge_fileindex::{FileSpec, Index, PayloadPiece, PayloadSlice};

use super::{stigmerge_capnp::index, Decoder, Digest, Encoder, Error, Result, MAX_INDEX_BYTES};

impl Encoder for Index {
    /// Encode the portable fields (file and payload specs) of an Index to a binary representation with capnp.
    ///
    /// Index::root is not encoded, as it's specific to an Index's application
    /// to the local filesystem.
    fn encode(&self) -> Result<Vec<u8>> {
        let mut builder = message::Builder::new_default();
        let mut index_builder = builder.get_root::<index::Builder>()?;

        // Encode the pieces
        let mut pieces_builder = index_builder
            .reborrow()
            .init_pieces(self.payload().pieces().len() as u32);
        for (i, idx_piece) in self.payload().pieces().iter().enumerate() {
            let mut piece_builder = pieces_builder.reborrow().get(i as u32);
            let mut piece_digest_builder = piece_builder.reborrow().init_digest();
            let piece_digest = idx_piece.digest();
            piece_digest_builder.set_p0(u64::from_be_bytes(piece_digest[0..8].try_into()?));
            piece_digest_builder.set_p1(u64::from_be_bytes(piece_digest[8..16].try_into()?));
            piece_digest_builder.set_p2(u64::from_be_bytes(piece_digest[16..24].try_into()?));
            piece_digest_builder.set_p3(u64::from_be_bytes(piece_digest[24..32].try_into()?));
            piece_builder.set_length(idx_piece.length() as u32);
        }

        // Encode the files
        let mut files_builder = index_builder
            .reborrow()
            .init_files(self.files().len() as u32);
        for (i, idx_file) in self.files().iter().enumerate() {
            let mut file_builder = files_builder.reborrow().get(i as u32);
            let mut slice_builder = file_builder.reborrow().init_contents();
            slice_builder.set_starting_piece(idx_file.contents().starting_piece() as u32);
            slice_builder.set_piece_offset(idx_file.contents().piece_offset() as u32);
            slice_builder.set_length(idx_file.contents().length() as u64);
            file_builder.set_path(
                idx_file
                    .path()
                    .as_os_str()
                    .to_str()
                    .ok_or(Error::EncodePath(idx_file.path().to_owned()))?,
            );
        }

        let message = serialize::write_message_segments_to_words(&builder);
        if message.len() > MAX_INDEX_BYTES {
            return Err(Error::IndexTooLarge(message.len()));
        }
        Ok(message)
    }
}

impl Decoder for (Vec<PayloadPiece>, Vec<FileSpec>) {
    fn decode(buf: &[u8]) -> Result<Self> {
        let reader = serialize::read_message(buf, ReaderOptions::new())?;
        let index_reader = reader.get_root::<index::Reader>()?;
        let pieces_reader = index_reader.get_pieces()?;
        let files_reader = index_reader.get_files()?;

        let mut idx_pieces = vec![];
        for piece in pieces_reader.iter() {
            let mut piece_digest = Digest::default();
            let piece_digest_reader = piece.get_digest()?;
            piece_digest[0..8].clone_from_slice(&piece_digest_reader.get_p0().to_be_bytes()[..]);
            piece_digest[8..16].clone_from_slice(&piece_digest_reader.get_p1().to_be_bytes()[..]);
            piece_digest[16..24].clone_from_slice(&piece_digest_reader.get_p2().to_be_bytes()[..]);
            piece_digest[24..32].clone_from_slice(&piece_digest_reader.get_p3().to_be_bytes()[..]);
            idx_pieces.push(PayloadPiece::new(piece_digest, piece.get_length() as usize));
        }

        let mut idx_files = vec![];
        for file in files_reader.iter() {
            let payload_slice_reader = file.get_contents()?;
            idx_files.push(FileSpec::new(
                PathBuf::from_str(file.get_path()?.to_str()?)
                    .map_err(|e| Error::Other(format!("{:?}", e)))?,
                PayloadSlice::new(
                    payload_slice_reader.get_starting_piece() as usize,
                    payload_slice_reader.get_piece_offset() as usize,
                    payload_slice_reader.get_length() as usize,
                ),
            ));
        }

        Ok((idx_pieces, idx_files))
    }
}

#[cfg(test)]
mod tests {
    use crate::proto::{Decoder, Header};

    use super::*;

    use std::io::Write;
    use stigmerge_fileindex::{Index, Indexer, PayloadSpec};
    use tempfile::NamedTempFile;

    fn temp_file(pattern: u8, count: usize) -> NamedTempFile {
        let mut tempf = NamedTempFile::new().expect("temp file");
        let contents = vec![pattern; count];
        tempf.write(contents.as_slice()).expect("write temp file");
        tempf
    }

    #[tokio::test]
    async fn round_trip_index() {
        let tempf = temp_file(b'@', 4194304);
        let indexer = Indexer::from_file(tempf.path()).await.expect("indexer");
        let idx = indexer.index().await.expect("index");
        let header = Header::new(
            idx.payload().digest().try_into().expect("digest fits"),
            idx.payload().length(),
            1,
            &[],
            None,
            None,
        );
        let message = idx.encode().expect("encode index");
        let (payload_pieces, payload_files) =
            <(Vec<PayloadPiece>, Vec<FileSpec>)>::decode(message.as_slice())
                .expect("decode payload pieces and files");
        let idx2 = Index::new(
            tempf.path().parent().unwrap().to_owned(),
            PayloadSpec::new(
                header.payload_digest(),
                header.payload_length(),
                payload_pieces,
            ),
            payload_files,
        );
        assert_eq!(idx, idx2);
    }
}
