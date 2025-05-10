use std::io::Write;

use hex_literal::hex;
use tempfile::NamedTempFile;

use super::*;

pub fn temp_file(pattern: u8, count: usize) -> NamedTempFile {
    let mut tempf = NamedTempFile::new().expect("temp file");
    let contents = vec![pattern; count];
    tempf.write(contents.as_slice()).expect("write temp file");
    tempf
}

#[tokio::test]
async fn single_file_index() {
    let tempf = temp_file(b'.', 1049600);

    let indexer = Indexer::from_file(tempf.path().into())
        .await
        .expect("Index::from_file");
    let index = indexer.index().await.expect("index");

    assert_eq!(
        index.root().to_owned(),
        tempf.path().parent().unwrap().to_owned()
    );

    // Index files
    assert_eq!(index.files().len(), 1);
    assert_eq!(
        index.files()[0].path().to_owned(),
        tempf.path().file_name().unwrap().to_owned()
    );
    assert_eq!(
        index.files()[0].contents(),
        PayloadSlice {
            piece_offset: 0,
            starting_piece: 0,
            length: 1049600,
        }
    );

    // Index payload
    assert_eq!(
        index.payload().digest(),
        hex!("529df3a7e7acab0e3b53e7cd930faa22e62cd07a948005b1c3f7f481f32a7297")
    );
    assert_eq!(index.payload().length(), 1049600);
    assert_eq!(index.payload().pieces().len(), 2);
    assert_eq!(index.payload().pieces()[0].length(), 1048576);
    assert_eq!(
        index.payload().pieces()[0].digest(),
        hex!("153faf1f2a007097d33120bbee6944a41cb8be7643c1222f6bc6bc69ec31688f")
    );
    assert_eq!(index.payload().pieces()[1].length(), 1024);
    assert_eq!(
        index.payload().pieces()[1].digest(),
        hex!("ca33403cfcb21bae20f21507475a3525c7f4bd36bb2a7074891e3307c5fd47d5")
    );
}

#[tokio::test]
async fn empty_file_index() {
    let tempf = temp_file(b'.', 0);

    let indexer = Indexer::from_file(tempf.path().into())
        .await
        .expect("Index::from_file");
    let index = indexer.index().await.expect("index");

    assert_eq!(
        index.root().to_owned(),
        tempf.path().parent().unwrap().to_owned()
    );

    // Index files
    assert_eq!(index.files().len(), 1);
    assert_eq!(
        index.files()[0].path().to_owned(),
        tempf.path().file_name().unwrap().to_owned()
    );
    assert_eq!(
        index.files()[0].contents(),
        PayloadSlice {
            piece_offset: 0,
            starting_piece: 0,
            length: 0,
        }
    );
    assert_eq!(index.payload().pieces().len(), 0);
    assert_eq!(
        index.payload().digest(),
        hex!("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
    );
}

#[tokio::test]
async fn large_file_index() {
    let tempf = temp_file(b'.', 134217728);

    let indexer = Indexer::from_file(tempf.path().into())
        .await
        .expect("Index::from_file");
    let index = indexer.index().await.expect("index");

    assert_eq!(
        index.root().to_owned(),
        tempf.path().parent().unwrap().to_owned()
    );

    // Index files
    assert_eq!(index.files().len(), 1);
    assert_eq!(
        index.files()[0].path().to_owned(),
        tempf.path().file_name().unwrap().to_owned()
    );
    assert_eq!(index.payload().pieces().len(), 128);
    assert_eq!(
        index.files()[0].contents(),
        PayloadSlice {
            piece_offset: 0,
            starting_piece: 0,
            length: 134217728,
        }
    );
}
