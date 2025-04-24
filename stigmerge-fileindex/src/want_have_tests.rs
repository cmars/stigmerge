use super::*;
use std::path::PathBuf;

// Helper function to create an Index with a single file
fn create_index(root: PathBuf, file_path: PathBuf, piece_digests: Vec<[u8; 32]>) -> Index {
    let pieces: Vec<PayloadPiece> = piece_digests
        .into_iter()
        .map(|digest| PayloadPiece {
            digest,
            length: PIECE_SIZE_BYTES,
        })
        .collect();
    let payload = PayloadSpec {
        digest: [0; 32], // Placeholder, not used in this test
        length: pieces.len() * PIECE_SIZE_BYTES,
        pieces,
    };
    let file_spec = FileSpec {
        path: file_path,
        contents: PayloadSlice {
            starting_piece: 0,
            piece_offset: 0,
            length: payload.length,
        },
    };
    Index {
        root,
        payload,
        files: vec![file_spec],
    }
}

fn create_index_partial_block(
    root: PathBuf,
    file_path: PathBuf,
    piece_digests: Vec<[u8; 32]>,
) -> Index {
    let pieces: Vec<PayloadPiece> = piece_digests
        .into_iter()
        .map(|digest| PayloadPiece {
            digest,
            length: PIECE_SIZE_BYTES,
        })
        .collect();
    assert!(!pieces.is_empty());
    let payload = PayloadSpec {
        digest: [0; 32], // Placeholder, not used in this test
        // Same number of pieces, but the last block is short a byte.
        length: ((pieces.len() - 1) * PIECE_SIZE_BYTES) + PIECE_SIZE_BYTES - 1,
        pieces,
    };
    let file_spec = FileSpec {
        path: file_path,
        contents: PayloadSlice {
            starting_piece: 0,
            piece_offset: 0,
            length: payload.length,
        },
    };
    Index {
        root,
        payload,
        files: vec![file_spec],
    }
}

#[test]
fn test_identical_files() {
    let root = PathBuf::from("/root");
    let file_path = PathBuf::from("file.txt");
    let piece_digests = vec![[1; 32], [2; 32]];

    let want_index = create_index(root.clone(), file_path.clone(), piece_digests.clone());
    let have_index = create_index(root, file_path, piece_digests);

    let diff = want_index.diff(&have_index);
    assert_eq!(diff.want.len(), 0);
    assert_eq!(diff.have.len(), 2 * PIECE_SIZE_BLOCKS);
}

#[test]
fn test_identical_files_partial_block() {
    let root = PathBuf::from("/root");
    let file_path = PathBuf::from("file.txt");
    let piece_digests = vec![[1; 32], [2; 32]];

    let want_index =
        create_index_partial_block(root.clone(), file_path.clone(), piece_digests.clone());
    let have_index = create_index_partial_block(root, file_path, piece_digests);

    let diff = want_index.diff(&have_index);
    assert_eq!(diff.want.len(), 0);
    assert_eq!(diff.have.len(), 2 * PIECE_SIZE_BLOCKS);
}

#[test]
fn test_have_index_empty() {
    let root = PathBuf::from("/root");
    let file_path = PathBuf::from("file.txt");
    let piece_digests = vec![[1; 32], [2; 32]];

    let want_index = create_index(root.clone(), file_path.clone(), piece_digests.clone());
    let have_index: Index = Index {
        root,
        payload: PayloadSpec {
            digest: [0; 32],
            length: 0,
            pieces: vec![],
        },
        files: vec![],
    };

    let diff = want_index.diff(&have_index);
    assert_eq!(diff.want.len(), 2 * PIECE_SIZE_BLOCKS); // Two pieces worth of blocks should be missing
    assert_eq!(diff.have.len(), 0)
}

#[test]
fn test_have_index_partial_contents() {
    let root = PathBuf::from("/root");
    let file_path = PathBuf::from("file.txt");
    let want_piece_digests = vec![[1; 32], [2; 32], [3; 32]];
    let have_piece_digests = vec![[1; 32], [2; 32]];

    let want_index = create_index(root.clone(), file_path.clone(), want_piece_digests);
    let have_index = create_index(root, file_path, have_piece_digests);

    let diff = want_index.diff(&have_index);
    assert_eq!(diff.want.len(), 1 * PIECE_SIZE_BLOCKS); // One piece worth of blocks should be missing
    assert_eq!(diff.have.len(), 2 * PIECE_SIZE_BLOCKS);
}
