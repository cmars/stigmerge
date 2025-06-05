use std::path::PathBuf;

use stigmerge_fileindex::{Index, BLOCK_SIZE_BYTES, PIECE_SIZE_BLOCKS, PIECE_SIZE_BYTES};
use veilid_core::TypedRecordKey;

use crate::proto::Header;

#[derive(Debug, Clone)]
pub struct ShareInfo {
    pub key: TypedRecordKey,
    pub header: Header,
    pub want_index: Index,
    pub want_index_digest: [u8; 32],
    pub root: PathBuf,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FileBlockFetch {
    pub file_index: usize,
    pub piece_index: usize,
    pub piece_offset: usize,
    pub block_index: usize,
}

impl FileBlockFetch {
    pub fn block_offset(&self) -> usize {
        (self.piece_index * PIECE_SIZE_BYTES)
            + self.piece_offset
            + (self.block_index * BLOCK_SIZE_BYTES)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PieceState {
    pub file_index: usize,
    pub piece_index: usize,
    pub piece_offset: usize,
    pub block_count: usize,
    pub blocks: u32,
}

impl PieceState {
    pub fn new(
        file_index: usize,
        piece_index: usize,
        piece_offset: usize,
        block_count: usize,
        block_index: usize,
    ) -> PieceState {
        PieceState {
            file_index,
            piece_index,
            piece_offset,
            block_count,
            blocks: 1 << block_index,
        }
    }

    pub fn empty(
        file_index: usize,
        piece_index: usize,
        piece_offset: usize,
        block_count: usize,
    ) -> PieceState {
        PieceState {
            file_index,
            piece_index,
            piece_offset,
            block_count,
            blocks: 0u32,
        }
    }

    pub fn key(&self) -> (usize, usize) {
        (self.file_index, self.piece_index)
    }

    pub fn is_complete(&self) -> bool {
        match self.block_count {
            0 => true,
            size if size == PIECE_SIZE_BLOCKS => self.blocks == 0xffffffff,
            _ => {
                let mask = 1 << self.block_count - 1;
                self.blocks & mask == mask
            }
        }
    }

    pub fn merged(&mut self, other: PieceState) -> PieceState {
        if self.file_index != other.file_index || self.piece_index != other.piece_index {
            panic!("attempt to merge mismatched pieces");
        }
        self.blocks |= other.blocks;
        self.clone()
    }
}
