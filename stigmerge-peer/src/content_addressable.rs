use sha2::{Digest as _, Sha256};
use stigmerge_fileindex::Index;

use crate::proto::{Digest, Encoder};
use crate::Result;

pub trait ContentAddressable: Encoder {
    fn digest(&mut self) -> Result<Digest>;
}

impl ContentAddressable for Index {
    fn digest(&mut self) -> Result<Digest> {
        self.canonicalize();
        let mut digest = Sha256::new();
        digest.update(self.encode()?);
        Ok(digest.finalize().try_into()?)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;
    use stigmerge_fileindex::{FileSpec, PayloadPiece, PayloadSlice, PayloadSpec};

    #[test]
    fn test_content_addressable_order_independence() {
        // Create test data
        let piece1 = PayloadPiece::new([1u8; 32], 1024);
        let piece2 = PayloadPiece::new([2u8; 32], 1024);
        let pieces = vec![piece1.clone(), piece2.clone()];
        let payload = PayloadSpec::new([0u8; 32], 2048, pieces);

        // Create first index with files in one order
        let mut index1 = Index::new(
            PathBuf::from("/test"),
            payload.clone(),
            vec![
                FileSpec::new(PathBuf::from("a.txt"), PayloadSlice::new(0, 0, 1024)),
                FileSpec::new(PathBuf::from("b.txt"), PayloadSlice::new(1, 0, 1024)),
            ],
        );

        // Create second index with same files in different order
        let mut index2 = Index::new(
            PathBuf::from("/test"),
            payload.clone(),
            vec![
                FileSpec::new(PathBuf::from("b.txt"), PayloadSlice::new(1, 0, 1024)),
                FileSpec::new(PathBuf::from("a.txt"), PayloadSlice::new(0, 0, 1024)),
            ],
        );

        // Create third index with different content
        let mut index3 = Index::new(
            PathBuf::from("/test"),
            PayloadSpec::new([0u8; 32], 2048, vec![piece2.clone(), piece1.clone()]),
            vec![
                FileSpec::new(PathBuf::from("a.txt"), PayloadSlice::new(0, 0, 1024)),
                FileSpec::new(PathBuf::from("b.txt"), PayloadSlice::new(1, 0, 1024)),
            ],
        );

        // Get digests
        let digest1 = index1.digest().expect("digest index1");
        let digest2 = index2.digest().expect("digest index2");
        let digest3 = index3.digest().expect("digest index3");

        // Same content in different order should have same digest
        assert_eq!(digest1, digest2);

        // Different content should have different digest
        assert_ne!(digest1, digest3);
    }
}
