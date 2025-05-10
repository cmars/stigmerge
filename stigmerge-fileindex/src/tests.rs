use super::*;
use std::path::PathBuf;

#[test]
fn test_canonicalize() {
    let mut index = Index {
        root: PathBuf::from("/test"),
        payload: PayloadSpec::default(),
        files: vec![
            FileSpec::new(PathBuf::from("z.txt"), PayloadSlice::new(0, 0, 0)),
            FileSpec::new(PathBuf::from("a.txt"), PayloadSlice::new(0, 0, 0)),
            FileSpec::new(PathBuf::from("m/file.txt"), PayloadSlice::new(0, 0, 0)),
        ],
    };

    // Store original files for comparison
    let original_files: Vec<PathBuf> = index.files.iter().map(|f| f.path().to_owned()).collect();

    index.canonicalize();

    // Verify files are sorted by path
    let sorted_files: Vec<PathBuf> = index.files.iter().map(|f| f.path().to_owned()).collect();

    assert_eq!(
        sorted_files,
        vec![
            PathBuf::from("a.txt"),
            PathBuf::from("m/file.txt"),
            PathBuf::from("z.txt"),
        ]
    );

    // Verify all original files are still present
    assert_eq!(
        original_files
            .iter()
            .collect::<std::collections::HashSet<_>>(),
        sorted_files
            .iter()
            .collect::<std::collections::HashSet<_>>()
    );
}
