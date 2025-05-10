use std::io::Write;

use tempfile::NamedTempFile;

mod stub_node;

pub use stub_node::StubNode;

pub fn temp_file(pattern: u8, count: usize) -> NamedTempFile {
    let mut tempf = NamedTempFile::new().expect("temp file");
    let contents = vec![pattern; count];
    tempf.write(contents.as_slice()).expect("write temp file");
    tempf
}
