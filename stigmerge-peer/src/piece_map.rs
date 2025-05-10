use std::iter::repeat;

use veilid_core::ValueData;

#[derive(Clone, Debug)]
pub struct PieceMap(Vec<u8>);

impl PieceMap {
    pub fn new() -> PieceMap {
        Self(vec![])
    }

    pub fn subkeys(n_pieces: usize) -> u16 {
        let cap = Self::capacity(n_pieces);
        TryInto::<u16>::try_into(cap / ValueData::MAX_LEN).unwrap()
            + if cap % ValueData::MAX_LEN != 0 {
                1u16
            } else {
                0u16
            }
    }

    pub fn capacity(n_pieces: usize) -> usize {
        n_pieces / 8 + if n_pieces % 8 != 0 { 1 } else { 0 }
    }

    pub fn get(&self, bit_index: u32) -> bool {
        let byte_index: usize = (bit_index / 8).try_into().unwrap();
        let bit_offset_in_byte: usize = (bit_index % 8).try_into().unwrap();
        if self.0.len() <= byte_index {
            false
        } else {
            self.0[byte_index] & 1 << bit_offset_in_byte != 0
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().any(|b| *b != 0u8)
    }

    pub fn set(&mut self, bit_index: u32) {
        let byte_index: usize = (bit_index / 8).try_into().unwrap();
        let bit_offset_in_byte: usize = (bit_index % 8).try_into().unwrap();
        if self.0.len() <= byte_index {
            self.0
                .extend(repeat(0u8).take(byte_index - self.0.len() + 1));
        }
        self.0[byte_index] |= 1 << bit_offset_in_byte;
    }

    pub fn clear(&mut self, bit_index: u32) {
        let byte_index: usize = (bit_index / 8).try_into().unwrap();
        let bit_offset_in_byte: usize = (bit_index % 8).try_into().unwrap();
        if self.0.len() <= byte_index {
            return;
        }
        self.0[byte_index] &= !(1 << bit_offset_in_byte);
    }

    pub fn reset(&mut self) {
        self.0 = vec![];
    }

    pub fn write_bytes(&mut self, byte_offset: usize, bytes: &[u8]) {
        let required_len = byte_offset + bytes.len();
        if self.0.len() <= required_len {
            self.0.extend(repeat(0u8).take(required_len - self.0.len()));
        }
        for (i, b) in bytes.iter().enumerate() {
            self.0[byte_offset + i] = *b;
        }
    }
}

impl From<Vec<u8>> for PieceMap {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for PieceMap {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<PieceMap> for Vec<u8> {
    fn from(value: PieceMap) -> Self {
        value.0
    }
}

impl AsRef<[u8]> for PieceMap {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

#[cfg(test)]
mod tests {
    use super::PieceMap;

    #[test]
    fn empty_havemap() {
        let have_map = PieceMap::new();
        assert_eq!(have_map.get(0), false);
        assert_eq!(have_map.get(21389213), false);
    }

    #[test]
    fn non_empty_havemap() {
        let mut have_map = PieceMap::new();
        have_map.set(999);
        assert_eq!(have_map.get(999), true);

        let bytes: Vec<u8> = have_map.clone().into();
        assert_eq!(bytes.len(), 125);

        have_map.set(1000);
        assert_eq!(have_map.get(1000), true);

        let bytes: Vec<u8> = have_map.clone().into();
        assert_eq!(bytes.len(), 126);

        assert_eq!(have_map.get(0), false);
        assert_eq!(have_map.get(1), false);
        assert_eq!(have_map.get(991), false);
        assert_eq!(have_map.get(998), false);
        assert_eq!(have_map.get(1008), false);
    }

    #[test]
    fn reset_havemap() {
        let mut have_map = PieceMap::new();
        have_map.set(1000);
        have_map.reset();
        assert_eq!(have_map.get(1000), false);
    }

    #[test]
    fn byte_representation() {
        let have_map: PieceMap = [0x55u8; 32].as_slice().into();
        for i in 0..256 {
            assert_eq!(
                have_map.get(i),
                i % 2 == 0,
                "bit {} was {}",
                i,
                have_map.get(i)
            );
        }

        let have_map: PieceMap = [0xaau8; 32].as_slice().into();
        for i in 0..256 {
            assert_eq!(
                have_map.get(i),
                i % 2 == 1,
                "bit {} was {}",
                i,
                have_map.get(i)
            );
        }
    }

    #[test]
    fn byte_manipulation() {
        let mut have_map: PieceMap = [0x55u8; 32].as_slice().into();
        assert_eq!(have_map.get(56), true);
        assert_eq!(have_map.get(57), false);
        assert_eq!(have_map.get(64), true);
        assert_eq!(have_map.get(65), false);

        have_map.write_bytes(7, [0xaau8; 2].as_slice());
        assert_eq!(have_map.get(56), false);
        assert_eq!(have_map.get(57), true);
        assert_eq!(have_map.get(64), false);
        assert_eq!(have_map.get(65), true);
    }

    #[test]
    fn byte_extension() {
        let mut have_map: PieceMap = [0x55u8; 32].as_slice().into();
        assert_eq!(have_map.as_ref().len(), 32);
        have_map.write_bytes(32, [0xaau8; 24].as_slice().into());
        assert_eq!(have_map.as_ref().len(), 56);
    }
}
