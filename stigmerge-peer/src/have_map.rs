use std::iter::repeat;

#[derive(Clone, Debug)]
pub struct HaveMap(Vec<u8>);

impl HaveMap {
    pub fn new() -> HaveMap {
        Self(vec![])
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
}

impl From<Vec<u8>> for HaveMap {
    fn from(value: Vec<u8>) -> Self {
        Self(value)
    }
}

impl From<&[u8]> for HaveMap {
    fn from(value: &[u8]) -> Self {
        Self(value.to_vec())
    }
}

impl From<HaveMap> for Vec<u8> {
    fn from(value: HaveMap) -> Self {
        value.0
    }
}

#[cfg(test)]
mod tests {
    use super::HaveMap;

    #[test]
    fn empty_havemap() {
        let have_map = HaveMap::new();
        assert_eq!(have_map.get(0), false);
        assert_eq!(have_map.get(21389213), false);
    }

    #[test]
    fn non_empty_havemap() {
        let mut have_map = HaveMap::new();
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
        let mut have_map = HaveMap::new();
        have_map.set(1000);
        have_map.reset();
        assert_eq!(have_map.get(1000), false);
    }

    #[test]
    fn byte_representation() {
        let have_map: HaveMap = [0x55u8; 32].as_slice().into();
        for i in 0..256 {
            assert_eq!(
                have_map.get(i),
                i % 2 == 0,
                "bit {} was {}",
                i,
                have_map.get(i)
            );
        }

        let have_map: HaveMap = [0xaau8; 32].as_slice().into();
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
}
