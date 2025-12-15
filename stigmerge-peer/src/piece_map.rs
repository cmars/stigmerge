use veilid_core::ValueData;

#[derive(Clone, Debug)]
pub struct PieceMap(Vec<u8>);

impl Default for PieceMap {
    fn default() -> Self {
        Self::new()
    }
}

impl PieceMap {
    pub fn new() -> PieceMap {
        Self(vec![])
    }

    pub fn subkeys(n_pieces: usize) -> u16 {
        let cap = Self::capacity(n_pieces);
        TryInto::<u16>::try_into(cap / ValueData::MAX_LEN).unwrap()
            + if !cap.is_multiple_of(ValueData::MAX_LEN) {
                1u16
            } else {
                0u16
            }
    }

    pub fn capacity(n_pieces: usize) -> usize {
        n_pieces / 8 + if !n_pieces.is_multiple_of(8) { 1 } else { 0 }
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

    pub fn intersection(&self, other: PieceMap) -> PieceMap {
        self.0
            .iter()
            .zip(other.0.iter())
            .map(|(l, r)| l & r)
            .collect::<Vec<u8>>()
            .into()
    }

    pub fn pop_intersect(&mut self, other: PieceMap) -> Option<usize> {
        for (i, (l, r)) in self.0.iter().zip(other.0.iter()).enumerate() {
            let intersect = l & r;
            for bit_offset in 0..8 {
                if intersect & 1 << bit_offset != 0 {
                    let bit_index = bit_offset + i * 8;
                    self.clear(TryInto::<u32>::try_into(bit_index).unwrap());
                    return Some(bit_index);
                }
            }
        }
        None
    }

    pub fn is_empty(&self) -> bool {
        self.0.iter().any(|b| *b != 0u8)
    }

    pub fn set(&mut self, bit_index: u32) {
        let byte_index: usize = (bit_index / 8).try_into().unwrap();
        let bit_offset_in_byte: usize = (bit_index % 8).try_into().unwrap();
        if self.0.len() <= byte_index {
            let old_len = self.0.len();
            self.0.resize(byte_index + 1, 0);
            // Ensure newly added bytes are properly zeroed
            for i in old_len..=byte_index {
                self.0[i] = 0;
            }
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
            self.0.resize(required_len, 0);
        }
        for (i, b) in bytes.iter().enumerate() {
            self.0[byte_offset + i] = *b;
        }
    }

    pub fn iter(&self) -> PieceMapIter<'_> {
        PieceMapIter {
            bitmap: &self.0,
            byte_index: 0,
            bit_offset: 0,
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

pub struct PieceMapIter<'a> {
    bitmap: &'a Vec<u8>,
    byte_index: usize,
    bit_offset: usize,
}

impl<'a> Iterator for PieceMapIter<'a> {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        // Find next set bit
        while self.byte_index < self.bitmap.len() {
            let current_byte = self.bitmap[self.byte_index];

            // Skip bits that are already checked
            let remaining_bits = current_byte >> self.bit_offset;

            if remaining_bits != 0 {
                // Find position of first set bit in remaining bits
                let bit_position = remaining_bits.trailing_zeros() as usize;
                let bit_index = self.byte_index * 8 + self.bit_offset + bit_position;

                // Update position for next iteration
                self.bit_offset += bit_position + 1;
                if self.bit_offset >= 8 {
                    self.bit_offset = 0;
                    self.byte_index += 1;
                }

                return Some(bit_index);
            }

            // Move to next byte
            self.byte_index += 1;
            self.bit_offset = 0;
        }

        None
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

    #[test]
    fn iterator_empty() {
        let have_map = PieceMap::new();
        let collected: Vec<usize> = have_map.iter().collect();
        assert_eq!(collected.len(), 0);
    }

    #[test]
    fn iterator_single_bit() {
        let mut have_map = PieceMap::new();
        have_map.set(42);
        let collected: Vec<usize> = have_map.iter().collect();
        assert_eq!(collected, vec![42]);
    }

    #[test]
    fn iterator_multiple_bits() {
        let mut have_map = PieceMap::new();
        have_map.set(0);
        have_map.set(7);
        have_map.set(8);
        have_map.set(15);
        have_map.set(100);

        let collected: Vec<usize> = have_map.iter().collect();
        assert_eq!(collected, vec![0, 7, 8, 15, 100]);
    }

    #[test]
    fn iterator_pattern_55() {
        let have_map: PieceMap = [0x55u8; 4].as_slice().into(); // 01010101 pattern
        let collected: Vec<usize> = have_map.iter().collect();
        let expected: Vec<usize> = (0..32).filter(|i| i % 2 == 0).collect();
        assert_eq!(collected, expected);
    }

    #[test]
    fn iterator_pattern_aa() {
        let have_map: PieceMap = [0xaau8; 4].as_slice().into(); // 10101010 pattern
        let collected: Vec<usize> = have_map.iter().collect();
        let expected: Vec<usize> = (0..32).filter(|i| i % 2 == 1).collect();
        assert_eq!(collected, expected);
    }

    #[test]
    fn iterator_pattern_ff() {
        let have_map: PieceMap = [0xffu8; 4].as_slice().into(); // 10101010 pattern
        assert_eq!(have_map.iter().count(), 32);
    }

    #[test]
    fn iterator_sparse_bits() {
        let mut have_map = PieceMap::new();
        let bits = vec![10, 50, 100, 200, 500];
        for &bit in &bits {
            have_map.set(bit as u32);
        }

        let collected: Vec<usize> = have_map.iter().collect();
        assert_eq!(collected, bits);
    }

    #[test]
    fn intersection_empty_maps() {
        let map1 = PieceMap::new();
        let map2 = PieceMap::new();
        let result = map1.intersection(map2);
        assert!(result.iter().count() == 0);
    }

    #[test]
    fn intersection_one_empty() {
        let mut map1 = PieceMap::new();
        map1.set(10);
        map1.set(20);
        let map2 = PieceMap::new();
        let result = map1.intersection(map2);
        assert!(result.iter().count() == 0);
    }

    #[test]
    fn intersection_no_overlap() {
        let mut map1 = PieceMap::new();
        map1.set(10);
        map1.set(20);
        let mut map2 = PieceMap::new();
        map2.set(30);
        map2.set(40);
        let result = map1.intersection(map2);
        assert!(result.iter().count() == 0);
    }

    #[test]
    fn intersection_partial_overlap() {
        let mut map1 = PieceMap::new();
        map1.set(10);
        map1.set(20);
        map1.set(30);
        let mut map2 = PieceMap::new();
        map2.set(20);
        map2.set(30);
        map2.set(40);
        let result = map1.intersection(map2);
        let bits: Vec<usize> = result.iter().collect();
        assert_eq!(bits, vec![20, 30]);
    }

    #[test]
    fn intersection_complete_overlap() {
        let mut map1 = PieceMap::new();
        map1.set(10);
        map1.set(20);
        let mut map2 = PieceMap::new();
        map2.set(10);
        map2.set(20);
        let result = map1.intersection(map2);
        let bits: Vec<usize> = result.iter().collect();
        assert_eq!(bits, vec![10, 20]);
    }

    #[test]
    fn intersection_same_map() {
        let mut map1 = PieceMap::new();
        map1.set(5);
        map1.set(15);
        map1.set(25);
        let result = map1.intersection(map1.clone());
        let bits: Vec<usize> = result.iter().collect();
        assert_eq!(bits, vec![5, 15, 25]);
    }

    #[test]
    fn intersection_different_sizes() {
        let mut map1 = PieceMap::new();
        map1.set(5);
        map1.set(1000); // Far apart to test different byte lengths
        let mut map2 = PieceMap::new();
        map2.set(5);
        map2.set(6);
        let result = map1.intersection(map2);
        let bits: Vec<usize> = result.iter().collect();
        assert_eq!(bits, vec![5]);
    }

    #[test]
    fn intersection_byte_patterns() {
        // Test with specific byte patterns to verify bitwise AND operation
        let map1: PieceMap = [0b10101010u8, 0b11110000u8].as_slice().into(); // Bits: 1,3,5,7, 8-11
        let map2: PieceMap = [0b01010101u8, 0b00001111u8].as_slice().into(); // Bits: 0,2,4,6, 12-15
        let result = map1.intersection(map2);
        let bits: Vec<usize> = result.iter().collect();
        assert_eq!(bits, Vec::<usize>::new()); // No overlapping bits

        // Test with some overlapping bits
        let map3: PieceMap = [0b11110000u8, 0b10101010u8].as_slice().into(); // Bits: 4-7, 8,10,12,14
        let map4: PieceMap = [0b11001100u8, 0b01010101u8].as_slice().into(); // Bits: 2-3,6-7, 9,11,13,15
        let result2 = map3.intersection(map4);
        let bits2: Vec<usize> = result2.iter().collect();
        assert_eq!(bits2, vec![6, 7]); // Only bits 6 and 7 overlap
    }

    #[test]
    fn pop_intersect_empty_maps() {
        let mut map1 = PieceMap::new();
        let map2 = PieceMap::new();
        let result = map1.pop_intersect(map2);
        assert_eq!(result, None);
    }

    #[test]
    fn pop_intersect_one_empty() {
        let mut map1 = PieceMap::new();
        map1.set(10);
        let map2 = PieceMap::new();
        let result = map1.pop_intersect(map2);
        assert_eq!(result, None);
        assert!(map1.get(10)); // Original map unchanged
    }

    #[test]
    fn pop_intersect_no_overlap() {
        let mut map1 = PieceMap::new();
        map1.set(10);
        map1.set(20);
        let mut map2 = PieceMap::new();
        map2.set(30);
        map2.set(40);
        let result = map1.pop_intersect(map2);
        assert_eq!(result, None);
        assert!(map1.get(10));
        assert!(map1.get(20));
    }

    #[test]
    fn pop_intersect_single_overlap() {
        let mut map1 = PieceMap::new();
        map1.set(10);
        map1.set(20);
        let mut map2 = PieceMap::new();
        map2.set(20);
        map2.set(30);

        let result = map1.pop_intersect(map2.clone());

        // Should find and clear an intersecting bit
        assert!(result.is_some());
        let result_bit = result.unwrap();
        assert_eq!(result_bit, 20);

        // The returned bit should no longer be set in map1
        assert!(!map1.get(result_bit as u32));

        // The returned bit should be one that was set in both maps
        assert!(map2.get(result_bit as u32));

        // Bit 10 should still be set (it wasn't intersecting)
        assert!(map1.get(10));

        // There should be no more intersections after clearing the intersecting bit
        let result2 = map1.pop_intersect(map2.clone());
        assert_eq!(result2, None);
    }

    #[test]
    fn pop_intersect_multiple_overlaps() {
        let mut map1 = PieceMap::new();
        map1.set(5);
        map1.set(15);
        map1.set(25);
        let mut map2 = PieceMap::new();
        map2.set(5);
        map2.set(15);
        map2.set(35);

        // Should find and clear one intersecting bit
        let result = map1.pop_intersect(map2.clone());
        assert!(result.is_some());
        let result_bit = result.unwrap();
        assert!(!map1.get(result_bit as u32));

        // The result should be one of the intersecting bits (5 or 15)
        assert!(result_bit == 5 || result_bit == 15);

        // Should find and clear the other intersecting bit
        let result2 = map1.pop_intersect(map2.clone());
        if result2.is_some() {
            let result_bit2 = result2.unwrap();
            assert!(!map1.get(result_bit2 as u32));
            // Should be the other intersecting bit
            assert!(result_bit2 == 5 || result_bit2 == 15);
            assert_ne!(result_bit, result_bit2);
        }

        // No more intersections
        let result3 = map1.pop_intersect(map2);
        assert_eq!(result3, None);

        // Bit 25 should still be set (it wasn't intersecting)
        assert!(map1.get(25));
    }

    #[test]
    fn pop_intersect_byte_boundary() {
        let mut map1 = PieceMap::new();
        map1.set(7); // Last bit of first byte
        map1.set(8); // First bit of second byte
        map1.set(15); // Last bit of second byte

        let mut map2 = PieceMap::new();
        map2.set(8);
        map2.set(15);

        // Should find and clear one intersecting bit
        let result = map1.pop_intersect(map2.clone());
        assert!(result.is_some());
        let result_bit = result.unwrap();
        assert!(!map1.get(result_bit as u32));

        // Should be one of the intersecting bits (8 or 15)
        assert!(result_bit == 8 || result_bit == 15);

        // Should find and clear the other intersecting bit
        let result2 = map1.pop_intersect(map2);
        if result2.is_some() {
            let result_bit2 = result2.unwrap();
            assert!(!map1.get(result_bit2 as u32));
            // Should be the other intersecting bit
            assert!(result_bit2 == 8 || result_bit2 == 15);
            assert_ne!(result_bit, result_bit2);
        }

        // Bit 7 should still be set (it wasn't intersecting)
        assert!(map1.get(7));
    }

    #[test]
    fn pop_intersect_same_map() {
        let mut map1 = PieceMap::new();
        map1.set(10);
        map1.set(20);

        // Should find and clear one of the set bits
        let result = map1.pop_intersect(map1.clone());
        assert!(result.is_some());
        let result_bit = result.unwrap();
        assert!(!map1.get(result_bit as u32));

        // Should be one of the set bits (10 or 20)
        assert!(result_bit == 10 || result_bit == 20);

        // Should find and clear the other set bit
        let result2 = map1.pop_intersect(map1.clone());
        if result2.is_some() {
            let result_bit2 = result2.unwrap();
            assert!(!map1.get(result_bit2 as u32));
            // Should be the other set bit
            assert!(result_bit2 == 10 || result_bit2 == 20);
            assert_ne!(result_bit, result_bit2);
        }

        // No more bits
        let result3 = map1.pop_intersect(map1.clone());
        assert_eq!(result3, None);
    }

    #[test]
    fn pop_intersect_large_indices() {
        let mut map1 = PieceMap::new();
        map1.set(1000);
        map1.set(2000);
        let mut map2 = PieceMap::new();
        map2.set(2000);
        map2.set(3000);

        let result = map1.pop_intersect(map2);
        assert!(result.is_some());
        let result_bit = result.unwrap();
        assert!(!map1.get(result_bit as u32));
        assert!(map1.get(1000)); // Bit 1000 should still be set

        // The result should be the intersecting bit (2000)
        assert_eq!(result_bit, 2000);
    }

    #[test]
    fn pop_intersect_sparse_patterns() {
        let mut map1 = PieceMap::new();
        map1.set(0);
        map1.set(63); // Different bytes, far apart

        let mut map2 = PieceMap::new();
        map2.set(63);
        map2.set(127); // Even further apart

        let result = map1.pop_intersect(map2);
        assert!(result.is_some());
        let result_bit = result.unwrap();
        assert!(!map1.get(result_bit as u32));
        assert!(map1.get(0)); // Bit 0 should still be set

        // The result should be the intersecting bit (63)
        assert_eq!(result_bit, 63);
    }
}
