use crate::lru::LruStore;

pub struct LatticeSliceCache {
    slices: LruStore<String, Vec<u8>>,
}

impl LatticeSliceCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            slices: LruStore::new(capacity),
        }
    }

    pub fn put_slice(&mut self, key: impl Into<String>, payload: Vec<u8>) {
        self.slices.put(key.into(), payload);
    }

    pub fn get_slice(&mut self, key: &str) -> Option<&[u8]> {
        self.slices.get(key).map(Vec::as_slice)
    }
}
