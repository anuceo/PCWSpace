use std::borrow::Borrow;
use std::hash::Hash;
use std::num::NonZeroUsize;

pub struct LruStore<K, V> {
    inner: lru::LruCache<K, V>,
}

impl<K: Hash + Eq, V> LruStore<K, V> {
    pub fn new(capacity: usize) -> Self {
        let safe_capacity =
            NonZeroUsize::new(capacity.max(1)).expect("capacity should be non-zero");
        Self {
            inner: lru::LruCache::new(safe_capacity),
        }
    }

    pub fn put(&mut self, key: K, value: V) {
        self.inner.put(key, value);
    }

    pub fn get<Q>(&mut self, key: &Q) -> Option<&V>
    where
        K: Borrow<Q>,
        Q: Hash + Eq + ?Sized,
    {
        self.inner.get(key)
    }
}
