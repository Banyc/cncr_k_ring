use std::{
    collections::{hash_map::DefaultHasher, HashMap, VecDeque},
    hash::{self, Hasher},
    sync::{Arc, Mutex},
};

pub struct CncrKLtdRing<K, V>
where
    K: Eq + hash::Hash,
{
    shards: Arc<Vec<Shard<K, V>>>,
}

impl<K, V> CncrKLtdRing<K, V>
where
    K: Eq + hash::Hash,
{
    pub fn new(shard_count: usize, ring_size: usize) -> Self {
        let shards = (0..shard_count)
            .map(|_| Shard::new(ring_size))
            .collect::<Vec<_>>();
        Self {
            shards: Arc::new(shards),
        }
    }

    #[must_use]
    pub fn take_ring(&self, key: &K) -> Option<VecDeque<V>> {
        let index = self.shard_index(key);
        self.shards[index].take_ring(key)
    }

    pub fn push(&self, key: K, value: V) {
        let index = self.shard_index(&key);
        self.shards[index].push(key, value);
    }

    fn shard_index(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish() as usize % self.shards.len()
    }
}

struct Shard<K, V>
where
    K: Eq + hash::Hash,
{
    map: Mutex<HashMap<K, VecDeque<V>>>,
    ring_size: usize,
}

impl<K, V> Shard<K, V>
where
    K: Eq + hash::Hash,
{
    fn new(ring_size: usize) -> Self {
        Self {
            map: Mutex::new(HashMap::new()),
            ring_size,
        }
    }

    #[must_use]
    fn take_ring(&self, key: &K) -> Option<VecDeque<V>> {
        let mut map = self.map.lock().unwrap();
        // swap
        let value = map
            .get_mut(key)
            .map(|v| std::mem::replace(v, VecDeque::new()));
        value
    }

    fn push(&self, key: K, value: V) {
        let mut map = self.map.lock().unwrap();
        let ring = map
            .entry(key)
            .or_insert_with(|| VecDeque::with_capacity(self.ring_size));
        if ring.len() == self.ring_size {
            ring.pop_front();
        }
        ring.push_back(value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test() {
        let ring_map = CncrKLtdRing::new(4, 3);

        let ring = ring_map.take_ring(&"a");
        assert!(ring.is_none());

        let ring = ring_map.take_ring(&"a");
        assert!(ring.is_none());

        ring_map.push("a", 1);
        ring_map.push("a", 2);
        ring_map.push("a", 3);
        ring_map.push("a", 4);
        let ring = ring_map.take_ring(&"a");
        let ring = ring.unwrap();
        assert_eq!(ring.len(), 3);
        assert_eq!(ring[0], 2);
        assert_eq!(ring[1], 3);
        assert_eq!(ring[2], 4);

        let ring = ring_map.take_ring(&"a");
        let ring = ring.unwrap();
        assert_eq!(ring.len(), 0);
    }

    #[test]
    fn test_concurrency_same_key() {
        let ring_map = CncrKLtdRing::new(4, 3);
        let ring_map = Arc::new(ring_map);
        let mut threads = Vec::new();
        for i in 0..100 {
            let ring_map = ring_map.clone();
            let thread = std::thread::spawn(move || {
                ring_map.push("a", i);
            });
            threads.push(thread);
        }
        for thread in threads {
            thread.join().unwrap();
        }
        let ring = ring_map.take_ring(&"a");
        let ring = ring.unwrap();
        assert_eq!(ring.len(), 3);
    }

    #[test]
    fn test_concurrency_different_keys() {
        let ring_map = CncrKLtdRing::new(4, 3);
        let ring_map = Arc::new(ring_map);
        let mut threads = Vec::new();
        for key in 0..100 {
            let ring_map = Arc::clone(&ring_map);
            let thread = std::thread::spawn(move || {
                for value in 0..100 {
                    ring_map.push(key, value);
                }
            });
            threads.push(thread);
        }
        for thread in threads {
            thread.join().unwrap();
        }
        for i in 0..100 {
            let ring = ring_map.take_ring(&i);
            let ring = ring.unwrap();
            assert_eq!(ring.len(), 3);
            assert_eq!(ring[0], 97);
            assert_eq!(ring[1], 98);
            assert_eq!(ring[2], 99);
        }
    }
}
