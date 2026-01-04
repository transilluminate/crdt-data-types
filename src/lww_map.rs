// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crate::lww_map_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::Hash;

/// LWW-Map: A Last-Write-Wins Map CRDT.
///
/// An LWW-Map is a key-value map where each entry independently resolves conflicts
/// using Last-Write-Wins (LWW) semantics. This is achieved by storing a timestamp
/// and node identifier for each key-value pair.
///
/// # Key Properties
///
/// - **Map Semantics**: Stores key-value pairs.
/// - **Per-Key LWW**: Each key's value is determined by the latest timestamp.
/// - **Add/Update Wins**: Updates with higher timestamps overwrite older ones.
///
/// # Algebraic Properties
///
/// - **Commutativity**: Yes.
/// - **Associativity**: Yes.
/// - **Idempotence**: Yes.
///
/// # Example
///
/// ```
/// use crdt_data_types::LWWMap;
///
/// let mut map1 = LWWMap::new();
/// map1.insert("node_a", "key1".to_string(), "value1".to_string(), 100);
///
/// let mut map2 = LWWMap::new();
/// map2.insert("node_b", "key1".to_string(), "value2".to_string(), 200);
///
/// map1.merge(&map2);
/// assert_eq!(map1.get(&"key1".to_string()), Some(&"value2".to_string())); // Higher timestamp wins
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: DeserializeOwned + Eq + Hash + Ord, V: DeserializeOwned"
))]
pub struct LWWMap<K: Eq + Hash + Ord, V> {
    /// Internal storage for map entries: key -> (value, timestamp, node_id).
    #[serde(serialize_with = "serialize_entries", deserialize_with = "deserialize_entries")]
    pub entries: Vec<(K, (V, u64, String))>,
    /// Vector clock representing the causal history of the map.
    #[serde(default)]
    pub vclock: VectorClock,
}

fn serialize_entries<S, K, V>(
    entries: &Vec<(K, (V, u64, String))>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    K: Serialize,
    V: Serialize,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(entries.len()))?;
    for (k, v) in entries {
        map.serialize_entry(k, v)?;
    }
    map.end()
}

type LWWMapEntry<K, V> = (K, (V, u64, String));

fn deserialize_entries<'de, D, K, V>(deserializer: D) -> Result<Vec<LWWMapEntry<K, V>>, D::Error>
where
    D: serde::Deserializer<'de>,
    K: DeserializeOwned + Eq + Hash + Ord,
    V: DeserializeOwned,
{
    struct EntriesVisitor<K, V>(std::marker::PhantomData<(K, V)>);

    impl<'de, K, V> serde::de::Visitor<'de> for EntriesVisitor<K, V>
    where
        K: DeserializeOwned + Eq + Hash + Ord,
        V: DeserializeOwned,
    {
        type Value = Vec<LWWMapEntry<K, V>>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a map of entries")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>,
        {
            let mut entries: Vec<LWWMapEntry<K, V>> =
                Vec::with_capacity(access.size_hint().unwrap_or(0));
            while let Some((key, value)) = access.next_entry()? {
                entries.push((key, value));
            }
            // Sort to maintain invariant
            entries.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(entries)
        }
    }

    deserializer.deserialize_map(EntriesVisitor(std::marker::PhantomData))
}

impl<K: Eq + Hash + Ord, V> Default for LWWMap<K, V> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<K: Eq + Hash + Ord, V> LWWMap<K, V> {
    /// Creates a new, empty LWW-Map.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K, V> LWWMap<K, V>
where
    K: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Inserts or updates a value for a specific key.
    ///
    /// The update is applied only if the new timestamp is higher than the current
    /// one for that key, or if they are equal and the new node_id is lexicographically greater.
    pub fn insert(&mut self, node_id: &str, key: K, value: V, timestamp: u64) {
        let node_id_str = node_id.to_string();
        
        match self.entries.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(idx) => {
                let (_, (val, ts, nid)) = &self.entries[idx];
                let update = timestamp > *ts
                    || (timestamp == *ts && node_id_str > *nid)
                    || (timestamp == *ts
                        && node_id_str == *nid
                        && value > *val);
                
                if update {
                    self.entries[idx] = (key, (value, timestamp, node_id_str));
                    self.vclock.increment(node_id);
                }
            }
            Err(idx) => {
                self.entries.insert(idx, (key, (value, timestamp, node_id_str)));
                self.vclock.increment(node_id);
            }
        }
    }

    /// Removes a key (and its value) from the map.
    ///
    /// Note: Standard LWW-Map removals usually require tombstones to be
    /// commutative in all scenarios. This simple implementation clears
    /// local state.
    pub fn remove(&mut self, key: &K) {
        if let Ok(idx) = self.entries.binary_search_by(|(k, _)| k.cmp(key)) {
            self.entries.remove(idx);
        }
    }

    /// Returns the value associated with the key, if any.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries
            .binary_search_by(|(k, _)| k.cmp(key))
            .ok()
            .map(|idx| &self.entries[idx].1.0)
    }

    /// Merges another LWW-Map into this one.
    pub fn merge(&mut self, other: &Self) {
        let mut result = Vec::with_capacity(self.entries.len() + other.entries.len());
        let mut i = 0;
        let mut j = 0;

        while i < self.entries.len() && j < other.entries.len() {
            let (k1, (v1, ts1, nid1)) = &self.entries[i];
            let (k2, (v2, ts2, nid2)) = &other.entries[j];

            match k1.cmp(k2) {
                Ordering::Less => {
                    result.push(self.entries[i].clone());
                    i += 1;
                }
                Ordering::Greater => {
                    result.push(other.entries[j].clone());
                    j += 1;
                }
                Ordering::Equal => {
                    // Conflict resolution
                    let update = *ts2 > *ts1
                        || (*ts2 == *ts1 && nid2 > nid1)
                        || (*ts2 == *ts1
                            && nid2 == nid1
                            && v2 > v1);
                    
                    if update {
                        result.push(other.entries[j].clone());
                    } else {
                        result.push(self.entries[i].clone());
                    }
                    i += 1;
                    j += 1;
                }
            }
        }

        while i < self.entries.len() {
            result.push(self.entries[i].clone());
            i += 1;
        }
        while j < other.entries.len() {
            result.push(other.entries[j].clone());
            j += 1;
        }

        self.entries = result;
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct LWWMapReader<'a, K: Eq + Hash + Ord, V> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<'a, K, V> LWWMapReader<'a, K, V>
where
    K: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_map(&self) -> Result<LWWMap<K, V>, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let lww_map = reader
            .get_root::<lww_map_capnp::lww_map::Reader>()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let mut entries = Vec::new();
        let entries_list = lww_map
            .get_entries()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        for entry in entries_list {
            let key_bytes = entry
                .get_key()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            let key: K = bincode::deserialize(key_bytes)
                .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;

            let value_bytes = entry
                .get_value()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            let value: V = bincode::deserialize(value_bytes)
                .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;

            let timestamp = entry.get_timestamp();
            let node_id = entry
                .get_node_id()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?
                .to_string()
                .map_err(|e: std::str::Utf8Error| CrdtError::Deserialization(e.to_string()))?;

            entries.push((key, (value, timestamp, node_id)));
        }
        // Sort to maintain invariant
        entries.sort_by(|a, b| a.0.cmp(&b.0));

        let vclock = if lww_map.has_vclock() {
            let vc_bytes = lww_map
                .get_vclock()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                vc_bytes,
            )])?
        } else {
            VectorClock::new()
        };

        Ok(LWWMap { entries, vclock })
    }
}

impl<'a, K, V> CrdtReader<'a> for LWWMapReader<'a, K, V>
where
    K: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_map()?.entries.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<K, V> Crdt for LWWMap<K, V>
where
    K: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Reader<'a> = LWWMapReader<'a, K, V>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = LWWMap::new();
        for reader in readers {
            result.merge(&reader.to_map()?);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut lww_map = message.init_root::<lww_map_capnp::lww_map::Builder>();
            let mut entries = lww_map.reborrow().init_entries(self.entries.len() as u32);
            for (idx, (key, (value, timestamp, node_id))) in self.entries.iter().enumerate() {
                let mut entry = entries.reborrow().get(idx as u32);
                let key_bytes = bincode::serialize(key).expect("LWWMap key serialization fail");
                let value_bytes =
                    bincode::serialize(value).expect("LWWMap value serialization fail");
                entry.set_key(&key_bytes);
                entry.set_value(&value_bytes);
                entry.set_timestamp(*timestamp);
                entry.set_node_id(node_id.as_str().into());
            }
            let vclock_bytes = self.vclock.to_capnp_bytes();
            lww_map.set_vclock(&vclock_bytes);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("LWWMap serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
