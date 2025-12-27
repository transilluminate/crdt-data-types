use crate::lww_map_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;

/// LWW-Map: A Last-Write-Wins Map CRDT.
///
/// An LWW-Map is a key-value map where each entry independently resolves conflicts
/// using Last-Write-Wins (LWW) semantics. This is achieved by storing a timestamp
/// and node identifier for each key-value pair.
///
/// # Algebraic Properties
/// - **Commutativity**: Merge order does not affect the final map contents.
/// - **Idempotence**: Merging the same state multiple times is safe.
/// - **Convergence**: All replicas will eventually reach the same state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize, V: Serialize",
    deserialize = "K: DeserializeOwned + Eq + Hash, V: DeserializeOwned"
))]
pub struct LWWMap<K: Eq + Hash, V> {
    /// Internal storage for map entries: key -> (value, timestamp, node_id).
    pub entries: HashMap<K, (V, u64, String)>,
    /// Vector clock representing the causal history of the map.
    pub vclock: VectorClock,
}

impl<K: Eq + Hash, V> Default for LWWMap<K, V> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<K: Eq + Hash, V> LWWMap<K, V> {
    /// Creates a new, empty LWW-Map.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<K, V> LWWMap<K, V>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    /// Inserts or updates a value for a specific key.
    ///
    /// The update is applied only if the new timestamp is higher than the current
    /// one for that key, or if they are equal and the new node_id is lexicographically greater.
    pub fn insert(&mut self, node_id: &str, key: K, value: V, timestamp: u64) {
        let node_id = node_id.to_string();
        let current_entry = self.entries.get(&key);

        let update = match current_entry {
            Some((val, ts, nid)) => {
                timestamp > *ts
                    || (timestamp == *ts && node_id > *nid)
                    || (timestamp == *ts
                        && node_id == *nid
                        && bincode::serialize(&value).unwrap_or_default()
                            > bincode::serialize(val).unwrap_or_default())
            }
            None => true,
        };
        if update {
            self.entries
                .insert(key, (value, timestamp, node_id.clone()));
            self.vclock.increment(&node_id);
        }
    }

    /// Removes a key (and its value) from the map.
    ///
    /// Note: Standard LWW-Map removals usually require tombstones to be
    /// commutative in all scenarios. This simple implementation clears
    /// local state.
    pub fn remove(&mut self, key: &K) {
        self.entries.remove(key);
    }

    /// Returns the value associated with the key, if any.
    pub fn get(&self, key: &K) -> Option<&V> {
        self.entries.get(key).map(|(v, _, _)| v)
    }

    /// Merges another LWW-Map into this one.
    pub fn merge(&mut self, other: &Self) {
        for (key, other_entry) in &other.entries {
            let update = match self.entries.get(key) {
                Some((val, ts, nid)) => {
                    other_entry.1 > *ts
                        || (other_entry.1 == *ts && other_entry.2 > *nid)
                        || (other_entry.1 == *ts
                            && other_entry.2 == *nid
                            && bincode::serialize(&other_entry.0).unwrap_or_default()
                                > bincode::serialize(val).unwrap_or_default())
                }
                None => true,
            };
            if update {
                self.entries.insert(key.clone(), other_entry.clone());
            }
        }
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct LWWMapReader<'a, K: Eq + Hash, V> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<'a, K, V> LWWMapReader<'a, K, V>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
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

        let mut entries = HashMap::new();
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

            entries.insert(key, (value, timestamp, node_id));
        }

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
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
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
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
    V: Clone + Serialize + DeserializeOwned + Send + Sync + 'static,
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
