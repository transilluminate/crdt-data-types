use crate::or_map_capnp;
use crate::or_set::ORSet;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::Hash;

use serde::de::DeserializeOwned;

/// OR-Map: An Observed-Remove Map CRDT.
///
/// An OR-Map (Observed-Remove Map) is a key-value map that supports both
/// addition and removal of key-value pairs with add-win semantics.
/// It treats each key-value pair as an element in an internal OR-Set,
/// ensuring that concurrent operations resolve consistently.
///
/// # Algebraic Properties
/// - **Commutativity**: Merge order does not affect the final map contents.
/// - **Idempotence**: Merging the same state multiple times is safe.
/// - **Convergence**: All replicas will eventually reach the same state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "K: Serialize + Eq + Hash + Ord, V: Serialize + Eq + Hash + Ord",
    deserialize = "K: DeserializeOwned + Eq + Hash + Ord, V: DeserializeOwned + Eq + Hash + Ord"
))]
pub struct ORMap<K: Eq + Hash + Ord, V: Eq + Hash + Ord> {
    /// Internal storage using an OR-Set of (K, V) tuples.
    pub elements: ORSet<(K, V)>,
    /// Vector clock representing the causal history of the map.
    pub vclock: VectorClock,
}

impl<K: Eq + Hash + Ord, V: Eq + Hash + Ord> Default for ORMap<K, V> {
    fn default() -> Self {
        Self {
            elements: ORSet::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<K, V> ORMap<K, V>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static + Ord,
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static + Ord,
{
    /// Creates a new, empty OR-Map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts or updates a value for a specific key.
    ///
    /// # Arguments
    /// * `node_id` - The identifier of the node performing the write.
    /// * `key` - The key to insert or update.
    /// * `value` - The value to associate with the key.
    pub fn insert(&mut self, node_id: &str, key: K, value: V) {
        // Remove existing versions of this key before adding new one
        self.remove(&key);
        self.elements.insert(node_id, (key, value));
        self.vclock.increment(node_id);
    }

    /// Removes a key (and its value) from the map.
    ///
    /// # Arguments
    /// * `key` - The key to remove.
    pub fn remove(&mut self, key: &K) {
        let to_remove: Vec<_> = self
            .elements
            .iter()
            .filter(|(k, _)| k == key)
            .cloned()
            .collect();
        for item in to_remove {
            self.elements.remove(&item);
        }
    }

    /// Returns the current value(s) associated with the key.
    ///
    /// In cases of concurrent writes to the same key, OR-Map may hold
    /// multiple concurrent values. This method returns all of them.
    pub fn get_concurrent(&self, key: &K) -> HashSet<V> {
        self.elements
            .iter()
            .filter(|(k, _)| k == key)
            .map(|(_, v)| v.clone())
            .collect()
    }

    /// Merges another OR-Map into this one.
    pub fn merge(&mut self, other: &Self) {
        self.elements.merge(&other.elements);
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct ORMapReader<'a, K: Eq + Hash + Ord, V: Eq + Hash + Ord> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<(K, V)>,
}

impl<'a, K, V> ORMapReader<'a, K, V>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static + Ord,
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static + Ord,
{
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_map(&self) -> Result<ORMap<K, V>, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let or_map = reader
            .get_root::<or_map_capnp::or_map::Reader>()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let orset_bytes = or_map
            .get_elements()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let elements = ORSet::merge_from_readers(&[crate::or_set::ORSetReader::new(orset_bytes)])?;

        let vclock = if or_map.has_vclock() {
            let vc_bytes = or_map
                .get_vclock()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                vc_bytes,
            )])?
        } else {
            VectorClock::new()
        };

        Ok(ORMap { elements, vclock })
    }
}

impl<'a, K, V> CrdtReader<'a> for ORMapReader<'a, K, V>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static + Ord,
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static + Ord,
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_map()?.elements.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<K, V> Crdt for ORMap<K, V>
where
    K: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static + Ord,
    V: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static + Ord,
{
    type Reader<'a> = ORMapReader<'a, K, V>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = ORMap::new();
        for reader in readers {
            result.merge(&reader.to_map()?);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut or_map = message.init_root::<or_map_capnp::or_map::Builder>();
            or_map.set_elements(&self.elements.to_capnp_bytes());
            or_map.set_vclock(&self.vclock.to_capnp_bytes());
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("ORMap serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
