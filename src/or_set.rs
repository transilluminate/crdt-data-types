use crate::orset_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

use serde::de::DeserializeOwned;

/// OR-Set: An Observed-Remove Set CRDT.
///
/// An OR-Set (Observed-Remove Set) allows both addition and removal of elements.
/// It resolves conflicts by preferring additions over removals in the case of
/// concurrent operations (add-win semantics). Internally, it tracks each element
/// with a set of unique identifiers (using a vector clock) that represent the
/// "observations" of that element.
///
/// # Algebraic Properties
/// - **Commutativity**: Merge order does not affect the final set contents.
/// - **Idempotence**: Merging the same state multiple times is safe.
/// - **Convergence**: All replicas will eventually reach the same state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: DeserializeOwned + Eq + Hash"
))]
pub struct ORSet<T: Eq + Hash> {
    /// Tracks which elements are "present" and their associated observation IDs.
    pub elements: HashMap<T, HashSet<(String, u64)>>,
    /// Vector clock representing the causal history of the set.
    pub vclock: VectorClock,
}

impl<T: Eq + Hash> Default for ORSet<T> {
    fn default() -> Self {
        Self {
            elements: HashMap::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<T: Eq + Hash> ORSet<T> {
    /// Creates a new, empty OR-Set.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T> ORSet<T>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static,
{
    /// Adds an element to the set.
    ///
    /// # Arguments
    /// * `node_id` - The identifier of the node performing the addition.
    /// * `element` - The element to add.
    pub fn insert(&mut self, node_id: &str, element: T) {
        self.vclock.increment(node_id);
        let id = self.vclock.clocks.get(node_id).copied().unwrap_or((0, 0));
        self.elements
            .entry(element)
            .or_insert_with(HashSet::new)
            .insert((node_id.to_string(), id.0));
    }

    /// Removes an element from the set by clearing its observations.
    ///
    /// # Arguments
    /// * `element` - The element to remove.
    pub fn remove(&mut self, element: &T) {
        // In OR-Set, removal simply clears the observed IDs for that element.
        self.elements.remove(element);
    }

    /// Returns true if the set contains the specified element.
    pub fn contains(&self, element: &T) -> bool {
        self.elements.contains_key(element)
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Returns true if the set is empty.
    pub fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    /// Iterator over the elements currently in the set.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.elements.keys()
    }

    /// Merges another OR-Set into this one.
    ///
    /// For each element, the merged set contains the union of the observed IDs,
    /// but only those that are not causally overshadowed by a removal.
    pub fn merge(&mut self, other: &Self) {
        let mut new_elements = HashMap::new();

        // 1. Combine all observed IDs from both sets
        let all_keys: HashSet<_> = self
            .elements
            .keys()
            .chain(other.elements.keys())
            .cloned()
            .collect();

        for key in all_keys {
            let mut merged_ids = HashSet::new();

            if let Some(ids) = self.elements.get(&key) {
                for id in ids {
                    let other_version =
                        other.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                    if id.1 > other_version {
                        merged_ids.insert(id.clone());
                    } else if other
                        .elements
                        .get(&key)
                        .map(|other_ids| other_ids.contains(id))
                        .unwrap_or(false)
                    {
                        merged_ids.insert(id.clone());
                    }
                }
            }

            if let Some(ids) = other.elements.get(&key) {
                for id in ids {
                    let self_version = self.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                    if id.1 > self_version {
                        merged_ids.insert(id.clone());
                    } else if self
                        .elements
                        .get(&key)
                        .map(|self_ids| self_ids.contains(id))
                        .unwrap_or(false)
                    {
                        merged_ids.insert(id.clone());
                    }
                }
            }

            if !merged_ids.is_empty() {
                new_elements.insert(key, merged_ids);
            }
        }

        self.elements = new_elements;
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct ORSetReader<'a, T> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static>
    ORSetReader<'a, T>
{
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_orset(&self) -> Result<ORSet<T>, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let orset = reader
            .get_root::<orset_capnp::or_set::Reader>()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let mut elements = HashMap::new();
        let entries = orset
            .get_elements()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        for entry in entries {
            let entry: orset_capnp::or_set::element::Reader = entry;
            let item_bytes = entry
                .get_element()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            let element: T = bincode::deserialize(item_bytes)
                .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;

            let mut ids = HashSet::new();
            let id_list = entry
                .get_ids()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            for id_entry in id_list {
                let id_entry: orset_capnp::or_set::id_entry::Reader = id_entry;
                let node_id = id_entry
                    .get_node_id()
                    .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?
                    .to_string()
                    .map_err(|e: std::str::Utf8Error| CrdtError::Deserialization(e.to_string()))?;
                ids.insert((node_id, id_entry.get_counter()));
            }
            elements.insert(element, ids);
        }

        let vclock = if orset.has_vclock() {
            let vc_bytes = orset
                .get_vclock()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                vc_bytes,
            )])?
        } else {
            VectorClock::new()
        };

        Ok(ORSet { elements, vclock })
    }
}

impl<'a, T: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static>
    CrdtReader<'a> for ORSetReader<'a, T>
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_orset()?.elements.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<T: Clone + Eq + Hash + Serialize + DeserializeOwned + Default + Send + Sync + 'static> Crdt
    for ORSet<T>
{
    type Reader<'a> = ORSetReader<'a, T>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = ORSet::new();
        for reader in readers {
            let msg_reader = serialize::read_message(reader.bytes, ReaderOptions::new())
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let orset = msg_reader
                .get_root::<orset_capnp::or_set::Reader>()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

            let other_vclock = if orset.has_vclock() {
                let vc_bytes = orset
                    .get_vclock()
                    .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
                VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                    vc_bytes,
                )])?
            } else {
                VectorClock::new()
            };

            let entries = orset
                .get_elements()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

            let mut other_keys = HashSet::new();

            for entry in entries {
                let item_bytes = entry
                    .get_element()
                    .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
                let element: T = bincode::deserialize(item_bytes)
                    .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;

                other_keys.insert(element.clone());

                let mut other_ids = HashSet::new();
                let id_list = entry
                    .get_ids()
                    .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
                for id_entry in id_list {
                    let node_id = id_entry
                        .get_node_id()
                        .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?
                        .to_str()
                        .map_err(|e: std::str::Utf8Error| {
                            CrdtError::Deserialization(e.to_string())
                        })?;
                    other_ids.insert((node_id.to_string(), id_entry.get_counter()));
                }

                let merged_ids = result
                    .elements
                    .entry(element.clone())
                    .or_insert_with(HashSet::new);

                // Keep existing IDs if not overshadowed by other ORSet's vclock OR if they exist in other's IDs
                merged_ids.retain(|id| {
                    let other_version =
                        other_vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                    id.1 > other_version || other_ids.contains(id)
                });

                // Add other's IDs if not overshadowed by result's vclock OR if they already exist in result
                // (Note: we don't need a formal contain check if we just check overshadowed)
                for id in other_ids {
                    let self_version = result
                        .vclock
                        .clocks
                        .get(&id.0)
                        .map(|(c, _)| *c)
                        .unwrap_or(0);
                    if id.1 > self_version || merged_ids.contains(&id) {
                        merged_ids.insert(id);
                    }
                }

                if merged_ids.is_empty() {
                    result.elements.remove(&element);
                }
            }

            // Also check for elements in result that were NOT in other_keys
            // These might be overshadowed by other's vclock (removals)
            let mut keys_to_remove = Vec::new();
            for (element, ids) in &mut result.elements {
                if !other_keys.contains(element) {
                    ids.retain(|id| {
                        let other_version =
                            other_vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                        id.1 > other_version
                    });
                    if ids.is_empty() {
                        keys_to_remove.push(element.clone());
                    }
                }
            }
            for key in keys_to_remove {
                result.elements.remove(&key);
            }

            result.vclock.merge(&other_vclock);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut orset = message.init_root::<orset_capnp::or_set::Builder>();
            let mut elements = orset.reborrow().init_elements(self.elements.len() as u32);
            for (idx, (element, ids)) in self.elements.iter().enumerate() {
                let mut entry = elements.reborrow().get(idx as u32);
                let bytes = bincode::serialize(element).expect("ORSet element serialization fail");
                entry.set_element(&bytes);

                let mut ids_builder = entry.init_ids(ids.len() as u32);
                for (j, (node_id, counter)) in ids.iter().enumerate() {
                    let mut id_entry = ids_builder.reborrow().get(j as u32);
                    id_entry.set_node_id(node_id.as_str().into());
                    id_entry.set_counter(*counter);
                }
            }
            let vclock_bytes = self.vclock.to_capnp_bytes();
            orset.set_vclock(&vclock_bytes);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("ORSet serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
