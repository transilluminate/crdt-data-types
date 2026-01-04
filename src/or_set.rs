// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crate::orset_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::Hash;

use serde::de::DeserializeOwned;

/// OR-Set: An Observed-Remove Set CRDT.
///
/// An OR-Set (Observed-Remove Set) allows both addition and removal of elements.
/// It resolves conflicts by preferring additions over removals in the case of
/// concurrent operations (Add-Wins semantics).
///
/// # Key Properties
///
/// - **Add-Wins**: If an element is concurrently added and removed, the addition wins.
/// - **Unique Tags**: Each addition is tagged with a unique identifier (from the vector clock).
/// - **Removal**: Removing an element removes all currently observed tags for that element.
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
/// use crdt_data_types::ORSet;
///
/// let mut set = ORSet::new();
/// set.insert("node_a", "apple".to_string());
/// set.remove(&"apple".to_string());
/// set.insert("node_b", "apple".to_string()); // Concurrent add
///
/// assert!(set.contains(&"apple".to_string()));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: DeserializeOwned + Eq + Hash + Ord"
))]
pub struct ORSet<T: Eq + Hash + Ord> {
    /// List of (element, set of observations) pairs, sorted by element.
    #[serde(serialize_with = "serialize_elements", deserialize_with = "deserialize_elements")]
    pub elements: Vec<(T, HashSet<(String, u64)>)>,
    /// Vector clock representing the causal history of the set.
    #[serde(default)]
    pub vclock: VectorClock,
}

fn serialize_elements<S, T>(
    elements: &Vec<(T, HashSet<(String, u64)>)>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: Serialize,
{
    use serde::ser::SerializeSeq;

    #[derive(Serialize)]
    struct EntryRef<'a, T> {
        element: &'a T,
        observations: &'a HashSet<(String, u64)>,
    }

    let mut seq = serializer.serialize_seq(Some(elements.len()))?;
    for (k, v) in elements {
        seq.serialize_element(&EntryRef {
            element: k,
            observations: v,
        })?;
    }
    seq.end()
}

type ORSetEntry<T> = (T, HashSet<(String, u64)>);

fn deserialize_elements<'de, D, T>(deserializer: D) -> Result<Vec<ORSetEntry<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: DeserializeOwned + Eq + Hash + Ord,
{
    struct ElementsVisitor<T>(std::marker::PhantomData<T>);

    impl<'de, T> serde::de::Visitor<'de> for ElementsVisitor<T>
    where
        T: DeserializeOwned + Eq + Hash + Ord,
    {
        type Value = Vec<ORSetEntry<T>>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of elements with observations")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            #[derive(Deserialize)]
            struct Entry<T> {
                element: T,
                observations: HashSet<(String, u64)>,
            }

            let mut elements = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(entry) = seq.next_element::<Entry<T>>()? {
                elements.push((entry.element, entry.observations));
            }
            // Sort to maintain invariant
            elements.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(elements)
        }
    }

    deserializer.deserialize_seq(ElementsVisitor(std::marker::PhantomData))
}

impl<T: Eq + Hash + Ord> Default for ORSet<T> {
    fn default() -> Self {
        Self {
            elements: Vec::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<T: Eq + Hash + Ord> ORSet<T> {
    /// Creates a new, empty OR-Set.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T> ORSet<T>
where
    T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Default + Send + Sync + 'static,
{
    /// Adds an element to the set.
    ///
    /// # Arguments
    /// * `node_id` - The identifier of the node performing the addition.
    /// * `element` - The element to add.
    pub fn insert(&mut self, node_id: &str, element: T) {
        self.vclock.increment(node_id);
        let id = self.vclock.clocks.get(node_id).copied().unwrap_or((0, 0));
        
        match self.elements.binary_search_by(|(e, _)| e.cmp(&element)) {
            Ok(idx) => {
                self.elements[idx].1.insert((node_id.to_string(), id.0));
            }
            Err(idx) => {
                let mut obs = HashSet::new();
                obs.insert((node_id.to_string(), id.0));
                self.elements.insert(idx, (element, obs));
            }
        }
    }

    /// Removes an element from the set by clearing its observations.
    ///
    /// # Arguments
    /// * `element` - The element to remove.
    pub fn remove(&mut self, element: &T) {
        if let Ok(idx) = self.elements.binary_search_by(|(e, _)| e.cmp(element)) {
            self.elements.remove(idx);
        }
    }

    /// Returns true if the set contains the specified element.
    pub fn contains(&self, element: &T) -> bool {
        self.elements.binary_search_by(|(e, _)| e.cmp(element)).is_ok()
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
        self.elements.iter().map(|(e, _)| e)
    }

    /// Merges another OR-Set into this one.
    ///
    /// For each element, the merged set contains the union of the observed IDs,
    /// but only those that are not causally overshadowed by a removal.
    pub fn merge(&mut self, other: &Self) {
        let mut new_elements = Vec::with_capacity(self.elements.len() + other.elements.len());
        let mut i = 0;
        let mut j = 0;

        while i < self.elements.len() && j < other.elements.len() {
            let (k1, v1) = &self.elements[i];
            let (k2, v2) = &other.elements[j];

            match k1.cmp(k2) {
                std::cmp::Ordering::Less => {
                    // Element only in self. Check if it was removed in other.
                    let mut kept_ids = HashSet::new();
                    for id in v1 {
                        let other_version = other.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                        // Keep the ID if the other replica hasn't observed this addition yet.
                        // If the other replica *has* observed this addition (id.1 <= other_version)
                        // but the element is missing from `other`, it implies `other` has removed it.
                        if id.1 > other_version {
                            kept_ids.insert(id.clone());
                        }
                    }
                    if !kept_ids.is_empty() {
                        new_elements.push((k1.clone(), kept_ids));
                    }
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    // Element only in other. Check if it was removed in self.
                    let mut kept_ids = HashSet::new();
                    for id in v2 {
                        let self_version = self.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                        if id.1 > self_version {
                            kept_ids.insert(id.clone());
                        }
                    }
                    if !kept_ids.is_empty() {
                        new_elements.push((k2.clone(), kept_ids));
                    }
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    // Element in both. Merge observations.
                    let mut merged_ids = HashSet::new();
                    
                    // Process IDs from self
                    for id in v1 {
                        let other_version = other.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                        if id.1 > other_version || v2.contains(id) {
                            merged_ids.insert(id.clone());
                        }
                    }
                    
                    // Process IDs from other
                    for id in v2 {
                        let self_version = self.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                        if id.1 > self_version || v1.contains(id) {
                            merged_ids.insert(id.clone());
                        }
                    }
                    
                    if !merged_ids.is_empty() {
                        new_elements.push((k1.clone(), merged_ids));
                    }
                    i += 1;
                    j += 1;
                }
            }
        }

        // Process remaining elements in self
        while i < self.elements.len() {
            let (k1, v1) = &self.elements[i];
            let mut kept_ids = HashSet::new();
            for id in v1 {
                let other_version = other.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                if id.1 > other_version {
                    kept_ids.insert(id.clone());
                }
            }
            if !kept_ids.is_empty() {
                new_elements.push((k1.clone(), kept_ids));
            }
            i += 1;
        }

        // Process remaining elements in other
        while j < other.elements.len() {
            let (k2, v2) = &other.elements[j];
            let mut kept_ids = HashSet::new();
            for id in v2 {
                let self_version = self.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                if id.1 > self_version {
                    kept_ids.insert(id.clone());
                }
            }
            if !kept_ids.is_empty() {
                new_elements.push((k2.clone(), kept_ids));
            }
            j += 1;
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

impl<'a, T> ORSetReader<'a, T> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }
}

impl<'a, T> CrdtReader<'a> for ORSetReader<'a, T>
where
    T: DeserializeOwned + Eq + Hash + Ord + Send + Sync,
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        let orset = reader
            .get_root::<orset_capnp::or_set::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        let elements = orset
            .get_elements()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        Ok(elements.len() == 0)
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<T> Crdt for ORSet<T>
where
    T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Default + Send + Sync + 'static,
{
    type Reader<'a> = ORSetReader<'a, T>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = ORSet::new();
        for reader in readers {
            let msg_reader = serialize::read_message(reader.bytes, ReaderOptions::new())
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let orset_reader = msg_reader
                .get_root::<orset_capnp::or_set::Reader>()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

            // Deserialize to temp ORSet
            let mut temp_set = ORSet::new();
            
            // VClock
            if orset_reader.has_vclock() {
                let vc_bytes = orset_reader
                    .get_vclock()
                    .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
                temp_set.vclock = VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(vc_bytes)])?;
            }

            // Elements
            let elements_reader = orset_reader
                .get_elements()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

            for element_entry in elements_reader {
                let element_bytes = element_entry
                    .get_element()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let element: T = serde_json::from_slice(element_bytes)
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                
                let mut obs = HashSet::new();
                let ids = element_entry
                    .get_ids()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                
                for id_entry in ids {
                    let node_id = id_entry
                        .get_node_id()
                        .map_err(|e| CrdtError::Deserialization(e.to_string()))?
                        .to_string()
                        .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    let counter = id_entry.get_counter();
                    obs.insert((node_id, counter));
                }
                temp_set.elements.push((element, obs));
            }
            // Ensure sorted invariant
            temp_set.elements.sort_by(|a, b| a.0.cmp(&b.0));

            result.merge(&temp_set);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut orset = message.init_root::<orset_capnp::or_set::Builder>();
            let mut elements = orset.reborrow().init_elements(self.elements.len() as u32);
            
            for (i, (element, obs)) in self.elements.iter().enumerate() {
                let mut element_entry = elements.reborrow().get(i as u32);
                let element_bytes = serde_json::to_vec(element).expect("Failed to serialize element");
                element_entry.set_element(&element_bytes);
                
                let mut ids = element_entry.init_ids(obs.len() as u32);
                for (j, (node_id, counter)) in obs.iter().enumerate() {
                    let mut id_entry = ids.reborrow().get(j as u32);
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
