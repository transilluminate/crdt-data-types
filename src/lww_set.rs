// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crate::lww_set_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::Hash;

/// LWW-Set: A Last-Write-Wins Set CRDT.
///
/// An LWW-Set (Last-Write-Wins Set) stores elements where each element's
/// presence is determined by the latest timestamp associated with an add
/// or remove operation. It resolves conflicts between concurrent additions
/// and removals by choosing the operation with the highest timestamp.
///
/// # Key Properties
///
/// - **Add/Remove Sets**: Maintains separate sets for additions and removals, each with timestamps.
/// - **LWW Resolution**: An element is present if its addition timestamp is greater than its removal timestamp.
/// - **Bias**: Typically biased towards addition in case of timestamp ties (configurable, but usually Add-Wins).
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
/// use crdt_data_types::LWWSet;
///
/// let mut set = LWWSet::new();
/// set.insert("node_a", "apple".to_string(), 100);
/// set.remove("node_b", "apple".to_string(), 50); // Older removal
///
/// assert!(set.contains(&"apple".to_string())); // Addition wins (100 > 50)
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: DeserializeOwned + Eq + Hash + Ord"
))]
pub struct LWWSet<T: Eq + Hash + Ord> {
    /// Tracks addition timestamps: element -> (timestamp, node_id).
    #[serde(serialize_with = "serialize_lww_map", deserialize_with = "deserialize_lww_map")]
    pub add_set: Vec<(T, (u64, String))>,
    /// Tracks removal timestamps: element -> (timestamp, node_id).
    #[serde(serialize_with = "serialize_lww_map", deserialize_with = "deserialize_lww_map")]
    pub remove_set: Vec<(T, (u64, String))>,
    /// Vector clock representing the causal history of the set.
    #[serde(default)]
    pub vclock: VectorClock,
}

fn serialize_lww_map<S, T>(
    elements: &Vec<(T, (u64, String))>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: Serialize,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(elements.len()))?;
    for (k, v) in elements {
        map.serialize_entry(k, v)?;
    }
    map.end()
}

type LWWSetEntry<T> = (T, (u64, String));

fn deserialize_lww_map<'de, D, T>(deserializer: D) -> Result<Vec<LWWSetEntry<T>>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: DeserializeOwned + Eq + Hash + Ord,
{
    struct LWWMapVisitor<T>(std::marker::PhantomData<T>);

    impl<'de, T> serde::de::Visitor<'de> for LWWMapVisitor<T>
    where
        T: DeserializeOwned + Eq + Hash + Ord,
    {
        type Value = Vec<LWWSetEntry<T>>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a map of elements to timestamps")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>,
        {
            let mut elements: Vec<LWWSetEntry<T>> =
                Vec::with_capacity(access.size_hint().unwrap_or(0));
            while let Some((key, value)) = access.next_entry()? {
                elements.push((key, value));
            }
            // Sort to maintain invariant
            elements.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(elements)
        }
    }

    deserializer.deserialize_map(LWWMapVisitor(std::marker::PhantomData))
}

impl<T: Eq + Hash + Ord> Default for LWWSet<T> {
    fn default() -> Self {
        Self {
            add_set: Vec::new(),
            remove_set: Vec::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<T: Eq + Hash + Ord> LWWSet<T> {
    /// Creates a new, empty LWW-Set.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static> LWWSet<T> {
    /// Adds an element to the set with a specific timestamp.
    pub fn insert(&mut self, node_id: &str, element: T, timestamp: u64) {
        let node_id_str = node_id.to_string();
        match self.add_set.binary_search_by(|(e, _)| e.cmp(&element)) {
            Ok(idx) => {
                let (_, (ts, nid)) = &self.add_set[idx];
                if timestamp > *ts || (timestamp == *ts && node_id_str > *nid) {
                    self.add_set[idx] = (element, (timestamp, node_id_str));
                    self.vclock.increment(node_id);
                }
            }
            Err(idx) => {
                self.add_set.insert(idx, (element, (timestamp, node_id_str)));
                self.vclock.increment(node_id);
            }
        }
    }

    /// Removes an element from the set by adding a tombstone with a specific timestamp.
    pub fn remove(&mut self, node_id: &str, element: T, timestamp: u64) {
        let node_id_str = node_id.to_string();
        match self.remove_set.binary_search_by(|(e, _)| e.cmp(&element)) {
            Ok(idx) => {
                let (_, (ts, nid)) = &self.remove_set[idx];
                if timestamp > *ts || (timestamp == *ts && node_id_str > *nid) {
                    self.remove_set[idx] = (element, (timestamp, node_id_str));
                    self.vclock.increment(node_id);
                }
            }
            Err(idx) => {
                self.remove_set.insert(idx, (element, (timestamp, node_id_str)));
                self.vclock.increment(node_id);
            }
        }
    }

    /// Returns true if the set contains the specified element.
    ///
    /// An element is present if its latest add timestamp is strictly greater
    /// than its latest remove timestamp (or if no removal exists).
    pub fn contains(&self, element: &T) -> bool {
        let add_entry = self
            .add_set
            .binary_search_by(|(e, _)| e.cmp(element))
            .map(|idx| &self.add_set[idx].1)
            .ok();
        
        let remove_entry = self
            .remove_set
            .binary_search_by(|(e, _)| e.cmp(element))
            .map(|idx| &self.remove_set[idx].1)
            .ok();

        match (add_entry, remove_entry) {
            (Some((a_ts, a_id)), Some((r_ts, r_id))) => {
                *a_ts > *r_ts || (*a_ts == *r_ts && a_id > r_id)
            }
            (Some(_), None) => true,
            _ => false,
        }
    }

    /// Iterator over the elements currently in the set.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.add_set.iter().filter_map(move |(e, _)| {
            if self.contains(e) {
                Some(e)
            } else {
                None
            }
        })
    }

    /// Merges another LWW-Set into this one.
    pub fn merge(&mut self, other: &Self) {
        self.add_set = Self::merge_vecs(&self.add_set, &other.add_set);
        self.remove_set = Self::merge_vecs(&self.remove_set, &other.remove_set);
        self.vclock.merge(&other.vclock);
    }

    fn merge_vecs(
        left: &[(T, (u64, String))],
        right: &[(T, (u64, String))],
    ) -> Vec<(T, (u64, String))> {
        let mut result = Vec::with_capacity(left.len() + right.len());
        let mut i = 0;
        let mut j = 0;

        while i < left.len() && j < right.len() {
            let (k1, (ts1, id1)) = &left[i];
            let (k2, (ts2, id2)) = &right[j];

            match k1.cmp(k2) {
                Ordering::Less => {
                    result.push((k1.clone(), (*ts1, id1.clone())));
                    i += 1;
                }
                Ordering::Greater => {
                    result.push((k2.clone(), (*ts2, id2.clone())));
                    j += 1;
                }
                Ordering::Equal => {
                    // Both have the element, keep the one with higher timestamp/id
                    if *ts1 > *ts2 || (*ts1 == *ts2 && id1 > id2) {
                        result.push((k1.clone(), (*ts1, id1.clone())));
                    } else {
                        result.push((k2.clone(), (*ts2, id2.clone())));
                    }
                    i += 1;
                    j += 1;
                }
            }
        }

        while i < left.len() {
            result.push(left[i].clone());
            i += 1;
        }
        while j < right.len() {
            result.push(right[j].clone());
            j += 1;
        }

        result
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct LWWSetReader<'a, T: Eq + Hash + Ord> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static>
    LWWSetReader<'a, T>
{
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_set(&self) -> Result<LWWSet<T>, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let lww_set = reader
            .get_root::<lww_set_capnp::lww_set::Reader>()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let mut add_set = Vec::new();
        let adds = lww_set
            .get_add_set()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        for entry in adds {
            let entry: lww_set_capnp::lww_set::entry::Reader = entry;
            let element: T = bincode::deserialize(
                entry
                    .get_element()
                    .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?,
            )
            .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;
            add_set.push((
                element,
                (
                    entry.get_timestamp(),
                    entry
                        .get_node_id()
                        .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?
                        .to_string()
                        .map_err(|e: std::str::Utf8Error| {
                            CrdtError::Deserialization(e.to_string())
                        })?,
                ),
            ));
        }
        // Sort to maintain invariant
        add_set.sort_by(|a, b| a.0.cmp(&b.0));

        let mut remove_set = Vec::new();
        let removes = lww_set
            .get_remove_set()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        for entry in removes {
            let entry: lww_set_capnp::lww_set::entry::Reader = entry;
            let element: T = bincode::deserialize(
                entry
                    .get_element()
                    .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?,
            )
            .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;
            remove_set.push((
                element,
                (
                    entry.get_timestamp(),
                    entry
                        .get_node_id()
                        .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?
                        .to_string()
                        .map_err(|e: std::str::Utf8Error| {
                            CrdtError::Deserialization(e.to_string())
                        })?,
                ),
            ));
        }
        // Sort to maintain invariant
        remove_set.sort_by(|a, b| a.0.cmp(&b.0));

        let vclock = if lww_set.has_vclock() {
            let vc_bytes = lww_set
                .get_vclock()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                vc_bytes,
            )])?
        } else {
            VectorClock::new()
        };

        Ok(LWWSet {
            add_set,
            remove_set,
            vclock,
        })
    }
}

impl<'a, T> CrdtReader<'a> for LWWSetReader<'a, T>
where
    T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_set()?.add_set.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static> Crdt
    for LWWSet<T>
{
    type Reader<'a> = LWWSetReader<'a, T>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = LWWSet::new();
        for reader in readers {
            result.merge(&reader.to_set()?);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut lww_set = message.init_root::<lww_set_capnp::lww_set::Builder>();

            let mut adds = lww_set.reborrow().init_add_set(self.add_set.len() as u32);
            for (idx, (element, (timestamp, node_id))) in self.add_set.iter().enumerate() {
                let mut entry = adds.reborrow().get(idx as u32);
                let bytes = bincode::serialize(element).expect("LWWSet element serialization fail");
                entry.set_element(&bytes);
                entry.set_timestamp(*timestamp);
                entry.set_node_id(node_id.as_str().into());
            }

            let mut removes = lww_set
                .reborrow()
                .init_remove_set(self.remove_set.len() as u32);
            for (idx, (element, (timestamp, node_id))) in self.remove_set.iter().enumerate() {
                let mut entry = removes.reborrow().get(idx as u32);
                let bytes = bincode::serialize(element).expect("LWWSet element serialization fail");
                entry.set_element(&bytes);
                entry.set_timestamp(*timestamp);
                entry.set_node_id(node_id.as_str().into());
            }

            let vclock_bytes = self.vclock.to_capnp_bytes();
            lww_set.set_vclock(&vclock_bytes);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("LWWSet serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.add_set.is_empty()
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
