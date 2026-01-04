// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crate::gset_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::Hash;

/// G-Set: A Grow-only Set CRDT.
///
/// A G-Set is a set that only supports element addition. Elements cannot be
/// removed once added. This leads to simple merge semantics (set union).
///
/// # Key Properties
///
/// - **Grow-only**: Elements can be added but never removed.
/// - **Merge Strategy**: Set union.
/// - **Simplicity**: Very low overhead and simple implementation.
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
/// use crdt_data_types::GSet;
///
/// let mut set1 = GSet::new();
/// set1.insert("node_a", "apple".to_string());
///
/// let mut set2 = GSet::new();
/// set2.insert("node_b", "banana".to_string());
///
/// set1.merge(&set2);
/// assert!(set1.contains(&"apple".to_string()));
/// assert!(set1.contains(&"banana".to_string()));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: DeserializeOwned + Eq + Hash + Ord"
))]
pub struct GSet<T: Eq + Hash + Ord> {
    /// Internal storage for set elements.
    #[serde(serialize_with = "serialize_elements", deserialize_with = "deserialize_elements")]
    pub elements: Vec<T>,
    /// Vector clock for tracking causal history.
    #[serde(default)]
    pub vclock: VectorClock,
}

fn serialize_elements<S, T>(elements: &Vec<T>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
    T: Serialize,
{
    use serde::ser::SerializeSeq;
    let mut seq = serializer.serialize_seq(Some(elements.len()))?;
    for e in elements {
        seq.serialize_element(e)?;
    }
    seq.end()
}

fn deserialize_elements<'de, D, T>(deserializer: D) -> Result<Vec<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: DeserializeOwned + Eq + Hash + Ord,
{
    struct ElementsVisitor<T>(std::marker::PhantomData<T>);

    impl<'de, T> serde::de::Visitor<'de> for ElementsVisitor<T>
    where
        T: DeserializeOwned + Eq + Hash + Ord,
    {
        type Value = Vec<T>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a sequence of elements")
        }

        fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
        where
            A: serde::de::SeqAccess<'de>,
        {
            let mut elements = Vec::with_capacity(seq.size_hint().unwrap_or(0));
            while let Some(elem) = seq.next_element()? {
                elements.push(elem);
            }
            elements.sort();
            elements.dedup(); // Ensure uniqueness just in case
            Ok(elements)
        }
    }

    deserializer.deserialize_seq(ElementsVisitor(std::marker::PhantomData))
}

impl<T: Eq + Hash + Ord> Default for GSet<T> {
    fn default() -> Self {
        Self {
            elements: Vec::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<T: Eq + Hash + Ord> GSet<T> {
    /// Creates a new, empty G-Set.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static> GSet<T> {
    /// Adds an element to the set.
    pub fn insert(&mut self, node_id: &str, element: T) {
        if let Err(idx) = self.elements.binary_search(&element) {
            self.elements.insert(idx, element);
            self.vclock.increment(node_id);
        }
    }

    /// Returns true if the set contains the element.
    pub fn contains(&self, element: &T) -> bool {
        self.elements.binary_search(element).is_ok()
    }

    /// Iterator over the elements in the set.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.elements.iter()
    }

    /// Merges another G-Set into this one.
    pub fn merge(&mut self, other: &Self) {
        let mut result = Vec::with_capacity(self.elements.len() + other.elements.len());
        let mut i = 0;
        let mut j = 0;

        while i < self.elements.len() && j < other.elements.len() {
            match self.elements[i].cmp(&other.elements[j]) {
                Ordering::Less => {
                    result.push(self.elements[i].clone());
                    i += 1;
                }
                Ordering::Greater => {
                    result.push(other.elements[j].clone());
                    j += 1;
                }
                Ordering::Equal => {
                    result.push(self.elements[i].clone());
                    i += 1;
                    j += 1;
                }
            }
        }

        result.extend_from_slice(&self.elements[i..]);
        result.extend_from_slice(&other.elements[j..]);

        self.elements = result;
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct GSetReader<'a, T: Eq + Hash + Ord> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static>
    GSetReader<'a, T>
{
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_set(&self) -> Result<GSet<T>, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let gset = reader
            .get_root::<gset_capnp::g_set::Reader>()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let mut elements = Vec::new();
        let elements_list = gset
            .get_elements()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        for entry in elements_list {
            let bytes = entry.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let element: T = bincode::deserialize(bytes)
                .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;
            elements.push(element);
        }
        elements.sort();
        elements.dedup();

        let vclock = if gset.has_vclock() {
            let vc_bytes = gset
                .get_vclock()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                vc_bytes,
            )])?
        } else {
            VectorClock::new()
        };

        Ok(GSet { elements, vclock })
    }
}

impl<'a, T> CrdtReader<'a> for GSetReader<'a, T>
where
    T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_set()?.elements.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<T: Clone + Eq + Hash + Ord + Serialize + DeserializeOwned + Send + Sync + 'static> Crdt
    for GSet<T>
{
    type Reader<'a> = GSetReader<'a, T>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = GSet::new();
        for reader in readers {
            result.merge(&reader.to_set()?);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut gset = message.init_root::<gset_capnp::g_set::Builder>();
            let mut elements = gset.reborrow().init_elements(self.elements.len() as u32);
            for (idx, element) in self.elements.iter().enumerate() {
                let bytes = bincode::serialize(element).expect("GSet element serialization fail");
                elements.set(idx as u32, &bytes);
            }
            let vclock_bytes = self.vclock.to_capnp_bytes();
            gset.set_vclock(&vclock_bytes);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("GSet serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.elements.is_empty()
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
