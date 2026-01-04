// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crate::gcounter_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};

/// G-Counter: A Grow-only Counter CRDT.
///
/// A G-Counter is a monotonically increasing counter that supports concurrent
/// increments across multiple nodes. Each node maintains its own counter value,
/// and the total count is the sum of all node-specific counters.
///
/// # Key Properties
///
/// - **Grow-only**: The counter can only increase. Decrements are not supported.
/// - **Distributed**: Multiple replicas can increment independently.
/// - **Mergeable**: Merging takes the maximum value for each node ID.
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
/// use crdt_data_types::GCounter;
///
/// let mut gc1 = GCounter::new();
/// gc1.increment("node_a", 10);
///
/// let mut gc2 = GCounter::new();
/// gc2.increment("node_b", 20);
///
/// gc1.merge(&gc2);
/// assert_eq!(gc1.value(), 30);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GCounter {
    /// List of (node_id, increment count) pairs, sorted by node_id.
    #[serde(serialize_with = "serialize_counters", deserialize_with = "deserialize_counters")]
    pub counters: Vec<(String, i64)>,
    /// Vector clock for causal ordering and tracking updates.
    #[serde(default)]
    pub vclock: VectorClock,
}

fn serialize_counters<S>(counters: &Vec<(String, i64)>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    use serde::ser::SerializeMap;
    let mut map = serializer.serialize_map(Some(counters.len()))?;
    for (k, v) in counters {
        map.serialize_entry(k, v)?;
    }
    map.end()
}

fn deserialize_counters<'de, D>(deserializer: D) -> Result<Vec<(String, i64)>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    struct CountersVisitor;

    impl<'de> serde::de::Visitor<'de> for CountersVisitor {
        type Value = Vec<(String, i64)>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a map of counters")
        }

        fn visit_map<M>(self, mut access: M) -> Result<Self::Value, M::Error>
        where
            M: serde::de::MapAccess<'de>,
        {
            let mut counters: Vec<(String, i64)> = Vec::with_capacity(access.size_hint().unwrap_or(0));
            while let Some((key, value)) = access.next_entry()? {
                counters.push((key, value));
            }
            // Sort to maintain invariant
            counters.sort_by(|a, b| a.0.cmp(&b.0));
            Ok(counters)
        }
    }

    deserializer.deserialize_map(CountersVisitor)
}

impl GCounter {
    /// Creates a new, empty G-Counter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the counter for a specific node by a given delta.
    ///
    /// # Arguments
    /// * `node_id` - The unique identifier of the node performing the increment.
    /// * `delta` - The amount to increment by. Must be non-negative for standard G-Counter semantics.
    pub fn increment(&mut self, node_id: &str, delta: i64) {
        if delta < 0 {
            // Logically, a G-Counter only grows.
            return;
        }
        
        match self.counters.binary_search_by(|(k, _)| k.as_str().cmp(node_id)) {
            Ok(idx) => {
                self.counters[idx].1 += delta;
            }
            Err(idx) => {
                self.counters.insert(idx, (node_id.to_string(), delta));
            }
        }
        self.vclock.increment(node_id);
    }

    /// Returns the total aggregated value of the counter.
    pub fn value(&self) -> i64 {
        self.counters.iter().map(|(_, v)| v).sum()
    }

    /// Merges another G-Counter into this one.
    ///
    /// This implementation uses a linear scan merge of the sorted vectors,
    /// which is significantly faster and more cache-friendly than HashMap merging.
    pub fn merge(&mut self, other: &Self) {
        let mut new_counters = Vec::with_capacity(self.counters.len() + other.counters.len());
        let mut i = 0;
        let mut j = 0;

        while i < self.counters.len() && j < other.counters.len() {
            let (k1, v1) = &self.counters[i];
            let (k2, v2) = &other.counters[j];

            match k1.cmp(k2) {
                std::cmp::Ordering::Less => {
                    new_counters.push((k1.clone(), *v1));
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    new_counters.push((k2.clone(), *v2));
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    new_counters.push((k1.clone(), (*v1).max(*v2)));
                    i += 1;
                    j += 1;
                }
            }
        }

        if i < self.counters.len() {
            new_counters.extend_from_slice(&self.counters[i..]);
        }
        if j < other.counters.len() {
            new_counters.extend_from_slice(&other.counters[j..]);
        }

        self.counters = new_counters;
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct GCounterReader<'a> {
    bytes: &'a [u8],
}

impl<'a> GCounterReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn to_gcounter(&self) -> Result<GCounter, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        let gcounter = reader
            .get_root::<gcounter_capnp::g_counter::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let mut counters = Vec::new();
        let entries = gcounter
            .get_entries()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        for entry in entries {
            let node_id = entry
                .get_node_id()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?
                .to_string()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            counters.push((node_id, entry.get_count()));
        }
        
        // Ensure sorted order as Cap'n Proto doesn't guarantee it
        counters.sort_by(|a, b| a.0.cmp(&b.0));

        // Deserialize vclock if present
        let vclock = if gcounter.has_vclock() {
            let vc_bytes = gcounter
                .get_vclock()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                vc_bytes,
            )])?
        } else {
            VectorClock::new()
        };

        Ok(GCounter { counters, vclock })
    }
}

impl<'a> CrdtReader<'a> for GCounterReader<'a> {
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_gcounter()?.counters.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl Crdt for GCounter {
    type Reader<'a> = GCounterReader<'a>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = GCounter::new();
        
        // We'll collect all entries and then sort/merge them.
        // A more optimized version could do a k-way merge if we trusted the inputs were sorted.
        let mut all_entries = Vec::new();
        
        for reader in readers {
            let msg_reader = serialize::read_message(reader.bytes, ReaderOptions::new())
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let gcounter = msg_reader
                .get_root::<gcounter_capnp::g_counter::Reader>()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

            let entries = gcounter
                .get_entries()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

            for entry in entries {
                let node_id = entry
                    .get_node_id()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?
                    .to_str()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let count = entry.get_count();
                all_entries.push((node_id.to_string(), count));
            }

            if gcounter.has_vclock() {
                let vc_bytes = gcounter
                    .get_vclock()
                    .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
                result
                    .vclock
                    .merge_reader(&crate::vector_clock::VectorClockReader::new(vc_bytes))?;
            }
        }
        
        // Sort by node_id to prepare for merging
        all_entries.sort_by(|a, b| a.0.cmp(&b.0));
        
        // Merge duplicates by taking the max
        if !all_entries.is_empty() {
            let mut current_node = all_entries[0].0.clone();
            let mut current_max = all_entries[0].1;
            
            for (node_id, count) in all_entries.into_iter().skip(1) {
                if node_id == current_node {
                    current_max = current_max.max(count);
                } else {
                    result.counters.push((current_node, current_max));
                    current_node = node_id;
                    current_max = count;
                }
            }
            result.counters.push((current_node, current_max));
        }
        
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut gcounter = message.init_root::<gcounter_capnp::g_counter::Builder>();
            let mut entries = gcounter.reborrow().init_entries(self.counters.len() as u32);
            for (idx, (node_id, count)) in self.counters.iter().enumerate() {
                let mut entry = entries.reborrow().get(idx as u32);
                entry.set_node_id(node_id.as_str().into());
                entry.set_count(*count);
            }
            let vclock_bytes = self.vclock.to_capnp_bytes();
            gcounter.set_vclock(&vclock_bytes);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("GCounter serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.counters.is_empty()
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}

