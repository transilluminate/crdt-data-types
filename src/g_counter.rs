use crate::gcounter_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// G-Counter: A Grow-only Counter CRDT.
///
/// A G-Counter is a monotonically increasing counter that supports concurrent
/// increments across multiple nodes. Each node maintains its own counter value,
/// and the total count is the sum of all node-specific counters.
///
/// # Algebraic Properties
/// - **Monotonicity**: The value can only increase.
/// - **Commutativity**: Merge order does not affect the final sum.
/// - **Idempotence**: Merging the same state multiple times has no effect on the sum.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct GCounter {
    /// Map of node_id -> increment count for that node.
    pub counters: HashMap<String, i64>,
    /// Vector clock for causal ordering and tracking updates.
    pub vclock: VectorClock,
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
        *self.counters.entry(node_id.to_string()).or_insert(0) += delta;
        self.vclock.increment(node_id);
    }

    /// Returns the total aggregated value of the counter.
    pub fn value(&self) -> i64 {
        self.counters.values().sum()
    }

    /// Merges another G-Counter into this one.
    ///
    /// This implementation takes the maximum of each node's internal counter,
    /// which ensures that the counter only ever grows and correctly merges
    /// concurrent increments.
    pub fn merge(&mut self, other: &Self) {
        for (node_id, &count) in &other.counters {
            let entry = self.counters.entry(node_id.clone()).or_insert(0);
            *entry = (*entry).max(count);
        }
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

        let mut counters = HashMap::new();
        let entries = gcounter
            .get_entries()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        for entry in entries {
            let node_id = entry
                .get_node_id()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?
                .to_string()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            counters.insert(node_id, entry.get_count());
        }

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
                let existing = result.counters.entry(node_id.to_string()).or_insert(0);
                *existing = (*existing).max(count);
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
