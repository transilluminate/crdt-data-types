use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vclock_capnp;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A Hybrid Vector Clock for causal ordering and stable temporal queries.
///
/// This implementation combines traditional logical vector clocks with
/// wall-clock timestamps. It enables both:
/// 1. **Causal ordering**: Determining if one state "happened-before" another.
/// 2. **Temporal stability**: Identifying when data has not been modified for a duration,
///    which is essential for safe compaction and tombstone removal.
///
/// # Causal Properties
/// - If `vc1 < vc2`, then `vc1` causally precedes `vc2`.
/// - If `vc1` and `vc2` are incomparable, they are concurrent.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct VectorClock {
    /// Map of node_id -> (logical_counter, epoch_seconds)
    pub clocks: HashMap<String, (u64, u64)>,
}

impl Hash for VectorClock {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        let mut sorted: Vec<_> = self.clocks.iter().collect();
        sorted.sort_by_key(|(node, _)| node.as_str());
        for (node, (counter, ts)) in sorted {
            node.hash(state);
            counter.hash(state);
            ts.hash(state);
        }
    }
}

impl VectorClock {
    /// Returns a new, empty vector clock.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the clock for a specific node and updates its timestamp.
    pub fn increment(&mut self, node_id: &str) -> (u64, u64) {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let entry = self
            .clocks
            .entry(node_id.to_string())
            .and_modify(|(counter, timestamp)| {
                *counter += 1;
                *timestamp = now;
            })
            .or_insert((1, now));
        *entry
    }

    /// Merges another vector clock into this one, keeping the maximum values.
    pub fn merge(&mut self, other: &Self) {
        for (node_id, &(other_counter, other_timestamp)) in &other.clocks {
            self.clocks
                .entry(node_id.clone())
                .and_modify(|(counter, timestamp)| {
                    *counter = (*counter).max(other_counter);
                    *timestamp = (*timestamp).max(other_timestamp);
                })
                .or_insert((other_counter, other_timestamp));
        }
    }

    /// Returns true if this vector clock causally precedes another.
    pub fn happens_before(&self, other: &VectorClock) -> bool {
        let mut strictly_less = false;
        let mut all_nodes: Vec<_> = self.clocks.keys().collect();
        for node in other.clocks.keys() {
            if !self.clocks.contains_key(node) {
                all_nodes.push(node);
            }
        }

        for node_id in all_nodes {
            let self_val = self.clocks.get(node_id).map(|(c, _)| *c).unwrap_or(0);
            let other_val = other.clocks.get(node_id).map(|(c, _)| *c).unwrap_or(0);

            if self_val > other_val {
                return false;
            }
            if self_val < other_val {
                strictly_less = true;
            }
        }
        strictly_less
    }

    /// Checks for temporal stability across all tracked nodes.
    pub fn is_stable_for(&self, duration: Duration) -> bool {
        if self.clocks.is_empty() {
            return false;
        }
        let cutoff = SystemTime::now()
            .checked_sub(duration)
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs())
            .unwrap_or(0);

        self.clocks.values().all(|(_, ts)| *ts < cutoff)
    }

    pub fn merge_reader(&mut self, reader: &VectorClockReader) -> Result<(), CrdtError> {
        let msg_reader = serialize::read_message(reader.bytes, ReaderOptions::new())
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        let vclock = msg_reader
            .get_root::<vclock_capnp::vector_clock::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let entries = vclock
            .get_entries()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        for entry in entries {
            let node_id = entry
                .get_node_id()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?
                .to_str()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let other_counter = entry.get_logical_counter();
            let other_timestamp = entry.get_epoch_seconds();

            self.clocks
                .entry(node_id.to_string())
                .and_modify(|(counter, timestamp)| {
                    *counter = (*counter).max(other_counter);
                    *timestamp = (*timestamp).max(other_timestamp);
                })
                .or_insert((other_counter, other_timestamp));
        }
        Ok(())
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct VectorClockReader<'a> {
    bytes: &'a [u8],
}

impl<'a> VectorClockReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn to_vclock(&self) -> Result<VectorClock, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        let vclock = reader
            .get_root::<vclock_capnp::vector_clock::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let mut clocks = HashMap::new();
        let entries = vclock
            .get_entries()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        for entry in entries {
            let node_id = entry
                .get_node_id()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?
                .to_string()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            clocks.insert(
                node_id,
                (entry.get_logical_counter(), entry.get_epoch_seconds()),
            );
        }
        Ok(VectorClock { clocks })
    }
}

impl<'a> CrdtReader<'a> for VectorClockReader<'a> {
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_vclock()?.clocks.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl Crdt for VectorClock {
    type Reader<'a> = VectorClockReader<'a>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = VectorClock::new();
        for reader in readers {
            result.merge(&reader.to_vclock()?);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut vclock = message.init_root::<vclock_capnp::vector_clock::Builder>();
            let mut entries = vclock.reborrow().init_entries(self.clocks.len() as u32);
            for (idx, (node_id, (counter, ts))) in self.clocks.iter().enumerate() {
                let mut entry = entries.reborrow().get(idx as u32);
                entry.set_node_id(node_id.as_str().into());
                entry.set_logical_counter(*counter);
                entry.set_epoch_seconds(*ts);
            }
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("VectorClock serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.clocks.is_empty()
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
