use crate::g_counter::GCounter;
use crate::pncounter_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};

/// PN-Counter: A Positive-Negative Counter CRDT.
///
/// A PN-Counter allows both increments and decrements by maintaining two internal
/// G-Counters: one for increments (positive) and one for decrements (negative).
/// The total value is the difference between the positive and negative counts.
///
/// # Algebraic Properties
/// - **Commutativity**: Merge order does not affect the final value.
/// - **Idempotence**: Merging the same state multiple times result in the same value.
/// - **Convergence**: All replicas eventually reach the same value given the same set of operations.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct PNCounter {
    /// Internal G-Counter for positive increments.
    pub positive: GCounter,
    /// Internal G-Counter for negative decrements.
    pub negative: GCounter,
    /// Vector clock for tracking causal history.
    pub vclock: VectorClock,
}

impl PNCounter {
    /// Creates a new, empty PN-Counter.
    pub fn new() -> Self {
        Self::default()
    }

    /// Increments the counter for a specific node.
    pub fn increment(&mut self, node_id: &str, delta: i64) {
        if delta < 0 {
            self.decrement(node_id, -delta);
            return;
        }
        self.positive.increment(node_id, delta);
        self.vclock.increment(node_id);
    }

    /// Decrements the counter for a specific node.
    pub fn decrement(&mut self, node_id: &str, delta: i64) {
        if delta < 0 {
            self.increment(node_id, -delta);
            return;
        }
        self.negative.increment(node_id, delta);
        self.vclock.increment(node_id);
    }

    /// Returns the current aggregated value (positive sum - negative sum).
    pub fn value(&self) -> i64 {
        self.positive.value() - self.negative.value()
    }

    /// Merges another PN-Counter into this one.
    pub fn merge(&mut self, other: &Self) {
        self.positive.merge(&other.positive);
        self.negative.merge(&other.negative);
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct PNCounterReader<'a> {
    bytes: &'a [u8],
}

impl<'a> PNCounterReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    fn to_pncounter(&self) -> Result<PNCounter, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let pn_counter = reader
            .get_root::<pncounter_capnp::pn_counter::Reader>()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let positive_bytes = pn_counter
            .get_positive()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let negative_bytes = pn_counter
            .get_negative()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let positive =
            GCounter::merge_from_readers(&[crate::g_counter::GCounterReader::new(positive_bytes)])?;
        let negative =
            GCounter::merge_from_readers(&[crate::g_counter::GCounterReader::new(negative_bytes)])?;

        let vclock = if pn_counter.has_vclock() {
            let vc_bytes = pn_counter
                .get_vclock()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                vc_bytes,
            )])?
        } else {
            VectorClock::new()
        };

        Ok(PNCounter {
            positive,
            negative,
            vclock,
        })
    }
}

impl<'a> CrdtReader<'a> for PNCounterReader<'a> {
    fn is_empty(&self) -> Result<bool, CrdtError> {
        let pn = self.to_pncounter()?;
        Ok(pn.positive.is_empty() && pn.negative.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl Crdt for PNCounter {
    type Reader<'a> = PNCounterReader<'a>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = PNCounter::new();
        for reader in readers {
            result.merge(&reader.to_pncounter()?);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut pn_counter = message.init_root::<pncounter_capnp::pn_counter::Builder>();
            pn_counter.set_positive(&self.positive.to_capnp_bytes());
            pn_counter.set_negative(&self.negative.to_capnp_bytes());
            pn_counter.set_vclock(&self.vclock.to_capnp_bytes());
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("PNCounter serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.positive.is_empty() && self.negative.is_empty()
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
