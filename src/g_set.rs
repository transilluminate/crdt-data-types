use crate::gset_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashSet;
use std::hash::Hash;

/// G-Set: A Grow-only Set CRDT.
///
/// A G-Set is a set that only supports element addition. Elements cannot be
/// removed once added. This leads to simple merge semantics (set union).
///
/// # Algebraic Properties
/// - **Monotonicity**: The set only grows; elements are never removed.
/// - **Commutativity**: The order in which elements are added or sets are merged
///   does not affect the final set contents.
/// - **Idempotence**: Adding the same element multiple times or merging the
///   same set multiple times does not change the state.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize",
    deserialize = "T: DeserializeOwned + Eq + Hash"
))]
pub struct GSet<T: Eq + Hash> {
    /// Internal storage for set elements.
    pub elements: HashSet<T>,
    /// Vector clock for tracking causal history.
    pub vclock: VectorClock,
}

impl<T: Eq + Hash> Default for GSet<T> {
    fn default() -> Self {
        Self {
            elements: HashSet::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<T: Eq + Hash> GSet<T> {
    /// Creates a new, empty G-Set.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static> GSet<T> {
    /// Adds an element to the set.
    ///
    /// # Arguments
    /// * `node_id` - The identifier of the node performing the addition.
    /// * `element` - The element to add.
    pub fn insert(&mut self, node_id: &str, element: T) {
        if self.elements.insert(element) {
            self.vclock.increment(node_id);
        }
    }

    /// Returns true if the set contains the specified element.
    pub fn contains(&self, element: &T) -> bool {
        self.elements.contains(element)
    }

    /// Returns an iterator over the elements in the set.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.elements.iter()
    }

    /// Returns the number of elements in the set.
    pub fn len(&self) -> usize {
        self.elements.len()
    }

    /// Merges another G-Set into this one using set union.
    pub fn merge(&mut self, other: &Self) {
        for element in &other.elements {
            self.elements.insert(element.clone());
        }
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct GSetReader<'a, T: Eq + Hash> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static>
    GSetReader<'a, T>
{
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_gset(&self) -> Result<GSet<T>, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let gset = reader
            .get_root::<gset_capnp::g_set::Reader>()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let mut elements = HashSet::new();
        let items = gset
            .get_elements()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        for item in items {
            let item_bytes: &[u8] =
                item.map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            let element: T = bincode::deserialize(item_bytes)
                .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;
            elements.insert(element);
        }

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

impl<'a, T: Eq + Hash> CrdtReader<'a> for GSetReader<'a, T>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_gset()?.elements.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<T: Eq + Hash> Crdt for GSet<T>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    type Reader<'a> = GSetReader<'a, T>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = GSet::new();
        for reader in readers {
            result.merge(&reader.to_gset()?);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut g_set = message.init_root::<gset_capnp::g_set::Builder>();
            let mut elements = g_set.reborrow().init_elements(self.elements.len() as u32);
            for (idx, item) in self.elements.iter().enumerate() {
                let bytes = bincode::serialize(item).expect("GSet item serialization fail");
                elements.set(idx as u32, &bytes);
            }
            let vclock_bytes = self.vclock.to_capnp_bytes();
            g_set.set_vclock(&vclock_bytes);
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
