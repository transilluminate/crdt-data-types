use crate::lww_set_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::HashMap;
use std::hash::Hash;

/// LWW-Set: A Last-Write-Wins Set CRDT.
///
/// An LWW-Set (Last-Write-Wins Set) stores elements where each element's
/// presence is determined by the latest timestamp associated with an add
/// or remove operation. It resolves conflicts between concurrent additions
/// and removals by choosing the operation with the highest timestamp.
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
pub struct LWWSet<T: Eq + Hash> {
    /// Tracks addition timestamps: element -> (timestamp, node_id).
    pub add_set: HashMap<T, (u64, String)>,
    /// Tracks removal timestamps: element -> (timestamp, node_id).
    pub remove_set: HashMap<T, (u64, String)>,
    /// Vector clock representing the causal history of the set.
    pub vclock: VectorClock,
}

impl<T: Eq + Hash> Default for LWWSet<T> {
    fn default() -> Self {
        Self {
            add_set: HashMap::new(),
            remove_set: HashMap::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<T: Eq + Hash> LWWSet<T> {
    /// Creates a new, empty LWW-Set.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static> LWWSet<T> {
    /// Adds an element to the set with a specific timestamp.
    pub fn insert(&mut self, node_id: &str, element: T, timestamp: u64) {
        let node_id_str = node_id.to_string();
        let update = match self.add_set.get(&element) {
            Some((ts, nid)) => {
                timestamp > *ts
                    || (timestamp == *ts && node_id_str > *nid)
                    || (timestamp == *ts
                        && node_id_str == *nid
                        && bincode::serialize(&element).unwrap_or_default()
                            > bincode::serialize(&element).unwrap_or_default()) // Wait, same element?
            }
            None => true,
        };
        if update {
            self.add_set.insert(element, (timestamp, node_id_str));
            self.vclock.increment(&node_id);
        }
    }

    /// Removes an element from the set by adding a tombstone with a specific timestamp.
    pub fn remove(&mut self, node_id: &str, element: T, timestamp: u64) {
        let node_id_str = node_id.to_string();
        let update = match self.remove_set.get(&element) {
            Some((ts, nid)) => {
                timestamp > *ts
                    || (timestamp == *ts && node_id_str > *nid)
                    || (timestamp == *ts
                        && node_id_str == *nid
                        && bincode::serialize(&element).unwrap_or_default()
                            > bincode::serialize(&element).unwrap_or_default())
            }
            None => true,
        };
        if update {
            self.remove_set.insert(element, (timestamp, node_id_str));
            self.vclock.increment(&node_id);
        }
    }

    /// Returns true if the set contains the specified element.
    ///
    /// An element is present if its latest add timestamp is strictly greater
    /// than its latest remove timestamp (or if no removal exists).
    pub fn contains(&self, element: &T) -> bool {
        match (self.add_set.get(element), self.remove_set.get(element)) {
            (Some((a_ts, a_id)), Some((r_ts, r_id))) => {
                a_ts > r_ts || (a_ts == r_ts && a_id > r_id)
            }
            (Some(_), None) => true,
            _ => false,
        }
    }

    /// Iterator over the elements currently in the set.
    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.add_set.keys().filter(move |e| self.contains(e))
    }

    /// Merges another LWW-Set into this one.
    pub fn merge(&mut self, other: &Self) {
        for (element, (timestamp, node_id)) in &other.add_set {
            let update = match self.add_set.get(element) {
                Some((ts, nid)) => *timestamp > *ts || (*timestamp == *ts && node_id > nid),
                None => true,
            };
            if update {
                self.add_set
                    .insert(element.clone(), (*timestamp, node_id.clone()));
            }
        }
        for (element, (timestamp, node_id)) in &other.remove_set {
            let update = match self.remove_set.get(element) {
                Some((ts, nid)) => *timestamp > *ts || (*timestamp == *ts && node_id > nid),
                None => true,
            };
            if update {
                self.remove_set
                    .insert(element.clone(), (*timestamp, node_id.clone()));
            }
        }
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct LWWSetReader<'a, T: Eq + Hash> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static>
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

        let mut add_set = HashMap::new();
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
            add_set.insert(
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
            );
        }

        let mut remove_set = HashMap::new();
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
            remove_set.insert(
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
            );
        }

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
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_set()?.add_set.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static> Crdt
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
