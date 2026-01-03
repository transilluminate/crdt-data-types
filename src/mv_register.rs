// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crate::mv_register_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::hash::Hash;

/// MV-Register: A Multi-Value Register CRDT.
///
/// An MV-Register (Multi-Value Register) retains all concurrently written values.
/// When multiple replicas write to the register without causal knowledge of each other,
/// all values are kept. Conflicts are resolved only when a new write causally dominates
/// the previous ones.
///
/// # Key Properties
///
/// - **Multi-Value**: Can hold multiple values simultaneously if they are concurrent.
/// - **Causal History**: Uses vector clocks to track causality and determine which values are obsolete.
/// - **Conflict Resolution**: Client-side resolution (the client sees all concurrent values and must decide).
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
/// use crdt_data_types::MVRegister;
///
/// let mut reg1 = MVRegister::new();
/// reg1.set("node_a", "value1".to_string());
///
/// let mut reg2 = MVRegister::new();
/// reg2.set("node_b", "value2".to_string());
///
/// reg1.merge(&reg2);
/// let values: Vec<&String> = reg1.entries.keys().collect();
/// assert!(values.contains(&&"value1".to_string()));
/// assert!(values.contains(&&"value2".to_string()));
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: Serialize + Eq + Hash",
    deserialize = "T: DeserializeOwned + Eq + Hash"
))]
pub struct MVRegister<T: Eq + Hash> {
    /// Each value is associated with one or more observation IDs (node_id, counter).
    pub entries: HashMap<T, HashSet<(String, u64)>>,
    /// Vector clock representing the cumulative causal history.
    pub vclock: VectorClock,
}

impl<T: Eq + Hash> Default for MVRegister<T> {
    fn default() -> Self {
        Self {
            entries: HashMap::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<T: Eq + Hash> MVRegister<T> {
    /// Creates a new, empty MV-Register.
    pub fn new() -> Self {
        Self::default()
    }
}

impl<T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static> MVRegister<T> {
    /// Sets the value of the register, overshadowing all current versions.
    pub fn set(&mut self, node_id: &str, value: T) {
        // Increment the clock for this node
        let (counter, _) = self.vclock.increment(node_id);

        // Causal overshadowing: all current versions are now "in the past"
        // relative to this new write. We clear them.
        self.entries.clear();

        // Add the new version with its unique observation ID (dot)
        let mut ids = HashSet::new();
        ids.insert((node_id.to_string(), counter));
        self.entries.insert(value, ids);
    }

    /// Returns the current versions held in the register.
    pub fn versions(&self) -> HashSet<T> {
        self.entries.keys().cloned().collect()
    }

    /// Merges another MV-Register into this one.
    pub fn merge(&mut self, other: &Self) {
        let mut new_entries = HashMap::new();

        let all_values: HashSet<_> = self
            .entries
            .keys()
            .chain(other.entries.keys())
            .cloned()
            .collect();

        for val in all_values {
            let mut merged_ids = HashSet::new();

            if let Some(ids) = self.entries.get(&val) {
                for id in ids {
                    let other_version =
                        other.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                    let in_other = other
                        .entries
                        .get(&val)
                        .map(|other_ids| other_ids.contains(id))
                        .unwrap_or(false);

                    if id.1 > other_version || in_other {
                        merged_ids.insert(id.clone());
                    }
                }
            }

            if let Some(ids) = other.entries.get(&val) {
                for id in ids {
                    let self_version = self.vclock.clocks.get(&id.0).map(|(c, _)| *c).unwrap_or(0);
                    let in_self = self
                        .entries
                        .get(&val)
                        .map(|self_ids| self_ids.contains(id))
                        .unwrap_or(false);

                    if id.1 > self_version || in_self {
                        merged_ids.insert(id.clone());
                    }
                }
            }

            if !merged_ids.is_empty() {
                new_entries.insert(val, merged_ids);
            }
        }

        self.entries = new_entries;
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct MVRegisterReader<'a, T: Eq + Hash> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T> MVRegisterReader<'a, T>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_register(&self) -> Result<MVRegister<T>, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let reg_reader = reader
            .get_root::<mv_register_capnp::mv_register::Reader>()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let mut entries = HashMap::new();
        let entry_list = reg_reader
            .get_entries()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        for entry in entry_list {
            let val_bytes = entry
                .get_value()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            let value: T = bincode::deserialize(val_bytes)
                .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;

            let node_id = entry
                .get_node_id()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?
                .to_str()
                .map_err(|e: std::str::Utf8Error| CrdtError::Deserialization(e.to_string()))?;
            let counter = entry.get_counter();

            entries
                .entry(value)
                .or_insert_with(HashSet::new)
                .insert((node_id.to_string(), counter));
        }

        let vclock = if reg_reader.has_vclock() {
            let vc_bytes = reg_reader
                .get_vclock()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                vc_bytes,
            )])?
        } else {
            VectorClock::new()
        };

        Ok(MVRegister { entries, vclock })
    }
}

impl<'a, T> CrdtReader<'a> for MVRegisterReader<'a, T>
where
    T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static,
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_register()?.entries.is_empty())
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<T: Clone + Eq + Hash + Serialize + DeserializeOwned + Send + Sync + 'static> Crdt
    for MVRegister<T>
{
    type Reader<'a> = MVRegisterReader<'a, T>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut result = MVRegister::new();
        for reader in readers {
            result.merge(&reader.to_register()?);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut reg = message.init_root::<mv_register_capnp::mv_register::Builder>();

            let num_dots: usize = self.entries.values().map(|ids| ids.len()).sum();
            let mut entries = reg.reborrow().init_entries(num_dots as u32);

            let mut idx = 0;
            for (val, dots) in &self.entries {
                let val_bytes =
                    bincode::serialize(val).expect("MVRegister value serialization fail");
                for (node_id, counter) in dots {
                    let mut entry = entries.reborrow().get(idx);
                    entry.set_value(&val_bytes);
                    entry.set_node_id(node_id.as_str().into());
                    entry.set_counter(*counter);
                    idx += 1;
                }
            }

            let vclock_bytes = self.vclock.to_capnp_bytes();
            reg.set_vclock(&vclock_bytes);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("MVRegister serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
