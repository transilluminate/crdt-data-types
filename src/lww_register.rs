// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crate::lww_register_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// LWW-Register: A Last-Write-Wins Register CRDT.
///
/// An LWW-Register stores a single value and resolves conflicts by choosing
/// the value with the highest timestamp. On timestamp ties, a lexicographic
/// comparison of the node identifiers is used as a deterministic tie-breaker.
///
/// # Key Properties
///
/// - **Last-Write-Wins**: The update with the highest timestamp wins.
/// - **Tie-Breaking**: Deterministic tie-breaking using node IDs ensures convergence.
/// - **Simplicity**: Easy to understand and implement.
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
/// use crdt_data_types::LWWRegister;
///
/// let mut reg1 = LWWRegister::new("value1".to_string(), 100, "node_a");
/// let mut reg2 = LWWRegister::new("value2".to_string(), 200, "node_b");
///
/// reg1.merge(&reg2);
/// assert_eq!(reg1.value, "value2"); // Higher timestamp wins
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: DeserializeOwned"))]
pub struct LWWRegister<T> {
    /// The current value stored in the register.
    pub value: T,
    /// Timestamp of the last write.
    pub timestamp: u64,
    /// Identifier of the node that performed the last write.
    pub node_id: String,
    /// Vector clock for tracking causal history.
    #[serde(default)]
    pub vclock: VectorClock,
}

impl<T: Clone + Default + Serialize + DeserializeOwned + Send + Sync + 'static> Default
    for LWWRegister<T>
{
    fn default() -> Self {
        Self {
            value: T::default(),
            timestamp: 0,
            node_id: String::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<T: Clone + Serialize + DeserializeOwned + Ord + Send + Sync + 'static> LWWRegister<T> {
    /// Creates a new LWW-Register with an initial value.
    pub fn new(value: T, timestamp: u64, node_id: impl Into<String>) -> Self {
        let node_id = node_id.into();
        let mut vclock = VectorClock::new();
        vclock.increment(&node_id);
        Self {
            value,
            timestamp,
            node_id,
            vclock,
        }
    }

    /// Updates the register with a new value and timestamp.
    ///
    /// The update is only applied if the new timestamp is higher than the current
    /// one, or if they are equal and the new node_id is lexicographically greater.
    pub fn set(&mut self, value: T, timestamp: u64, node_id: impl Into<String>) {
        let node_id = node_id.into();
        let update = timestamp > self.timestamp
            || (timestamp == self.timestamp && node_id > self.node_id)
            || (timestamp == self.timestamp
                && node_id == self.node_id
                && value > self.value);

        if update {
            self.value = value;
            self.timestamp = timestamp;
            self.node_id = node_id.clone();
            self.vclock.increment(&node_id);
        }
    }

    /// Merges another LWW-Register into this one.
    pub fn merge(&mut self, other: &Self) {
        let update = other.timestamp > self.timestamp
            || (other.timestamp == self.timestamp && other.node_id > self.node_id)
            || (other.timestamp == self.timestamp
                && other.node_id == self.node_id
                && other.value > self.value);

        if update {
            self.value = other.value.clone();
            self.timestamp = other.timestamp;
            self.node_id = other.node_id.clone();
        }
        self.vclock.merge(&other.vclock);
    }
}

// ============================================================================
// Zero-Copy Reader
// ============================================================================

pub struct LWWRegisterReader<'a, T> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static> LWWRegisterReader<'a, T> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_register(&self) -> Result<LWWRegister<T>, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let reg = reader
            .get_root::<lww_register_capnp::lww_register::Reader>()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;

        let value_bytes = reg
            .get_value()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let value: T = bincode::deserialize(value_bytes)
            .map_err(|e: bincode::Error| CrdtError::Deserialization(e.to_string()))?;

        let node_id = reg
            .get_node_id()
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?
            .to_string()
            .map_err(|e: std::str::Utf8Error| CrdtError::Deserialization(e.to_string()))?;

        let vclock = if reg.has_vclock() {
            let vc_bytes = reg
                .get_vclock()
                .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
            VectorClock::merge_from_readers(&[crate::vector_clock::VectorClockReader::new(
                vc_bytes,
            )])?
        } else {
            VectorClock::new()
        };

        Ok(LWWRegister {
            value,
            timestamp: reg.get_timestamp(),
            node_id,
            vclock,
        })
    }
}

impl<'a, T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static> CrdtReader<'a>
    for LWWRegisterReader<'a, T>
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        // A register with timestamp 0 is considered "empty" in our context.
        Ok(self.to_register()?.timestamp == 0)
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<T: Clone + Default + Serialize + DeserializeOwned + Ord + Send + Sync + 'static> Crdt
    for LWWRegister<T>
{
    type Reader<'a> = LWWRegisterReader<'a, T>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        if readers.is_empty() {
            return Ok(LWWRegister::default());
        }
        let mut result = readers[0].to_register()?;
        for reader in &readers[1..] {
            result.merge(&reader.to_register()?);
        }
        Ok(result)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new(HeapAllocator::new());
        {
            let mut reg = message.init_root::<lww_register_capnp::lww_register::Builder>();
            let bytes =
                bincode::serialize(&self.value).expect("LWWRegister value serialization fail");
            reg.set_value(&bytes);
            reg.set_timestamp(self.timestamp);
            reg.set_node_id(self.node_id.as_str().into());
            let vclock_bytes = self.vclock.to_capnp_bytes();
            reg.set_vclock(&vclock_bytes);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("LWWRegister serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.timestamp == 0
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
