use crate::fww_register_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::vector_clock::VectorClock;
use capnp::message::{Builder, HeapAllocator, ReaderOptions};
use capnp::serialize;
use serde::{de::DeserializeOwned, Deserialize, Serialize};

/// FWW-Register: A First-Write-Wins Register CRDT.
///
/// An FWW-Register (First-Write-Wins) stores a single value and resolves
/// conflicts by choosing the value with the *lowest* non-zero timestamp.
/// This is the dual of the LWW-Register and is useful in scenarios where
/// the first recorded state should be preserved (e.g., "creation date").
///
/// # Key Properties
///
/// - **First-Write-Wins**: The update with the lowest timestamp wins.
/// - **Initialization**: Initialized with `u64::MAX` so any valid write overwrites the default.
/// - **Tie-Breaking**: Deterministic tie-breaking using node IDs.
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
/// use crdt_data_types::FWWRegister;
///
/// let mut reg1 = FWWRegister::new("value1".to_string(), 100, "node_a");
/// let mut reg2 = FWWRegister::new("value2".to_string(), 200, "node_b");
///
/// reg1.merge(&reg2);
/// assert_eq!(reg1.value, "value1"); // Lower timestamp wins
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(bound(serialize = "T: Serialize", deserialize = "T: DeserializeOwned"))]
pub struct FWWRegister<T> {
    /// The current value stored in the register.
    pub value: T,
    /// Timestamp of the first write.
    pub timestamp: u64,
    /// Identifier of the node that performed the first write.
    pub node_id: String,
    /// Vector clock for tracking causal history.
    pub vclock: VectorClock,
}

impl<T: Clone + Default + Serialize + DeserializeOwned + Send + Sync + 'static> Default
    for FWWRegister<T>
{
    fn default() -> Self {
        Self {
            value: T::default(),
            timestamp: u64::MAX, // Initialize with MAX so any real timestamp wins first.
            node_id: String::new(),
            vclock: VectorClock::new(),
        }
    }
}

impl<T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static> FWWRegister<T> {
    /// Creates a new FWW-Register with an initial value.
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

    /// Updates the register with a new value and timestamp if it's "earlier".
    pub fn set(&mut self, value: T, timestamp: u64, node_id: impl Into<String>) {
        let node_id = node_id.into();
        // First-write-wins: keep the lowest timestamp.
        if timestamp < self.timestamp || (timestamp == self.timestamp && node_id < self.node_id) {
            self.value = value;
            self.timestamp = timestamp;
            self.node_id = node_id.clone();
            self.vclock.increment(&node_id);
        }
    }

    /// Merges another FWW-Register into this one.
    pub fn merge(&mut self, other: &Self) {
        if other.timestamp < self.timestamp
            || (other.timestamp == self.timestamp && other.node_id < self.node_id)
        {
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

pub struct FWWRegisterReader<'a, T> {
    bytes: &'a [u8],
    _phantom: std::marker::PhantomData<T>,
}

impl<'a, T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static> FWWRegisterReader<'a, T> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self {
            bytes,
            _phantom: std::marker::PhantomData,
        }
    }

    fn to_register(&self) -> Result<FWWRegister<T>, CrdtError> {
        let reader = serialize::read_message(self.bytes, ReaderOptions::new())
            .map_err(|e: capnp::Error| CrdtError::Deserialization(e.to_string()))?;
        let reg = reader
            .get_root::<fww_register_capnp::fww_register::Reader>()
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

        Ok(FWWRegister {
            value,
            timestamp: reg.get_timestamp(),
            node_id,
            vclock,
        })
    }
}

impl<'a, T: Clone + Serialize + DeserializeOwned + Send + Sync + 'static> CrdtReader<'a>
    for FWWRegisterReader<'a, T>
{
    fn is_empty(&self) -> Result<bool, CrdtError> {
        Ok(self.to_register()?.timestamp == u64::MAX)
    }
}

// ============================================================================
// CRDT Trait Implementation
// ============================================================================

impl<T: Clone + Default + Serialize + DeserializeOwned + Send + Sync + 'static> Crdt
    for FWWRegister<T>
{
    type Reader<'a> = FWWRegisterReader<'a, T>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        if readers.is_empty() {
            return Ok(FWWRegister::default());
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
            let mut reg = message.init_root::<fww_register_capnp::fww_register::Builder>();
            let bytes =
                bincode::serialize(&self.value).expect("FWWRegister value serialization fail");
            reg.set_value(&bytes);
            reg.set_timestamp(self.timestamp);
            reg.set_node_id(self.node_id.as_str().into());
            let vclock_bytes = self.vclock.to_capnp_bytes();
            reg.set_vclock(&vclock_bytes);
        }
        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message).expect("FWWRegister serialization fail");
        buf
    }

    fn is_empty(&self) -> bool {
        self.timestamp == u64::MAX
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }
}
