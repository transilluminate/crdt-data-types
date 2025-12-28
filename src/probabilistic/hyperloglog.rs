use crate::hyperloglog_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};
use siphasher::sip::SipHasher13;
use std::hash::{Hash, Hasher};

/// Precision (number of bits for register index)
const PRECISION: usize = 14;

/// Number of registers (2^14 = 16,384)
const NUM_REGISTERS: usize = 1 << PRECISION;

/// Alpha constant for bias correction
const ALPHA: f64 = 0.7213 / (1.0 + 1.079 / NUM_REGISTERS as f64);

/// HyperLogLog - Cardinality Estimation CRDT
///
/// A probabilistic data structure for estimating the number of unique elements (cardinality)
/// in a set. It uses significantly less memory than storing the elements themselves.
///
/// # Key Properties
///
/// - **Fixed Memory**: Uses ~16KB of memory (16,384 registers) regardless of the number of elements.
/// - **High Accuracy**: Standard error is approximately 0.81% with the default precision (p=14).
/// - **Mergeable**: Can be merged from multiple replicas by taking the element-wise maximum of the registers.
/// - **Idempotent**: Adding the same element multiple times does not change the estimate.
///
/// # Example
///
/// ```
/// use crdt_data_types::HyperLogLog;
///
/// let mut hll = HyperLogLog::new();
/// hll.add("user1");
/// hll.add("user2");
/// hll.add("user3");
/// hll.add("user1"); // Duplicate
///
/// let count = hll.cardinality();
/// assert!(count >= 3 && count <= 4); // Approximate count
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct HyperLogLog {
    /// 16,384 registers (each stores max leading zeros + 1)
    registers: Vec<u8>,
}

impl Default for HyperLogLog {
    fn default() -> Self {
        Self::new()
    }
}

impl HyperLogLog {
    /// Create a new empty HyperLogLog
    pub fn new() -> Self {
        Self {
            registers: vec![0u8; NUM_REGISTERS],
        }
    }

    /// Add an element to the HyperLogLog
    ///
    /// Returns `true` if the internal state changed.
    pub fn add<T: Hash + ?Sized>(&mut self, element: &T) -> bool {
        let hash = self.hash_element(element);

        // Extract register index from first PRECISION bits
        let register_idx = (hash & ((1 << PRECISION) - 1)) as usize;

        // Extract remaining bits for leading zero count
        let remaining_bits = hash >> PRECISION;

        // Count leading zeros + 1 (HLL algorithm convention)
        let leading_zeros = if remaining_bits == 0 {
            (64 - PRECISION) as u8 + 1
        } else {
            remaining_bits.leading_zeros() as u8 + 1
        };

        // Update register if new value is larger (CRDT merge rule)
        let old_value = self.registers[register_idx];
        if leading_zeros > old_value {
            self.registers[register_idx] = leading_zeros;
            true // State changed
        } else {
            false // No change
        }
    }

    /// Estimate the cardinality (number of unique elements)
    pub fn cardinality(&self) -> u64 {
        // Calculate harmonic mean of registers
        let mut sum = 0.0;
        let mut zeros = 0;

        for &val in &self.registers {
            if val == 0 {
                zeros += 1;
            } else {
                sum += 1.0 / (1u64 << val) as f64;
            }
        }

        // Apply HyperLogLog formula
        let mut estimate = ALPHA * (NUM_REGISTERS as f64).powi(2) / (sum + zeros as f64);

        // Apply range corrections
        if estimate <= 2.5 * NUM_REGISTERS as f64 {
            // Small range correction (LinearCounting)
            if zeros > 0 {
                estimate = (NUM_REGISTERS as f64) * (NUM_REGISTERS as f64 / zeros as f64).ln();
            }
        } else if estimate > (1u64 << 32) as f64 / 30.0 {
            // Large range correction
            estimate = -((1u64 << 32) as f64) * (1.0 - estimate / (1u64 << 32) as f64).ln();
        }

        estimate as u64
    }

    /// Merge another HyperLogLog into this one
    pub fn merge(&mut self, other: &Self) {
        if self.registers.len() != other.registers.len() {
            // Should not happen with fixed size, but good to check
            return;
        }
        for (i, &val) in other.registers.iter().enumerate() {
            if val > self.registers[i] {
                self.registers[i] = val;
            }
        }
    }

    fn hash_element<T: Hash + ?Sized>(&self, element: &T) -> u64 {
        let mut hasher = SipHasher13::new();
        element.hash(&mut hasher);
        hasher.finish()
    }

    pub fn from_capnp_bytes(data: &[u8]) -> Result<Self, CrdtError> {
        let message_reader = serialize::read_message(
            data,
            ReaderOptions {
                traversal_limit_in_words: None,
                nesting_limit: 64,
            },
        )
        .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<hyperloglog_capnp::hyper_log_log::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let registers_data = root
            .get_registers()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        if registers_data.len() != NUM_REGISTERS {
             return Err(CrdtError::Deserialization(format!(
                "Invalid register count: expected {}, got {}",
                NUM_REGISTERS,
                registers_data.len()
            )));
        }

        Ok(Self {
            registers: registers_data.to_vec(),
        })
    }
}

pub struct HyperLogLogReader<'a> {
    bytes: &'a [u8],
}

impl<'a> HyperLogLogReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }

    pub fn get_registers(&self) -> Result<Vec<u8>, CrdtError> {
         let message_reader = serialize::read_message(
            self.bytes,
            ReaderOptions::new(),
        ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<hyperloglog_capnp::hyper_log_log::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
        let registers = root.get_registers().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        Ok(registers.to_vec())
    }
}

impl<'a> CrdtReader<'a> for HyperLogLogReader<'a> {
    fn is_empty(&self) -> Result<bool, CrdtError> {
        let message_reader = serialize::read_message(
            self.bytes,
            ReaderOptions::new(),
        ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<hyperloglog_capnp::hyper_log_log::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
        let registers = root.get_registers().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        
        // Check if all zero
        for &byte in registers {
            if byte != 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }
}

impl Crdt for HyperLogLog {
    type Reader<'a> = HyperLogLogReader<'a>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut merged = Self::new();

        // Open all readers and get access to their raw register bytes
        for reader in readers {
            let message_reader = serialize::read_message(
                reader.bytes,
                ReaderOptions::new(),
            ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

            let root = message_reader
                .get_root::<hyperloglog_capnp::hyper_log_log::Reader>()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                
            let registers = root.get_registers().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
            if registers.len() != NUM_REGISTERS {
                 return Err(CrdtError::Merge(format!(
                    "Invalid register count in merge: expected {}, got {}",
                    NUM_REGISTERS,
                    registers.len()
                )));
            }

            // Zero-copy merge: iterate over the slice directly
            for (i, &val) in registers.iter().enumerate() {
                if val > merged.registers[i] {
                    merged.registers[i] = val;
                }
            }
        }

        Ok(merged)
    }

    fn validate(&self) -> Result<(), CrdtError> {
        if self.registers.len() != NUM_REGISTERS {
            return Err(CrdtError::Validation(format!(
                "Invalid register count: expected {}, got {}",
                NUM_REGISTERS,
                self.registers.len()
            )));
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.registers.iter().all(|&x| x == 0)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new_default();
        let mut root = message.init_root::<hyperloglog_capnp::hyper_log_log::Builder>();
        
        root.set_registers(&self.registers);

        let mut data = Vec::new();
        serialize::write_message(&mut data, &message).unwrap();
        data
    }
}
