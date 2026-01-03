// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

//! CRDT Compaction Module
//!
//! Provides utilities for compacting multiple CRDT states into a single merged state.
//! Compaction is essential for storage efficiency and reducing synchronization overhead
//! in distributed systems.
//!
//! # Two Compaction Pathways
//!
//! - **JSON Compaction**: For web APIs and JSON-based storage systems.
//! - **Cap'n Proto Compaction**: For high-performance binary storage and transport.
//!
//! # Example
//!
//! ```
//! use crdt_data_types::compaction::compact_json_values;
//! use serde_json::json;
//!
//! let values = vec![
//!     json!({"counters": {"node_a": 10}, "vclock": {"clocks": {"node_a": [1, 100]}}}),
//!     json!({"counters": {"node_b": 20}, "vclock": {"clocks": {"node_b": [1, 100]}}}),
//! ];
//!
//! let compacted = compact_json_values("GCounter", &values).unwrap();
//! ```

use crate::bridge::SerdeCapnpBridge;
use crate::traits::{Crdt, CrdtError};
use crate::*;
use serde_json::Value;

/// Compacts multiple CRDT JSON values into a single merged value.
///
/// # Arguments
/// * `crdt_type` - The CRDT type name (e.g., "GCounter", "ORSet").
/// * `values` - Slice of JSON values representing CRDT states to compact.
///
/// # Example
///
/// ```
/// use crdt_data_types::compaction::compact_json_values;
/// use serde_json::json;
///
/// let values = vec![
///     json!({"counters": {"node_a": 5}, "vclock": {"clocks": {"node_a": [1, 100]}}}),
///     json!({"counters": {"node_a": 10}, "vclock": {"clocks": {"node_a": [2, 200]}}}),
/// ];
///
/// let result = compact_json_values("GCounter", &values).unwrap();
/// ```
pub fn compact_json_values(crdt_type: &str, values: &[Value]) -> Result<Value, CrdtError> {
    SerdeCapnpBridge::merge_json_values(crdt_type, values)
}

/// Compacts multiple Cap'n Proto byte buffers into a single buffer.
///
/// This is the high-performance pathway for binary storage systems.
/// It avoids JSON serialization overhead entirely.
///
/// # Arguments
/// * `crdt_type` - The CRDT type name (e.g., "GCounter", "ORSet").
/// * `buffers` - Slice of Cap'n Proto byte buffers to compact.
///
/// # Example
///
/// ```
/// use crdt_data_types::{GCounter, Crdt};
/// use crdt_data_types::compaction::compact_capnp_bytes;
///
/// let mut gc1 = GCounter::new();
/// gc1.increment("node_a", 10);
/// let bytes1 = gc1.to_capnp_bytes();
///
/// let mut gc2 = GCounter::new();
/// gc2.increment("node_b", 20);
/// let bytes2 = gc2.to_capnp_bytes();
///
/// let compacted = compact_capnp_bytes("GCounter", &[&bytes1, &bytes2]).unwrap();
/// ```
pub fn compact_capnp_bytes(crdt_type: &str, buffers: &[&[u8]]) -> Result<Vec<u8>, CrdtError> {
    if buffers.is_empty() {
        return Ok(Vec::new());
    }

    match crdt_type {
        "GCounter" => {
            let readers: Vec<_> = buffers.iter().map(|b| GCounterReader::new(b)).collect();
            let merged = GCounter::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        "PNCounter" => {
            let readers: Vec<_> = buffers.iter().map(|b| PNCounterReader::new(b)).collect();
            let merged = PNCounter::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        "GSet" => {
            let readers: Vec<_> = buffers
                .iter()
                .map(|b| GSetReader::<String>::new(b))
                .collect();
            let merged = GSet::<String>::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        "ORSet" => {
            let readers: Vec<_> = buffers
                .iter()
                .map(|b| ORSetReader::<String>::new(b))
                .collect();
            let merged = ORSet::<String>::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        "LWWRegister" => {
            let readers: Vec<_> = buffers
                .iter()
                .map(|b| LWWRegisterReader::<String>::new(b))
                .collect();
            let merged = LWWRegister::<String>::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        "FWWRegister" => {
            let readers: Vec<_> = buffers
                .iter()
                .map(|b| FWWRegisterReader::<String>::new(b))
                .collect();
            let merged = FWWRegister::<String>::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        "MVRegister" => {
            let readers: Vec<_> = buffers
                .iter()
                .map(|b| MVRegisterReader::<String>::new(b))
                .collect();
            let merged = MVRegister::<String>::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        "LWWMap" => {
            let readers: Vec<_> = buffers
                .iter()
                .map(|b| LWWMapReader::<String, String>::new(b))
                .collect();
            let merged = LWWMap::<String, String>::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        "ORMap" => {
            let readers: Vec<_> = buffers
                .iter()
                .map(|b| ORMapReader::<String, String>::new(b))
                .collect();
            let merged = ORMap::<String, String>::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        "LWWSet" => {
            let readers: Vec<_> = buffers
                .iter()
                .map(|b| LWWSetReader::<String>::new(b))
                .collect();
            let merged = LWWSet::<String>::merge_from_readers(&readers)?;
            Ok(merged.to_capnp_bytes())
        }
        _ => Err(CrdtError::InvalidInput(format!(
            "Compaction not supported for type: {}",
            crdt_type
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_compact_json_gcounter() {
        let values = vec![
            json!({"counters": {"node_a": 10}, "vclock": {"clocks": {"node_a": [1, 100]}}}),
            json!({"counters": {"node_b": 20}, "vclock": {"clocks": {"node_b": [1, 100]}}}),
        ];

        let result = compact_json_values("GCounter", &values).unwrap();
        assert!(result.get("counters").is_some());
    }

    #[test]
    fn test_compact_capnp_gcounter() {
        let mut gc1 = GCounter::new();
        gc1.increment("node_a", 10);
        let bytes1 = gc1.to_capnp_bytes();

        let mut gc2 = GCounter::new();
        gc2.increment("node_b", 20);
        let bytes2 = gc2.to_capnp_bytes();

        let compacted = compact_capnp_bytes("GCounter", &[&bytes1, &bytes2]).unwrap();
        assert!(!compacted.is_empty());

        // Verify the merged result
        let reader = GCounterReader::new(&compacted);
        let merged = GCounter::merge_from_readers(&[reader]).unwrap();
        assert_eq!(merged.value(), 30);
    }

    #[test]
    fn test_compact_empty() {
        let result = compact_json_values("GCounter", &[]).unwrap();
        assert_eq!(result, Value::Null);

        let bytes = compact_capnp_bytes("GCounter", &[]).unwrap();
        assert!(bytes.is_empty());
    }
}