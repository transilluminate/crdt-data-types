// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

pub mod serialization;
pub mod merging;
pub mod deltas;

use crate::enums::CrdtType;
use crate::traits::CrdtError;
use serde_json::Value;

/// A bridge for validating and converting Serde-compatible data to Cap'n Proto.
///
/// This module enables seamless integration with JSON-based external systems
/// by providing a safe, validated path to the optimized zero-copy Cap'n Proto format.
///
/// # Example
///
/// ```
/// use crdt_data_types::{SerdeCapnpBridge, CrdtType};
/// use serde_json::json;
///
/// let json_data = json!({
///     "counters": {"node_a": 10},
///     "vclock": {"clocks": {"node_a": [1, 100]}}
/// });
///
/// let bytes = SerdeCapnpBridge::json_to_capnp_bytes(CrdtType::GCounter, json_data).unwrap();
/// assert!(!bytes.is_empty());
/// ```
pub struct SerdeCapnpBridge;

impl SerdeCapnpBridge {
    /// Converts a JSON value to Cap'n Proto bytes for a specific CRDT type.
    pub fn json_to_capnp_bytes(crdt_type: CrdtType, json_value: Value) -> Result<Vec<u8>, CrdtError> {
        serialization::json_to_capnp_bytes(crdt_type, json_value)
    }

    /// Validates a JSON value against a specific CRDT's internal rules.
    pub fn validate_json(crdt_type: CrdtType, json_value: Value) -> Result<(), CrdtError> {
        serialization::validate_json(crdt_type, json_value)
    }

    /// Converts Cap'n Proto bytes back to a JSON value for a specific CRDT type.
    pub fn capnp_bytes_to_json(crdt_type: CrdtType, bytes: &[u8]) -> Result<Value, CrdtError> {
        serialization::capnp_bytes_to_json(crdt_type, bytes)
    }

    /// Merges multiple JSON values representing CRDT states into a single JSON value.
    pub fn merge_json_values(crdt_type: CrdtType, values: &[Value]) -> Result<Value, CrdtError> {
        merging::merge_json_values(crdt_type, values)
    }

    /// Additively merge accumulated delta state into current state.
    pub fn add_accumulated_state(
        crdt_type: CrdtType,
        current: Value,
        accumulated: Value,
    ) -> Result<Value, CrdtError> {
        merging::add_accumulated_state(crdt_type, current, accumulated)
    }

    /// Apply a delta operation to an existing CRDT state.
    pub fn apply_json_delta(
        crdt_type: CrdtType,
        current_state: Option<&Value>,
        delta: &Value,
        node_id: &str,
    ) -> Result<Value, CrdtError> {
        deltas::apply_json_delta(crdt_type, current_state, delta, node_id)
    }

    /// Apply a JSON delta to a Cap'n Proto binary state, returning new Cap'n Proto bytes.
    pub fn apply_bytes_delta(
         crdt_type: CrdtType,
         current_state_bytes: Option<&[u8]>,
         delta: &Value,
         node_id: &str,
    ) -> Result<Vec<u8>, CrdtError> {
        deltas::apply_bytes_delta(crdt_type, current_state_bytes, delta, node_id)
    }

    /// Apply a Cap'n Proto delta to a Cap'n Proto binary state.
    pub fn apply_capnp_delta(
        crdt_type: CrdtType,
        current_state_bytes: Option<&[u8]>,
        delta_bytes: &[u8],
        node_id: &str,
    ) -> Result<Vec<u8>, CrdtError> {
        deltas::apply_capnp_delta(crdt_type, current_state_bytes, delta_bytes, node_id)
    }

    /// Apply a batch of Cap'n Proto deltas to a Cap'n Proto binary state.
    pub fn apply_batch_capnp_deltas(
        crdt_type: CrdtType,
        current_state_bytes: Option<&[u8]>,
        deltas_bytes: &[&[u8]],
        node_id: &str,
    ) -> Result<Vec<u8>, CrdtError> {
        deltas::apply_batch_capnp_deltas(crdt_type, current_state_bytes, deltas_bytes, node_id)
    }
}
