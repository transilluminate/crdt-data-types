use crate::traits::{Crdt, CrdtError};
use crate::*;
use serde_json::Value;

/// A bridge for validating and converting Serde-compatible data to Cap'n Proto.
///
/// This module enables seamless integration with JSON-based external systems
/// by providing a safe, validated path to the optimized zero-copy Cap'n Proto format.
///
/// # Example
///
/// ```
/// use crdt_data_types::SerdeCapnpBridge;
/// use serde_json::json;
///
/// let json_data = json!({
///     "counters": {"node_a": 10},
///     "vclock": {"clocks": {"node_a": [1, 100]}}
/// });
///
/// let bytes = SerdeCapnpBridge::json_to_capnp_bytes("GCounter", json_data).unwrap();
/// assert!(!bytes.is_empty());
/// ```
pub struct SerdeCapnpBridge;

impl SerdeCapnpBridge {
    /// Converts a JSON value to Cap'n Proto bytes for a specific CRDT type.
    ///
    /// # Arguments
    /// * `crdt_type` - The name of the CRDT type (e.g., "GCounter", "LWWMap").
    /// * `json_value` - The JSON representation of the CRDT state.
    pub fn json_to_capnp_bytes(crdt_type: &str, json_value: Value) -> Result<Vec<u8>, CrdtError> {
        match crdt_type {
            "GCounter" => {
                let crdt: GCounter = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            "PNCounter" => {
                let crdt: PNCounter = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            "GSet" => {
                // Note: GSet involves generics, assume common primitive elements or
                // handle specific common types. For a general bridge, we might need
                // a more dynamic approach or just support common types here.
                // Assuming T = String for this example bridge dispatch.
                let crdt: GSet<String> = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            "ORSet" => {
                let crdt: ORSet<String> = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            "LWWRegister" => {
                let crdt: LWWRegister<String> = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            "FWWRegister" => {
                let crdt: FWWRegister<String> = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            "MVRegister" => {
                let crdt: MVRegister<String> = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            "LWWMap" => {
                let crdt: LWWMap<String, String> = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            "ORMap" => {
                let crdt: ORMap<String, String> = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            "LWWSet" => {
                let crdt: LWWSet<String> = serde_json::from_value(json_value)
                    .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
                crdt.validate()?;
                Ok(crdt.to_capnp_bytes())
            }
            _ => Err(CrdtError::InvalidInput(format!(
                "Unknown CRDT type: {}",
                crdt_type
            ))),
        }
    }

    /// Validates a JSON value against a specific CRDT's internal rules.
    pub fn validate_json(crdt_type: &str, json_value: Value) -> Result<(), CrdtError> {
        // Validation is implicitly handled by the conversion logic above.
        Self::json_to_capnp_bytes(crdt_type, json_value).map(|_| ())
    }

    /// Converts Cap'n Proto bytes back to a JSON value for a specific CRDT type.
    ///
    /// # Arguments
    /// * `crdt_type` - The name of the CRDT type (e.g., "GCounter", "LWWMap").
    /// * `bytes` - The Cap'n Proto serialized bytes.
    pub fn capnp_bytes_to_json(crdt_type: &str, bytes: &[u8]) -> Result<Value, CrdtError> {
        match crdt_type {
            "GCounter" => {
                let reader = GCounterReader::new(bytes);
                let crdt = GCounter::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "PNCounter" => {
                let reader = PNCounterReader::new(bytes);
                let crdt = PNCounter::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "GSet" => {
                let reader = GSetReader::<String>::new(bytes);
                let crdt = GSet::<String>::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "ORSet" => {
                let reader = ORSetReader::<String>::new(bytes);
                let crdt = ORSet::<String>::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "LWWRegister" => {
                let reader = LWWRegisterReader::<String>::new(bytes);
                let crdt = LWWRegister::<String>::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "FWWRegister" => {
                let reader = FWWRegisterReader::<String>::new(bytes);
                let crdt = FWWRegister::<String>::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "MVRegister" => {
                let reader = MVRegisterReader::<String>::new(bytes);
                let crdt = MVRegister::<String>::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "LWWMap" => {
                let reader = LWWMapReader::<String, String>::new(bytes);
                let crdt = LWWMap::<String, String>::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "ORMap" => {
                let reader = ORMapReader::<String, String>::new(bytes);
                let crdt = ORMap::<String, String>::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "LWWSet" => {
                let reader = LWWSetReader::<String>::new(bytes);
                let crdt = LWWSet::<String>::merge_from_readers(&[reader])?;
                serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            _ => Err(CrdtError::InvalidInput(format!(
                "Unknown CRDT type: {}",
                crdt_type
            ))),
        }
    }

    /// Merges multiple JSON values representing CRDT states into a single JSON value.
    ///
    /// This simulates a typical Web API scenario where multiple updates are merged
    /// in-memory before being returned or stored.
    pub fn merge_json_values(crdt_type: &str, values: &[Value]) -> Result<Value, CrdtError> {
        if values.is_empty() {
            return Ok(Value::Null);
        }

        match crdt_type {
            "GCounter" => {
                let mut base: GCounter = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: GCounter = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "PNCounter" => {
                let mut base: PNCounter = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: PNCounter = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "GSet" => {
                let mut base: GSet<String> = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: GSet<String> = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "ORSet" => {
                let mut base: ORSet<String> = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: ORSet<String> = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "LWWRegister" => {
                let mut base: LWWRegister<String> = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: LWWRegister<String> = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "FWWRegister" => {
                let mut base: FWWRegister<String> = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: FWWRegister<String> = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "MVRegister" => {
                let mut base: MVRegister<String> = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: MVRegister<String> = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "LWWMap" => {
                let mut base: LWWMap<String, String> = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: LWWMap<String, String> = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "ORMap" => {
                let mut base: ORMap<String, String> = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: ORMap<String, String> = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            "LWWSet" => {
                let mut base: LWWSet<String> = serde_json::from_value(values[0].clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                for val in &values[1..] {
                    let other: LWWSet<String> = serde_json::from_value(val.clone())
                        .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                    base.merge(&other);
                }
                serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
            }
            _ => Err(CrdtError::InvalidInput(format!(
                "Merge not supported for type via JSON: {}",
                crdt_type
            ))),
        }
    }
}
