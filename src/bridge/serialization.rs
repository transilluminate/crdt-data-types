use crate::traits::{Crdt, CrdtError};
use crate::*;
use crate::enums::CrdtType;
use serde_json::Value;

/// Converts a JSON value to Cap'n Proto bytes for a specific CRDT type.
pub fn json_to_capnp_bytes(crdt_type: CrdtType, json_value: Value) -> Result<Vec<u8>, CrdtError> {
    match crdt_type {
        CrdtType::GCounter => {
            let crdt: GCounter = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::PNCounter => {
            let crdt: PNCounter = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::GSet => {
            let crdt: GSet<String> = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::ORSet => {
            let crdt: ORSet<String> = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::LWWRegister => {
            let crdt: LWWRegister<String> = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::FWWRegister => {
            let crdt: FWWRegister<String> = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::MVRegister => {
            let crdt: MVRegister<String> = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::LWWMap => {
            let crdt: LWWMap<String, String> = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::ORMap => {
            let crdt: ORMap<String, String> = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::LWWSet => {
            let crdt: LWWSet<String> = serde_json::from_value(json_value)
                .map_err(|e| CrdtError::InvalidInput(format!("JSON parse error: {}", e)))?;
            crdt.validate()?;
            Ok(crdt.to_capnp_bytes())
        }
    }
}

/// Validates a JSON value against a specific CRDT's internal rules.
pub fn validate_json(crdt_type: CrdtType, json_value: Value) -> Result<(), CrdtError> {
    // Reuse conversion logic for validation
    json_to_capnp_bytes(crdt_type, json_value).map(|_| ())
}

/// Converts Cap'n Proto bytes back to a JSON value for a specific CRDT type.
pub fn capnp_bytes_to_json(crdt_type: CrdtType, bytes: &[u8]) -> Result<Value, CrdtError> {
    match crdt_type {
        CrdtType::GCounter => {
            let reader = GCounterReader::new(bytes);
            let crdt = GCounter::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::PNCounter => {
            let reader = PNCounterReader::new(bytes);
            let crdt = PNCounter::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::GSet => {
            let reader = GSetReader::<String>::new(bytes);
            let crdt = GSet::<String>::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::ORSet => {
            let reader = ORSetReader::<String>::new(bytes);
            let crdt = ORSet::<String>::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::LWWRegister => {
            let reader = LWWRegisterReader::<String>::new(bytes);
            let crdt = LWWRegister::<String>::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::FWWRegister => {
            let reader = FWWRegisterReader::<String>::new(bytes);
            let crdt = FWWRegister::<String>::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::MVRegister => {
            let reader = MVRegisterReader::<String>::new(bytes);
            let crdt = MVRegister::<String>::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::LWWMap => {
            let reader = LWWMapReader::<String, String>::new(bytes);
            let crdt = LWWMap::<String, String>::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::ORMap => {
            let reader = ORMapReader::<String, String>::new(bytes);
            let crdt = ORMap::<String, String>::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::LWWSet => {
            let reader = LWWSetReader::<String>::new(bytes);
            let crdt = LWWSet::<String>::merge_from_readers(&[reader])?;
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
    }
}
