use crate::traits::{CrdtError};
use crate::*;
use crate::enums::CrdtType;
use serde_json::Value;

/// Merges multiple JSON values representing CRDT states into a single JSON value.
pub fn merge_json_values(crdt_type: CrdtType, values: &[Value]) -> Result<Value, CrdtError> {
    if values.is_empty() {
        return Ok(Value::Null);
    }

    match crdt_type {
        CrdtType::GCounter => {
            let mut base: GCounter = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: GCounter = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::PNCounter => {
            let mut base: PNCounter = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: PNCounter = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::GSet => {
            let mut base: GSet<String> = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: GSet<String> = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::ORSet => {
            let mut base: ORSet<String> = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: ORSet<String> = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::LWWRegister => {
            let mut base: LWWRegister<String> = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: LWWRegister<String> = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::FWWRegister => {
            let mut base: FWWRegister<String> = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: FWWRegister<String> = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::MVRegister => {
            let mut base: MVRegister<String> = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: MVRegister<String> = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::LWWMap => {
            let mut base: LWWMap<String, String> = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: LWWMap<String, String> = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::ORMap => {
            let mut base: ORMap<String, String> = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: ORMap<String, String> = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::LWWSet => {
            let mut base: LWWSet<String> = serde_json::from_value(values[0].clone())
                .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
            for val in &values[1..] {
                let other: LWWSet<String> = serde_json::from_value(val.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?;
                base.merge(&other);
            }
            serde_json::to_value(base).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
    }
}
