use crate::traits::{Crdt, CrdtError};
use crate::*;
use crate::enums::CrdtType;
use crate::deltas::*; 
use crate::deltas_capnp::delta;
use serde_json::Value;

/// Apply a delta operation to an existing CRDT state.
///
/// Unlike `merge()` which uses max/union semantics for state replication,
/// this uses additive semantics for client operations.
pub fn apply_json_delta(
    crdt_type: CrdtType,
    current_state: Option<&Value>,
    delta: &Value,
    node_id: &str,
) -> Result<Value, CrdtError> {
    match crdt_type {
        CrdtType::GCounter => {
            let mut crdt: GCounter = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                GCounter::new()
            };

            let delta_struct: GCounterDelta = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid GCounter delta: {}", e)))?;
            
            let amount = match delta_struct {
                GCounterDelta::Direct(v) => v,
                GCounterDelta::Object { increment } => increment,
            };

            crdt.increment(node_id, amount);
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::PNCounter => {
            let mut crdt: PNCounter = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                PNCounter::new()
            };

            let delta_struct: PNCounterDelta = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid PNCounter delta: {}", e)))?;
            
            let amount = match delta_struct {
                PNCounterDelta::Direct(v) => v,
                PNCounterDelta::Object { increment } => increment,
            };

            crdt.increment(node_id, amount);
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::GSet => {
            let mut crdt: GSet<String> = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                GSet::new()
            };

            let delta_struct: GSetDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid GSet delta: {}", e)))?;

            let items = match delta_struct {
                GSetDelta::List(v) => v,
                GSetDelta::Object { add } => add,
            };

            for s in items {
                crdt.insert(node_id, s);
            }
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::ORSet => {
            let mut crdt: ORSet<String> = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                ORSet::new()
            };

            let delta_struct: ORSetDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid ORSet delta: {}", e)))?;

            if let Some(add) = delta_struct.add {
                for s in add {
                    crdt.insert(node_id, s);
                }
            }
            if let Some(remove) = delta_struct.remove {
                for s in remove {
                    crdt.remove(&s);
                }
            }
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::LWWSet => {
            let mut crdt: LWWSet<String> = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                LWWSet::new()
            };

            let delta_struct: LWWSetDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid LWWSet delta: {}", e)))?;

            let timestamp = delta_struct.timestamp;

            if let Some(add) = delta_struct.add {
                for s in add {
                    crdt.insert(node_id, s, timestamp);
                }
            }
            if let Some(remove) = delta_struct.remove {
                for s in remove {
                    crdt.remove(node_id, s, timestamp);
                }
            }
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::LWWRegister => {
            let mut crdt: LWWRegister<String> = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                LWWRegister::default()
            };

            let delta_struct: LWWRegisterDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid LWWRegister delta: {}", e)))?;

            crdt.set(delta_struct.value, delta_struct.timestamp, node_id);
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::FWWRegister => {
            let mut crdt: FWWRegister<String> = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                FWWRegister::default()
            };

            let delta_struct: FWWRegisterDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid FWWRegister delta: {}", e)))?;

            crdt.set(delta_struct.value, delta_struct.timestamp, node_id);
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::MVRegister => {
            let mut crdt: MVRegister<String> = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                MVRegister::default()
            };

            let delta_struct: MVRegisterDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid MVRegister delta: {}", e)))?;

            let val = match delta_struct {
                MVRegisterDelta::Direct(v) => v,
                MVRegisterDelta::Object { value } => value,
            };

            crdt.set(node_id, val);
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::LWWMap => {
            let mut crdt: LWWMap<String, String> = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                LWWMap::new()
            };

            let delta_struct: LWWMapDelta<String, String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid LWWMap delta: {}", e)))?;

            if let Some(set) = delta_struct.set {
                for (k, v) in set {
                    crdt.insert(node_id, k, v, delta_struct.timestamp);
                }
            }
            if let Some(remove) = delta_struct.remove {
                for k in remove {
                    crdt.remove(&k);
                }
            }
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
        CrdtType::ORMap => {
            let mut crdt: ORMap<String, String> = if let Some(state) = current_state {
                serde_json::from_value(state.clone())
                    .map_err(|e| CrdtError::InvalidInput(e.to_string()))?
            } else {
                ORMap::new()
            };

            let delta_struct: ORMapDelta<String, String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid ORMap delta: {}", e)))?;

            if let Some(set) = delta_struct.set {
                for (k, v) in set {
                    crdt.insert(node_id, k, v);
                }
            }
            if let Some(remove) = delta_struct.remove {
                for k in remove {
                    crdt.remove(&k);
                }
            }
            serde_json::to_value(crdt).map_err(|e| CrdtError::InvalidInput(e.to_string()))
        }
    }
}

/// Apply a JSON delta to a Cap'n Proto binary state, returning new Cap'n Proto bytes.
pub fn apply_bytes_delta(
        crdt_type: CrdtType,
        current_state_bytes: Option<&[u8]>,
        delta: &Value,
        node_id: &str,
) -> Result<Vec<u8>, CrdtError> {
    match crdt_type {
        CrdtType::GCounter => {
            let mut crdt: GCounter = if let Some(bytes) = current_state_bytes {
                    let reader = GCounterReader::new(bytes);
                    GCounter::merge_from_readers(&[reader])?
            } else {
                GCounter::new()
            };

            let delta_struct: GCounterDelta = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid GCounter delta: {}", e)))?;

            let amount = match delta_struct {
                GCounterDelta::Direct(v) => v,
                GCounterDelta::Object { increment } => increment,
            };

            crdt.increment(node_id, amount);
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::PNCounter => {
                let mut crdt: PNCounter = if let Some(bytes) = current_state_bytes {
                    let reader = PNCounterReader::new(bytes);
                    PNCounter::merge_from_readers(&[reader])?
            } else {
                PNCounter::new()
            };

            let delta_struct: PNCounterDelta = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid PNCounter delta: {}", e)))?;

            let amount = match delta_struct {
                PNCounterDelta::Direct(v) => v,
                PNCounterDelta::Object { increment } => increment,
            };

            crdt.increment(node_id, amount);
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::GSet => {
                let mut crdt: GSet<String> = if let Some(bytes) = current_state_bytes {
                    let reader = GSetReader::<String>::new(bytes);
                    GSet::<String>::merge_from_readers(&[reader])?
            } else {
                GSet::new()
            };

            let delta_struct: GSetDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid GSet delta: {}", e)))?;

            match delta_struct {
                GSetDelta::List(list) => {
                    for v in list {
                        crdt.insert(node_id, v);
                    }
                }
                GSetDelta::Object { add } => {
                    for v in add {
                        crdt.insert(node_id, v);
                    }
                }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::ORSet => {
                let mut crdt: ORSet<String> = if let Some(bytes) = current_state_bytes {
                    let reader = ORSetReader::<String>::new(bytes);
                    ORSet::<String>::merge_from_readers(&[reader])?
            } else {
                ORSet::new()
            };

            let delta_struct: ORSetDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid ORSet delta: {}", e)))?;

            if let Some(add) = delta_struct.add {
                for v in add {
                    crdt.insert(node_id, v);
                }
            }
            if let Some(remove) = delta_struct.remove {
                for v in remove {
                    crdt.remove(&v);
                }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::LWWSet => {
            let mut crdt: LWWSet<String> = if let Some(bytes) = current_state_bytes {
                    let reader = LWWSetReader::<String>::new(bytes);
                    LWWSet::<String>::merge_from_readers(&[reader])?
            } else {
                LWWSet::new()
            };

            let delta_struct: LWWSetDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid LWWSet delta: {}", e)))?;

            if let Some(add) = delta_struct.add {
                for v in add {
                    crdt.insert(node_id, v, delta_struct.timestamp);
                }
            }
            if let Some(remove) = delta_struct.remove {
                for v in remove {
                    crdt.remove(node_id, v, delta_struct.timestamp);
                }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::LWWRegister => {
            let mut crdt: LWWRegister<String> = if let Some(bytes) = current_state_bytes {
                    let reader = LWWRegisterReader::<String>::new(bytes);
                    LWWRegister::<String>::merge_from_readers(&[reader])?
            } else {
                LWWRegister::default()
            };

            let delta_struct: LWWRegisterDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid LWWRegister delta: {}", e)))?;

            crdt.set(delta_struct.value, delta_struct.timestamp, node_id);
                Ok(crdt.to_capnp_bytes())
        }
        CrdtType::FWWRegister => {
            let mut crdt: FWWRegister<String> = if let Some(bytes) = current_state_bytes {
                    let reader = FWWRegisterReader::<String>::new(bytes);
                    FWWRegister::<String>::merge_from_readers(&[reader])?
            } else {
                FWWRegister::default()
            };

            let delta_struct: FWWRegisterDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid FWWRegister delta: {}", e)))?;

            crdt.set(delta_struct.value, delta_struct.timestamp, node_id);
                Ok(crdt.to_capnp_bytes())
        }
        CrdtType::MVRegister => {
            let mut crdt: MVRegister<String> = if let Some(bytes) = current_state_bytes {
                    let reader = MVRegisterReader::<String>::new(bytes);
                    MVRegister::<String>::merge_from_readers(&[reader])?
            } else {
                MVRegister::default()
            };

            let delta_struct: MVRegisterDelta<String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid MVRegister delta: {}", e)))?;

            let value = match delta_struct {
                MVRegisterDelta::Direct(v) => v,
                MVRegisterDelta::Object { value } => value,
            };
            crdt.set(node_id, value);
                Ok(crdt.to_capnp_bytes())
        }
        CrdtType::LWWMap => {
                let mut crdt: LWWMap<String, String> = if let Some(bytes) = current_state_bytes {
                    let reader = LWWMapReader::<String, String>::new(bytes);
                    LWWMap::<String, String>::merge_from_readers(&[reader])?
            } else {
                LWWMap::new()
            };

            let delta_struct: LWWMapDelta<String, String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid LWWMap delta: {}", e)))?;

            if let Some(set) = delta_struct.set {
                for (k, v) in set {
                    crdt.insert(node_id, k, v, delta_struct.timestamp);
                }
            }
            if let Some(remove) = delta_struct.remove {
                for k in remove {
                    crdt.remove(&k);
                }
            }
                Ok(crdt.to_capnp_bytes())
        }
        CrdtType::ORMap => {
                let mut crdt: ORMap<String, String> = if let Some(bytes) = current_state_bytes {
                    let reader = ORMapReader::<String, String>::new(bytes);
                    ORMap::<String, String>::merge_from_readers(&[reader])?
            } else {
                ORMap::new()
            };

            let delta_struct: ORMapDelta<String, String> = serde_json::from_value(delta.clone())
                .map_err(|e| CrdtError::InvalidInput(format!("Invalid ORMap delta: {}", e)))?;

            if let Some(set) = delta_struct.set {
                for (k, v) in set {
                    crdt.insert(node_id, k, v);
                }
            }
            if let Some(remove) = delta_struct.remove {
                for k in remove {
                    crdt.remove(&k);
                }
            }
                Ok(crdt.to_capnp_bytes())
        }
    }
}

/// Apply a Cap'n Proto delta to a Cap'n Proto binary state.
pub fn apply_capnp_delta(
    crdt_type: CrdtType,
    current_state_bytes: Option<&[u8]>,
    delta_bytes: &[u8],
    node_id: &str,
) -> Result<Vec<u8>, CrdtError> {
    let mut delta_slice = delta_bytes;
    let message_reader = capnp::serialize::read_message(
        &mut delta_slice,
        capnp::message::ReaderOptions::new()
    ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

    let delta_reader = message_reader.get_root::<delta::Reader>()
        .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

    match (crdt_type, delta_reader.which().map_err(|e| CrdtError::Deserialization(e.to_string()))?) {
        (CrdtType::GCounter, delta::Which::GCounter(amount)) => {
                let mut crdt: GCounter = if let Some(bytes) = current_state_bytes {
                    let reader = GCounterReader::new(bytes);
                    GCounter::merge_from_readers(&[reader])?
            } else {
                GCounter::new()
            };
            crdt.increment(node_id, amount);
            Ok(crdt.to_capnp_bytes())
        }
        (CrdtType::PNCounter, delta::Which::PnCounter(amount)) => {
                let mut crdt: PNCounter = if let Some(bytes) = current_state_bytes {
                    let reader = PNCounterReader::new(bytes);
                    PNCounter::merge_from_readers(&[reader])?
            } else {
                PNCounter::new()
            };
            crdt.increment(node_id, amount);
            Ok(crdt.to_capnp_bytes())
        }
        (CrdtType::GSet, delta::Which::GSet(list_reader)) => {
                let mut crdt: GSet<String> = if let Some(bytes) = current_state_bytes {
                    let reader = GSetReader::<String>::new(bytes);
                    GSet::<String>::merge_from_readers(&[reader])?
            } else {
                GSet::new()
            };
            for res in list_reader.map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                crdt.insert(node_id, item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
            }
            Ok(crdt.to_capnp_bytes())
        }
        (CrdtType::ORSet, delta::Which::OrSet(orset_delta)) => {
                let mut crdt: ORSet<String> = if let Some(bytes) = current_state_bytes {
                    let reader = ORSetReader::<String>::new(bytes);
                    ORSet::<String>::merge_from_readers(&[reader])?
            } else {
                ORSet::new()
            };
            let orset_delta = orset_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
            let add = orset_delta.get_add().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            for res in add {
                let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                crdt.insert(node_id, item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
            }
            
            let remove = orset_delta.get_remove().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            for res in remove {
                let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                crdt.remove(&item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
            }
            Ok(crdt.to_capnp_bytes())
        }
        (CrdtType::LWWSet, delta::Which::LwwSet(lwwset_delta)) => {
                let mut crdt: LWWSet<String> = if let Some(bytes) = current_state_bytes {
                    let reader = LWWSetReader::<String>::new(bytes);
                    LWWSet::<String>::merge_from_readers(&[reader])?
            } else {
                LWWSet::new()
            };
            let lwwset_delta = lwwset_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let timestamp = lwwset_delta.get_timestamp();
            
            let add = lwwset_delta.get_add().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            for res in add {
                let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                crdt.insert(node_id, item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, timestamp);
            }
            
            let remove = lwwset_delta.get_remove().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            for res in remove {
                let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                crdt.remove(node_id, item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, timestamp);
            }
            Ok(crdt.to_capnp_bytes())
        }
        (CrdtType::LWWRegister, delta::Which::LwwRegister(reg_delta)) => {
                let mut crdt: LWWRegister<String> = if let Some(bytes) = current_state_bytes {
                    let reader = LWWRegisterReader::<String>::new(bytes);
                    LWWRegister::<String>::merge_from_readers(&[reader])?
            } else {
                LWWRegister::default()
            };
            let reg_delta = reg_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let value = reg_delta.get_value().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let timestamp = reg_delta.get_timestamp();
            crdt.set(value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, timestamp, node_id);
            Ok(crdt.to_capnp_bytes())
        }
        (CrdtType::FWWRegister, delta::Which::FwwRegister(reg_delta)) => {
                let mut crdt: FWWRegister<String> = if let Some(bytes) = current_state_bytes {
                    let reader = FWWRegisterReader::<String>::new(bytes);
                    FWWRegister::<String>::merge_from_readers(&[reader])?
            } else {
                FWWRegister::default()
            };
            let reg_delta = reg_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let value = reg_delta.get_value().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let timestamp = reg_delta.get_timestamp();
            crdt.set(value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, timestamp, node_id);
            Ok(crdt.to_capnp_bytes())
        }
        (CrdtType::MVRegister, delta::Which::MvRegister(val)) => {
                let mut crdt: MVRegister<String> = if let Some(bytes) = current_state_bytes {
                    let reader = MVRegisterReader::<String>::new(bytes);
                    MVRegister::<String>::merge_from_readers(&[reader])?
            } else {
                MVRegister::default()
            };
            let value = val.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            crdt.set(node_id, value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
            Ok(crdt.to_capnp_bytes())
        }
        (CrdtType::LWWMap, delta::Which::LwwMap(map_delta)) => {
                let mut crdt: LWWMap<String, String> = if let Some(bytes) = current_state_bytes {
                    let reader = LWWMapReader::<String, String>::new(bytes);
                    LWWMap::<String, String>::merge_from_readers(&[reader])?
            } else {
                LWWMap::new()
            };
            let map_delta = map_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let timestamp = map_delta.get_timestamp();

            let set = map_delta.get_set().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            for res in set {
                let entry = res; // Struct reader
                let key = entry.get_key().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let value = entry.get_value().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                crdt.insert(
                    node_id, 
                    key.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, 
                    value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, 
                    timestamp
                );
            }

            let remove = map_delta.get_remove().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            for res in remove {
                let key = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                crdt.remove(&key.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
            }
            Ok(crdt.to_capnp_bytes())
        }
        (CrdtType::ORMap, delta::Which::OrMap(map_delta)) => {
                let mut crdt: ORMap<String, String> = if let Some(bytes) = current_state_bytes {
                    let reader = ORMapReader::<String, String>::new(bytes);
                    ORMap::<String, String>::merge_from_readers(&[reader])?
            } else {
                ORMap::new()
            };
            let map_delta = map_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;

            let set = map_delta.get_set().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            for res in set {
                let entry = res; 
                let key = entry.get_key().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let value = entry.get_value().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                crdt.insert(
                    node_id, 
                    key.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, 
                    value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?
                );
            }

            let remove = map_delta.get_remove().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            for res in remove {
                let key = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                crdt.remove(&key.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
            }
            Ok(crdt.to_capnp_bytes())
        }
        _ => Err(CrdtError::InvalidInput("Delta type mismatch or invalid delta".into()))
    }
}

/// Apply a batch of Cap'n Proto deltas to a Cap'n Proto binary state.
pub fn apply_batch_capnp_deltas(
    crdt_type: CrdtType,
    current_state_bytes: Option<&[u8]>,
    deltas_bytes: &[&[u8]],
    node_id: &str,
) -> Result<Vec<u8>, CrdtError> {
    match crdt_type {
        CrdtType::GCounter => {
            let mut crdt: GCounter = if let Some(bytes) = current_state_bytes {
                let reader = GCounterReader::new(bytes);
                GCounter::merge_from_readers(&[reader])?
            } else {
                GCounter::new()
            };
            for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                if let delta::Which::GCounter(amount) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                    crdt.increment(node_id, amount);
                } else {
                    return Err(CrdtError::InvalidInput("Invalid delta for GCounter".into()));
                }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::PNCounter => {
            let mut crdt: PNCounter = if let Some(bytes) = current_state_bytes {
                let reader = PNCounterReader::new(bytes);
                PNCounter::merge_from_readers(&[reader])?
            } else {
                PNCounter::new()
            };
            for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                if let delta::Which::PnCounter(amount) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                    crdt.increment(node_id, amount);
                } else {
                    return Err(CrdtError::InvalidInput("Invalid delta for PNCounter".into()));
                }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::GSet => {
            let mut crdt: GSet<String> = if let Some(bytes) = current_state_bytes {
                let reader = GSetReader::<String>::new(bytes);
                GSet::<String>::merge_from_readers(&[reader])?
            } else {
                GSet::new()
            };
            for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                if let delta::Which::GSet(list_reader) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        for res in list_reader.map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        crdt.insert(node_id, item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
                    }
                } else {
                    return Err(CrdtError::InvalidInput("Invalid delta for GSet".into()));
                }
            }
            Ok(crdt.to_capnp_bytes())
        }
            CrdtType::ORSet => {
                let mut crdt: ORSet<String> = if let Some(bytes) = current_state_bytes {
                    let reader = ORSetReader::<String>::new(bytes);
                    ORSet::<String>::merge_from_readers(&[reader])?
            } else {
                ORSet::new()
            };
            for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                
                if let delta::Which::OrSet(orset_delta) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                    let orset_delta = orset_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    for res in orset_delta.get_add().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        crdt.insert(node_id, item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
                    }
                    for res in orset_delta.get_remove().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        crdt.remove(&item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
                    }
                } else {
                        return Err(CrdtError::InvalidInput("Invalid delta for ORSet".into()));
                }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::LWWSet => {
                let mut crdt: LWWSet<String> = if let Some(bytes) = current_state_bytes {
                    let reader = LWWSetReader::<String>::new(bytes);
                    LWWSet::<String>::merge_from_readers(&[reader])?
            } else {
                LWWSet::new()
            };
            for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    
                    if let delta::Which::LwwSet(lwwset_delta) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                    let lwwset_delta = lwwset_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    let timestamp = lwwset_delta.get_timestamp();
                    for res in lwwset_delta.get_add().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        crdt.insert(node_id, item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, timestamp);
                    }
                    for res in lwwset_delta.get_remove().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let item = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        crdt.remove(node_id, item.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, timestamp);
                    }
                    } else {
                        return Err(CrdtError::InvalidInput("Invalid delta for LWWSet".into()));
                    }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::LWWRegister => {
                let mut crdt: LWWRegister<String> = if let Some(bytes) = current_state_bytes {
                    let reader = LWWRegisterReader::<String>::new(bytes);
                    LWWRegister::<String>::merge_from_readers(&[reader])?
            } else {
                LWWRegister::default()
            };
            for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    
                    if let delta::Which::LwwRegister(reg_delta) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                    let reg_delta = reg_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    let value = reg_delta.get_value().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    let timestamp = reg_delta.get_timestamp();
                    crdt.set(value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, timestamp, node_id);
                    } else {
                        return Err(CrdtError::InvalidInput("Invalid delta for LWWRegister".into()));
                    }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::FWWRegister => {
                let mut crdt: FWWRegister<String> = if let Some(bytes) = current_state_bytes {
                    let reader = FWWRegisterReader::<String>::new(bytes);
                    FWWRegister::<String>::merge_from_readers(&[reader])?
            } else {
                FWWRegister::default()
            };
            for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    
                    if let delta::Which::FwwRegister(reg_delta) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                    let reg_delta = reg_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    let value = reg_delta.get_value().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    let timestamp = reg_delta.get_timestamp();
                    crdt.set(value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, timestamp, node_id);
                    } else {
                        return Err(CrdtError::InvalidInput("Invalid delta for FWWRegister".into()));
                    }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::MVRegister => {
                let mut crdt: MVRegister<String> = if let Some(bytes) = current_state_bytes {
                    let reader = MVRegisterReader::<String>::new(bytes);
                    MVRegister::<String>::merge_from_readers(&[reader])?
            } else {
                MVRegister::default()
            };
            for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    
                    if let delta::Which::MvRegister(val) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                    let value = val.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    crdt.set(node_id, value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
                    } else {
                        return Err(CrdtError::InvalidInput("Invalid delta for MVRegister".into()));
                    }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::LWWMap => {
                let mut crdt: LWWMap<String, String> = if let Some(bytes) = current_state_bytes {
                    let reader = LWWMapReader::<String, String>::new(bytes);
                    LWWMap::<String, String>::merge_from_readers(&[reader])?
            } else {
                LWWMap::new()
            };
            for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    
                    if let delta::Which::LwwMap(map_delta) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let map_delta = map_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    let timestamp = map_delta.get_timestamp();
                    for res in map_delta.get_set().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let entry = res;
                        let key = entry.get_key().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        let value = entry.get_value().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        crdt.insert(node_id, key.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, timestamp);
                    }
                    for res in map_delta.get_remove().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let key = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        crdt.remove(&key.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
                    }
                    } else {
                        return Err(CrdtError::InvalidInput("Invalid delta for LWWMap".into()));
                    }
            }
            Ok(crdt.to_capnp_bytes())
        }
        CrdtType::ORMap => {
                let mut crdt: ORMap<String, String> = if let Some(bytes) = current_state_bytes {
                    let reader = ORMapReader::<String, String>::new(bytes);
                    ORMap::<String, String>::merge_from_readers(&[reader])?
            } else {
                ORMap::new()
            };
                for bytes in deltas_bytes {
                let mut slice = *bytes;
                let message = capnp::serialize::read_message(&mut slice, capnp::message::ReaderOptions::new())
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                let root = message.get_root::<delta::Reader>()
                    .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    
                    if let delta::Which::OrMap(map_delta) = root.which().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let map_delta = map_delta.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    for res in map_delta.get_set().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let entry = res;
                        let key = entry.get_key().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        let value = entry.get_value().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        crdt.insert(node_id, key.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?, value.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
                    }
                    for res in map_delta.get_remove().map_err(|e| CrdtError::Deserialization(e.to_string()))? {
                        let key = res.map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                        crdt.remove(&key.to_string().map_err(|e| CrdtError::Deserialization(e.to_string()))?);
                    }
                    } else {
                        return Err(CrdtError::InvalidInput("Invalid delta for ORMap".into()));
                    }
            }
            Ok(crdt.to_capnp_bytes())
        }
    }
}
