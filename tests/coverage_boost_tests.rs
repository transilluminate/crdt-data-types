// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crdt_data_types::*;
use serde_json::json;

// ============================================================================
// FWWRegister Tests
// ============================================================================

#[test]
fn test_fww_register_logic() {
    let mut reg = FWWRegister::new("initial".to_string(), 100, "node_a");

    // Try to set with higher timestamp (should fail - First Write Wins)
    reg.set("newer".to_string(), 200, "node_a");
    assert_eq!(reg.value, "initial");
    assert_eq!(reg.timestamp, 100);

    // Try to set with lower timestamp (should succeed)
    reg.set("older".to_string(), 50, "node_a");
    assert_eq!(reg.value, "older");
    assert_eq!(reg.timestamp, 50);

    // Tie-breaking: same timestamp, lower node_id wins
    reg.set("tie_loser".to_string(), 50, "node_z");
    assert_eq!(reg.value, "older"); 
    
    reg.set("tie_winner".to_string(), 50, "node_0"); // "node_0" < "node_a"
    assert_eq!(reg.value, "tie_winner");
    assert_eq!(reg.node_id, "node_0");
}

#[test]
fn test_fww_register_capnp_roundtrip() {
    let reg = FWWRegister::new("data".to_string(), 12345, "node_x");
    let bytes = reg.to_capnp_bytes();
    
    let reader = FWWRegisterReader::<String>::new(&bytes);
    let decoded = FWWRegister::<String>::merge_from_readers(&[reader]).unwrap();
    
    assert_eq!(decoded.value, "data");
    assert_eq!(decoded.timestamp, 12345);
    assert_eq!(decoded.node_id, "node_x");
}

// ============================================================================
// PNCounter Tests
// ============================================================================

#[test]
fn test_pn_counter_negative_deltas() {
    let mut pn = PNCounter::new();
    
    // Increment with negative value -> should be decrement
    pn.increment("node_a", -10);
    assert_eq!(pn.value(), -10);
    assert_eq!(pn.negative.value(), 10);
    assert_eq!(pn.positive.value(), 0);
    
    // Decrement with negative value -> should be increment
    pn.decrement("node_a", -5); // -(-5) = +5
    assert_eq!(pn.value(), -5); // -10 + 5 = -5
    assert_eq!(pn.positive.value(), 5);
}

#[test]
fn test_pn_counter_capnp_roundtrip() {
    let mut pn = PNCounter::new();
    pn.increment("node_a", 100);
    pn.decrement("node_b", 50);
    
    let bytes = pn.to_capnp_bytes();
    let reader = PNCounterReader::new(&bytes);
    let decoded = PNCounter::merge_from_readers(&[reader]).unwrap();
    
    assert_eq!(decoded.value(), 50);
    assert_eq!(decoded.positive.value(), 100);
    assert_eq!(decoded.negative.value(), 50);
}

// ============================================================================
// Bridge Coverage Tests
// ============================================================================

#[test]
fn test_bridge_gset() {
    let json = json!({
        "elements": ["a", "b", "c"],
        "vclock": { "clocks": {} }
    });
    let bytes = SerdeCapnpBridge::json_to_capnp_bytes("GSet", json.clone()).unwrap();
    let back = SerdeCapnpBridge::capnp_bytes_to_json("GSet", &bytes).unwrap();
    
    let set: GSet<String> = serde_json::from_value(back).unwrap();
    assert!(set.elements.contains(&"a".to_string()));
    assert!(set.elements.contains(&"b".to_string()));
    assert!(set.elements.contains(&"c".to_string()));
    
    // Merge
    let json2 = json!({
        "elements": ["d"],
        "vclock": { "clocks": {} }
    });
    let merged = SerdeCapnpBridge::merge_json_values("GSet", &[json, json2]).unwrap();
    let set_merged: GSet<String> = serde_json::from_value(merged).unwrap();
    assert!(set_merged.elements.contains(&"d".to_string()));
    assert!(set_merged.elements.contains(&"a".to_string()));
}

#[test]
fn test_bridge_orset() {
    // ORSet expects elements as a sequence of objects: { element: T, observations: ... }
    let json = json!({
        "elements": [
            { "element": "elem1", "observations": [["node1", 1], ["node2", 2]] },
            { "element": "elem2", "observations": [["node1", 3]] }
        ],
        "vclock": { "clocks": {} }
    });
    
    let bytes = SerdeCapnpBridge::json_to_capnp_bytes("ORSet", json.clone()).unwrap();
    let back = SerdeCapnpBridge::capnp_bytes_to_json("ORSet", &bytes).unwrap();
    
    // Check roundtrip
    let set: ORSet<String> = serde_json::from_value(back).unwrap();
    assert!(set.elements.iter().any(|(e, _)| e == "elem1"));
    
    // Merge
    let json2 = json!({
        "elements": [
            { "element": "elem3", "observations": [["node3", 4]] }
        ],
        "vclock": { "clocks": {} }
    });
    let merged = SerdeCapnpBridge::merge_json_values("ORSet", &[json, json2]).unwrap();
    let merged_set: ORSet<String> = serde_json::from_value(merged).unwrap();
    assert!(merged_set.elements.iter().any(|(e, _)| e == "elem3"));
}

#[test]
fn test_bridge_lww_register() {
    let json = json!({
        "value": "test_val",
        "timestamp": 100,
        "node_id": "node1",
        "vclock": { "clocks": {} }
    });
    
    let bytes = SerdeCapnpBridge::json_to_capnp_bytes("LWWRegister", json.clone()).unwrap();
    let back = SerdeCapnpBridge::capnp_bytes_to_json("LWWRegister", &bytes).unwrap();
    assert_eq!(back["value"], "test_val");
    
    // Merge
    let json2 = json!({
        "value": "newer_val",
        "timestamp": 200,
        "node_id": "node1",
        "vclock": { "clocks": {} }
    });
    let merged = SerdeCapnpBridge::merge_json_values("LWWRegister", &[json, json2]).unwrap();
    assert_eq!(merged["value"], "newer_val");
}

#[test]
fn test_bridge_fww_register() {
    let json = json!({
        "value": "first_val",
        "timestamp": 100,
        "node_id": "node1",
        "vclock": { "clocks": {} }
    });
    
    let bytes = SerdeCapnpBridge::json_to_capnp_bytes("FWWRegister", json.clone()).unwrap();
    let back = SerdeCapnpBridge::capnp_bytes_to_json("FWWRegister", &bytes).unwrap();
    assert_eq!(back["value"], "first_val");
    
    // Merge (older timestamp wins)
    let json2 = json!({
        "value": "older_val",
        "timestamp": 50,
        "node_id": "node1",
        "vclock": { "clocks": {} }
    });
    let merged = SerdeCapnpBridge::merge_json_values("FWWRegister", &[json, json2]).unwrap();
    assert_eq!(merged["value"], "older_val");
}

#[test]
fn test_bridge_lwwset() {
    // LWWSet: add_set and remove_set are maps: element -> (timestamp, node_id)
    let json = json!({
        "add_set": {
            "item1": [100, "node1"]
        },
        "remove_set": {},
        "vclock": { "clocks": {} }
    });
    
    let bytes = SerdeCapnpBridge::json_to_capnp_bytes("LWWSet", json.clone()).unwrap();
    let back = SerdeCapnpBridge::capnp_bytes_to_json("LWWSet", &bytes).unwrap();
    
    let set: LWWSet<String> = serde_json::from_value(back).unwrap();
    assert!(set.add_set.iter().any(|(e, _)| e == "item1"));
    
    // Merge
    let json2 = json!({
        "add_set": {
            "item2": [100, "node1"]
        },
        "remove_set": {},
        "vclock": { "clocks": {} }
    });
    let merged = SerdeCapnpBridge::merge_json_values("LWWSet", &[json, json2]).unwrap();
    let merged_set: LWWSet<String> = serde_json::from_value(merged).unwrap();
    assert!(merged_set.add_set.iter().any(|(e, _)| e == "item1"));
    assert!(merged_set.add_set.iter().any(|(e, _)| e == "item2"));
}

#[test]
fn test_bridge_ormap() {
    // ORMap wraps ORSet<(K, V)>.
    // ORSet serializes as a sequence of objects.
    // ORMap has fields: elements (ORSet), vclock.
    let json = json!({
        "elements": {
            "elements": [
                { 
                    "element": ["key1", "val1"], 
                    "observations": [["node1", 1]] 
                }
            ],
            "vclock": { "clocks": {} }
        },
        "vclock": { "clocks": {} }
    });
    
    let bytes = SerdeCapnpBridge::json_to_capnp_bytes("ORMap", json.clone()).unwrap();
    let back = SerdeCapnpBridge::capnp_bytes_to_json("ORMap", &bytes).unwrap();
    
    let map: ORMap<String, String> = serde_json::from_value(back).unwrap();
    assert!(!map.elements.is_empty());
    
    // Merge
    let json2 = json!({
        "elements": {
            "elements": [
                { 
                    "element": ["key2", "val2"], 
                    "observations": [["node2", 2]] 
                }
            ],
            "vclock": { "clocks": {} }
        },
        "vclock": { "clocks": {} }
    });
    let merged = SerdeCapnpBridge::merge_json_values("ORMap", &[json, json2]).unwrap();
    let merged_map: ORMap<String, String> = serde_json::from_value(merged).unwrap();
    assert!(merged_map.elements.iter().any(|(k, v)| k == "key2" && v == "val2"));
}

#[test]
fn test_bridge_errors() {
    // Unknown type
    assert!(SerdeCapnpBridge::json_to_capnp_bytes("UnknownType", json!({})).is_err());
    assert!(SerdeCapnpBridge::capnp_bytes_to_json("UnknownType", &[]).is_err());
    assert!(SerdeCapnpBridge::merge_json_values("UnknownType", &[json!({})]).is_err());
    
    // Invalid JSON for type
    assert!(SerdeCapnpBridge::json_to_capnp_bytes("GCounter", json!(["not", "a", "counter"])).is_err());
    
    // Empty merge
    assert!(SerdeCapnpBridge::merge_json_values("GCounter", &[]).unwrap().is_null());
}
