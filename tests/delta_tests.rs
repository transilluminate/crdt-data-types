use crdt_data_types::{SerdeCapnpBridge, CrdtType};
use serde_json::json;

#[test]
fn test_delta_gcounter() {
    // 1. New GCounter (0) + 5
    let state = SerdeCapnpBridge::apply_json_delta(
        CrdtType::GCounter,
        None,
        &json!(5),
        "node_a"
    ).unwrap();
    
    // Check it's 5
    let counters_obj = state.get("counters").unwrap().as_object().unwrap();
    assert_eq!(counters_obj.get("node_a").unwrap().as_i64(), Some(5));

    // 2. Existing GCounter (5) + 10 = 15
    let state2 = SerdeCapnpBridge::apply_json_delta(
        CrdtType::GCounter,
        Some(&state),
        &json!({"increment": 10}),
        "node_a"
    ).unwrap();
    
    let counters_obj2 = state2.get("counters").unwrap().as_object().unwrap();
    assert_eq!(counters_obj2.get("node_a").unwrap().as_i64(), Some(15));
    
    // 3. Different node
    let state3 = SerdeCapnpBridge::apply_json_delta(
        CrdtType::GCounter,
        Some(&state2),
        &json!(20),
        "node_b"
    ).unwrap();
    
    let counters_obj3 = state3.get("counters").unwrap().as_object().unwrap();
    assert_eq!(counters_obj3.get("node_a").unwrap().as_i64(), Some(15));
    assert_eq!(counters_obj3.get("node_b").unwrap().as_i64(), Some(20));
}

#[test]
fn test_delta_gset() {
    // 1. New GSet + ["a", "b"]
    let state = SerdeCapnpBridge::apply_json_delta(
        CrdtType::GSet,
        None,
        &json!(["a", "b"]),
        "node_a"
    ).unwrap();
    
    let elements = state.get("elements").unwrap().as_array().unwrap();
    assert!(elements.contains(&json!("a")));
    assert!(elements.contains(&json!("b")));

    // 2. Existing GSet + {"add": ["c"]}
    let state2 = SerdeCapnpBridge::apply_json_delta(
        CrdtType::GSet,
        Some(&state),
        &json!({"add": ["c"]}),
        "node_a"
    ).unwrap();
    
     let elements2 = state2.get("elements").unwrap().as_array().unwrap();
     assert!(elements2.contains(&json!("a")));
     assert!(elements2.contains(&json!("b")));
     assert!(elements2.contains(&json!("c")));
}

#[test]
fn test_delta_orset() {
    let state = SerdeCapnpBridge::apply_json_delta(
        CrdtType::ORSet,
        None,
        &json!({"add": ["apple"]}),
        "node_a"
    ).unwrap();

    // Check contains apple
    // ORSet serialization is object {"elements": [...], "vclock": ...}
    let elements = state.get("elements").unwrap().as_array().unwrap();
    assert_eq!(elements.len(), 1);
    assert_eq!(elements[0].get("element").unwrap().as_str(), Some("apple"));

    // Remove apple
    let state2 = SerdeCapnpBridge::apply_json_delta(
        CrdtType::ORSet,
        Some(&state),
        &json!({"remove": ["apple"]}),
        "node_b" // Removed by B
    ).unwrap();

    let elements2 = state2.get("elements").unwrap().as_array().unwrap();
    // In this ORSet implementation, remove clears the entry if found.
    // So it should be empty array.
    assert!(elements2.is_empty());
}

#[test]
fn test_delta_lwwregister() {
    let state = SerdeCapnpBridge::apply_json_delta(
        CrdtType::LWWRegister,
        None,
        &json!({"value": "first", "timestamp": 100}),
        "node_a"
    ).unwrap();
    
    assert_eq!(state.get("value").unwrap().as_str(), Some("first"));
    assert_eq!(state.get("timestamp").unwrap().as_u64(), Some(100));

    // Update with older timestamp (should fail/ignore)
    let state2 = SerdeCapnpBridge::apply_json_delta(
        CrdtType::LWWRegister,
        Some(&state),
        &json!({"value": "ignore_me", "timestamp": 50}),
        "node_a" // Same node, older ts
    ).unwrap();
    
    assert_eq!(state2.get("value").unwrap().as_str(), Some("first")); // Still first

    // Update with newer timestamp
    let state3 = SerdeCapnpBridge::apply_json_delta(
        CrdtType::LWWRegister,
        Some(&state),
        &json!({"value": "second", "timestamp": 200}),
        "node_a"
    ).unwrap();
    
    assert_eq!(state3.get("value").unwrap().as_str(), Some("second"));
    assert_eq!(state3.get("timestamp").unwrap().as_u64(), Some(200));
}

#[test]
fn test_delta_lwwmap() {
    // 1. Set key1=v1
    let state = SerdeCapnpBridge::apply_json_delta(
        CrdtType::LWWMap,
        None,
        &json!({
            "set": {"key1": "v1"},
            "timestamp": 100
        }),
        "node_a"
    ).unwrap();

    // LWWMap serialization: "entries": [ [key, [val, ts, nid]] ... ] or similar?
    // Let's check logic rather than representation structure details if possible, or just print it.
    // LWWMap uses specific serialize_with.
    // src/lww_map.rs: serialize_entries -> map.
    // entries: {"key1": ["v1", 100, "node_a"]}
    
    let entries = state.get("entries").unwrap().as_object().unwrap();
    let entry = entries.get("key1").unwrap().as_array().unwrap();
    assert_eq!(entry[0].as_str(), Some("v1"));
    assert_eq!(entry[1].as_u64(), Some(100));

    // 2. Remove key1
    let state2 = SerdeCapnpBridge::apply_json_delta(
        CrdtType::LWWMap,
        Some(&state),
        &json!({
            "remove": ["key1"],
            "timestamp": 200 // Timestamp required by our bridge implementation for LWWMap
        }),
        "node_a"
    ).unwrap();
    
    let entries2 = state2.get("entries").unwrap().as_object().unwrap();
    assert!(entries2.get("key1").is_none());
}

#[test]
fn test_delta_bytes_gcounter() {
    // 1. New GCounter (0) + 5
    let bytes = SerdeCapnpBridge::apply_bytes_delta(
        CrdtType::GCounter,
        None,
        &json!(5),
        "node_a"
    ).unwrap();
    
    // Verify by converting to JSON
    let state = SerdeCapnpBridge::capnp_bytes_to_json(CrdtType::GCounter, &bytes).unwrap();
    let counters_obj = state.get("counters").unwrap().as_object().unwrap();
    assert_eq!(counters_obj.get("node_a").unwrap().as_i64(), Some(5));

    // 2. Existing GCounter (5) + 10 = 15
    let bytes2 = SerdeCapnpBridge::apply_bytes_delta(
        CrdtType::GCounter,
        Some(&bytes),
        &json!({"increment": 10}),
        "node_a"
    ).unwrap();
    
    let state2 = SerdeCapnpBridge::capnp_bytes_to_json(CrdtType::GCounter, &bytes2).unwrap();
    let counters_obj2 = state2.get("counters").unwrap().as_object().unwrap();
    assert_eq!(counters_obj2.get("node_a").unwrap().as_i64(), Some(15));
}

#[test]
fn test_delta_bytes_lwwmap() {
    let bytes = SerdeCapnpBridge::apply_bytes_delta(
        CrdtType::LWWMap,
        None,
        &json!({
            "set": {"k1": "v1"},
            "timestamp": 100
        }),
        "node_a"
    ).unwrap();
    
    let state = SerdeCapnpBridge::capnp_bytes_to_json(CrdtType::LWWMap, &bytes).unwrap();
    let entries = state.get("entries").unwrap().as_object().unwrap();
    let entry = entries.get("k1").unwrap().as_array().unwrap();
    assert_eq!(entry[0].as_str(), Some("v1")); 
}
