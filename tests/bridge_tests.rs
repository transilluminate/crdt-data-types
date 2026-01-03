// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crdt_data_types::*;
use serde_json::json;

#[test]
fn test_gcounter_bridge_roundtrip() {
    let initial_json = json!({
        "counters": {
            "node1": 10,
            "node2": 20
        },
        "vclock": {
            "clocks": {}
        }
    });

    let bytes = SerdeCapnpBridge::json_to_capnp_bytes("GCounter", initial_json.clone()).unwrap();
    let final_json = SerdeCapnpBridge::capnp_bytes_to_json("GCounter", &bytes).unwrap();

    // Note: GCounter internal vclock might be updated by increment but here we just test roundtrip of state
    assert_eq!(initial_json["counters"], final_json["counters"]);
}

#[test]
fn test_lwwmap_bridge_roundtrip() {
    let initial_json = json!({
        "entries": {
            "key1": ["val1", 100, "node1"],
            "key2": ["val2", 200, "node2"]
        },
        "vclock": {
            "clocks": {}
        }
    });

    let bytes = SerdeCapnpBridge::json_to_capnp_bytes("LWWMap", initial_json.clone()).unwrap();
    let final_json = SerdeCapnpBridge::capnp_bytes_to_json("LWWMap", &bytes).unwrap();

    assert_eq!(initial_json["entries"], final_json["entries"]);
}

#[test]
fn test_mvregister_bridge_roundtrip() {
    let initial_json = json!({
        "entries": {
            "val1": [["node1", 1]]
        },
        "vclock": {
            "clocks": {
                "node1": [1, 1000]
            }
        }
    });

    let bytes = SerdeCapnpBridge::json_to_capnp_bytes("MVRegister", initial_json.clone()).unwrap();
    let final_json = SerdeCapnpBridge::capnp_bytes_to_json("MVRegister", &bytes).unwrap();

    assert_eq!(initial_json["entries"], final_json["entries"]);
}

#[test]
fn test_merge_json_values_gcounter() {
    let json1 = json!({
        "counters": { "node1": 10 },
        "vclock": { "clocks": {} }
    });
    let json2 = json!({
        "counters": { "node1": 5, "node2": 20 },
        "vclock": { "clocks": {} }
    });

    let merged = SerdeCapnpBridge::merge_json_values("GCounter", &[json1, json2]).unwrap();

    assert_eq!(merged["counters"]["node1"], 10);
    assert_eq!(merged["counters"]["node2"], 20);
}

#[test]
fn test_merge_json_values_pncounter() {
    let json1 = json!({
        "positive": { "counters": { "node1": 10 }, "vclock": { "clocks": {} } },
        "negative": { "counters": { "node2": 5 }, "vclock": { "clocks": {} } },
        "vclock": { "clocks": {} }
    });
    let json2 = json!({
        "positive": { "counters": { "node1": 5, "node3": 15 }, "vclock": { "clocks": {} } },
        "negative": { "counters": { "node2": 10 }, "vclock": { "clocks": {} } },
        "vclock": { "clocks": {} }
    });

    let merged = SerdeCapnpBridge::merge_json_values("PNCounter", &[json1, json2]).unwrap();

    assert_eq!(merged["positive"]["counters"]["node1"], 10);
    assert_eq!(merged["positive"]["counters"]["node3"], 15);
    assert_eq!(merged["negative"]["counters"]["node2"], 10);
}

#[test]
fn test_case_insensitive_input() {
    let json_data = json!({
        "counters": {"node_a": 10},
        "vclock": {"clocks": {"node_a": [1, 100]}}
    });

    // Test snake_case
    let result_snake = SerdeCapnpBridge::json_to_capnp_bytes("g_counter", json_data.clone());
    assert!(result_snake.is_ok(), "Expected success for snake_case input");

    // Test lowercase
    let result_lower = SerdeCapnpBridge::json_to_capnp_bytes("gcounter", json_data.clone());
    assert!(result_lower.is_ok(), "Expected success for lowercase input");

    // Test PascalCase
    let result_pascal = SerdeCapnpBridge::json_to_capnp_bytes("GCounter", json_data);
    assert!(result_pascal.is_ok(), "Expected success for PascalCase input");
}

