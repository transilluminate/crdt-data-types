// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crdt_data_types::*;
use crdt_data_types::enums::CrdtType;
use serde_json::json;

#[test]
fn test_gcounter_additive_merge() {
    // Correct JSON structure for VectorClock: (counter, epoch)
    let current_json = json!({
        "counters": {
            "node1": 10
        },
        "vclock": {
            "clocks": {"node1": [1, 0]} 
        }
    });

    let accumulated_json = json!({
        "counters": {
            "node1": 5, 
            "node2": 2
        },
        "vclock": {
            "clocks": {"node2": [1, 0], "node1": [1, 0]}
        }
    });

    // Expect additive behavior: node1 should be 10 + 5 = 15
    // node2 should be 0 + 2 = 2
    let res = SerdeCapnpBridge::add_accumulated_state(
        CrdtType::GCounter, 
        current_json, 
        accumulated_json
    ).unwrap();

    let expected_counters = json!({
        "node1": 15,
        "node2": 2
    });

    assert_eq!(res["counters"], expected_counters);
}

#[test]
fn test_pncounter_additive_merge() {
    let current_json = json!({
        "positive": { "counters": { "node1": 10 }, "vclock": { "clocks": {} } },
        "negative": { "counters": { "node1": 2 }, "vclock": { "clocks": {} } },
        "vclock": { "clocks": {} }
    });

    let accumulated_json = json!({
        "positive": { "counters": { "node1": 5 }, "vclock": { "clocks": {} } },
        "negative": { "counters": { "node1": 1 }, "vclock": { "clocks": {} } },
        "vclock": { "clocks": {} }
    });

    // Expect additive:
    // Positive: 10 + 5 = 15
    // Negative: 2 + 1 = 3
    // Total value: 15 - 3 = 12
    let res = SerdeCapnpBridge::add_accumulated_state(
        CrdtType::PNCounter, 
        current_json, 
        accumulated_json
    ).unwrap();

    assert_eq!(res["positive"]["counters"]["node1"], 15);
    assert_eq!(res["negative"]["counters"]["node1"], 3);
}

#[test]
fn test_fallback_merge_orset() {
    // Construct ORSets using the Struct API to ensure valid JSON structure
    let mut current_set: ORSet<String> = ORSet::new();
    current_set.insert("node1", "A".to_string());
    
    let mut accumulated_set: ORSet<String> = ORSet::new();
    accumulated_set.insert("node2", "B".to_string());

    let current_json = serde_json::to_value(current_set).unwrap();
    let accumulated_json = serde_json::to_value(accumulated_set).unwrap();

    // The add_accumulated_state should fall back to standard merge for ORSet
    let res = SerdeCapnpBridge::add_accumulated_state(
        CrdtType::ORSet, 
        current_json, 
        accumulated_json
    ).unwrap();

    // Result should look like an ORSet containing "A" and "B"
    let merged_set: ORSet<String> = serde_json::from_value(res).unwrap();
    
    assert!(merged_set.contains(&"A".to_string()));
    assert!(merged_set.contains(&"B".to_string()));
}
