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
