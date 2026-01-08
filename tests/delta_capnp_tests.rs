use crdt_data_types::{SerdeCapnpBridge, CrdtType};
use crdt_data_types::deltas_capnp::delta;
use capnp::serialize;

#[test]
fn test_capnp_delta_gcounter() {
    // 1. Create delta bytes
    let mut message = capnp::message::Builder::new_default();
    let mut root = message.init_root::<delta::Builder>();
    root.set_g_counter(10);
    
    let mut delta_bytes = Vec::new();
    serialize::write_message(&mut delta_bytes, &message).unwrap();
    
    // 2. Apply (empty state)
    let result_bytes = SerdeCapnpBridge::apply_delta_capnp(
        CrdtType::GCounter,
        None,
        &delta_bytes,
        "node1"
    ).unwrap();
    
    // 3. Verify result (convert to JSON for easy check)
    let json_val = SerdeCapnpBridge::capnp_bytes_to_json(CrdtType::GCounter, &result_bytes).unwrap();
    assert_eq!(json_val["counters"]["node1"], 10);
}

#[test]
fn test_capnp_delta_gset() {
    // 1. Create delta bytes
    let mut message = capnp::message::Builder::new_default();
    let root = message.init_root::<delta::Builder>();
    let mut list = root.init_g_set(2);
    list.set(0, "A".into());
    list.set(1, "B".into());
    
    let mut delta_bytes = Vec::new();
    serialize::write_message(&mut delta_bytes, &message).unwrap();
    
    // 2. Apply (empty state)
    let result_bytes = SerdeCapnpBridge::apply_delta_capnp(
        CrdtType::GSet,
        None,
        &delta_bytes,
        "node1"
    ).unwrap();
    
    // 3. Verify result
    let json_val = SerdeCapnpBridge::capnp_bytes_to_json(CrdtType::GSet, &result_bytes).unwrap();
    // GSet serializes to a struct with an "elements" field
    let elements = json_val["elements"].as_array().unwrap();
    let contains_a = elements.iter().any(|v| v.as_str() == Some("A"));
    let contains_b = elements.iter().any(|v| v.as_str() == Some("B"));
    assert!(contains_a);
    assert!(contains_b);
}

#[test]
fn test_capnp_delta_lwwregister() {
    let mut message = capnp::message::Builder::new_default();
    let root = message.init_root::<delta::Builder>();
    let mut lww = root.init_lww_register();
    lww.set_value("new_val".into());
    lww.set_timestamp(1000);
    
    let mut delta_bytes = Vec::new();
    serialize::write_message(&mut delta_bytes, &message).unwrap();
    
    let result_bytes = SerdeCapnpBridge::apply_delta_capnp(
        CrdtType::LWWRegister,
        None,
        &delta_bytes,
        "node1"
    ).unwrap();
    
    let json_val = SerdeCapnpBridge::capnp_bytes_to_json(CrdtType::LWWRegister, &result_bytes).unwrap();
    assert_eq!(json_val["value"], serde_json::json!("new_val"));
}

#[test]
fn test_capnp_batch_deltas_gcounter() {
    // Delta 1: +10
    let mut message1 = capnp::message::Builder::new_default();
    message1.init_root::<delta::Builder>().set_g_counter(10);
    let mut delta1_bytes = Vec::new();
    serialize::write_message(&mut delta1_bytes, &message1).unwrap();

    // Delta 2: +5
    let mut message2 = capnp::message::Builder::new_default();
    message2.init_root::<delta::Builder>().set_g_counter(5);
    let mut delta2_bytes = Vec::new();
    serialize::write_message(&mut delta2_bytes, &message2).unwrap();

    // Delta 3: +20
    let mut message3 = capnp::message::Builder::new_default();
    message3.init_root::<delta::Builder>().set_g_counter(20);
    let mut delta3_bytes = Vec::new();
    serialize::write_message(&mut delta3_bytes, &message3).unwrap();

    let batch = vec![delta1_bytes.as_slice(), delta2_bytes.as_slice(), delta3_bytes.as_slice()];

    // Apply batch (Starting from None)
    let result_bytes = SerdeCapnpBridge::apply_batch_deltas_capnp(
        CrdtType::GCounter,
        None,
        &batch,
        "node1",
    ).unwrap();

    // Verify: Total should be 35
    let json_val = SerdeCapnpBridge::capnp_bytes_to_json(CrdtType::GCounter, &result_bytes).unwrap();
    assert_eq!(json_val["counters"]["node1"], 35);
}
