// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crdt_data_types::*;

#[test]
fn test_gcounter() {
    let mut c1 = GCounter::new();
    c1.increment("node1", 5);
    let mut c2 = GCounter::new();
    c2.increment("node2", 3);

    c1.merge(&c2);
    assert_eq!(c1.value(), 8);
}

#[test]
fn test_pncounter() {
    let mut c1 = PNCounter::new();
    c1.increment("node1", 5);
    c1.decrement("node1", 2);

    assert_eq!(c1.value(), 3);
}

#[test]
fn test_gset() {
    let mut s1 = GSet::new();
    s1.insert("node1", "apple".to_string());
    let mut s2 = GSet::new();
    s2.insert("node2", "banana".to_string());

    s1.merge(&s2);
    assert!(s1.contains(&"apple".to_string()));
    assert!(s1.contains(&"banana".to_string()));
}

#[test]
fn test_orset() {
    let mut s1 = ORSet::new();
    s1.insert("node1", "apple".to_string());
    s1.remove(&"apple".to_string());

    assert!(!s1.contains(&"apple".to_string()));

    let mut s2 = ORSet::new();
    s2.insert("node1", "apple".to_string());

    s1.merge(&s2);
    // Since s1's vclock[node1] is 1, it overshadows (node1, 1) from s2.
    assert!(!s1.contains(&"apple".to_string()));
}

#[test]
fn test_lww_register() {
    // Correcting LWWRegister::new(value, timestamp, node_id)
    let mut r1 = LWWRegister::new("initial".to_string(), 0, "node1".to_string());
    r1.set("val1".to_string(), 100, "node1");

    let mut r2 = LWWRegister::new("initial".to_string(), 0, "node2".to_string());
    r2.set("val2".to_string(), 200, "node2");

    r1.merge(&r2);
    assert_eq!(r1.value, "val2");
}

#[test]
fn test_zero_copy_merge() {
    let mut s1 = GSet::new();
    s1.insert("node1", "a".to_string());
    let bytes1 = s1.to_capnp_bytes();

    let mut s2 = GSet::new();
    s2.insert("node2", "b".to_string());
    let bytes2 = s2.to_capnp_bytes();

    let reader1 = GSetReader::new(&bytes1);
    let reader2 = GSetReader::new(&bytes2);

    let merged = GSet::merge_from_readers(&[reader1, reader2]).unwrap();
    assert!(merged.contains(&"a".to_string()));
    assert!(merged.contains(&"b".to_string()));
}
