// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crdt_data_types::*;
use crdt_data_types::compaction::compact_capnp_bytes;
use crdt_data_types::enums::CrdtType;
use std::time::Duration;

// ============================================================================
// Vector Clock Tests
// ============================================================================

#[test]
fn test_vector_clock_happens_before() {
    let mut vc1 = VectorClock::new();
    let mut vc2 = VectorClock::new();

    // Empty clocks are not strictly less than each other (they are equal)
    assert!(!vc1.happens_before(&vc2));
    assert!(!vc2.happens_before(&vc1));

    // vc1: {A: 1}
    vc1.increment("A");
    // vc1 > vc2 (since vc2 is empty/zeros)
    assert!(!vc1.happens_before(&vc2));
    // vc2 < vc1
    assert!(vc2.happens_before(&vc1));

    // vc2: {A: 1}
    vc2.merge(&vc1);
    // Equal again
    assert!(!vc1.happens_before(&vc2));

    // vc2: {A: 1, B: 1}
    vc2.increment("B");
    // vc1 < vc2
    assert!(vc1.happens_before(&vc2));

    // vc1: {A: 2}
    vc1.increment("A");
    // Concurrent: vc1 has more A, vc2 has more B
    assert!(!vc1.happens_before(&vc2));
    assert!(!vc2.happens_before(&vc1));
}

#[test]
fn test_vector_clock_stability() {
    let mut vc = VectorClock::new();
    
    // Empty clock is not stable (or doesn't make sense to be)
    assert!(!vc.is_stable_for(Duration::from_secs(1)));

    vc.increment("A");
    
    // Should not be stable immediately (unless duration is 0, but let's test normal case)
    assert!(!vc.is_stable_for(Duration::from_secs(10)));

    // The implementation uses seconds resolution for timestamps.
    // So we need to wait at least 1 second for the timestamp to be "old enough".
    // However, waiting 1 second in a unit test is bad practice.
    // We can't easily mock SystemTime without refactoring.
    // So we will just test the negative case which we already did.
    // And we can test that if we manually insert an old timestamp, it works.
    
    let old_ts = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() - 100;
        
    vc.clocks.insert("B".to_string(), (1, old_ts));
    
    // "A" is still recent, so it should fail
    assert!(!vc.is_stable_for(Duration::from_secs(10)));
    
    // If we remove A, leaving only B (old), it should pass
    vc.clocks.remove("A");
    assert!(vc.is_stable_for(Duration::from_secs(10)));
}

#[test]
fn test_vector_clock_hashing() {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut vc1 = VectorClock::new();
    vc1.increment("A");
    vc1.increment("B");

    let mut vc2 = VectorClock::new();
    vc2.increment("B");
    vc2.increment("A");

    // Hashing should be order-independent of insertion
    let mut h1 = DefaultHasher::new();
    vc1.hash(&mut h1);
    
    let mut h2 = DefaultHasher::new();
    vc2.hash(&mut h2);

    // Note: Timestamps might differ slightly, so we might need to manually set them equal
    // to test hash consistency.
    let ts = 1000;
    vc1.clocks.insert("A".to_string(), (1, ts));
    vc1.clocks.insert("B".to_string(), (1, ts));
    
    vc2.clocks.insert("A".to_string(), (1, ts));
    vc2.clocks.insert("B".to_string(), (1, ts));

    let mut h1 = DefaultHasher::new();
    vc1.hash(&mut h1);
    
    let mut h2 = DefaultHasher::new();
    vc2.hash(&mut h2);

    assert_eq!(h1.finish(), h2.finish());
}

// ============================================================================
// Compaction Tests
// ============================================================================

#[test]
fn test_compact_capnp_all_types() {
    // GSet
    let mut gset = GSet::new();
    gset.insert("node", "val".to_string());
    let bytes = gset.to_capnp_bytes();
    let res = compact_capnp_bytes(CrdtType::GSet, &[&bytes]).unwrap();
    assert!(!res.is_empty());

    // ORSet
    let mut orset = ORSet::new();
    orset.insert("node", "val".to_string());
    let bytes = orset.to_capnp_bytes();
    let res = compact_capnp_bytes(CrdtType::ORSet, &[&bytes]).unwrap();
    assert!(!res.is_empty());

    // PNCounter
    let mut pn = PNCounter::new();
    pn.increment("node", 1);
    let bytes = pn.to_capnp_bytes();
    let res = compact_capnp_bytes(CrdtType::PNCounter, &[&bytes]).unwrap();
    assert!(!res.is_empty());

    // LWWRegister
    let lww = LWWRegister::new("val".to_string(), 1, "node");
    let bytes = lww.to_capnp_bytes();
    let res = compact_capnp_bytes(CrdtType::LWWRegister, &[&bytes]).unwrap();
    assert!(!res.is_empty());

    // FWWRegister
    let fww = FWWRegister::new("val".to_string(), 1, "node");
    let bytes = fww.to_capnp_bytes();
    let res = compact_capnp_bytes(CrdtType::FWWRegister, &[&bytes]).unwrap();
    assert!(!res.is_empty());

    // MVRegister
    let mut mv = MVRegister::new();
    mv.set("node", "val".to_string());
    let bytes = mv.to_capnp_bytes();
    let res = compact_capnp_bytes(CrdtType::MVRegister, &[&bytes]).unwrap();
    assert!(!res.is_empty());

    // LWWMap
    let mut lwwmap = LWWMap::new();
    lwwmap.insert("node", "key".to_string(), "val".to_string(), 100);
    let bytes = lwwmap.to_capnp_bytes();
    let res = compact_capnp_bytes(CrdtType::LWWMap, &[&bytes]).unwrap();
    assert!(!res.is_empty());

    // ORMap
    let mut ormap = ORMap::new();
    ormap.insert("node", "key".to_string(), "val".to_string());
    let bytes = ormap.to_capnp_bytes();
    let res = compact_capnp_bytes(CrdtType::ORMap, &[&bytes]).unwrap();
    assert!(!res.is_empty());

    // LWWSet
    let mut lwwset = LWWSet::new();
    lwwset.insert("node", "val".to_string(), 100);
    let bytes = lwwset.to_capnp_bytes();
    let res = compact_capnp_bytes(CrdtType::LWWSet, &[&bytes]).unwrap();
    assert!(!res.is_empty());
}

