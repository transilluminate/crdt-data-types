// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crdt_data_types::*;
use proptest::prelude::*;

// ============================================================================
// Strategies
// ============================================================================

fn arb_gcounter() -> impl Strategy<Value = GCounter> {
    prop::collection::hash_map("[a-z]", 0i64..1000i64, 0..10).prop_map(|m| {
        let mut counters: Vec<_> = m.into_iter().collect();
        counters.sort_by(|a, b| a.0.cmp(&b.0));
        GCounter {
            counters,
            vclock: VectorClock::new(),
        }
    })
}

fn arb_vclock() -> impl Strategy<Value = VectorClock> {
    prop::collection::hash_map("[a-z]", 1u64..100u64, 0..5).prop_map(|m| {
        let mut vc = VectorClock::new();
        for (node, count) in m {
            vc.clocks.insert(node, (count, 0));
        }
        vc
    })
}

fn arb_pncounter() -> impl Strategy<Value = PNCounter> {
    (arb_gcounter(), arb_gcounter(), arb_vclock()).prop_map(|(p, n, vc)| PNCounter {
        positive: p,
        negative: n,
        vclock: vc,
    })
}

fn arb_gset() -> impl Strategy<Value = GSet<String>> {
    prop::collection::hash_set("[a-z]", 0..10).prop_map(|s| {
        let mut gs = GSet::new();
        let mut elements: Vec<_> = s.into_iter().collect();
        elements.sort();
        gs.elements = elements;
        gs
    })
}

#[derive(Debug, Clone)]
enum ORSetOp {
    Insert(String, String),
    Remove(String),
}

fn arb_orset_op() -> impl Strategy<Value = ORSetOp> {
    let node_strategy = prop::sample::select(vec!["node1", "node2", "node3"]);
    let element_strategy = prop::sample::select(vec!["apple", "banana", "cherry"]);
    prop_oneof![
        (node_strategy, element_strategy.clone())
            .prop_map(|(n, e)| ORSetOp::Insert(n.to_string(), e.to_string())),
        element_strategy.prop_map(|e| ORSetOp::Remove(e.to_string())),
    ]
}

fn apply_orset_op(set: &mut ORSet<String>, op: ORSetOp) {
    match op {
        ORSetOp::Insert(node, elem) => set.insert(&node, elem),
        ORSetOp::Remove(elem) => set.remove(&elem),
    }
}

fn arb_orset() -> impl Strategy<Value = ORSet<String>> {
    prop::collection::vec(arb_orset_op(), 0..20).prop_map(|ops| {
        let mut set = ORSet::new();
        for op in ops {
            apply_orset_op(&mut set, op);
        }
        set
    })
}

fn arb_ormap() -> impl Strategy<Value = ORMap<String, i64>> {
    let node_strategy = prop::sample::select(vec!["node1", "node2", "node3"]);
    let key_strategy = prop::sample::select(vec!["k1", "k2", "k3"]);

    prop::collection::vec((node_strategy, key_strategy, 1i64..100i64), 0..10).prop_map(|ops| {
        let mut map = ORMap::new();
        for (node, key, val) in ops {
            map.insert(node, key.to_string(), val);
        }
        map
    })
}

fn arb_mvreg_op() -> impl Strategy<Value = (String, String)> {
    let node_strategy = prop::sample::select(vec!["node1", "node2", "node3"]);
    let value_strategy = prop::sample::select(vec!["a", "b", "c"]);
    (
        node_strategy.prop_map(|s| s.to_string()),
        value_strategy.prop_map(|s| s.to_string()),
    )
}

fn arb_mvreg() -> impl Strategy<Value = MVRegister<String>> {
    prop::collection::vec(arb_mvreg_op(), 0..10).prop_map(|ops| {
        let mut reg = MVRegister::new();
        for (node, val) in ops {
            reg.set(&node, val);
        }
        reg
    })
}

fn arb_lwwreg_op() -> impl Strategy<Value = (String, String, u64)> {
    let node_strategy = prop::sample::select(vec!["node1", "node2", "node3"]);
    let value_strategy = prop::sample::select(vec!["a", "b", "c"]);
    (
        node_strategy.prop_map(|s| s.to_string()),
        value_strategy.prop_map(|s| s.to_string()),
        0u64..1000u64,
    )
}

fn arb_lwwreg() -> impl Strategy<Value = LWWRegister<String>> {
    prop::collection::vec(arb_lwwreg_op(), 0..10).prop_map(|ops| {
        let mut reg = LWWRegister::new("".to_string(), 0, "node1".to_string());
        for (node, val, ts) in ops {
            reg.set(val, ts, &node);
        }
        reg
    })
}

#[derive(Debug, Clone)]
enum LWWMapOp {
    Insert(String, String, String, u64),
    Remove(String),
}

fn arb_lwwmap_op() -> impl Strategy<Value = LWWMapOp> {
    let node_strategy = prop::sample::select(vec!["node1", "node2", "node3"]);
    let key_strategy = prop::sample::select(vec!["k1", "k2", "k3"]);
    let value_strategy = prop::sample::select(vec!["v1", "v2", "v3"]);
    prop_oneof![
        (
            node_strategy,
            key_strategy.clone(),
            value_strategy,
            0u64..1000u64
        )
            .prop_map(|(n, k, v, ts)| LWWMapOp::Insert(
                n.to_string(),
                k.to_string(),
                v.to_string(),
                ts
            )),
        key_strategy.prop_map(|k| LWWMapOp::Remove(k.to_string())),
    ]
}

fn apply_lwwmap_op(map: &mut LWWMap<String, String>, op: LWWMapOp) {
    match op {
        LWWMapOp::Insert(node, key, val, ts) => map.insert(&node, key, val, ts),
        LWWMapOp::Remove(key) => map.remove(&key),
    }
}

fn arb_lwwmap() -> impl Strategy<Value = LWWMap<String, String>> {
    prop::collection::vec(arb_lwwmap_op(), 0..20).prop_map(|ops| {
        let mut map = LWWMap::new();
        for op in ops {
            apply_lwwmap_op(&mut map, op);
        }
        map
    })
}

#[derive(Debug, Clone)]
enum LWWSetOp {
    Insert(String, String, u64),
    Remove(String, String, u64),
}

fn arb_lwwset_op() -> impl Strategy<Value = LWWSetOp> {
    let node_strategy = prop::sample::select(vec!["node1", "node2", "node3"]);
    let element_strategy = prop::sample::select(vec!["a", "b", "c"]);
    prop_oneof![
        (
            node_strategy.clone(),
            element_strategy.clone(),
            0u64..1000u64
        )
            .prop_map(|(n, e, ts)| LWWSetOp::Insert(n.to_string(), e.to_string(), ts)),
        (node_strategy, element_strategy, 0u64..1000u64).prop_map(|(n, e, ts)| LWWSetOp::Remove(
            n.to_string(),
            e.to_string(),
            ts
        )),
    ]
}

fn apply_lwwset_op(set: &mut LWWSet<String>, op: LWWSetOp) {
    match op {
        LWWSetOp::Insert(node, elem, ts) => set.insert(&node, elem, ts),
        LWWSetOp::Remove(node, elem, ts) => set.remove(&node, elem, ts),
    }
}

fn arb_lwwset() -> impl Strategy<Value = LWWSet<String>> {
    prop::collection::vec(arb_lwwset_op(), 0..20).prop_map(|ops| {
        let mut set = LWWSet::new();
        for op in ops {
            apply_lwwset_op(&mut set, op);
        }
        set
    })
}

fn arb_fwwreg_op() -> impl Strategy<Value = (String, String, u64)> {
    let node_strategy = prop::sample::select(vec!["node1", "node2", "node3"]);
    let value_strategy = prop::sample::select(vec!["a", "b", "c"]);
    (
        node_strategy.prop_map(|s| s.to_string()),
        value_strategy.prop_map(|s| s.to_string()),
        0u64..1000u64,
    )
}

fn arb_fwwreg() -> impl Strategy<Value = FWWRegister<String>> {
    prop::collection::vec(arb_fwwreg_op(), 0..10).prop_map(|ops| {
        let mut reg = FWWRegister::new("".to_string(), 0, "node1".to_string());
        for (node, val, ts) in ops {
            reg.set(val, ts, &node);
        }
        reg
    })
}

// ============================================================================
// Property Macros
// ============================================================================

macro_rules! test_properties {
    ($type:ident, $arb:expr) => {
        paste::paste! {
            proptest! {
                #[test]
                fn [< $type:lower _idempotence >](a in $arb) {
                    let mut a1 = a.clone();
                    a1.merge(&a);
                    prop_assert_eq!(a1, a);
                }

                #[test]
                fn [< $type:lower _commutativity >](a in $arb, b in $arb) {
                    let mut a_merged = a.clone();
                    a_merged.merge(&b);

                    let mut b_merged = b.clone();
                    b_merged.merge(&a);

                    prop_assert_eq!(a_merged, b_merged);
                }

                #[test]
                fn [< $type:lower _associativity >](a in $arb, b in $arb, c in $arb) {
                    let mut ab_c = a.clone();
                    ab_c.merge(&b);
                    ab_c.merge(&c);

                    let mut a_bc = a.clone();
                    let mut bc = b.clone();
                    bc.merge(&c);
                    a_bc.merge(&bc);

                    prop_assert_eq!(ab_c, a_bc);
                }
            }
        }
    };
}

// ============================================================================
// Tests
// ============================================================================

test_properties!(GCounter, arb_gcounter());
test_properties!(PNCounter, arb_pncounter());
test_properties!(GSet, arb_gset());
test_properties!(ORSet, arb_orset());
test_properties!(ORMap, arb_ormap());
test_properties!(MVRegister, arb_mvreg());
test_properties!(LWWRegister, arb_lwwreg());
test_properties!(FWWRegister, arb_fwwreg());
test_properties!(LWWMap, arb_lwwmap());
test_properties!(LWWSet, arb_lwwset());

// Divergence Tests
proptest! {
    #[test]
    fn orset_divergence_merge(
        common_ops in prop::collection::vec(arb_orset_op(), 0..10),
        ops_a in prop::collection::vec(arb_orset_op(), 0..10),
        ops_b in prop::collection::vec(arb_orset_op(), 0..10),
    ) {
        let mut base = ORSet::new();
        for op in common_ops { apply_orset_op(&mut base, op); }

        let mut a = base.clone();
        for op in ops_a { apply_orset_op(&mut a, op); }

        let mut b = base.clone();
        for op in ops_b { apply_orset_op(&mut b, op); }

        let mut a_merged = a.clone();
        a_merged.merge(&b);

        let mut b_merged = b.clone();
        b_merged.merge(&a);

        prop_assert_eq!(a_merged, b_merged);
    }

    #[test]
    fn lwwmap_divergence_merge(
        common_ops in prop::collection::vec(arb_lwwmap_op(), 0..10),
        ops_a in prop::collection::vec(arb_lwwmap_op(), 0..10),
        ops_b in prop::collection::vec(arb_lwwmap_op(), 0..10),
    ) {
        let mut base = LWWMap::<String, String>::new();
        for op in common_ops { apply_lwwmap_op(&mut base, op); }

        let mut a = base.clone();
        for op in ops_a { apply_lwwmap_op(&mut a, op); }

        let mut b = base.clone();
        for op in ops_b { apply_lwwmap_op(&mut b, op); }

        let mut a_merged = a.clone();
        a_merged.merge(&b);

        let mut b_merged = b.clone();
        b_merged.merge(&a);

        // This will likely FAIL for LWWMap if no tombstones are used!
        prop_assert_eq!(a_merged, b_merged);
    }
}

// Zero-Copy Equivalence
proptest! {
    #[test]
    fn gcounter_zero_copy_equivalence(a in arb_gcounter(), b in arb_gcounter()) {
        let mut expected = a.clone();
        expected.merge(&b);
        let bytes_a = a.to_capnp_bytes();
        let bytes_b = b.to_capnp_bytes();
        let reader_a = GCounterReader::new(&bytes_a);
        let reader_b = GCounterReader::new(&bytes_b);
        let actual = GCounter::merge_from_readers(&[reader_a, reader_b]).unwrap();
        prop_assert_eq!(actual, expected);
    }

    #[test]
    fn gset_zero_copy_equivalence(a in arb_gset(), b in arb_gset()) {
        let mut expected = a.clone();
        expected.merge(&b);
        let bytes_a = a.to_capnp_bytes();
        let bytes_b = b.to_capnp_bytes();
        let reader_a = GSetReader::<String>::new(&bytes_a);
        let reader_b = GSetReader::<String>::new(&bytes_b);
        let actual = GSet::merge_from_readers(&[reader_a, reader_b]).unwrap();
        prop_assert_eq!(actual, expected);
    }

    #[test]
    fn orset_zero_copy_equivalence(a in arb_orset(), b in arb_orset()) {
        let mut expected = a.clone();
        expected.merge(&b);
        let bytes_a = a.to_capnp_bytes();
        let bytes_b = b.to_capnp_bytes();
        let reader_a = ORSetReader::<String>::new(&bytes_a);
        let reader_b = ORSetReader::<String>::new(&bytes_b);
        let actual = ORSet::merge_from_readers(&[reader_a, reader_b]).unwrap();
        prop_assert_eq!(actual, expected);
    }

    #[test]
    fn lwwmap_zero_copy_equivalence(a in arb_lwwmap(), b in arb_lwwmap()) {
        let mut expected = a.clone();
        expected.merge(&b);
        let bytes_a = a.to_capnp_bytes();
        let bytes_b = b.to_capnp_bytes();
        let reader_a = LWWMapReader::<String, String>::new(&bytes_a);
        let reader_b = LWWMapReader::<String, String>::new(&bytes_b);
        let actual = LWWMap::merge_from_readers(&[reader_a, reader_b]).unwrap();
        prop_assert_eq!(actual, expected);
    }

    #[test]
    fn lwwset_zero_copy_equivalence(a in arb_lwwset(), b in arb_lwwset()) {
        let mut expected = a.clone();
        expected.merge(&b);
        let bytes_a = a.to_capnp_bytes();
        let bytes_b = b.to_capnp_bytes();
        let reader_a = LWWSetReader::<String>::new(&bytes_a);
        let reader_b = LWWSetReader::<String>::new(&bytes_b);
        let actual = LWWSet::merge_from_readers(&[reader_a, reader_b]).unwrap();
        prop_assert_eq!(actual, expected);
    }
}
