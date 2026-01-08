// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crdt_data_types::*;
use crdt_data_types::enums::CrdtType;
use crdt_data_types::deltas_capnp::delta;
use capnp::serialize;
use serde_json::json;
use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};

fn setup_gcounter_delta(increment: i64) -> Vec<u8> {
    let mut message = capnp::message::Builder::new_default();
    message.init_root::<delta::Builder>().set_pn_counter(increment); // Using PNCounter logic for signed int support if needed, or set_g_counter if unsigned.
    // Wait, GCounter delta in capnp scheam is just uint64? Or struct?
    // Let's check logic: GCounter usually unsigned. 
    // In full_coverage_tests: message.init_root::<delta::Builder>().set_pn_counter(10);
    // Let's use PNCounter for signed genericness in this bench.
    let mut delta_bytes = Vec::new();
    serialize::write_message(&mut delta_bytes, &message).unwrap();
    delta_bytes
}

fn bench_gcounter_deltas(c: &mut Criterion) {
    let mut group = c.benchmark_group("GCounter Delta Apply");

    let initial_state_json = json!({
        "positive": {
            "counters": {
                "node1": 100,
                "node2": 200
            },
            "vclock": {
                "clocks": {"node1": [1, 100], "node2": [2, 100]}
            }
        },
        "negative": { "counters": {}, "vclock": { "clocks": {} } },
        "vclock": {
            "clocks": {"node1": [1, 100], "node2": [2, 100]}
        }
    });

    let initial_state_bytes = SerdeCapnpBridge::json_to_capnp_bytes(
        CrdtType::PNCounter, 
        initial_state_json.clone()
    ).unwrap();

    let delta_val = 10;
    let delta_json = json!(delta_val);
    let delta_bytes = setup_gcounter_delta(delta_val);
    
    // JSON Benchmark
    group.bench_function("JSON Delta", |bencher| {
        bencher.iter(|| {
            SerdeCapnpBridge::apply_delta_json(
                CrdtType::PNCounter,
                Some(black_box(&initial_state_json)),
                black_box(&delta_json),
                "node1"
            ).unwrap()
        })
    });

    // Capnp Benchmark
    group.bench_function("Capnp Delta (Zero-Copy)", |bencher| {
        bencher.iter(|| {
            SerdeCapnpBridge::apply_delta_capnp(
                CrdtType::PNCounter,
                Some(black_box(&initial_state_bytes)),
                black_box(&delta_bytes),
                "node1"
            ).unwrap()
        })
    });
    
    // Batch Benchmark (Simulating 10 ops)
    let batch_deltas: Vec<&[u8]> = vec![&delta_bytes; 10];
    group.bench_function("Capnp Batch Apply (10 ops)", |bencher| {
        bencher.iter(|| {
             SerdeCapnpBridge::apply_batch_deltas_capnp(
                CrdtType::PNCounter,
                Some(black_box(&initial_state_bytes)),
                black_box(&batch_deltas),
                "node1"
            ).unwrap()
        })
    });

    group.finish();
}

fn setup_orset_delta_add(elem: &str) -> Vec<u8> {
    let mut message = capnp::message::Builder::new_default();
    let root = message.init_root::<delta::Builder>();
    let or_delta = root.init_or_set();
    let mut add = or_delta.init_add(1);
    add.set(0, elem.into());
    
    let mut delta_bytes = Vec::new();
    serialize::write_message(&mut delta_bytes, &message).unwrap();
    delta_bytes
}

fn bench_orset_deltas(c: &mut Criterion) {
    let mut group = c.benchmark_group("ORSet Delta Apply");
    
    // Setup initial state with 100 items
    let mut set = ORSet::new();
    for i in 0..100 {
        set.insert("node1", format!("item{}", i));
    }
    let initial_state_json = serde_json::to_value(&set).unwrap();
    let initial_state_bytes = SerdeCapnpBridge::json_to_capnp_bytes(CrdtType::ORSet, initial_state_json.clone()).unwrap();

    let delta_json = json!({
        "add": ["new_item"]
    });
    let delta_bytes = setup_orset_delta_add("new_item");

    group.bench_function("JSON Delta", |bencher| {
        bencher.iter(|| {
             SerdeCapnpBridge::apply_delta_json(
                CrdtType::ORSet,
                Some(black_box(&initial_state_json)),
                black_box(&delta_json),
                "node1"
            ).unwrap()
        })
    });

    group.bench_function("Capnp Delta (Zero-Copy)", |bencher| {
        bencher.iter(|| {
             SerdeCapnpBridge::apply_delta_capnp(
                CrdtType::ORSet,
                Some(black_box(&initial_state_bytes)),
                black_box(&delta_bytes),
                "node1"
            ).unwrap()
        })
    });

    group.finish();
}

criterion_group!(benches, bench_gcounter_deltas, bench_orset_deltas);
criterion_main!(benches);
