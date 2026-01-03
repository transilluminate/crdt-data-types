// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crdt_data_types::*;
use std::hint::black_box;
use criterion::{criterion_group, criterion_main, Criterion};

fn setup_orsets(n: usize) -> (ORSet<String>, ORSet<String>) {
    let mut a = ORSet::new();
    let mut b = ORSet::new();

    for i in 0..n {
        a.insert("node1", format!("elem_{}", i));
        b.insert("node2", format!("elem_{}", i + n / 2)); // Some overlap
    }

    (a, b)
}

fn bench_merge(c: &mut Criterion) {
    let mut group = c.benchmark_group("ORSet Merge");
    for n in [100, 1000].iter() {
        let (a, b) = setup_orsets(*n);

        group.bench_function(format!("Standard Merge (N={})", n), |bencher| {
            bencher.iter(|| {
                let mut a_clone = a.clone();
                a_clone.merge(black_box(&b));
            })
        });

        let bytes_a = a.to_capnp_bytes();
        let bytes_b = b.to_capnp_bytes();

        group.bench_function(format!("Zero-Copy Merge (N={})", n), |bencher| {
            bencher.iter(|| {
                let reader_a = ORSetReader::<String>::new(&bytes_a);
                let reader_b = ORSetReader::<String>::new(&bytes_b);
                ORSet::merge_from_readers(black_box(&[reader_a, reader_b])).unwrap()
            })
        });

        let initial_json = serde_json::to_value(&a).unwrap();

        group.bench_function(format!("JSON to Capnp (N={})", n), |bencher| {
            bencher.iter(|| {
                SerdeCapnpBridge::json_to_capnp_bytes("ORSet", black_box(initial_json.clone()))
                    .unwrap()
            })
        });

        group.bench_function(format!("Capnp to JSON (N={})", n), |bencher| {
            bencher.iter(|| {
                SerdeCapnpBridge::capnp_bytes_to_json("ORSet", black_box(&bytes_a)).unwrap()
            })
        });

        let json_a = serde_json::to_value(&a).unwrap();
        let json_b = serde_json::to_value(&b).unwrap();

        group.bench_function(format!("Full JSON Merge Cycle (N={})", n), |bencher| {
            bencher.iter(|| {
                SerdeCapnpBridge::merge_json_values(
                    "ORSet",
                    black_box(&[json_a.clone(), json_b.clone()]),
                )
                .unwrap()
            })
        });

        group.bench_function(format!("Full Capnp Merge Cycle (N={})", n), |bencher| {
            bencher.iter(|| {
                let reader_a = ORSetReader::<String>::new(&bytes_a);
                let reader_b = ORSetReader::<String>::new(&bytes_b);
                let merged =
                    ORSet::<String>::merge_from_readers(black_box(&[reader_a, reader_b])).unwrap();
                merged.to_capnp_bytes()
            })
        });
    }
    group.finish();

    let mut group = c.benchmark_group("GCounter Merge");
    for n in [100, 1000].iter() {
        let mut a = GCounter::new();
        let mut b = GCounter::new();
        for i in 0..*n {
            a.increment(&format!("node_{}", i), 1);
            b.increment(&format!("node_{}", i + *n / 2), 1);
        }

        group.bench_function(format!("Standard Merge (N={})", n), |bencher| {
            bencher.iter(|| {
                let mut a_clone = a.clone();
                a_clone.merge(black_box(&b));
            })
        });

        let bytes_a = a.to_capnp_bytes();
        let bytes_b = b.to_capnp_bytes();

        group.bench_function(format!("Optimized Zero-Copy Merge (N={})", n), |bencher| {
            bencher.iter(|| {
                let reader_a = GCounterReader::new(&bytes_a);
                let reader_b = GCounterReader::new(&bytes_b);
                GCounter::merge_from_readers(black_box(&[reader_a, reader_b])).unwrap()
            })
        });

        let json_a = serde_json::to_value(&a).unwrap();
        let json_b = serde_json::to_value(&b).unwrap();

        group.bench_function(format!("Full JSON Merge Cycle (N={})", n), |bencher| {
            bencher.iter(|| {
                SerdeCapnpBridge::merge_json_values(
                    "GCounter",
                    black_box(&[json_a.clone(), json_b.clone()]),
                )
                .unwrap()
            })
        });

        group.bench_function(format!("Full Capnp Merge Cycle (N={})", n), |bencher| {
            bencher.iter(|| {
                let reader_a = GCounterReader::new(&bytes_a);
                let reader_b = GCounterReader::new(&bytes_b);
                let merged =
                    GCounter::merge_from_readers(black_box(&[reader_a, reader_b])).unwrap();
                merged.to_capnp_bytes()
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_merge);
criterion_main!(benches);
