// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use criterion::{criterion_group, criterion_main, Criterion};
use std::hint::black_box;
use std::collections::HashMap;
use std::time::Duration;

// --- HashMap Implementation ---
#[derive(Clone)]
struct MapCounter {
    counters: HashMap<String, i64>,
}

impl MapCounter {
    fn new() -> Self {
        Self { counters: HashMap::new() }
    }
    
    fn merge(&mut self, other: &Self) {
        for (k, v) in &other.counters {
            let entry = self.counters.entry(k.clone()).or_insert(0);
            *entry = (*entry).max(*v);
        }
    }
}

// --- Sorted Vec Implementation ---
#[derive(Clone)]
struct VecCounter {
    // Kept sorted by String key
    counters: Vec<(String, i64)>,
}

impl VecCounter {
    fn new() -> Self {
        Self { counters: Vec::new() }
    }

    fn insert(&mut self, key: String, val: i64) {
        match self.counters.binary_search_by(|(k, _)| k.cmp(&key)) {
            Ok(idx) => self.counters[idx].1 = val,
            Err(idx) => self.counters.insert(idx, (key, val)),
        }
    }

    fn merge(&mut self, other: &Self) {
        let mut new_counters = Vec::with_capacity(self.counters.len() + other.counters.len());
        let mut i = 0;
        let mut j = 0;

        while i < self.counters.len() && j < other.counters.len() {
            let (k1, v1) = &self.counters[i];
            let (k2, v2) = &other.counters[j];

            match k1.cmp(k2) {
                std::cmp::Ordering::Less => {
                    new_counters.push((k1.clone(), *v1));
                    i += 1;
                }
                std::cmp::Ordering::Greater => {
                    new_counters.push((k2.clone(), *v2));
                    j += 1;
                }
                std::cmp::Ordering::Equal => {
                    new_counters.push((k1.clone(), (*v1).max(*v2)));
                    i += 1;
                    j += 1;
                }
            }
        }

        if i < self.counters.len() {
            new_counters.extend_from_slice(&self.counters[i..]);
        }
        if j < other.counters.len() {
            new_counters.extend_from_slice(&other.counters[j..]);
        }

        self.counters = new_counters;
    }
}

fn setup_data(n: usize) -> (MapCounter, MapCounter, VecCounter, VecCounter) {
    let mut m1 = MapCounter::new();
    let mut m2 = MapCounter::new();
    let mut v1 = VecCounter::new();
    let mut v2 = VecCounter::new();

    for i in 0..n {
        let k1 = format!("node_{:05}", i);
        let k2 = format!("node_{:05}", i + n/2); // 50% overlap
        
        m1.counters.insert(k1.clone(), i as i64);
        m2.counters.insert(k2.clone(), i as i64);
        
        v1.insert(k1, i as i64);
        v2.insert(k2, i as i64);
    }

    (m1, m2, v1, v2)
}

fn bench_layout_comparison(c: &mut Criterion) {
    let mut group = c.benchmark_group("Layout Comparison");
    group.measurement_time(Duration::from_secs(5));

    for n in [100, 1000, 10000].iter() {
        let (m1, m2, v1, v2) = setup_data(*n);

        group.bench_function(format!("HashMap Merge (N={})", n), |b| {
            b.iter(|| {
                let mut local = m1.clone();
                local.merge(black_box(&m2));
            })
        });

        group.bench_function(format!("Sorted Vec Merge (N={})", n), |b| {
            b.iter(|| {
                let mut local = v1.clone();
                local.merge(black_box(&v2));
            })
        });
    }
    group.finish();
}

criterion_group!(benches, bench_layout_comparison);
criterion_main!(benches);
