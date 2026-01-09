# CRDT Data Types (Rust)

High-performance Conflict-free Replicated Data Types (CRDTs) with dual-pathway optimization for Web APIs and zero-copy storage.

[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)

## Philosophy: The "Two-Gear" Strategy

`crdt-data-types` is designed to support different performance requirements by providing two optimized integration pathways ("gears"):

- **JSON-Native (Low Gear)**: Seamless integration with Web APIs and frontends. Uses `serde_json` for direct struct manipulation.
- **Capnp-Native (High Gear)**: Zero-copy, high-throughput merging for distributed systems and storage engines.

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       Application Layer                     │
│  • Web APIs (JSON)                                          │
│  • Storage Engines (Binary)                                 │
└─────────────────────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────┐
│                  SerdeCapnpBridge (The Bridge)              │
│  • Validates JSON input                                     │
│  • Normalizes snake_case / PascalCase                       │
└─────────────────────────────────────────────────────────────┘
              │                              │
              ▼                              ▼
┌─────────────────────────────┐  ┌─────────────────────────────┐
│ Low Gear: JSON-Native       │  │ High Gear: Capnp-Native.    │
│ • serde_json deserialization│  │ • Zero-copy Reader access   │
│ • In-memory struct merge    │  │ • Linear-time merge O(N)    │
│ • Best for: Web/Frontend    │  │ • Best for: Storage/Network │
└─────────────────────────────┘  └─────────────────────────────┘
```

## Features

- **Dual-Pathway Merging**: Choose between developer-friendly JSON or performance-critical Cap'\''n Proto.
- **Zero-Copy Deserialization**: `merge_from_readers` operates directly on byte buffers without allocation.
- **Comprehensive CRDT Suite**:
    - **Counters**: `GCounter`, `PNCounter`
    - **Sets**: `GSet`, `ORSet`, `LWWSet`
    - **Registers**: `LWWRegister`, `FWWRegister`, `MVRegister`
    - **Maps**: `LWWMap`, `ORMap`
- **Probabilistic Structures**: `HyperLogLog`, `CountMinSketch`, `RoaringBitmap`, `TDigest`, `TopK` (via feature flag).
- **Vector Clocks**: Standard logical clocks for causality tracking.
- **Compaction**: Utilities to squash history and reduce payload size.
- **Binary Deltas**: Apply small, strict delta updates directly to binary states (skip JSON).
- **Batch Processing**: Amortize IO overhead by applying multiple deltas in one pass.
    - **Additive Merging**: Sum values for GCounter/PNCounter (vs Max/Union) for flush operations.
Add to your `Cargo.toml`:

```toml
[dependencies]
crdt-data-types = "0.1.10"
# Optional: Enable probabilistic structures
# crdt-data-types = { version = "0.1.10", features = ["probabilistic"] }
```

### JSON Pathway (Web API)

```rust
use crdt_data_types::{SerdeCapnpBridge, CrdtType};
use serde_json::json;

let json1 = json!({ "counters": { "node1": 10 } });
let json2 = json!({ "counters": { "node2": 20 } });

// Merges JSON directly
let merged = SerdeCapnpBridge::merge_json_values(CrdtType::GCounter, &[json1, json2]).unwrap();
```

### Zero-Copy Pathway (Binary/Storage)

```rust
use crdt_data_types::{GCounter, GCounterReader, Crdt};

let gc1_bytes = gc1.to_capnp_bytes();
let gc2_bytes = gc2.to_capnp_bytes();

let reader1 = GCounterReader::new(&gc1_bytes);
let reader2 = GCounterReader::new(&gc2_bytes);

// Merges without deserializing full structs
let merged_gc = GCounter::merge_from_readers(&[reader1, reader2]).unwrap();
```

### High-Performance Delta API (Batching)

For maximum throughput (e.g., ingestion pipelines), use the batch delta API:

```rust
// Apply 3 binary deltas in a single pass
let final_state = SerdeCapnpBridge::apply_batch_capnp_deltas(
    CrdtType::GCounter,
    Some(&current_state_bytes),
    &[&delta1_bytes, &delta2_bytes, &delta3_bytes],
    "node1"
).unwrap();
```

### State Accumulation (Additive Merge)

For counters (`GCounter`, `PNCounter`), standard merging uses `MAX` (greatest value seen). Use `add_accumulated_state` when you want to **sum** differences (e.g., flushing a temporary counter to a main counter):

```rust
// Standard merge: MAX(10, 5) = 10
// Additive merge: 10 + 5 = 15

let current = json!({ "counters": { "node1": 10 } });
let flush_delta = json!({ "counters": { "node1": 5 } });

let new_state = SerdeCapnpBridge::add_accumulated_state(
    CrdtType::GCounter,
    current,
    flush_delta
).unwrap();
// Result: node1 = 15
```

## Performance Tipping Point

| Operation | JSON-Native (N=100) | Capnp-Native (N=100) | Winner |
| :--- | :--- | :--- | :--- |
| **GCounter Merge** | 986 µs | **374 µs** | **Capnp (2.6x fast)** |
| **ORSet Merge** | 650 µs | **~270 µs** | **Capnp (2.4x fast)** |
| **ORSet Delta** | 52 µs | **35 µs** | **Capnp (1.5x fast)** |
| **Batch Apply (10 ops)** | ~25 µs (est) | **5.2 µs** | **Capnp (4.8x fast)** |

## Testing

Comprehensive test suite covering unit logic, bridge integration, and property-based fuzzing:

| Test Suite | Count | Description |
|------------|-------|-------------|
| **Unit Tests** | 3 ✅ | Core logic & compaction |
| **Basic Tests** | 6 ✅ | Standard CRDT operations |
| **Bridge Tests** | 6 ✅ | JSON <-> Capnp bridge & case-insensitivity |
| **Coverage Tests** | 17 ✅ | Edge cases, compaction, & vector clocks |
| **Delta Tests** | 11 ✅ | Binary Cap'n Proto deltas, JSON delta logic |
| **Additive Merge** | 3 ✅ | State accumulation & counter summation |
| **Property Tests** | 38 ✅ | Proptest fuzzing for commutativity/associativity & delta equivalence |
| **Total** | **84** ✅ | ~70% code coverage |

Run tests with:
```bash
cargo test
cargo llvm-cov --summary-only  # Requires cargo-llvm-cov
```

## Changelog

### v0.1.4
- **Usability**: `SerdeCapnpBridge` now accepts case-insensitive CRDT type names (e.g., "g_counter", "gcounter", "GCounter"). This simplifies integration with external systems that use snake_case.

## License

[MIT License](LICENSE)

Copyright (c) 2026 Adrian Robinson. All rights reserved.