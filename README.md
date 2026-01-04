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

## Quick Start

Add to your `Cargo.toml`:

```toml
[dependencies]
crdt-data-types = "0.1.7"
# Optional: Enable probabilistic structures
# crdt-data-types = { version = "0.1.6", features = ["probabilistic"] }
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

## Performance Tipping Point

| CRDT Type | JSON-Native (N=1000) | Capnp-Native (N=1000) | Winner |
| :--- | :--- | :--- | :--- |
| **GCounter** | 986 µs | **374 µs** | **Capnp (2.6x fast)** |
| **ORSet** | 650 µs | **~270 µs** | **Capnp (2.4x fast)** |

## Testing

Comprehensive test suite covering unit logic, bridge integration, and property-based fuzzing:

| Test Suite | Count | Description |
|------------|-------|-------------|
| **Unit Tests** | 3 ✅ | Core logic & compaction |
| **Basic Tests** | 6 ✅ | Standard CRDT operations |
| **Bridge Tests** | 6 ✅ | JSON <-> Capnp bridge & case-insensitivity |
| **Coverage Tests** | 16 ✅ | Edge cases, compaction, & vector clocks |
| **Property Tests** | 37 ✅ | Proptest fuzzing for commutativity/associativity |
| **Total** | **68** ✅ | ~82% code coverage |

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