# CRDT Data Types (Rust)

A high-performance library of Conflict-free Replicated Data Types (CRDTs) built on [Cap'n Proto](https://capnproto.org/) for zero-copy deserialization, with a native **JSON-first gear** for seamless Web API integration.

Includes a [demo-server](demo-server/README.md) with a rogue-like game to demonstrate CRDT merge ops. See how long you can survive! ⚔️

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
crdt-data-types = "0.1.1"
```

To enable **Probabilistic Data Structures** (HyperLogLog, CountMinSketch, etc.), add the `probabilistic` feature:

```toml
[dependencies]
crdt-data-types = { version = "0.1.1", features = ["probabilistic"] }
```

## The "Two-Gear" Strategy

This library is designed to support different performance requirements by providing two optimized integration pathways ("gears").

### 🏎️ Low Gear: JSON-Native
**Best for**: Web APIs, Frontend synchronization, and developer simplicity.

- **How it works**: Uses `serde_json` to deserialize directly into Rust structs, performs in-memory merging, and serializes back to JSON.
- **Performance**: Fastest for JSON-locked environments (avoids the ~1ms "translation tax" between JSON and binary formats).
- **Complexity**: $O(N \log N)$ or $O(N)$ depending on the data type.

### 🚀 High Gear: Capnp-Native
**Best for**: High-throughput distributed systems, persistent storage, and massive state synchronization.

- **How it works**: Operates directly on Cap'n Proto byte buffers using specialized `Reader` and `merge_from_readers` logic. Uses **Sorted Vector Layouts** to enable $O(N)$ linear-time merging, bypassing full struct allocations and expensive hash calculations.
- **Performance**: **2.5x - 3x faster** than JSON for both flat and nested data types.
- **Trade-off**: Requires binary transport (gRPC, P2P) or scale large enough to justify the translation overhead. Alternatively, use a reverse proxy which pre-converts to capnp.

---

## Performance Tipping Point

| CRDT Type | JSON-Native (N=1000) | Capnp-Native (N=1000) | Winner |
| :--- | :--- | :--- | :--- |
| **GCounter** | 986 µs | **374 µs** | **Capnp (2.6x fast)** |
| **ORSet** | 650 µs | **~270 µs** | **Capnp (2.4x fast)** |

> [!NOTE]
> With the **Sorted Vector Layout** optimization, Cap'n Proto zero-copy merging now dominates across both flat and complex nested types.

---

## Supported CRDTs

- **Counters**: `GCounter`, `PNCounter`
- **Sets**: `GSet`, `ORSet`, `LWWSet`
- **Registers**: `LWWRegister`, `FWWRegister`, `MVRegister`
- **Maps**: `LWWMap`, `ORMap`
- **Probabilistic** (Optional, requires `features = ["probabilistic"]`): `HyperLogLog`, `CountMinSketch`, `RoaringBitmap`, `TDigest`, `TopK`
- **Utilities**: `VectorClock`

---

## Usage

### JSON Pathway (Web API)
```rust
use crdt_data_types::SerdeCapnpBridge;
use serde_json::json;

let json1 = json!({ "counters": { "node1": 10 } });
let json2 = json!({ "counters": { "node2": 20 } });

let merged = SerdeCapnpBridge::merge_json_values("GCounter", &[json1, json2]).unwrap();
```

### Zero-Copy Pathway (Binary/Storage)
```rust
use crdt_data_types::{GCounter, GCounterReader, Crdt};

let gc1_bytes = gc1.to_capnp_bytes();
let gc2_bytes = gc2.to_capnp_bytes();

let reader1 = GCounterReader::new(&gc1_bytes);
let reader2 = GCounterReader::new(&gc2_bytes);

let merged_gc = GCounter::merge_from_readers(&[reader1, reader2]).unwrap();
```

---

## License
MIT
