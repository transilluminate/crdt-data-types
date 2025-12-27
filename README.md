# CRDT Data Types (Rust)

A high-performance library of Conflict-free Replicated Data Types (CRDTs) built on [Cap'n Proto](https://capnproto.org/) for zero-copy deserialization, with a native **JSON-first gear** for seamless Web API integration.

## The "Two-Gear" Strategy

This library is designed to support different performance requirements by providing two optimized integration pathways ("gears").

### 🏎️ Low Gear: JSON-Native
**Best for**: Web APIs, Frontend synchronization, and developer simplicity.

- **How it works**: Uses `serde_json` to deserialize directly into Rust structs, performs in-memory merging, and serializes back to JSON.
- **Performance**: Fastest for JSON-locked environments (avoids the ~1ms "translation tax" between JSON and binary formats).
- **Complexity**: $O(N \log N)$ or $O(N)$ depending on the data type.

### 🚀 High Gear: Capnp-Native
**Best for**: High-throughput distributed systems, persistent storage, and massive state synchronization.

- **How it works**: Operates directly on Cap'n Proto byte buffers using specialized `Reader` and `merge_from_readers` logic. Bypasses full struct allocations.
- **Performance**: **2.6x faster** than JSON for flat data types at N=1000.
- **Trade-off**: Requires binary transport (gRPC, P2P) or scale large enough to justify the translation overhead. Alternatively, use a reverse proxy which pre-converts to capnp.

---

## Performance Tipping Point

| CRDT Type | JSON-Native (N=1000) | Capnp-Native (N=1000) | Winner |
| :--- | :--- | :--- | :--- |
| **GCounter** | 986 µs | **374 µs** | **Capnp (2.6x fast)** |
| **ORSet** | **650 µs** | 1.14 ms | **JSON (1.7x fast)** |

> [!NOTE]
> JSON currently wins on complex nested types (like `ORSet`) due to `serde_json`'s extreme optimization for `HashMap` reconstruction. Cap'n Proto dominates on flat, high-volume data.

---

## Supported CRDTs

- **Counters**: `GCounter`, `PNCounter`
- **Sets**: `GSet`, `ORSet`, `LWWSet`
- **Registers**: `LWWRegister`, `FWWRegister`, `MVRegister`
- **Maps**: `LWWMap`, `ORMap`
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

## Future Roadmap: The "Holy Grails"
We are currently in Phase 2. Future optimizations include:
1. **Delta-CRDTs**: $O(1)$ updates by shipping only changes.
2. **Sorted Memory Layouts**: $O(N)$ linear-time merges using sorted vectors.
3. **rkyv Integration**: Even faster zero-copy via memmapped Rust structs.
4. **Lane Partitioning**: Multi-threaded parallel merging using `SeaHash` node affinity.

## License
MIT
