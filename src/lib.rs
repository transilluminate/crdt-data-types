// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

//! # zero-copy-crdt
//!
//! A high-performance library of Conflict-free Replicated Data Types (CRDTs)
//! optimized for zero-copy merge operations using Cap'n Proto.

pub mod bridge;
pub mod compaction;
pub mod enums;
pub mod fww_register;
pub mod g_counter;
pub mod g_set;
pub mod lww_map;
pub mod lww_register;
pub mod lww_set;
pub mod mv_register;
pub mod or_map;
pub mod or_set;
pub mod pn_counter;
#[cfg(feature = "probabilistic")]
pub mod probabilistic;
pub mod traits;
pub mod vector_clock;

// Re-export core traits
pub use traits::{Crdt, CrdtError, CrdtReader};

// Re-export enums
pub use enums::CrdtType;
#[cfg(feature = "probabilistic")]
pub use enums::ProbabilisticCrdtType;

// Re-export types as they are implemented
pub use bridge::SerdeCapnpBridge;
pub use fww_register::{FWWRegister, FWWRegisterReader};
pub use g_counter::{GCounter, GCounterReader};
pub use g_set::{GSet, GSetReader};
pub use lww_map::{LWWMap, LWWMapReader};
pub use lww_register::{LWWRegister, LWWRegisterReader};
pub use lww_set::{LWWSet, LWWSetReader};
pub use mv_register::{MVRegister, MVRegisterReader};
pub use or_map::{ORMap, ORMapReader};
pub use or_set::{ORSet, ORSetReader};
pub use pn_counter::{PNCounter, PNCounterReader};
pub use vector_clock::{VectorClock, VectorClockReader};

#[cfg(feature = "probabilistic")]
pub use probabilistic::{
    CountMinSketch, CountMinSketchReader, HyperLogLog, HyperLogLogReader, RoaringBitmap,
    RoaringBitmapReader, TDigest, TDigestReader, TopK, TopKReader,
};

// Include generated Cap'n Proto modules
pub mod gcounter_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/gcounter_capnp.rs"));
}
pub mod vclock_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/vclock_capnp.rs"));
}

#[cfg(feature = "probabilistic")]
pub mod count_min_sketch_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/count_min_sketch_capnp.rs"));
}
#[cfg(feature = "probabilistic")]
pub mod hyperloglog_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/hyperloglog_capnp.rs"));
}
#[cfg(feature = "probabilistic")]
pub mod roaring_bitmap_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/roaring_bitmap_capnp.rs"));
}
#[cfg(feature = "probabilistic")]
pub mod tdigest_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/tdigest_capnp.rs"));
}
#[cfg(feature = "probabilistic")]
pub mod topk_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/topk_capnp.rs"));
}
pub mod fww_register_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/fww_register_capnp.rs"));
}
pub mod gset_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/gset_capnp.rs"));
}
pub mod lww_map_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/lww_map_capnp.rs"));
}
pub mod lww_register_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/lww_register_capnp.rs"));
}
pub mod lww_set_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/lww_set_capnp.rs"));
}
pub mod mv_register_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/mv_register_capnp.rs"));
}
pub mod or_map_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/or_map_capnp.rs"));
}
pub mod orset_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/orset_capnp.rs"));
}
pub mod pncounter_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/pncounter_capnp.rs"));
}
pub mod deltas_capnp {
    include!(concat!(env!("OUT_DIR"), "/proto/deltas_capnp.rs"));
}

// Module for delta CRDTs
pub mod deltas;
