// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

//! Probabilistic Data Structures (Sketches)
//!
//! These data structures provide approximate answers to queries (cardinality, frequency, quantiles)
//! using significantly less memory than exact structures. They satisfy CRDT properties (commutativity,
//! associativity, idempotence) and can be merged from multiple replicas.

pub mod count_min_sketch;
pub mod hyperloglog;
pub mod roaring_bitmap;
pub mod tdigest;
pub mod topk;


pub use count_min_sketch::{CountMinSketch, CountMinSketchReader};
pub use hyperloglog::{HyperLogLog, HyperLogLogReader};
pub use roaring_bitmap::{RoaringBitmap, RoaringBitmapReader};
pub use tdigest::{TDigest, TDigestReader};
pub use topk::{TopK, TopKReader};

