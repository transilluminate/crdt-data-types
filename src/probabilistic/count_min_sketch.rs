use crate::count_min_sketch_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Count-Min Sketch - Frequency Estimation CRDT
///
/// A probabilistic data structure for estimating the frequency of events in a stream of data.
/// It uses a matrix of counters and multiple hash functions to map events to counters.
///
/// # Key Properties
///
/// - **Fixed Memory**: Uses a fixed size matrix (`width` × `depth` × 8 bytes), regardless of the number of unique items.
/// - **Conservative**: Frequencies are never underestimated, but may be overestimated due to collisions.
/// - **Mergeable**: Can be merged from multiple replicas by summing the corresponding counters.
///
/// # Algebraic Properties
///
/// - **Commutativity**: Yes (Matrix addition is commutative).
/// - **Associativity**: Yes (Matrix addition is associative).
/// - **Idempotence**: **NO**. Merging the same sketch twice doubles the counts. It behaves like a G-Counter.
///
/// # Example
///
/// ```
/// use crdt_data_types::CountMinSketch;
///
/// let mut cms = CountMinSketch::new(100, 5);
/// cms.increment("apple", 1);
/// cms.increment("apple", 1);
/// cms.increment("banana", 1);
///
/// assert!(cms.estimate("apple") >= 2);
/// assert!(cms.estimate("banana") >= 1);
/// assert_eq!(cms.estimate("cherry"), 0);
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CountMinSketch {
    /// Number of counters per row
    pub width: usize,
    /// Number of hash functions (rows)
    pub depth: usize,
    /// The matrix of counters (flattened or row-major)
    pub matrix: Vec<Vec<u64>>,
}

impl CountMinSketch {
    pub fn new(width: usize, depth: usize) -> Self {
        Self {
            width,
            depth,
            matrix: vec![vec![0; width]; depth],
        }
    }

    pub fn increment<T: Hash>(&mut self, item: T, count: u64) {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();

        for row in 0..self.depth {
            // Use different hash function for each row (simulated by re-hashing or salt)
            // Simple simulation: hash + row index
            let mut row_hasher = DefaultHasher::new();
            hash.hash(&mut row_hasher);
            row.hash(&mut row_hasher);
            let row_hash = row_hasher.finish();
            
            let col = (row_hash as usize) % self.width;
            self.matrix[row][col] = self.matrix[row][col].saturating_add(count);
        }
    }

    /// Merges another CountMinSketch into this one.
    ///
    /// # Arguments
    /// * `other` - The other CountMinSketch to merge.
    pub fn merge(&mut self, other: &Self) {
        if self.width != other.width || self.depth != other.depth {
            panic!("Dimension mismatch in CountMinSketch merge");
        }

        for r in 0..self.depth {
            for c in 0..self.width {
                self.matrix[r][c] = self.matrix[r][c].saturating_add(other.matrix[r][c]);
            }
        }
    }

    pub fn estimate<T: Hash>(&self, item: T) -> u64 {
        let mut hasher = DefaultHasher::new();
        item.hash(&mut hasher);
        let hash = hasher.finish();
        let mut min_count = u64::MAX;

        for row in 0..self.depth {
            let mut row_hasher = DefaultHasher::new();
            hash.hash(&mut row_hasher);
            row.hash(&mut row_hasher);
            let row_hash = row_hasher.finish();
            
            let col = (row_hash as usize) % self.width;
            min_count = std::cmp::min(min_count, self.matrix[row][col]);
        }

        if min_count == u64::MAX { 0 } else { min_count }
    }
}

impl Crdt for CountMinSketch {
    type Reader<'a> = CountMinSketchReader<'a>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        if readers.is_empty() {
            return Ok(Self::new(0, 0)); // Or error?
        }

        // 1. Open all readers
        let mut message_readers = Vec::with_capacity(readers.len());
        let mut capnp_roots = Vec::with_capacity(readers.len());

        for reader in readers {
             let msg_reader = serialize::read_message(reader.bytes, ReaderOptions::new())
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
             message_readers.push(msg_reader);
        }
        
        for msg in &message_readers {
            let root = msg.get_root::<count_min_sketch_capnp::count_min_sketch::Reader>()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            capnp_roots.push(root);
        }

        // Validate dimensions match
        let first = capnp_roots[0];
        let width = first.get_width() as usize;
        let depth = first.get_depth() as usize;

        for root in capnp_roots.iter().skip(1) {
            if root.get_width() as usize != width || root.get_depth() as usize != depth {
                return Err(CrdtError::Merge("Dimension mismatch in CountMinSketch merge".into()));
            }
        }

        let mut merged = Self::new(width, depth);

        // Naive merge: iterate and sum
        // Optimization: This could be SIMD if we had flat arrays
        for r in 0..depth {
            for c in 0..width {
                let mut sum: u64 = 0;
                for root in &capnp_roots {
                    // Access via reader (zero-copyish)
                    let counters = root.get_counters().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
                    let idx = r * width + c;
                    let val = counters.get(idx as u32);
                    sum = sum.saturating_add(val);
                }
                merged.matrix[r][c] = sum;
            }
        }

        Ok(merged)
    }

    fn validate(&self) -> Result<(), CrdtError> {
        if self.matrix.len() != self.depth {
            return Err(CrdtError::Validation("Matrix depth mismatch".into()));
        }
        for row in &self.matrix {
            if row.len() != self.width {
                return Err(CrdtError::Validation("Matrix width mismatch".into()));
            }
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.matrix.iter().all(|row| row.iter().all(|&x| x == 0))
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new_default();
        let mut root = message.init_root::<count_min_sketch_capnp::count_min_sketch::Builder>();
        
        root.set_width(self.width as u32);
        root.set_depth(self.depth as u32);
        
        // Flatten matrix for storage
        let total_size = self.width * self.depth;
        let mut counters_builder = root.init_counters(total_size as u32);
        
        for (r, row) in self.matrix.iter().enumerate() {
            for (c, &val) in row.iter().enumerate() {
                let idx = r * self.width + c;
                counters_builder.set(idx as u32, val);
            }
        }

        let mut data = Vec::new();
        serialize::write_message(&mut data, &message).unwrap();
        data
    }
}

impl CountMinSketch {
    pub fn from_capnp_bytes(data: &[u8]) -> Result<Self, CrdtError> {
        let message_reader = serialize::read_message(
            data,
            ReaderOptions {
                traversal_limit_in_words: None,
                nesting_limit: 64,
            },
        )
        .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<count_min_sketch_capnp::count_min_sketch::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        // Convert reader back to struct
        let width = root.get_width() as usize;
        let depth = root.get_depth() as usize;
        let mut matrix = vec![vec![0; width]; depth];
        
        let counters = root.get_counters().map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        for r in 0..depth {
            for c in 0..width {
                let idx = r * width + c;
                matrix[r][c] = counters.get(idx as u32);
            }
        }
        
        Ok(Self { width, depth, matrix })
    }
}

pub struct CountMinSketchReader<'a> {
    bytes: &'a [u8],
}

impl<'a> CountMinSketchReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl<'a> CrdtReader<'a> for CountMinSketchReader<'a> {
    fn is_empty(&self) -> Result<bool, CrdtError> {
        // We have to parse to check if empty, but we can stop early
        let message_reader = serialize::read_message(
            self.bytes,
            ReaderOptions::new(),
        ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<count_min_sketch_capnp::count_min_sketch::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
        let counters = root.get_counters().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        
        // Check if all zero
        for i in 0..counters.len() {
            if counters.get(i) != 0 {
                return Ok(false);
            }
        }
        Ok(true)
    }
}
