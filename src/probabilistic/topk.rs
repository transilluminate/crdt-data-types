// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use crate::topk_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use crate::probabilistic::count_min_sketch::CountMinSketch;
use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;


/// Item with frequency for heap storage
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
struct HeapItem {
    key: String,
    frequency: u64,
}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for Min-Heap behavior in a Max-Heap structure.
        // We want to evict the item with the smallest frequency when the heap is full.
        other
            .frequency
            .cmp(&self.frequency)
            .then_with(|| self.key.cmp(&other.key))
    }
}

/// TopK - Heavy Hitter Tracking CRDT
///
/// Tracks the K most frequent items in a stream using a Count-Min Sketch for frequency estimation
/// and a Min-Heap to maintain the top-K list.
///
/// # Key Properties
///
/// - **Memory Efficiency**: Uses a fixed-size sketch plus a small heap (size K).
/// - **Approximate**: Frequencies are estimates (Count-Min Sketch guarantees no underestimation).
/// - **Mergeable**: Can be merged from multiple replicas.
///
/// # Example
///
/// ```
/// use crdt_data_types::TopK;
///
/// let mut topk = TopK::new(3, 100, 5);
/// topk.increment("apple", 10);
/// topk.increment("banana", 20);
/// topk.increment("cherry", 5);
/// topk.increment("date", 15);
///
/// let top = topk.top_k();
/// assert_eq!(top.len(), 3);
/// assert_eq!(top[0].0, "banana"); // 20
/// assert_eq!(top[1].0, "date");   // 15
/// assert_eq!(top[2].0, "apple");  // 10
/// ```
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TopK {
    k: usize,
    sketch: CountMinSketch,
    heap: Vec<HeapItem>, // Store as Vec for serialization, but logic uses it as heap
}

impl TopK {
    pub fn new(k: usize, width: usize, depth: usize) -> Self {
        Self {
            k,
            sketch: CountMinSketch::new(width, depth),
            heap: Vec::new(),
        }
    }

    pub fn increment(&mut self, item: &str, count: u64) {
        self.sketch.increment(item, count);
        let freq = self.sketch.estimate(item);

        // Update or insert into heap
        // Since we store as Vec, we can iterate.
        if let Some(pos) = self.heap.iter().position(|h| h.key == item) {
            self.heap[pos].frequency = freq;
            // We update the frequency in place. Since we scan the entire vector to find the minimum
            // when the heap is full, we don't need to maintain heap invariants strictly at every step.
        } else {
            // New item
            if self.heap.len() < self.k {
                self.heap.push(HeapItem {
                    key: item.to_string(),
                    frequency: freq,
                });
            } else {
                // Heap is full. We need to replace the element with the lowest frequency
                // if the new item has a higher frequency.
                
                let (min_idx, min_val) = self.heap.iter().enumerate()
                    .min_by_key(|(_, item)| item.frequency)
                    .unwrap(); // Safe because len == k > 0
                
                if freq > min_val.frequency {
                    self.heap[min_idx] = HeapItem {
                        key: item.to_string(),
                        frequency: freq,
                    };
                }
            }
        }
        // Note: We do not maintain the heap in sorted order during updates.
        // We only scan for the minimum element when eviction is necessary.
        // Sorting is deferred until `top_k()` is called.
    }

    pub fn top_k(&self) -> Vec<(String, u64)> {
        let mut result: Vec<_> = self
            .heap
            .iter()
            .map(|item| (item.key.clone(), item.frequency))
            .collect();
        // Sort by frequency descending
        result.sort_by(|a, b| b.1.cmp(&a.1));
        result
    }

    pub fn merge(&mut self, other: &Self) {
        self.sketch.merge(&other.sketch);

        // Rebuild heap from union of top-K sets
        let mut all_items: Vec<HeapItem> = self.heap.clone();
        // Avoid duplicates
        for item in &other.heap {
            if !all_items.iter().any(|x| x.key == item.key) {
                all_items.push(item.clone());
            }
        }

        // Re-estimate frequencies and rebuild heap
        for item in &mut all_items {
            item.frequency = self.sketch.estimate(&item.key);
        }

        // Sort by frequency descending to pick top K
        all_items.sort_by(|a, b| b.frequency.cmp(&a.frequency));
        all_items.truncate(self.k);
        self.heap = all_items;
    }

    pub fn is_empty(&self) -> bool {
        self.heap.is_empty() && self.sketch.matrix.iter().all(|row| row.iter().all(|&x| x == 0))
    }
}

impl Default for TopK {
    fn default() -> Self {
        Self::new(10, 2000, 7)
    }
}

pub struct TopKReader<'a> {
    bytes: &'a [u8],
}

impl<'a> TopKReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl<'a> CrdtReader<'a> for TopKReader<'a> {
    fn is_empty(&self) -> Result<bool, CrdtError> {
        let message_reader = serialize::read_message(
            self.bytes,
            ReaderOptions::new(),
        ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<topk_capnp::top_k::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
        Ok(root.get_top_keys().map_err(|e| CrdtError::Deserialization(e.to_string()))?.len() == 0)
    }
}

impl Crdt for TopK {
    type Reader<'a> = TopKReader<'a>;

    fn validate(&self) -> Result<(), CrdtError> {
        if self.k == 0 {
            return Err(CrdtError::Validation("K must be positive".into()));
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.is_empty()
    }

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        if readers.is_empty() {
            return Ok(Self::default());
        }

        let mut merged = Self::from_capnp_bytes(readers[0].bytes)?;
        
        for reader in &readers[1..] {
            let other = Self::from_capnp_bytes(reader.bytes)?;
            merged.merge(&other);
        }

        Ok(merged)
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new_default();
        {
            let mut topk_builder = message.init_root::<topk_capnp::top_k::Builder>();

            topk_builder.set_k(self.k as u32);
            topk_builder.set_width(self.sketch.width as u32);
            topk_builder.set_depth(self.sketch.depth as u32);

            // Serialize sketch counters
            let counters_len = self.sketch.width * self.sketch.depth;
            let mut counters_builder = topk_builder.reborrow().init_counters(counters_len as u32);
            
            let mut idx = 0;
            for row in &self.sketch.matrix {
                for &val in row {
                    counters_builder.set(idx, val);
                    idx += 1;
                }
            }

            // Serialize heap
            let mut keys_builder = topk_builder
                .reborrow()
                .init_top_keys(self.heap.len() as u32);
            for (i, item) in self.heap.iter().enumerate() {
                keys_builder.set(i as u32, item.key.as_str().into());
            }

            let mut freqs_builder = topk_builder.init_top_frequencies(self.heap.len() as u32);
            for (i, item) in self.heap.iter().enumerate() {
                freqs_builder.set(i as u32, item.frequency);
            }
        }

        let mut buf = Vec::new();
        serialize::write_message(&mut buf, &message)
            .expect("TopK Cap'n Proto serialization should not fail");

        buf
    }
}

impl TopK {
    pub fn from_capnp_bytes(data: &[u8]) -> Result<Self, CrdtError> {
        let message_reader = serialize::read_message(
            data,
            ReaderOptions::new(),
        ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<topk_capnp::top_k::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let k = root.get_k() as usize;
        let width = root.get_width() as usize;
        let depth = root.get_depth() as usize;

        let counters_reader = root
            .get_counters()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let mut matrix = vec![vec![0; width]; depth];
        let mut idx = 0;
        for r in 0..depth {
            for c in 0..width {
                if idx < counters_reader.len() {
                    matrix[r][c] = counters_reader.get(idx);
                    idx += 1;
                }
            }
        }

        let sketch = CountMinSketch {
            width,
            depth,
            matrix,
        };

        let keys_reader = root
            .get_top_keys()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let freqs_reader = root
            .get_top_frequencies()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let heap: Vec<HeapItem> = (0..keys_reader.len().min(freqs_reader.len()))
            .filter_map(|i| {
                keys_reader.get(i).ok().and_then(|key| {
                    key.to_string().ok().map(|k| HeapItem {
                        key: k,
                        frequency: freqs_reader.get(i),
                    })
                })
            })
            .collect();

        Ok(TopK { k, sketch, heap })
    }
}
