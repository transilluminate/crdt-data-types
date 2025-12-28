use crate::tdigest_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;
use serde::{Deserialize, Serialize};
use tdigest::TDigest as Td;

/// TDigest - Percentile Estimation CRDT
///
/// A probabilistic data structure for accurate percentile estimation (P50, P95, P99, etc.)
/// on distributed data streams. It uses the `tdigest` algorithm to maintain a compressed
/// representation of the distribution (centroids).
///
/// # Key Properties
///
/// - **Memory Efficiency**: Uses a small number of centroids (typically < 1000) to represent millions of points.
/// - **Tail Accuracy**: Particularly accurate at the tails of the distribution (e.g., P99, P99.9).
/// - **Mergeable**: Can be merged from multiple replicas to produce a global summary.
///
/// # Example
///
/// ```
/// use crdt_data_types::TDigest;
///
/// let mut td = TDigest::new(100);
/// for i in 1..=100 {
///     td.insert(i as f64);
/// }
///
/// assert!((td.quantile(0.5) - 50.0).abs() < 1.0);
/// assert!((td.quantile(0.99) - 99.0).abs() < 1.0);
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TDigest {
    #[serde(skip)]
    digest: Td,
    // We keep these for serialization/deserialization consistency
    // but the real state is in `digest`
    pub min: f64,
    pub max: f64,
    pub sum: f64,
    pub count: u64,
}

impl Default for TDigest {
    fn default() -> Self {
        Self::new(100)
    }
}

impl PartialEq for TDigest {
    fn eq(&self, other: &Self) -> bool {
        // TDigest equality is tricky because of internal state (centroids).
        // We'll compare the observable properties.
        self.count == other.count &&
        self.min == other.min &&
        self.max == other.max &&
        self.sum == other.sum &&
        // This is a weak check, but exact centroid match is unlikely after merges
        self.digest.max_size() == other.digest.max_size()
    }
}

impl TDigest {
    pub fn new(compression: usize) -> Self {
        Self {
            digest: Td::new_with_size(compression),
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
            sum: 0.0,
            count: 0,
        }
    }

    pub fn insert(&mut self, value: f64) {
        self.digest = self.digest.merge_unsorted(vec![value]);
        self.count += 1;
        self.min = self.min.min(value);
        self.max = self.max.max(value);
        self.sum += value;
    }

    pub fn insert_weighted(&mut self, value: f64, weight: f64) {
        // The underlying `tdigest` crate does not currently expose a direct weighted insert API.
        // As a temporary workaround, we perform repeated insertions.
        // TODO: Optimize this when the upstream crate supports weighted insertion or by merging centroids directly.
        for _ in 0..weight as u64 {
            self.insert(value);
        }
    }

    pub fn quantile(&self, q: f64) -> f64 {
        self.digest.estimate_quantile(q)
    }

    pub fn merge(&mut self, other: &Self) {
        // To merge, we extract centroids from other and merge them into self.
        // The `tdigest` crate supports merging digests.
        let digests = vec![self.digest.clone(), other.digest.clone()];
        self.digest = Td::merge_digests(digests);
        
        self.count += other.count;
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
    }

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
            .get_root::<tdigest_capnp::t_digest::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let compression = root.get_compression();
        let count = root.get_sample_count();
        let min = root.get_min();
        let max = root.get_max();
        let sum = root.get_sum();
        
        let centroids_data = root.get_centroids().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        let mut centroids = Vec::new();
        
        // Centroids are stored as [mean1, weight1, mean2, weight2, ...]
        let len = centroids_data.len();
        if len % 2 != 0 {
             return Err(CrdtError::Deserialization("Invalid centroid data length".into()));
        }
        
        for i in (0..len).step_by(2) {
            let mean = centroids_data.get(i);
            let weight = centroids_data.get(i+1);
            use tdigest::Centroid;
            centroids.push(Centroid::new(mean, weight));
        }

        let digest = Td::new(centroids, sum, count as f64, max, min, compression as usize);

        Ok(Self {
            digest,
            min,
            max,
            sum,
            count,
        })
    }
}

pub struct TDigestReader<'a> {
    bytes: &'a [u8],
}

impl<'a> TDigestReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl<'a> CrdtReader<'a> for TDigestReader<'a> {
    fn is_empty(&self) -> Result<bool, CrdtError> {
        let message_reader = serialize::read_message(
            self.bytes,
            ReaderOptions::new(),
        ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<tdigest_capnp::t_digest::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
        Ok(root.get_sample_count() == 0)
    }
}

impl Crdt for TDigest {
    type Reader<'a> = TDigestReader<'a>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        if readers.is_empty() {
            return Ok(Self::default());
        }

        let mut all_centroids = Vec::new();
        let mut total_count = 0;
        let mut global_min = f64::INFINITY;
        let mut global_max = f64::NEG_INFINITY;
        let mut total_sum = 0.0;
        let mut compression = 100; // Default

        for (i, reader) in readers.iter().enumerate() {
            let message_reader = serialize::read_message(
                reader.bytes,
                ReaderOptions::new(),
            ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

            let root = message_reader
                .get_root::<tdigest_capnp::t_digest::Reader>()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
            if i == 0 {
                compression = root.get_compression();
            }

            total_count += root.get_sample_count();
            let min = root.get_min();
            let max = root.get_max();
            let sum = root.get_sum();
            
            if root.get_sample_count() > 0 {
                global_min = global_min.min(min);
                global_max = global_max.max(max);
                total_sum += sum;
            }

            let centroids_data = root.get_centroids().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            let len = centroids_data.len();
            
            for j in (0..len).step_by(2) {
                let mean = centroids_data.get(j);
                let weight = centroids_data.get(j+1);
                use tdigest::Centroid;
                all_centroids.push(Centroid::new(mean, weight));
            }
        }

        // Re-compress by creating a new TDigest from all centroids
        let digest = Td::new(all_centroids, total_sum, total_count as f64, global_max, global_min, compression as usize);

        Ok(Self {
            digest,
            min: if total_count > 0 { global_min } else { f64::INFINITY },
            max: if total_count > 0 { global_max } else { f64::NEG_INFINITY },
            sum: total_sum,
            count: total_count,
        })
    }

    fn validate(&self) -> Result<(), CrdtError> {
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new_default();
        let mut root = message.init_root::<tdigest_capnp::t_digest::Builder>();
        
        root.set_compression(self.digest.max_size() as u32);
        root.set_sample_count(self.count);
        root.set_min(self.min);
        root.set_max(self.max);
        root.set_sum(self.sum);
        
        // WORKAROUND: Extract centroids via serialization.
        //
        // The `tdigest` crate (v0.2) encapsulates the `centroids` vector and does not expose
        // a public accessor method. However, it implements `Serialize`.
        //
        // To persist the full state (including the distribution data), we serialize the
        // internal `digest` to a `serde_json::Value`, extract the "centroids" array,
        // and populate the Cap'n Proto list.
        //
        // This is less efficient than direct access but necessary to ensure data persistence
        // and correct merging after deserialization.
        
        let val = serde_json::to_value(&self.digest).unwrap();
        if let Some(centroids_array) = val.get("centroids").and_then(|v| v.as_array()) {
             let mut centroids_list = root.init_centroids((centroids_array.len() * 2) as u32);
             for (i, c_val) in centroids_array.iter().enumerate() {
                 let mean = c_val.get("mean").and_then(|v| v.as_f64()).unwrap_or(0.0);
                 let weight = c_val.get("weight").and_then(|v| v.as_f64()).unwrap_or(0.0);
                 centroids_list.set((i * 2) as u32, mean);
                 centroids_list.set((i * 2 + 1) as u32, weight);
             }
        } else {
            // Fallback or empty
            root.init_centroids(0);
        }

        let mut data = Vec::new();
        serialize::write_message(&mut data, &message).unwrap();
        data
    }
}

