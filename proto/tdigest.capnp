@0xe3c2009862ae8368;

# TDigest: Probabilistic percentile estimation
# Used for P50/P95/P99 latency tracking
#
# Memory: Variable (depends on centroids, typically 1-5KB)
# Accuracy: ~1% error, better at tails
# Merge: Combine centroids and re-compress

struct TDigest {
    # Compression parameter (controls accuracy vs memory)
    compression @0 :UInt32;
    
    # Number of samples added
    sampleCount @1 :UInt64;
    
    # Centroids (mean, weight pairs)
    # Stored as flat array: [mean1, weight1, mean2, weight2, ...]
    centroids @2 :List(Float64);
    
    # Statistics for empty digest handling
    min @3 :Float64;
    max @4 :Float64;
    sum @5 :Float64;
}
