@0x8ea8b00e1b53ebd6;

# Count-Min Sketch: Probabilistic frequency estimation
# Used for top-K tracking, heavy hitter detection
#
# Memory: width × depth × 8 bytes (e.g., 2000 × 7 = 112KB)
# Error: ε = e/width, δ = 1/e^depth
# Merge: Element-wise sum of counters

struct CountMinSketch {
    # Sketch dimensions
    width @0 :UInt32;
    depth @1 :UInt32;
    
    # Counter matrix (depth rows × width columns)
    # Stored as flat array: row*width + col
    counters @2 :List(UInt64);
    
    # Total count (for statistics)
    totalCount @3 :UInt64;
}
