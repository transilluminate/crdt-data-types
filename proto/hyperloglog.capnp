@0xcb63134f21943ad5;

# HyperLogLog: Probabilistic cardinality estimation
# Used for unique counting (e.g., "How many unique users?")
#
# Memory: Fixed 16KB (16,384 registers)
# Precision: 14 bits (2^14 = 16,384 buckets)
# Error rate: ~0.81% standard error
# Merge: Element-wise maximum of registers

struct HyperLogLog {
    # 16,384 registers (16KB fixed size)
    # Each register stores max leading zeros count (u8)
    # Stored as single Data blob for efficient serialization
    registers @0 :Data;
}
