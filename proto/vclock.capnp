@0x8a1b2c3d4e5f6789;

# VectorClock: Hybrid causal + temporal ordering
#
# Each entry is (node_id, logical_counter, epoch_seconds)
# - logical_counter: Increments on each operation (causal ordering)
# - epoch_seconds: Wall-clock time of last increment (compaction policies)

struct VectorClock {
  entries @0 :List(Entry);
  
  struct Entry {
    nodeId @0 :Text;
    logicalCounter @1 :UInt64;
    epochSeconds @2 :UInt64;
  }
}
