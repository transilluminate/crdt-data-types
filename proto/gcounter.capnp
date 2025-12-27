# GCounter Cap'n Proto Schema
#
# Grow-only counter with per-node counts

@0x9a5e3d2c1b4f8e7a;

struct GCounter {
  # Per-node counters: list of (node_id, count) pairs
  entries @0 :List(Entry);
  
  # Vector clock for causal ordering (Phase 2.5+)
  vclock @1 :Data;
  
  struct Entry {
    nodeId @0 :Text;
    count @1 :Int64;
  }
}
