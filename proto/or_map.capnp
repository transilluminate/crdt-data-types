@0xb57973cce80b52e8;

# ORMap: Observed-Remove Map CRDT

struct OrMap {
  elements @0 :Data;  # Serialized ORSet
  vclock @1 :Data;
}
