@0xf70d7759ab157ad8;

# OrSet: Observed-Remove Set CRDT

struct OrSet {
  elements @0 :List(Element);
  vclock @1 :Data;
  
  struct Element {
    element @0 :Data;
    ids @1 :List(IdEntry);
  }

  struct IdEntry {
    nodeId @0 :Text;
    counter @1 :UInt64;
  }
}
