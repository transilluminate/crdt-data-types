@0x9248686f93cad9d3;

# LwwSet: Last-Write-Wins Set CRDT

struct LwwSet {
  addSet @0 :List(Entry);
  removeSet @1 :List(Entry);
  vclock @2 :Data;
  
  struct Entry {
    element @0 :Data;
    timestamp @1 :UInt64;
    nodeId @2 :Text;
  }
}
