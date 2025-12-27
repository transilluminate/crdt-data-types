@0xfe8a157ca810d417;

# LwwMap: Last-Write-Wins Map CRDT

struct LwwMap {
  entries @0 :List(Entry);
  vclock @1 :Data;
  
  struct Entry {
    key @0 :Data;
    value @1 :Data;
    timestamp @2 :UInt64;
    nodeId @3 :Text;
  }
}
