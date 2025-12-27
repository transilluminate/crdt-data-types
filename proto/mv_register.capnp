@0xf3676366594042b2;

# MVRegister: Multi-Value Register CRDT

struct MvRegister {
  entries @0 :List(Entry);
  vclock @1 :Data;
}

struct Entry {
  value @0 :Data;
  nodeId @1 :Text;
  counter @2 :UInt64;
}
