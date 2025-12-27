@0xc49a157ca811e92d;

# FwwRegister: First-Write-Wins Register CRDT

struct FwwRegister {
  value @0 :Data;
  timestamp @1 :UInt64;
  nodeId @2 :Text;
  vclock @3 :Data;
}
