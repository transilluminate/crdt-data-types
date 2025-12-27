@0xb97e157ca8110b2a;

# LwwRegister: Last-Write-Wins Register CRDT

struct LwwRegister {
  value @0 :Data;
  timestamp @1 :UInt64;
  nodeId @2 :Text;
  vclock @3 :Data;
}
