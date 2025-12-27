@0xc16006f6c1de8292;

# GSet: Grow-only Set CRDT

struct GSet {
  elements @0 :List(Data);
  vclock @1 :Data;
}
