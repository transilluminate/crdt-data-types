@0xc8b2361622350811;

struct Delta {
  union {
    gCounter @0 :Int64;             # value to increment
    pnCounter @1 :Int64;            # value to increment (can be negative)
    gSet @2 :List(Text);            # items to add
    orSet @3 :OrSetDelta;
    lwwSet @4 :LwwSetDelta;
    lwwRegister @5 :LwwRegisterDelta;
    fwwRegister @6 :FwwRegisterDelta;
    mvRegister @7 :Text;            # value to set
    lwwMap @8 :LwwMapDelta;
    orMap @9 :OrMapDelta;
  }
}

struct OrSetDelta {
  add @0 :List(Text);
  remove @1 :List(Text);
}

struct LwwSetDelta {
  add @0 :List(Text);
  remove @1 :List(Text);
  timestamp @2 :UInt64;
}

struct LwwRegisterDelta {
  value @0 :Text;
  timestamp @1 :UInt64;
}

struct FwwRegisterDelta {
  value @0 :Text;
  timestamp @1 :UInt64;
}

struct LwwMapDelta {
    set @0 :List(MapEntry);
    remove @1 :List(Text);
    timestamp @2 :UInt64;
    
    struct MapEntry {
        key @0 :Text;
        value @1 :Text;
    }
}

struct OrMapDelta {
    set @0 :List(MapEntry);
    remove @1 :List(Text);
    
    struct MapEntry {
        key @0 :Text;
        value @1 :Text;
    }
}
