@0x9feb2beb7ea09b54;

# TopK: Heavy hitter tracking
# Uses Count-Min Sketch + Min-Heap
#
# Memory: Sketch (width×depth×8) + Heap (K×item_size)
# Merge: Merge sketches, rebuild heap from union

struct TopK {
    # Parameters
    k @0 :UInt32;
    width @1 :UInt32;
    depth @2 :UInt32;
    
    # Count-Min Sketch counters
    counters @3 :List(UInt64);
    
    # Top-K items (keys and frequencies as parallel lists)
    topKeys @4 :List(Text);
    topFrequencies @5 :List(UInt64);
}
