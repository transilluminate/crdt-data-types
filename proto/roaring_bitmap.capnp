@0xa2ac380230f9e754;

# RoaringBitmap: Compressed activity tracking
# Efficient integer set with CRDT union semantics
#
# Memory: Variable (compressed, typically 10-100x smaller than naive)
# Merge: Bitwise OR (union of sets)

struct RoaringBitmap {
    # Serialized Roaring Bitmap bytes
    # Uses official Roaring format (portable)
    bitmapData @0 :Data;
    
    # Max value that can be tracked
    maxValue @1 :UInt32;
    
    # Optional description
    description @2 :Text;
}
