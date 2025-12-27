@0xd2c3cee706cdddba;

# PnCounter: Positive-Negative Counter CRDT

struct PnCounter {
  positive @0 :Data;  # Serialized GCounter
  negative @1 :Data;  # Serialized GCounter
  vclock @2 :Data;
}
