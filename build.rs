fn main() {
    let mut config = capnpc::CompilerCommand::new();
    config.file("proto/fww_register.capnp");
    config.file("proto/gcounter.capnp");
    config.file("proto/gset.capnp");
    config.file("proto/lww_map.capnp");
    config.file("proto/lww_register.capnp");
    config.file("proto/lww_set.capnp");
    config.file("proto/mv_register.capnp");
    config.file("proto/or_map.capnp");
    config.file("proto/orset.capnp");
    config.file("proto/pncounter.capnp");
    config.file("proto/vclock.capnp");
    
    // Probabilistic
    config.file("proto/count_min_sketch.capnp");
    config.file("proto/hyperloglog.capnp");
    config.file("proto/roaring_bitmap.capnp");
    config.file("proto/tdigest.capnp");
    config.file("proto/topk.capnp");

    config.run().expect("Cap'n Proto compilation failed");
}
