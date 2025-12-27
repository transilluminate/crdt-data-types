use crdt_data_types::*;
use serde_json;

fn main() {
    let mut set = ORSet::new();
    for i in 0..1000 {
        set.insert("node1", format!("item_{}", i));
    }

    let json_bytes = serde_json::to_vec(&set).unwrap();
    let capnp_bytes = set.to_capnp_bytes();

    println!("ORSet (1000 items):");
    println!("  JSON size: {} bytes", json_bytes.len());
    println!("  Capnp size: {} bytes", capnp_bytes.len());
    println!(
        "  Overhead: {:.2}x",
        capnp_bytes.len() as f32 / json_bytes.len() as f32
    );

    let mut gc = GCounter::new();
    for i in 0..1000 {
        gc.increment(&format!("node_{}", i), 100);
    }

    let json_gc = serde_json::to_vec(&gc).unwrap();
    let capnp_gc = gc.to_capnp_bytes();

    println!("\nGCounter (1000 nodes):");
    println!("  JSON size: {} bytes", json_gc.len());
    println!("  Capnp size: {} bytes", capnp_gc.len());
    println!(
        "  Overhead: {:.2}x",
        capnp_gc.len() as f32 / json_gc.len() as f32
    );
}
