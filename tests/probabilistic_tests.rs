// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

#[cfg(feature = "probabilistic")]
mod tests {
    use crdt_data_types::{CountMinSketch, HyperLogLog, RoaringBitmap, TDigest, TopK, Crdt};

    #[test]
    fn test_count_min_sketch_basic() {
        let mut cms = CountMinSketch::new(10, 5);
        cms.increment("apple", 1);
        cms.increment("apple", 1);
        cms.increment("banana", 1);

        assert!(cms.estimate("apple") >= 2);
        assert!(cms.estimate("banana") >= 1);
        assert_eq!(cms.estimate("cherry"), 0);
    }

    #[test]
    fn test_count_min_sketch_merge() {
        let mut cms1 = CountMinSketch::new(10, 5);
        cms1.increment("apple", 1);
        cms1.increment("apple", 1);

        let mut cms2 = CountMinSketch::new(10, 5);
        cms2.increment("banana", 1);
        cms2.increment("apple", 1);

        cms1.merge(&cms2);

        assert!(cms1.estimate("apple") >= 3);
        assert!(cms1.estimate("banana") >= 1);
    }

    #[test]
    fn test_count_min_sketch_serialization() {
        let mut cms = CountMinSketch::new(10, 5);
        cms.increment("apple", 1);
        
        let bytes = cms.to_capnp_bytes();
        let cms2 = CountMinSketch::from_capnp_bytes(&bytes).unwrap();
        
        assert!(cms2.estimate("apple") >= 1);
    }

    #[test]
    fn test_hyperloglog_basic() {
        let mut hll = HyperLogLog::new();
        hll.add("user1");
        hll.add("user2");
        hll.add("user3");
        hll.add("user1"); // Duplicate

        let count = hll.cardinality();
        // HLL is probabilistic, but for small numbers with 14 bits precision it might be exact or close.
        // With 3 unique elements, it should be very close.
        assert!(count >= 2 && count <= 4);
    }

    #[test]
    fn test_hyperloglog_merge() {
        let mut hll1 = HyperLogLog::new();
        hll1.add("user1");
        hll1.add("user2");

        let mut hll2 = HyperLogLog::new();
        hll2.add("user2");
        hll2.add("user3");

        hll1.merge(&hll2);

        let count = hll1.cardinality();
        assert!(count >= 3 && count <= 4); // Should be around 3
    }

    #[test]
    fn test_hyperloglog_serialization() {
        let mut hll = HyperLogLog::new();
        for i in 0..100 {
            hll.add(&format!("user{}", i));
        }
        
        let bytes = hll.to_capnp_bytes();
        let hll2 = HyperLogLog::from_capnp_bytes(&bytes).unwrap();
        
        assert_eq!(hll.cardinality(), hll2.cardinality());
    }

    #[test]
    fn test_roaring_bitmap_basic() {
        let mut rb = RoaringBitmap::new(1000);
        rb.insert(1);
        rb.insert(2);
        rb.insert(3);
        rb.insert(1); // Duplicate

        assert_eq!(rb.cardinality(), 3);
        assert!(rb.contains(1));
        assert!(rb.contains(2));
        assert!(rb.contains(3));
        assert!(!rb.contains(4));
    }

    #[test]
    fn test_roaring_bitmap_merge() {
        let mut rb1 = RoaringBitmap::new(1000);
        rb1.insert(1);
        rb1.insert(2);

        let mut rb2 = RoaringBitmap::new(1000);
        rb2.insert(2);
        rb2.insert(3);

        rb1.merge(&rb2);

        assert_eq!(rb1.cardinality(), 3);
        assert!(rb1.contains(1));
        assert!(rb1.contains(2));
        assert!(rb1.contains(3));
    }

    #[test]
    fn test_roaring_bitmap_serialization() {
        let mut rb = RoaringBitmap::new(1000);
        for i in 0..100 {
            rb.insert(i * 2);
        }
        
        let bytes = rb.to_capnp_bytes();
        let rb2 = RoaringBitmap::from_capnp_bytes(&bytes).unwrap();
        
        assert_eq!(rb.cardinality(), rb2.cardinality());
        assert!(rb2.contains(0));
        assert!(rb2.contains(198));
    }

    #[test]
    fn test_tdigest_basic() {
        let mut td = TDigest::new(100);
        for i in 1..=100 {
            td.insert(i as f64);
        }

        let p50 = td.quantile(0.5);
        let p99 = td.quantile(0.99);

        // TDigest is approximate, but for uniform 1-100, P50 should be close to 50
        assert!((p50 - 50.0).abs() < 1.0);
        assert!((p99 - 99.0).abs() < 1.0);
    }

    #[test]
    fn test_tdigest_merge() {
        let mut td1 = TDigest::new(100);
        for i in 1..=50 {
            td1.insert(i as f64);
        }

        let mut td2 = TDigest::new(100);
        for i in 51..=100 {
            td2.insert(i as f64);
        }

        td1.merge(&td2);

        let p50 = td1.quantile(0.5);
        assert!((p50 - 50.0).abs() < 1.0);
        assert_eq!(td1.count, 100);
    }

    #[test]
    fn test_tdigest_serialization() {
        let mut td = TDigest::new(100);
        for i in 1..=100 {
            td.insert(i as f64);
        }
        
        let bytes = td.to_capnp_bytes();
        let td2 = TDigest::from_capnp_bytes(&bytes).unwrap();
        
        assert_eq!(td.count, td2.count);
        assert!((td.quantile(0.5) - td2.quantile(0.5)).abs() < 0.001);
    }

    #[test]
    fn test_topk_basic() {
        let mut topk = TopK::new(3, 100, 5);
        topk.increment("apple", 10);
        topk.increment("banana", 20);
        topk.increment("cherry", 5);
        topk.increment("date", 15);

        let top = topk.top_k();
        assert_eq!(top.len(), 3);
        
        // Should contain banana (20), date (15), apple (10)
        assert_eq!(top[0].0, "banana");
        assert_eq!(top[1].0, "date");
        assert_eq!(top[2].0, "apple");
    }

    #[test]
    fn test_topk_merge() {
        let mut topk1 = TopK::new(3, 100, 5);
        topk1.increment("apple", 10);
        topk1.increment("banana", 5);

        let mut topk2 = TopK::new(3, 100, 5);
        topk2.increment("banana", 15);
        topk2.increment("cherry", 25);

        topk1.merge(&topk2);
        let top = topk1.top_k();

        // banana: 5+15=20, cherry: 25, apple: 10
        // Order: cherry (25), banana (20), apple (10)
        assert_eq!(top[0].0, "cherry");
        assert_eq!(top[1].0, "banana");
        assert_eq!(top[2].0, "apple");
    }

    #[test]
    fn test_topk_serialization() {
        let mut topk = TopK::new(3, 100, 5);
        topk.increment("apple", 10);
        topk.increment("banana", 20);
        
        let bytes = topk.to_capnp_bytes();
        let topk2 = TopK::from_capnp_bytes(&bytes).unwrap();
        
        let top = topk2.top_k();
        assert_eq!(top.len(), 2);
        assert_eq!(top[0].0, "banana");
        assert_eq!(top[1].0, "apple");
    }
}
