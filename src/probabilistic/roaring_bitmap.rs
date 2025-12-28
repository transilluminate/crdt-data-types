use crate::roaring_bitmap_capnp;
use crate::traits::{Crdt, CrdtError, CrdtReader};
use capnp::message::{Builder, ReaderOptions};
use capnp::serialize;
use roaring::RoaringBitmap as Rb;
use serde::{Deserialize, Serialize};
use std::io::Cursor;

/// RoaringBitmap - Compressed Integer Set CRDT
///
/// A high-performance, compressed bitmap data structure for storing sets of 32-bit integers.
/// It is particularly effective for sparse data and set operations (union, intersection).
///
/// # Key Properties
///
/// - **Compression**: Uses Roaring Bitmap compression to store sets efficiently (often 10-100x smaller than uncompressed bitmaps).
/// - **Fast Operations**: Optimized for fast set operations like union (merge), intersection, and difference.
/// - **Mergeable**: Merging two RoaringBitmaps results in their union (bitwise OR).
/// - **Use Cases**: User segmentation, activity tracking, inverted indices.
///
/// # Example
///
/// ```
/// use crdt_data_types::RoaringBitmap;
///
/// let mut rb = RoaringBitmap::new(1000);
/// rb.insert(1);
/// rb.insert(100);
/// rb.insert(999);
///
/// assert!(rb.contains(100));
/// assert!(!rb.contains(50));
/// assert_eq!(rb.cardinality(), 3);
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RoaringBitmap {
    #[serde(with = "roaring_serde")]
    bitmap: Rb,
    pub max_value: u32,
    pub description: String,
}

mod roaring_serde {
    use super::*;
    use serde::{Deserializer, Serializer};

    pub fn serialize<S>(bitmap: &Rb, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut bytes = Vec::new();
        bitmap.serialize_into(&mut bytes).map_err(serde::ser::Error::custom)?;
        serializer.serialize_bytes(&bytes)
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Rb, D::Error>
    where
        D: Deserializer<'de>,
    {
        let bytes: Vec<u8> = Deserialize::deserialize(deserializer)?;
        Rb::deserialize_from(&mut Cursor::new(&bytes)).map_err(serde::de::Error::custom)
    }
}

impl Default for RoaringBitmap {
    fn default() -> Self {
        Self::new(u32::MAX)
    }
}

impl RoaringBitmap {
    pub fn new(max_value: u32) -> Self {
        Self {
            bitmap: Rb::new(),
            max_value,
            description: String::new(),
        }
    }

    pub fn with_description(max_value: u32, description: impl Into<String>) -> Self {
        Self {
            bitmap: Rb::new(),
            max_value,
            description: description.into(),
        }
    }

    pub fn insert(&mut self, value: u32) {
        if value <= self.max_value {
            self.bitmap.insert(value);
        }
    }

    pub fn contains(&self, value: u32) -> bool {
        self.bitmap.contains(value)
    }

    pub fn cardinality(&self) -> u64 {
        self.bitmap.len()
    }

    pub fn merge(&mut self, other: &Self) {
        self.bitmap |= &other.bitmap;
    }

    pub fn from_capnp_bytes(data: &[u8]) -> Result<Self, CrdtError> {
        let message_reader = serialize::read_message(
            data,
            ReaderOptions {
                traversal_limit_in_words: None,
                nesting_limit: 64,
            },
        )
        .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<roaring_bitmap_capnp::roaring_bitmap::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let bitmap_data = root
            .get_bitmap_data()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        
        let bitmap = Rb::deserialize_from(&mut Cursor::new(bitmap_data))
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let description = root
            .get_description()
            .map(|d| d.to_string().unwrap_or_default())
            .unwrap_or_default();

        Ok(Self {
            bitmap,
            max_value: root.get_max_value(),
            description,
        })
    }
}

pub struct RoaringBitmapReader<'a> {
    bytes: &'a [u8],
}

impl<'a> RoaringBitmapReader<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes }
    }
}

impl<'a> CrdtReader<'a> for RoaringBitmapReader<'a> {
    fn is_empty(&self) -> Result<bool, CrdtError> {
        // We have to parse to check if empty, or at least check the data length
        let message_reader = serialize::read_message(
            self.bytes,
            ReaderOptions::new(),
        ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

        let root = message_reader
            .get_root::<roaring_bitmap_capnp::roaring_bitmap::Reader>()
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
        let bitmap_data = root.get_bitmap_data().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
        
        // The roaring crate doesn't expose a way to check for emptiness directly from bytes
        // without at least partial deserialization. For safety, we deserialize the bitmap.
        
        let bitmap = Rb::deserialize_from(&mut Cursor::new(bitmap_data))
            .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
        Ok(bitmap.is_empty())
    }
}

impl Crdt for RoaringBitmap {
    type Reader<'a> = RoaringBitmapReader<'a>;

    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError> {
        let mut merged = Rb::new();
        let mut max_value = 0;
        let mut description = String::new();

        for (i, reader) in readers.iter().enumerate() {
            let message_reader = serialize::read_message(
                reader.bytes,
                ReaderOptions::new(),
            ).map_err(|e| CrdtError::Deserialization(e.to_string()))?;

            let root = message_reader
                .get_root::<roaring_bitmap_capnp::roaring_bitmap::Reader>()
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
            if i == 0 {
                max_value = root.get_max_value();
                if let Ok(desc) = root.get_description() {
                    description = desc.to_string().unwrap_or_default();
                }
            }

            let bitmap_data = root.get_bitmap_data().map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
            let bitmap = Rb::deserialize_from(&mut Cursor::new(bitmap_data))
                .map_err(|e| CrdtError::Deserialization(e.to_string()))?;
            
            merged |= bitmap;
        }

        Ok(Self {
            bitmap: merged,
            max_value,
            description,
        })
    }

    fn validate(&self) -> Result<(), CrdtError> {
        // Check if any value exceeds max_value
        if let Some(max) = self.bitmap.max() {
            if max > self.max_value {
                return Err(CrdtError::Validation(format!(
                    "Bitmap contains value {} greater than max_value {}",
                    max, self.max_value
                )));
            }
        }
        Ok(())
    }

    fn is_empty(&self) -> bool {
        self.bitmap.is_empty()
    }

    fn to_capnp_bytes(&self) -> Vec<u8> {
        let mut message = Builder::new_default();
        let mut root = message.init_root::<roaring_bitmap_capnp::roaring_bitmap::Builder>();
        
        root.set_max_value(self.max_value);
        root.set_description(self.description.as_str().into());
        
        let mut bytes = Vec::new();
        self.bitmap.serialize_into(&mut bytes).unwrap();
        root.set_bitmap_data(&bytes);

        let mut data = Vec::new();
        serialize::write_message(&mut data, &message).unwrap();
        data
    }
}
