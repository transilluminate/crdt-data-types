// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use serde::{de::DeserializeOwned, Serialize};
use thiserror::Error;

/// Error type for CRDT operations
#[derive(Debug, Error)]
pub enum CrdtError {
    #[error("Serialization error: {0}")]
    Serialization(String),
    #[error("Deserialization error: {0}")]
    Deserialization(String),
    #[error("Merge error: {0}")]
    Merge(String),
    #[error("Validation error: {0}")]
    Validation(String),
    #[error("Invalid input: {0}")]
    InvalidInput(String),
    #[error("Internal error: {0}")]
    Internal(String),
}

/// CRDT Reader trait - provides zero-copy access to serialized CRDT data.
///
/// Implementations of this trait should wrap Cap'n Proto readers to allow
/// inspection of CRDT fields without full deserialization. This is critical
/// for the performance of the zero-copy merge pattern.
pub trait CrdtReader<'a> {
    /// Returns true if the CRDT is in its default (empty) state.
    fn is_empty(&self) -> Result<bool, CrdtError>;
}

/// Core CRDT trait - defines the interface for state-based Conflict-free Replicated Data Types.
///
/// All CRDTs in this crate implement this trait to support the zero-copy merge pattern.
/// The pattern unifies merge and compaction, minimizing allocations and CPU overhead.
///
/// # Requirements
///
/// Implementations must satisfy the following algebraic properties:
/// - **Commutativity**: `merge([A, B]) == merge([B, A])`
/// - **Associativity**: `merge([merge([A, B]), C]) == merge([A, merge([B, C])])`
/// - **Idempotence**: `merge([A, A]) == merge([A])`
pub trait Crdt: Clone + Serialize + DeserializeOwned + Send + Sync {
    /// Zero-copy reader type associated with this CRDT.
    type Reader<'a>: CrdtReader<'a>
    where
        Self: 'a;

    /// Merges N CRDTs from zero-copy readers into a single new CRDT instance.
    ///
    /// This is the primary mechanism for both state synchronization and data compaction.
    /// By reading from multiple sources and producing a single merged result, we
    /// minimize memory churn and garbage collection pressure.
    ///
    /// # Arguments
    /// * `readers` - A slice of zero-copy readers pointing to serialized CRDT states.
    fn merge_from_readers(readers: &[Self::Reader<'_>]) -> Result<Self, CrdtError>
    where
        Self: Sized;

    /// Validates the internal consistency of the CRDT state.
    ///
    /// This is typically called after a merge operation to ensure that all
    /// invariants for the specific CRDT type are maintained.
    fn validate(&self) -> Result<(), CrdtError>;

    /// Returns true if the CRDT is in its default (empty) state.
    fn is_empty(&self) -> bool;

    /// Serializes the CRDT to a byte buffer using Cap'n Proto.
    ///
    /// The resulting bytes are optimized for zero-copy reading by `CrdtReader`.
    fn to_capnp_bytes(&self) -> Vec<u8>;
}
