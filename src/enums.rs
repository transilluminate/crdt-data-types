// Copyright (c) 2026 Adrian Robinson. All rights reserved.
// Licensed under the MIT License. See LICENSE file in the project root for full license information.

use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;
use crate::traits::CrdtError;

/// Enumeration of supported standard CRDT types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CrdtType {
    GCounter,
    PNCounter,
    GSet,
    ORSet,
    LWWSet,
    LWWRegister,
    FWWRegister,
    MVRegister,
    LWWMap,
    ORMap,
}

impl fmt::Display for CrdtType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CrdtType::GCounter => write!(f, "GCounter"),
            CrdtType::PNCounter => write!(f, "PNCounter"),
            CrdtType::GSet => write!(f, "GSet"),
            CrdtType::ORSet => write!(f, "ORSet"),
            CrdtType::LWWSet => write!(f, "LWWSet"),
            CrdtType::LWWRegister => write!(f, "LWWRegister"),
            CrdtType::FWWRegister => write!(f, "FWWRegister"),
            CrdtType::MVRegister => write!(f, "MVRegister"),
            CrdtType::LWWMap => write!(f, "LWWMap"),
            CrdtType::ORMap => write!(f, "ORMap"),
        }
    }
}

impl FromStr for CrdtType {
    type Err = CrdtError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.replace('_', "").to_lowercase().as_str() {
            "gcounter" => Ok(CrdtType::GCounter),
            "pncounter" => Ok(CrdtType::PNCounter),
            "gset" => Ok(CrdtType::GSet),
            "orset" => Ok(CrdtType::ORSet),
            "lwwset" => Ok(CrdtType::LWWSet),
            "lwwregister" => Ok(CrdtType::LWWRegister),
            "fwwregister" => Ok(CrdtType::FWWRegister),
            "mvregister" => Ok(CrdtType::MVRegister),
            "lwwmap" => Ok(CrdtType::LWWMap),
            "ormap" => Ok(CrdtType::ORMap),
            _ => Err(CrdtError::InvalidInput(format!("Unknown CRDT type: {}", s))),
        }
    }
}

/// Enumeration of supported probabilistic CRDT types.
#[cfg(feature = "probabilistic")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum ProbabilisticCrdtType {
    HyperLogLog,
    CountMinSketch,
    RoaringBitmap,
    TDigest,
    TopK,
}

#[cfg(feature = "probabilistic")]
impl fmt::Display for ProbabilisticCrdtType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ProbabilisticCrdtType::HyperLogLog => write!(f, "HyperLogLog"),
            ProbabilisticCrdtType::CountMinSketch => write!(f, "CountMinSketch"),
            ProbabilisticCrdtType::RoaringBitmap => write!(f, "RoaringBitmap"),
            ProbabilisticCrdtType::TDigest => write!(f, "TDigest"),
            ProbabilisticCrdtType::TopK => write!(f, "TopK"),
        }
    }
}

#[cfg(feature = "probabilistic")]
impl FromStr for ProbabilisticCrdtType {
    type Err = CrdtError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.replace('_', "").to_lowercase().as_str() {
            "hyperloglog" => Ok(ProbabilisticCrdtType::HyperLogLog),
            "countminsketch" => Ok(ProbabilisticCrdtType::CountMinSketch),
            "roaringbitmap" => Ok(ProbabilisticCrdtType::RoaringBitmap),
            "tdigest" => Ok(ProbabilisticCrdtType::TDigest),
            "topk" => Ok(ProbabilisticCrdtType::TopK),
            _ => Err(CrdtError::InvalidInput(format!("Unknown Probabilistic CRDT type: {}", s))),
        }
    }
}
