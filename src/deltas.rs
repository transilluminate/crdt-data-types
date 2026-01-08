use serde::Deserialize;
use std::collections::HashMap;

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum GCounterDelta {
    Direct(i64),
    Object { increment: i64 },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum PNCounterDelta {
    Direct(i64),
    Object { increment: i64 },
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum GSetDelta<T> {
    List(Vec<T>),
    Object { add: Vec<T> },
}

#[derive(Debug, Deserialize)]
pub struct ORSetDelta<T> {
    pub add: Option<Vec<T>>,
    pub remove: Option<Vec<T>>,
}

#[derive(Debug, Deserialize)]
pub struct LWWSetDelta<T> {
    pub add: Option<Vec<T>>,
    pub remove: Option<Vec<T>>,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize)]
pub struct LWWRegisterDelta<T> {
    pub value: T,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize)]
pub struct FWWRegisterDelta<T> {
    pub value: T,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
pub enum MVRegisterDelta<T> {
    Direct(T),
    Object { value: T },
}

#[derive(Debug, Deserialize)]
pub struct LWWMapDelta<K, V>
where
    K: std::hash::Hash + std::cmp::Eq,
{
    pub set: Option<HashMap<K, V>>,
    pub remove: Option<Vec<K>>,
    pub timestamp: u64,
}

#[derive(Debug, Deserialize)]
pub struct ORMapDelta<K, V>
where
    K: std::hash::Hash + std::cmp::Eq,
{
    pub set: Option<HashMap<K, V>>,
    pub remove: Option<Vec<K>>,
}
