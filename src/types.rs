use crate::{TitoError, TitoModel};
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde::Serialize;
use std::future::Future;
use std::ops::Range;
use std::time::Duration;
use uuid::Uuid;

pub trait TitoModelConstraints:
    Clone + Serialize + DeserializeOwned + Unpin + std::marker::Send + Sync + 'static + TitoModelTrait
{
}
impl<T> TitoModelConstraints for T where
    T: Clone
        + Serialize
        + DeserializeOwned
        + Unpin
        + std::marker::Send
        + Sync
        + 'static
        + TitoModelTrait
{
}

pub type TitoKey = Vec<u8>;
pub type TitoValue = Vec<u8>;
pub type TitoKvPair = (TitoKey, TitoValue);

pub(crate) fn validate_delete_range(start: &[u8], end: &[u8]) -> Result<(), TitoError> {
    if start.is_empty() || end.is_empty() {
        return Err(TitoError::InvalidInput(
            "Delete range bounds must not be empty".to_string(),
        ));
    }
    if start >= end {
        return Err(TitoError::InvalidInput(
            "Delete range start must be less than end".to_string(),
        ));
    }
    Ok(())
}

#[async_trait]
pub trait TitoEngine: Send + Sync + Clone {
    type Transaction: TitoTransaction;

    async fn begin_transaction(&self) -> Result<Self::Transaction, TitoError>;

    async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(Self::Transaction) -> Fut + Clone + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: From<TitoError> + Send + std::fmt::Debug;

    async fn clear_active_transactions(&self) -> Result<(), TitoError>;

    async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), TitoError>;

    async fn garbage_collect(&self, retention: Duration) -> Result<(), TitoError> {
        if retention.is_zero() {
            return Err(TitoError::InvalidInput(
                "Garbage collection retention must be greater than zero".to_string(),
            ));
        }
        Ok(())
    }

    fn model<T: TitoModelConstraints>(self) -> TitoModel<Self, T> {
        TitoModel::new(self)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Guard {
    None,
    Share,
    Update,
}

mod sealed {
    pub struct Sealed;
}
pub(crate) use sealed::Sealed;

#[async_trait]
pub trait TitoTransaction: Send + Sync {
    fn start_version(&self) -> u64;

    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<TitoValue>, TitoError>;

    async fn get_keys<K: AsRef<[u8]> + Send>(
        &self,
        keys: Vec<K>,
        sealed: Sealed,
    ) -> Result<Vec<TitoKvPair>, TitoError>;

    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), TitoError>;
    async fn delete<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<(), TitoError>;
    async fn scan<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError>;

    async fn scan_keys<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<Vec<u8>>, TitoError>;

    async fn scan_reverse<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError>;

    async fn commit(self) -> Result<(), TitoError>;
    async fn rollback(self) -> Result<(), TitoError>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TitoIndex {
    pub name: &'static str,
    pub fields: Vec<TitoIndexField>,
    pub value: TitoIndexValue,
    pub unique: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TitoIndexValue {
    Full,
    KeysOnly,
    Part(&'static [&'static str]),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TitoIndexField {
    Text(String),
    Number(i64),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum TitoIndexBound {
    Included(Vec<TitoIndexField>),
    Excluded(Vec<TitoIndexField>),
}

pub trait TitoModelTrait {
    fn indexes(&self) -> Vec<TitoIndex>;
    fn table() -> String;
    fn id(&self) -> String;

    fn key_prefix() -> String {
        format!("table:{}", Self::table())
    }
}

#[derive(Default, Serialize, Debug)]
pub struct TitoPaginated<T> {
    pub items: Vec<T>,
    pub cursor: Option<String>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub(crate) struct TitoCursor {
    pub(crate) ids: Vec<Option<String>>,
}

impl TitoCursor {
    pub(crate) fn first_id(&self) -> Result<String, TitoError> {
        self.ids
            .first()
            .and_then(|id_option| id_option.as_ref())
            .map(|id| id.to_string())
            .ok_or(TitoError::InvalidInput(
                "Cursor has no valid ID in first position".to_string(),
            ))
    }
}

impl<T> TitoPaginated<T> {
    pub fn new(items: Vec<T>, cursor: Option<String>) -> Self {
        Self { items, cursor }
    }
}

pub type DBUuid = Uuid;

pub const PARTITION_DIGITS: usize = 4;
