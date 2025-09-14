use crate::{TitoError, TitoModel};
use async_trait::async_trait;
use serde::Deserialize;
use serde::Serialize;
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use uuid::Uuid;
use serde::de::DeserializeOwned;

#[derive(Clone)]
pub struct TitoConfigs {
    pub is_read_only: Arc<AtomicBool>,
}

pub trait TitoModelConstraints: Default + Clone + Serialize + DeserializeOwned + Unpin + std::marker::Send + Sync + TitoModelTrait {}
impl<T> TitoModelConstraints for T where T: Default + Clone + Serialize + DeserializeOwned + Unpin + std::marker::Send + Sync + TitoModelTrait {}

pub type TitoKey = Vec<u8>;
pub type TitoValue = Vec<u8>;
pub type TitoKvPair = (TitoKey, TitoValue);
pub type TitoRange = Range<TitoKey>;

#[async_trait]
pub trait TitoEngine: Send + Sync + Clone {
    type Transaction: TitoTransaction;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error>;

    fn configs(&self) -> TitoConfigs;

    async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(Self::Transaction) -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: From<TitoError> + Send;

    async fn clear_active_transactions(&self) -> Result<(), TitoError>;

    fn model<T: TitoModelConstraints>(self) -> TitoModel<Self, T> {
        TitoModel::new(self)
    }
}

#[async_trait]
pub trait TitoTransaction: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<TitoValue>, Self::Error>;
    async fn get_for_update<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<TitoValue>, Self::Error>;
    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), Self::Error>;
    async fn delete<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<(), Self::Error>;
    async fn scan<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, Self::Error>;

    async fn scan_reverse<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, Self::Error>;

    async fn batch_get<K: AsRef<[u8]> + Send>(
        &self,
        keys: Vec<K>,
    ) -> Result<Vec<TitoKvPair>, Self::Error>;

    async fn batch_get_for_update<K: AsRef<[u8]> + Send>(
        &self,
        keys: Vec<K>,
    ) -> Result<Vec<TitoKvPair>, Self::Error>;

    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}

pub struct TitoLockItem {
    pub key: String,
    pub value: String,
}

pub struct TitoUtilsConnectPayload {
    pub uri: String,
}

pub struct TitoUtilsConnectInput {
    pub payload: TitoUtilsConnectPayload,
}

#[derive(Debug, Clone)]
pub struct TitoGenerateEventPayload {
    pub key: String,
    pub action: Option<String>,
    pub scheduled_for: Option<i64>,
    pub metadata: serde_json::Value,
}

#[derive(Debug, Clone)]
pub struct TitoEmbeddedRelationshipConfig {
    pub source_field_name: String,
    pub destination_field_name: String,
    pub model: String,
}

#[derive(Debug, Clone)]
pub enum TitoIndexBlockType {
    String,
    Number,
}

#[derive(Debug, Clone)]
pub struct TitoIndexField {
    pub name: String,
    pub r#type: TitoIndexBlockType,
}

#[derive(Debug, Clone)]
pub struct TitoTemporalIndex {
    pub from_field: String,
    pub to_field: String,
    pub range_field: String,
}

pub struct TitoIndexConfig {
    pub condition: bool,
    pub fields: Vec<TitoIndexField>,
    pub name: String,
    pub custom_generator: Option<Box<dyn Fn() -> Result<Vec<String>, TitoError> + Send + Sync>>,
}

#[derive(Debug, Clone)]
pub struct TitoRelIndexConfig {
    pub name: String,
    pub field: String,
}

pub trait TitoModelTrait {
    fn get_embedded_relationships(&self) -> Vec<TitoEmbeddedRelationshipConfig>;
    fn get_indexes(&self) -> Vec<TitoIndexConfig>;
    fn get_table_name(&self) -> String;
    fn get_events(&self) -> Vec<TitoEventConfig>;
    fn get_id(&self) -> String;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReverseIndex {
    pub value: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TitoEvent {
    pub id: String,
    pub key: String,
    pub entity: String,
    pub r#action: String,
    pub message: String,
    pub status: String,
    pub retries: u32,
    pub max_retries: u32,
    pub scheduled_for: i64,
    pub created_at: i64,
    pub updated_at: i64,
    pub metadata: serde_json::Value,
}

impl TitoEvent {
    pub fn entity_id(&self) -> String {
        let parts: Vec<&str> = self.entity.split(':').collect();
        parts
            .last()
            .map(|last| last.to_string())
            .unwrap_or_else(|| self.entity.clone())
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TitoId {
    id: String,
    r#type: String,
}

impl TitoId {
    pub fn new(id: &str, r#type: &str) -> TitoId {
        TitoId {
            id: id.to_string(),
            r#type: r#type.to_string(),
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}:{}", self.r#type, self.id)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TitoScanPayload {
    pub start: String,
    pub end: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TitoFindPayload {
    pub start: String,
    pub end: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    pub rels: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TitoFindByIndexPayload {
    pub index: String,
    pub values: Vec<String>,
    pub rels: Vec<String>,
    pub end: Option<String>,
    pub exact_match: bool,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TitoFindByMultipleIndexPayload {
    pub queries: Vec<TitoFindByMultipleIndexQuery>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TitoFindByMultipleIndexQuery {
    pub index: String,
    pub edge_name: Option<String>,
    pub values: Vec<String>,
    pub rels: Vec<String>,
    pub end: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct TitoFindOneByIndexPayload {
    pub index: String,
    pub values: Vec<String>,
    pub rels: Vec<String>,
}

#[derive(Default, Serialize, Debug)]
pub struct TitoPaginated<T> {
    pub items: Vec<T>,
    pub cursor: Option<String>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct TitoCursor {
    pub ids: Vec<Option<String>>,
}

impl TitoCursor {
    pub fn first_id(&self) -> Result<String, TitoError> {
        self.ids
            .get(0)
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

#[derive(Debug, Clone)]
pub struct TitoEventConfig {
    pub name: String,
    pub event_type: TitoEventType,
}

#[derive(Debug, Clone)]
pub enum TitoEventType {
    Queue,
    Audit,
}

#[derive(Debug, Clone, PartialEq)]
pub enum TitoOperationType {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Clone)]
pub struct TitoOptions {
    pub event_action: Option<String>,
    pub event_scheduled_at: Option<i64>,
    pub event_metadata: Option<serde_json::Value>,
    pub operation_type: TitoOperationType,
}

impl Default for TitoOptions {
    fn default() -> Self {
        Self {
            event_action: None,
            event_scheduled_at: None,
            event_metadata: None,
            operation_type: TitoOperationType::Update,
        }
    }
}

impl TitoOptions {
    pub fn with_event(action: &str) -> Self {
        Self {
            event_action: Some(action.to_string()),
            event_scheduled_at: None,
            event_metadata: None,
            operation_type: TitoOperationType::Update,
        }
    }
    
    pub fn with_scheduled_event(action: &str, timestamp: i64) -> Self {
        Self {
            event_action: Some(action.to_string()),
            event_scheduled_at: Some(timestamp),
            event_metadata: None,
            operation_type: TitoOperationType::Update,
        }
    }

    pub fn with_event_metadata(action: &str, metadata: serde_json::Value) -> Self {
        Self {
            event_action: Some(action.to_string()),
            event_scheduled_at: None,
            event_metadata: Some(metadata),
            operation_type: TitoOperationType::Update,
        }
    }

    pub fn with_scheduled_event_metadata(action: &str, timestamp: i64, metadata: serde_json::Value) -> Self {
        Self {
            event_action: Some(action.to_string()),
            event_scheduled_at: Some(timestamp),
            event_metadata: Some(metadata),
            operation_type: TitoOperationType::Update,
        }
    }

    pub fn insert_with_metadata(metadata: serde_json::Value) -> Self {
        Self {
            event_action: Some("INSERT".to_string()),
            event_scheduled_at: None,
            event_metadata: Some(metadata),
            operation_type: TitoOperationType::Insert,
        }
    }

    pub fn update_with_metadata(metadata: serde_json::Value) -> Self {
        Self {
            event_action: Some("UPDATE".to_string()),
            event_scheduled_at: None,
            event_metadata: Some(metadata),
            operation_type: TitoOperationType::Update,
        }
    }

    pub fn delete_with_metadata(metadata: serde_json::Value) -> Self {
        Self {
            event_action: Some("DELETE".to_string()),
            event_scheduled_at: None,
            event_metadata: Some(metadata),
            operation_type: TitoOperationType::Delete,
        }
    }
}
