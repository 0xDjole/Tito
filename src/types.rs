use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use thiserror::Error;
use tikv_client::Key;
use tikv_client::Transaction;
use tikv_client::TransactionClient;
use tokio::sync::Mutex;
use uuid::Uuid;

#[derive(Clone)]
pub struct TiKvConfigs {
    pub is_read_only: Arc<AtomicBool>,
}

pub type TiKvDatabase = Arc<TransactionClient>;

pub type TiKvKey = Key;

pub type TiKvCoreTransaction = Transaction;

pub struct TiKvLockItem {
    pub key: String,
    pub value: String,
}

#[derive(PartialEq, Eq, Error, Debug)]
pub enum TiKvError {
    #[error("TiKv: Failed to connect - {0}")]
    FailedToConnect(String),
    #[error("TiKv: NotFound - {0}")]
    NotFound(String),

    #[error("TiKv: Failed")]
    Failed,
    #[error("TiKv: Incorrect")]
    Incorrect,

    #[error("TiKv: FailedCreate - {0}")]
    FailedCreate(String),

    #[error("TiKv: FailedUpdate - {0}")]
    FailedUpdate(String),

    #[error("TiKv: FailedDelete - {0}")]
    FailedDelete(String),

    #[error("TiKv: Transaction - {0}")]
    TransactionFailed(String),

    #[error("TiKv: Failed to convert to ObjectId")]
    FailedToObjectId,

    #[error("TiKv: Deserialization Error: {0}")]
    DeserializationError(String),

    #[error("TiKv: Read-only mode")]
    ReadOnlyMode,
}

pub struct TiKvUtilsConnectPayload {
    pub uri: String,
}

pub struct TiKvUtilsConnectInput {
    pub payload: TiKvUtilsConnectPayload,
}

#[derive(Debug, Clone)]
pub struct TiKvGenerateJobPayload {
    pub id: String,
    pub action: Option<String>,
    pub clear_future: bool,
    pub scheduled_for: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct TiKvEmbeddedRelationshipConfig {
    pub source_field_name: String,
    pub destination_field_name: String,
    pub model: String,
}

#[derive(Debug, Clone)]
pub enum TiKvIndexBlockType {
    String,
    Number,
}

#[derive(Debug, Clone)]
pub struct TiKvIndexField {
    pub name: String,
    pub r#type: TiKvIndexBlockType,
}

#[derive(Debug, Clone)]
pub struct TiKvTemporalIndex {
    pub from_field: String,
    pub to_field: String,
    pub range_field: String,
}

pub struct TiKvIndexConfig {
    pub condition: bool,
    pub fields: Vec<TiKvIndexField>,
    pub name: String,
    pub custom_generator: Option<Box<dyn Fn() -> Result<Vec<String>, TiKvError> + Send + Sync>>,
}

#[derive(Debug, Clone)]
pub struct TiKvRelIndexConfig {
    pub name: String,
    pub field: String,
}

pub trait TiKvModelTrait {
    fn get_embedded_relationships(&self) -> Vec<TiKvEmbeddedRelationshipConfig>;
    fn get_indexes(&self) -> Vec<TiKvIndexConfig>;
    fn get_table_name(&self) -> String;
    fn get_event_table_name(&self) -> Option<String>;
    fn get_id(&self) -> String;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReverseIndex {
    pub value: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TiKvJob {
    pub id: String,
    pub key: String,
    pub entity_id: String,
    pub group_id: String,
    pub r#action: String,
    pub message: String,
    pub status: String,
    pub retries: u32,
    pub max_retries: u32,
    pub scheduled_for: i64,
    pub created_at: i64,
    pub updated_at: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct TiKvId {
    id: String,
    r#type: String,
}

impl TiKvId {
    pub fn new(id: &str, r#type: &str) -> TiKvId {
        TiKvId {
            id: id.to_string(),
            r#type: r#type.to_string(),
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}:{}", self.r#type, self.id)
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TiKvScanPayload {
    pub start: String,
    pub end: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Serialize, Deserialize, Debug)]
pub struct TiKvFindPayload {
    pub start: String,
    pub end: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
    pub rels: Vec<String>,
}

#[derive(Serialize, Deserialize)]
pub struct TiKvFindChangeLogSincePaylaod {
    pub timestamp: i64,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TiKvFindByIndexPayload {
    pub index: String,
    pub values: Vec<String>,
    pub rels: Vec<String>,
    pub end: Option<String>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TiKvFindByIndexRawPayload {
    pub index: String,
    pub values: Vec<String>,
    pub rels: Vec<String>,
    pub end: Option<String>,
    pub exact_match: bool,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TiKvFindByMultipleIndexPayload {
    pub queries: Vec<TiKvFindByMultipleIndexQuery>,
    pub limit: Option<u32>,
    pub cursor: Option<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TiKvFindByMultipleIndexQuery {
    pub index: String,
    pub edge_name: Option<String>,
    pub values: Vec<String>,
    pub rels: Vec<String>,
    pub end: Option<String>,
}

#[derive(Serialize, Deserialize)]
pub struct TiKvFindOneByIndexPayload {
    pub index: String,
    pub values: Vec<String>,
    pub rels: Vec<String>,
}

#[derive(Default, Serialize, Debug)]
pub struct TiKvPaginated<T> {
    pub items: Vec<T>,
    pub cursor: Option<String>,
}

#[derive(Default, Serialize, Deserialize, Debug)]
pub struct TiKvCursor {
    pub ids: Vec<Option<String>>,
}

impl TiKvCursor {
    pub fn first_id(&self) -> Result<String, TiKvError> {
        // Attempt to get the first element, which is an Option<&Option<String>>
        self.ids
            .get(0)
            .and_then(|id_option| id_option.as_ref())
            .map(|id| id.to_string())
            .ok_or(TiKvError::Failed)
    }
}

impl<T> TiKvPaginated<T> {
    pub fn new(items: Vec<T>, cursor: Option<String>) -> Self {
        Self { items, cursor }
    }
}

#[derive(Clone, Default, Serialize, Deserialize, Debug)]
pub struct TiKvChangeLog {
    pub id: String,
    pub record_id: String,
    pub operation: String,
    pub created_at: i64,
    pub data: Option<Value>,
    pub indexes: Vec<String>,
}

pub type DBUuid = Uuid;
