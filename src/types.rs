use crate::TitoError;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tikv_client::Key;
use tikv_client::Transaction;
use tikv_client::TransactionClient;
use uuid::Uuid;

#[derive(Clone)]
pub struct TitoConfigs {
    pub is_read_only: Arc<AtomicBool>,
}

pub type TitoDatabase = Arc<TransactionClient>;

pub type TitoKey = Key;

pub type TiKvTransaction = Transaction;

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
pub struct TitoGenerateJobPayload {
    pub id: String,
    pub action: Option<String>,
    pub scheduled_for: Option<i64>,
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
    fn has_event(&self) -> bool;
    fn get_id(&self) -> String;
}

#[derive(Clone, Serialize, Deserialize)]
pub struct ReverseIndex {
    pub value: Vec<String>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct TitoJob {
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
