use crate::TitoError;
use async_trait::async_trait;
use futures::lock::Mutex;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
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

pub type TitoValue = Vec<u8>;

pub type StorageKey = Vec<u8>;
pub type StorageValue = Vec<u8>;
pub type StorageKvPair = (StorageKey, StorageValue);
pub type StorageRange = Range<StorageKey>;

#[async_trait]
pub trait StorageEngine: Send + Sync + Clone {
    type Transaction: StorageTransaction;
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
}

#[async_trait]
pub trait StorageTransaction: Send + Sync {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn get(&self, key: StorageKey) -> Result<Option<StorageValue>, Self::Error>;
    async fn get_for_update(
        &mut self,
        key: StorageKey,
    ) -> Result<Option<StorageValue>, Self::Error>;
    async fn put(&self, key: StorageKey, value: StorageValue) -> Result<(), Self::Error>;
    async fn delete(&self, key: StorageKey) -> Result<(), Self::Error>;
    async fn scan(
        &self,
        range: StorageRange,
        limit: u32,
    ) -> Result<Vec<StorageKvPair>, Self::Error>;
    async fn scan_reverse(
        &self,
        range: StorageRange,
        limit: u32,
    ) -> Result<Vec<StorageKvPair>, Self::Error>;
    async fn batch_get(&self, keys: Vec<StorageKey>) -> Result<Vec<StorageKvPair>, Self::Error>;
    async fn batch_get_for_update(
        &self,
        keys: Vec<StorageKey>,
    ) -> Result<Vec<StorageKvPair>, Self::Error>;
    async fn commit(self) -> Result<(), Self::Error>;
    async fn rollback(self) -> Result<(), Self::Error>;
}

#[derive(Clone)]
pub struct TiKvStorageBackend {
    pub client: Arc<TransactionClient>,
    pub configs: TitoConfigs,
    pub active_transactions: Arc<Mutex<HashMap<String, TiKvStorageTransaction>>>,
}

#[async_trait]
impl StorageEngine for TiKvStorageBackend {
    type Transaction = TiKvStorageTransaction;
    type Error = TitoError;

    async fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let tx = self.client.begin_pessimistic().await.map_err(|e| {
            TitoError::TransactionFailed(format!("Failed to begin transaction: {}", e))
        })?;
        Ok(TiKvStorageTransaction {
            id: DBUuid::new_v4().to_string(),
            inner: Arc::new(tokio::sync::Mutex::new(tx)),
        })
    }

    fn configs(&self) -> TitoConfigs {
        self.configs.clone()
    }

    async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(Self::Transaction) -> Fut + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: From<TitoError> + Send,
    {
        let tx = self
            .begin_transaction()
            .await
            .map_err(|e| {
                TitoError::TransactionFailed(format!("Failed to begin transaction: {}", e))
            })
            .map_err(E::from)?;

        let mut active_transactions = self.active_transactions.lock().await;
        active_transactions.insert(tx.id.clone(), tx.clone());
        drop(active_transactions); // Release the lock early

        let result = f(tx.clone()).await;

        match &result {
            Ok(_) => {
                if let Err(e) = tx.clone().commit().await {
                    eprintln!("Warning: Transaction commit failed: {:?}", e);
                }
            }
            Err(_) => {
                if let Err(e) = tx.clone().rollback().await {
                    eprintln!("Warning: Transaction rollback failed: {:?}", e);
                }
            }
        };

        let mut active_transactions = self.active_transactions.lock().await;
        active_transactions.remove(&tx.id);

        result
    }

    async fn clear_active_transactions(&self) -> Result<(), TitoError> {
        let mut active_transactions = self.active_transactions.lock().await;
        for (_, mut tx) in active_transactions.drain() {
            if let Err(e) = tx.rollback().await {
                eprintln!(
                    "Warning: Failed to rollback transaction during cleanup: {:?}",
                    e
                );
            }
        }
        Ok(())
    }
}

#[derive(Clone)]
pub struct TiKvStorageTransaction {
    pub id: String,
    pub inner: Arc<tokio::sync::Mutex<Transaction>>,
}

#[async_trait]
impl StorageTransaction for TiKvStorageTransaction {
    type Error = TitoError;

    async fn get(&self, key: StorageKey) -> Result<Option<StorageValue>, Self::Error> {
        let tikv_key: tikv_client::Key = key.into();

        self.inner
            .lock()
            .await
            .get(tikv_key)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get operation failed: {}", e)))
    }

    async fn get_for_update(
        &mut self,
        key: StorageKey,
    ) -> Result<Option<StorageValue>, Self::Error> {
        let tikv_key: tikv_client::Key = key.into();

        self.inner
            .lock()
            .await
            .get_for_update(tikv_key)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get for update failed: {}", e)))
    }

    async fn put(&self, key: StorageKey, value: StorageValue) -> Result<(), Self::Error> {
        let tikv_key: tikv_client::Key = key.into();

        self.inner
            .lock()
            .await
            .put(tikv_key, value)
            .await
            .map_err(|e| TitoError::UpdateFailed(format!("Put operation failed: {}", e)))
    }

    async fn delete(&self, key: StorageKey) -> Result<(), Self::Error> {
        let tikv_key: tikv_client::Key = key.into();

        self.inner
            .lock()
            .await
            .delete(tikv_key)
            .await
            .map_err(|e| TitoError::DeleteFailed(format!("Delete operation failed: {}", e)))
    }

    async fn scan(
        &self,
        range: StorageRange,
        limit: u32,
    ) -> Result<Vec<StorageKvPair>, Self::Error> {
        // Convert Range<Vec<u8>> to tikv_client::BoundRange
        let bound_range: tikv_client::BoundRange = range.into();

        let result = self
            .inner
            .lock()
            .await
            .scan(bound_range, limit)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Scan operation failed: {}", e)))?;

        Ok(result.map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn scan_reverse(
        &self,
        range: StorageRange,
        limit: u32,
    ) -> Result<Vec<StorageKvPair>, Self::Error> {
        let bound_range: tikv_client::BoundRange = range.into();

        let result = self
            .inner
            .lock()
            .await
            .scan_reverse(bound_range, limit)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Reverse scan operation failed: {}", e)))?;

        Ok(result.map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn batch_get(&self, keys: Vec<StorageKey>) -> Result<Vec<StorageKvPair>, Self::Error> {
        // Convert Vec<Vec<u8>> to Vec<tikv_client::Key>
        let tikv_keys: Vec<tikv_client::Key> = keys.into_iter().map(|k| k.into()).collect();

        let result = self
            .inner
            .lock()
            .await
            .batch_get(tikv_keys)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Batch get operation failed: {}", e)))?;

        Ok(result.map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn batch_get_for_update(
        &self,
        keys: Vec<StorageKey>,
    ) -> Result<Vec<StorageKvPair>, Self::Error> {
        let tikv_keys: Vec<tikv_client::Key> = keys.into_iter().map(|k| k.into()).collect();

        let result = self
            .inner
            .lock()
            .await
            .batch_get_for_update(tikv_keys)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Batch get for update failed: {}", e)))?;

        Ok(result.into_iter().map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn commit(self) -> Result<(), Self::Error> {
        Arc::try_unwrap(self.inner)
            .map_err(|_| {
                TitoError::TransactionFailed("Transaction still has references".to_string())
            })?
            .into_inner()
            .commit()
            .await
            .map(|_| ())
            .map_err(|e| TitoError::TransactionFailed(format!("Transaction commit failed: {}", e)))
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        Arc::try_unwrap(self.inner)
            .map_err(|_| {
                TitoError::TransactionFailed("Transaction still has references".to_string())
            })?
            .into_inner()
            .rollback()
            .await
            .map_err(|e| {
                TitoError::TransactionFailed(format!("Transaction rollback failed: {}", e))
            })
    }
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
