use crate::TitoError;
use async_trait::async_trait;
use futures::lock::Mutex;
use serde::Deserialize;
use serde::Serialize;
use serde_json::Value;
use std::collections::HashMap;
use std::future::Future;
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

#[async_trait]
pub trait StorageEngine: Send + Sync + Clone {
    type Transaction: StorageTransaction;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error>;

    fn configs(&self) -> TitoConfigs;

    async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(Self::Transaction) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: From<TitoError>;

    async fn clear_active_transactions(&self) -> Result<(), TitoError>;
}

#[async_trait]
pub trait StorageTransaction: Send + Sync {
    type Key: Clone + Send + Sync;
    type Value: Clone + Send + Sync;
    type KvPair: Clone + Send + Sync;
    type Range: Send + Sync;
    type Error: std::error::Error + Send + Sync + 'static;

    async fn get(&mut self, key: Self::Key) -> Result<Option<Self::Value>, Self::Error>;
    async fn get_for_update(&mut self, key: Self::Key) -> Result<Option<Self::Value>, Self::Error>;
    async fn put(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error>;
    async fn delete(&mut self, key: Self::Key) -> Result<(), Self::Error>;
    async fn scan(
        &mut self,
        range: Self::Range,
        limit: u32,
    ) -> Result<Vec<Self::KvPair>, Self::Error>;
    async fn scan_reverse(
        &mut self,
        range: Self::Range,
        limit: u32,
    ) -> Result<Vec<Self::KvPair>, Self::Error>;
    async fn batch_get(&mut self, keys: Vec<Self::Key>) -> Result<Vec<Self::KvPair>, Self::Error>;
    async fn batch_get_for_update(
        &mut self,
        keys: Vec<Self::Key>,
    ) -> Result<Vec<Self::KvPair>, Self::Error>;
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
        F: FnOnce(Self::Transaction) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: From<TitoError>,
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
                if let Err(e) = tx.commit().await {
                    eprintln!("Warning: Transaction commit failed: {:?}", e);
                }
            }
            Err(_) => {
                if let Err(e) = tx.rollback().await {
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
    type Key = tikv_client::Key;
    type Value = tikv_client::Value;
    type KvPair = tikv_client::KvPair;
    type Range = tikv_client::BoundRange;
    type Error = TitoError;

    async fn get(&mut self, key: Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.inner.lock().await.get(key.clone()).await.map_err(|e| {
            TitoError::QueryFailed(format!("Get operation failed for key {:?}: {}", key, e))
        })
    }

    async fn get_for_update(&mut self, key: Self::Key) -> Result<Option<Self::Value>, Self::Error> {
        self.inner
            .lock()
            .await
            .get_for_update(key.clone())
            .await
            .map_err(|e| {
                TitoError::QueryFailed(format!("Get for update failed for key {:?}: {}", key, e))
            })
    }

    async fn put(&mut self, key: Self::Key, value: Self::Value) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .await
            .put(key.clone(), value)
            .await
            .map_err(|e| {
                TitoError::UpdateFailed(format!("Put operation failed for key {:?}: {}", key, e))
            })
    }

    async fn delete(&mut self, key: Self::Key) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .await
            .delete(key.clone())
            .await
            .map_err(|e| {
                TitoError::DeleteFailed(format!("Delete operation failed for key {:?}: {}", key, e))
            })
    }

    async fn scan(
        &mut self,
        range: Self::Range,
        limit: u32,
    ) -> Result<Vec<Self::KvPair>, Self::Error> {
        self.inner
            .lock()
            .await
            .scan(range, limit)
            .await
            .map(|iter| iter.collect())
            .map_err(|e| TitoError::QueryFailed(format!("Scan operation failed: {}", e)))
    }

    async fn scan_reverse(
        &mut self,
        range: Self::Range,
        limit: u32,
    ) -> Result<Vec<Self::KvPair>, Self::Error> {
        self.inner
            .lock()
            .await
            .scan_reverse(range, limit)
            .await
            .map(|iter| iter.collect())
            .map_err(|e| TitoError::QueryFailed(format!("Reverse scan operation failed: {}", e)))
    }

    async fn batch_get(&mut self, keys: Vec<Self::Key>) -> Result<Vec<Self::KvPair>, Self::Error> {
        self.inner
            .lock()
            .await
            .batch_get(keys)
            .await
            .map(|iter| iter.collect())
            .map_err(|e| TitoError::QueryFailed(format!("Batch get operation failed: {}", e)))
    }

    async fn batch_get_for_update(
        &mut self,
        keys: Vec<Self::Key>,
    ) -> Result<Vec<Self::KvPair>, Self::Error> {
        self.inner
            .lock()
            .await
            .batch_get_for_update(keys)
            .await
            .map(|iter| iter.into_iter().collect())
            .map_err(|e| TitoError::QueryFailed(format!("Batch get for update failed: {}", e)))
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
