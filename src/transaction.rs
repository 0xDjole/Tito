use crate::{
    types::{DBUuid, TiKvTransaction, TitoDatabase},
    TitoError,
};
use std::{collections::HashMap, future::Future, sync::Arc};
use tikv_client::{BoundRange, Key, KvPair, Timestamp, Value};
use tokio::sync::Mutex;

#[derive(Clone)]
pub struct TitoTransaction {
    pub id: String,
    pub inner: Arc<Mutex<TiKvTransaction>>,
}

impl TitoTransaction {
    pub fn new(tx: TiKvTransaction) -> Self {
        Self {
            id: DBUuid::new_v4().to_string(),
            inner: Arc::new(Mutex::new(tx)),
        }
    }

    pub async fn scan(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>, TitoError> {
        self.inner
            .lock()
            .await
            .scan(range, limit)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Scan operation failed: {}", e)))
    }

    pub async fn scan_reverse(
        &self,
        range: impl Into<BoundRange>,
        limit: u32,
    ) -> Result<impl Iterator<Item = KvPair>, TitoError> {
        self.inner
            .lock()
            .await
            .scan_reverse(range, limit)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Reverse scan operation failed: {}", e)))
    }

    pub async fn commit(&self) -> Result<Option<Timestamp>, TitoError> {
        self.inner
            .lock()
            .await
            .commit()
            .await
            .map_err(|e| TitoError::TransactionFailed(format!("Transaction commit failed: {}", e)))
    }

    pub async fn rollback(&self) -> Result<(), TitoError> {
        self.inner.lock().await.rollback().await.map_err(|e| {
            TitoError::TransactionFailed(format!("Transaction rollback failed: {}", e))
        })
    }

    pub async fn batch_get_for_update(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>, TitoError> {
        self.inner
            .lock()
            .await
            .batch_get_for_update(keys)
            .await
            .map(|iter| iter.into_iter().collect())
            .map_err(|e| TitoError::QueryFailed(format!("Batch get for update failed: {}", e)))
    }

    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>, TitoError> {
        let key_val = key.into();
        self.inner
            .lock()
            .await
            .get(key_val.clone())
            .await
            .map_err(|e| {
                TitoError::QueryFailed(format!("Get operation failed for key {:?}: {}", key_val, e))
            })
    }

    pub async fn get_for_update(&self, key: impl Into<Key>) -> Result<Option<Value>, TitoError> {
        let key_val = key.into();
        self.inner
            .lock()
            .await
            .get_for_update(key_val.clone())
            .await
            .map_err(|e| {
                TitoError::QueryFailed(format!(
                    "Get for update failed for key {:?}: {}",
                    key_val, e
                ))
            })
    }

    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<(), TitoError> {
        let key_val = key.into();
        self.inner
            .lock()
            .await
            .put(key_val.clone(), value)
            .await
            .map_err(|e| {
                TitoError::UpdateFailed(format!(
                    "Put operation failed for key {:?}: {}",
                    key_val, e
                ))
            })
    }

    pub async fn delete(&self, key: impl Into<Key>) -> Result<(), TitoError> {
        let key_val = key.into();
        self.inner
            .lock()
            .await
            .delete(key_val.clone())
            .await
            .map_err(|e| {
                TitoError::DeleteFailed(format!(
                    "Delete operation failed for key {:?}: {}",
                    key_val, e
                ))
            })
    }

    pub async fn batch_get(
        &self,
        keys: impl IntoIterator<Item = impl Into<Key>>,
    ) -> Result<Vec<KvPair>, TitoError> {
        self.inner
            .lock()
            .await
            .batch_get(keys)
            .await
            .map(|iter| iter.collect())
            .map_err(|e| TitoError::QueryFailed(format!("Batch get operation failed: {}", e)))
    }
}

#[derive(Clone)]
pub struct TransactionManager {
    pub db: TitoDatabase,
    pub active_transactions: Arc<Mutex<HashMap<String, TitoTransaction>>>, // HashMap for active transactions
}

impl TransactionManager {
    pub fn new(db: TitoDatabase) -> Self {
        Self {
            db,
            active_transactions: Arc::new(Mutex::new(HashMap::new())), // Initialize empty HashMap
        }
    }

    pub async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(TitoTransaction) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: From<TitoError>,
    {
        let tx = self
            .db
            .begin_pessimistic()
            .await
            .map(TitoTransaction::new)
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

    pub async fn clear_active_transactions(&self) -> Result<(), TitoError> {
        let mut active_transactions = self.active_transactions.lock().await;
        for (_, tx) in active_transactions.drain() {
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
