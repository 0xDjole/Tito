use std::{collections::HashMap, future::Future, sync::Arc};

use tikv_client::{BoundRange, Key, KvPair, Timestamp, Value};

use crate::types::{DBUuid, TiKvTransaction, TitoDatabase, TitoError};
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
            .map_err(|e| TitoError::Failed)
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
            .map_err(|e| TitoError::Failed)
    }

    pub async fn commit(&self) -> Result<Option<Timestamp>, TitoError> {
        self.inner
            .lock()
            .await
            .commit()
            .await
            .map_err(|e| TitoError::TransactionFailed(e.to_string()))
    }

    pub async fn rollback(&self) -> Result<(), TitoError> {
        self.inner
            .lock()
            .await
            .rollback()
            .await
            .map_err(|e| TitoError::TransactionFailed(e.to_string()))
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
            .map_err(|e| TitoError::TransactionFailed(e.to_string()))
    }

    pub async fn get(&self, key: impl Into<Key>) -> Result<Option<Value>, TitoError> {
        self.inner
            .lock()
            .await
            .get(key)
            .await
            .map_err(|e| TitoError::Failed)
    }

    pub async fn get_for_update(&self, key: impl Into<Key>) -> Result<Option<Value>, TitoError> {
        self.inner
            .lock()
            .await
            .get_for_update(key)
            .await
            .map_err(|e| TitoError::Failed)
    }

    pub async fn put(&self, key: impl Into<Key>, value: impl Into<Value>) -> Result<(), TitoError> {
        self.inner
            .lock()
            .await
            .put(key, value)
            .await
            .map_err(|e| TitoError::FailedUpdate(e.to_string()))
    }

    pub async fn delete(&self, key: impl Into<Key>) -> Result<(), TitoError> {
        self.inner
            .lock()
            .await
            .delete(key)
            .await
            .map_err(|e| TitoError::FailedDelete(e.to_string()))
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
            .map_err(|e| TitoError::Failed)
    }
}

#[derive(Clone)]
pub struct TransactionManager {
    pub db: Arc<TitoDatabase>,
    pub active_transactions: Arc<Mutex<HashMap<String, TitoTransaction>>>, // HashMap for active transactions
}

impl TransactionManager {
    pub fn new(db: TitoDatabase) -> Self {
        Self {
            db: Arc::new(db),
            active_transactions: Arc::new(Mutex::new(HashMap::new())), // Initialize empty HashMap
        }
    }

    pub async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(TitoTransaction) -> Fut,
        Fut: Future<Output = Result<T, E>>,
        E: From<TitoError>,
    {
        // Start a new transaction
        let tx = self
            .db
            .begin_pessimistic()
            .await
            .map(TitoTransaction::new)
            .map_err(|e| TitoError::TransactionFailed(e.to_string()))
            .map_err(E::from)?;

        // Add to active transactions
        let mut active_transactions = self.active_transactions.lock().await;
        active_transactions.insert(tx.id.clone(), tx.clone());
        drop(active_transactions); // Release the lock early

        // Execute the closure
        let result = f(tx.clone()).await;

        // Determine whether to commit or rollback
        match &result {
            Ok(_) => {
                tx.commit().await;
            }
            Err(_) => {
                tx.rollback().await;
            }
        };

        let mut active_transactions = self.active_transactions.lock().await;
        active_transactions.remove(&tx.id);

        result
    }

    pub async fn clear_active_transactions(&self) -> Result<(), TitoError> {
        let mut active_transactions = self.active_transactions.lock().await;
        for (_, tx) in active_transactions.drain() {
            println!("CLEAR");
            tx.rollback().await?;
        }
        Ok(())
    }
}
