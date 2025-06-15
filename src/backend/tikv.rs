use crate::error::TitoError;
use crate::types::{
    DBUuid, StorageKvPair, StorageValue, TitoConfigs, TitoEngine, TitoTransaction,
};
use async_trait::async_trait;
use futures::lock::Mutex;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use tikv_client::{Transaction, TransactionClient};

#[derive(Clone)]
pub struct TiKVBackend {
    pub client: Arc<TransactionClient>,
    pub configs: TitoConfigs,
    pub active_transactions: Arc<Mutex<HashMap<String, TiKVTransaction>>>,
}


#[async_trait]
impl TitoEngine for TiKVBackend {
    type Transaction = TiKVTransaction;
    type Error = TitoError;

    async fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let tx = self.client.begin_pessimistic().await.map_err(|e| {
            TitoError::TransactionFailed(format!("Failed to begin transaction: {}", e))
        })?;
        Ok(TiKVTransaction {
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
        drop(active_transactions);

        let result = f(tx.clone()).await;

        let mut active_transactions = self.active_transactions.lock().await;
        let tx = active_transactions.remove(&tx.id).unwrap_or(tx);
        drop(active_transactions);

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
pub struct TiKVTransaction {
    pub id: String,
    pub inner: Arc<tokio::sync::Mutex<Transaction>>,
}

#[async_trait]
impl TitoTransaction for TiKVTransaction {
    type Error = TitoError;

    async fn get<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<StorageValue>, Self::Error> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();

        self.inner
            .lock()
            .await
            .get(tikv_key)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get operation failed: {}", e)))
    }

    async fn get_for_update<K: AsRef<[u8]> + Send>(
        &self,
        key: K,
    ) -> Result<Option<StorageValue>, Self::Error> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();

        self.inner
            .lock()
            .await
            .get_for_update(tikv_key)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get for update failed: {}", e)))
    }

    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), Self::Error> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();
        let value_bytes = value.as_ref().to_vec();

        self.inner
            .lock()
            .await
            .put(tikv_key, value_bytes)
            .await
            .map_err(|e| TitoError::UpdateFailed(format!("Put operation failed: {}", e)))
    }

    async fn delete<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<(), Self::Error> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();

        self.inner
            .lock()
            .await
            .delete(tikv_key)
            .await
            .map_err(|e| TitoError::DeleteFailed(format!("Delete operation failed: {}", e)))
    }

    async fn scan<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<StorageKvPair>, Self::Error> {
        let start = range.start.as_ref().to_vec();
        let end = range.end.as_ref().to_vec();
        let bound_range: tikv_client::BoundRange = (start..end).into();

        let result = self
            .inner
            .lock()
            .await
            .scan(bound_range, limit)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Scan operation failed: {}", e)))?;

        Ok(result.map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn scan_reverse<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<StorageKvPair>, Self::Error> {
        let start = range.start.as_ref().to_vec();
        let end = range.end.as_ref().to_vec();
        let bound_range: tikv_client::BoundRange = (start..end).into();

        let result = self
            .inner
            .lock()
            .await
            .scan_reverse(bound_range, limit)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Reverse scan operation failed: {}", e)))?;

        Ok(result.map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn batch_get<K: AsRef<[u8]> + Send>(
        &self,
        keys: Vec<K>,
    ) -> Result<Vec<StorageKvPair>, Self::Error> {
        let tikv_keys: Vec<tikv_client::Key> =
            keys.iter().map(|k| k.as_ref().to_vec().into()).collect();

        let result = self
            .inner
            .lock()
            .await
            .batch_get(tikv_keys)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Batch get operation failed: {}", e)))?;

        Ok(result.map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn batch_get_for_update<K: AsRef<[u8]> + Send>(
        &self,
        keys: Vec<K>,
    ) -> Result<Vec<StorageKvPair>, Self::Error> {
        let tikv_keys: Vec<tikv_client::Key> =
            keys.iter().map(|k| k.as_ref().to_vec().into()).collect();

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

/// TiKV connection helper
pub struct TiKV;

impl TiKV {
    /// Connect to TiKV with PD endpoints
    pub async fn connect<S: AsRef<str>>(endpoints: Vec<S>) -> Result<TiKVBackend, TitoError> {
        let endpoint_strings: Vec<String> = endpoints.iter().map(|s| s.as_ref().to_string()).collect();
        let client = TransactionClient::new(endpoint_strings).await.map_err(|e| {
            TitoError::ConnectionFailed(format!("Failed to connect to TiKV: {}", e))
        })?;
        
        Ok(TiKVBackend {
            client: Arc::new(client),
            configs: TitoConfigs {
                is_read_only: Arc::new(AtomicBool::new(false)),
            },
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}
