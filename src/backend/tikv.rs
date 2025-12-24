use crate::error::TitoError;
use crate::types::{DBUuid, TitoEngine, TitoKvPair, TitoTransaction, TitoValue};
use async_trait::async_trait;
use futures::lock::Mutex;
use rand::Rng;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use tikv_client::{ColumnFamily, RawClient, Transaction, TransactionClient};
use tokio::time::{sleep, Duration};

#[derive(Clone)]
pub struct TiKVBackend {
    pub client: Arc<TransactionClient>,
    pub raw_client: Arc<RawClient>,
    pub active_transactions: Arc<Mutex<HashMap<String, TiKVTransaction>>>,
}

#[async_trait]
impl TitoEngine for TiKVBackend {
    type Transaction = TiKVTransaction;
    type Error = TitoError;

    async fn begin_transaction(&self) -> Result<Self::Transaction, Self::Error> {
        let tx = self.client.begin_optimistic().await.map_err(|e| {
            TitoError::TransactionFailed(format!("Failed to begin transaction: {}", e))
        })?;
        Ok(TiKVTransaction {
            id: DBUuid::new_v4().to_string(),
            inner: Arc::new(tokio::sync::Mutex::new(tx)),
        })
    }

    async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(Self::Transaction) -> Fut + Clone + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: From<TitoError> + Send + std::fmt::Debug,
    {
        const MAX_RETRIES: u32 = 10;
        let mut retries = 0;
        let mut base_delay_ms = 50u64;

        loop {
            let f_clone = f.clone();
            let tx = self
                .begin_transaction()
                .await
                .map_err(|e| {
                    TitoError::TransactionFailed(format!("Failed to begin transaction: {}", e))
                })
                .map_err(E::from)?;

            let tx_id = tx.id.clone();

            {
                let mut active_transactions = self.active_transactions.lock().await;
                active_transactions.insert(tx_id.clone(), tx.clone());
            }

            let result = f_clone(tx.clone()).await;

            match result {
                Ok(value) => {
                    match tx.commit().await {
                        Ok(_) => {
                            // Remove from tracking AFTER successful commit
                            let mut active_transactions = self.active_transactions.lock().await;
                            active_transactions.remove(&tx_id);
                            return Ok(value);
                        }
                        Err(e) => {
                            // Remove from tracking on commit failure before retry
                            {
                                let mut active_transactions = self.active_transactions.lock().await;
                                active_transactions.remove(&tx_id);
                            }

                            if Self::is_retryable_error(&e) && retries < MAX_RETRIES {
                                retries += 1;
                                // Exponential backoff with jitter to prevent thundering herd
                                let jitter = rand::thread_rng().gen_range(0..base_delay_ms / 2);
                                let delay = base_delay_ms + jitter;
                                sleep(Duration::from_millis(delay)).await;
                                base_delay_ms = (base_delay_ms * 2).min(2000); // Cap at 2 seconds
                                continue;
                            }
                            return Err(E::from(e));
                        }
                    }
                }
                Err(e) => {
                    let _ = tx.rollback().await;
                    // Remove from tracking AFTER rollback
                    {
                        let mut active_transactions = self.active_transactions.lock().await;
                        active_transactions.remove(&tx_id);
                    }

                    // Check if this is a retryable error (e.g., lock conflicts during reads)
                    let err_str = format!("{:?}", e);
                    let err_lower = err_str.to_lowercase();
                    let is_retryable = err_lower.contains("lock")
                        || err_lower.contains("conflict")
                        || err_lower.contains("retryable")
                        || err_lower.contains("region")
                        || err_lower.contains("stale");

                    if is_retryable && retries < MAX_RETRIES {
                        retries += 1;
                        let jitter = rand::thread_rng().gen_range(0..base_delay_ms / 2);
                        let delay = base_delay_ms + jitter;
                        sleep(Duration::from_millis(delay)).await;
                        base_delay_ms = (base_delay_ms * 2).min(2000);
                        continue;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn clear_active_transactions(&self) -> Result<(), TitoError> {
        let mut active_transactions = self.active_transactions.lock().await;
        let transactions: Vec<_> = active_transactions.drain().collect();
        drop(active_transactions);

        // Wait for and rollback each transaction - we CANNOT skip them
        // because skipping means they get dropped without commit/rollback
        for (tx_id, tx) in transactions {
            eprintln!(
                "Waiting for transaction {} to become available for cleanup...",
                tx_id
            );

            // Use .lock().await to WAIT for the transaction to become available
            // This blocks until the transaction is no longer being used
            match tx.inner.lock().await.rollback().await {
                Ok(_) => {
                    eprintln!(
                        "Successfully rolled back transaction {} during cleanup",
                        tx_id
                    );
                }
                Err(e) => {
                    eprintln!("Warning: Failed to rollback transaction {}: {:?}", tx_id, e);
                }
            }
        }
        Ok(())
    }

    async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), TitoError> {
        let range: tikv_client::BoundRange = (start.to_vec()..end.to_vec()).into();

        self.raw_client
            .with_cf(ColumnFamily::Default)
            .delete_range(range.clone())
            .await
            .map_err(|e| TitoError::DeleteFailed(format!("Delete range (Default CF) failed: {}", e)))?;

        self.raw_client
            .with_cf(ColumnFamily::Write)
            .delete_range(range.clone())
            .await
            .map_err(|e| TitoError::DeleteFailed(format!("Delete range (Write CF) failed: {}", e)))?;

        self.raw_client
            .with_cf(ColumnFamily::Lock)
            .delete_range(range)
            .await
            .map_err(|e| TitoError::DeleteFailed(format!("Delete range (Lock CF) failed: {}", e)))?;

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

    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<TitoValue>, Self::Error> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();

        self.inner
            .lock()
            .await
            .get(tikv_key)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get operation failed: {}", e)))
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
    ) -> Result<Vec<TitoKvPair>, Self::Error> {
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
    ) -> Result<Vec<TitoKvPair>, Self::Error> {
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
    ) -> Result<Vec<TitoKvPair>, Self::Error> {
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

    async fn commit(self) -> Result<(), Self::Error> {
        self.inner
            .lock()
            .await
            .commit()
            .await
            .map(|_| ())
            .map_err(|e| TitoError::TransactionFailed(format!("Transaction commit failed: {}", e)))
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        self.inner.lock().await.rollback().await.map_err(|e| {
            TitoError::TransactionFailed(format!("Transaction rollback failed: {}", e))
        })
    }
}

impl TiKVBackend {
    fn is_retryable_error(err: &TitoError) -> bool {
        let err_str = format!("{:?}", err);
        let err_lower = err_str.to_lowercase();
        err_lower.contains("writeconflict")
            || err_lower.contains("write conflict")
            || err_lower.contains("conflict")
            || err_lower.contains("txnlocknotfound")
            || err_lower.contains("keyislocked")
            || err_lower.contains("retryable")
            || err_lower.contains("region")
            || err_lower.contains("not leader")
            || err_lower.contains("stale")
    }
}

/// TiKV connection helper
pub struct TiKV;

impl TiKV {
    /// Connect to TiKV with PD endpoints
    pub async fn connect<S: AsRef<str>>(endpoints: Vec<S>) -> Result<TiKVBackend, TitoError> {
        let endpoint_strings: Vec<String> =
            endpoints.iter().map(|s| s.as_ref().to_string()).collect();
        let client = TransactionClient::new(endpoint_strings.clone())
            .await
            .map_err(|e| {
                TitoError::ConnectionFailed(format!("Failed to connect to TiKV: {}", e))
            })?;

        let raw_client = RawClient::new(endpoint_strings).await.map_err(|e| {
            TitoError::ConnectionFailed(format!("Failed to connect RawClient to TiKV: {}", e))
        })?;

        Ok(TiKVBackend {
            client: Arc::new(client),
            raw_client: Arc::new(raw_client),
            active_transactions: Arc::new(Mutex::new(HashMap::new())),
        })
    }
}
