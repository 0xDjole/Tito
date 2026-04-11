use crate::error::TitoError;
use crate::types::{DBUuid, TitoEngine, TitoKvPair, TitoTransaction, TitoValue};
use async_trait::async_trait;
use futures::lock::Mutex;
use rand::Rng;
use std::collections::HashMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tikv_client::{ColumnFamily, RawClient, Transaction, TransactionClient};
use tokio::time::{sleep, Duration};

fn classify_tikv_error(e: tikv_client::Error, context: &str, retryable_flag: &AtomicBool) -> TitoError {
    let msg = format!("{}: {}", context, e);
    let is_retryable = match &e {
        tikv_client::Error::RegionError(_)
        | tikv_client::Error::RegionForKeyNotFound { .. }
        | tikv_client::Error::RegionForRangeNotFound { .. }
        | tikv_client::Error::RegionNotFoundInResponse { .. }
        | tikv_client::Error::LeaderNotFound { .. }
        | tikv_client::Error::ResolveLockError(_)
        | tikv_client::Error::NoCurrentRegions
        | tikv_client::Error::EntryNotFoundInRegionCache
        | tikv_client::Error::UndeterminedError(_)
        | tikv_client::Error::PessimisticLockError { .. } => true,
        tikv_client::Error::KeyError(ke) => {
            ke.locked.is_some() || ke.conflict.is_some() || !ke.retryable.is_empty()
        }
        tikv_client::Error::MultipleKeyErrors(errs) | tikv_client::Error::ExtractedErrors(errs) => {
            errs.iter().any(|inner| {
                matches!(
                    inner,
                    tikv_client::Error::RegionError(_)
                    | tikv_client::Error::ResolveLockError(_)
                    | tikv_client::Error::LeaderNotFound { .. }
                    | tikv_client::Error::RegionForKeyNotFound { .. }
                    | tikv_client::Error::KeyError(_)
                )
            })
        }
        _ => false,
    };
    if is_retryable {
        retryable_flag.store(true, Ordering::Relaxed);
        TitoError::Retryable(msg)
    } else {
        TitoError::TransactionFailed(msg)
    }
}

#[derive(Clone)]
pub struct TiKVBackend {
    pub client: Arc<TransactionClient>,
    pub raw_client: Arc<RawClient>,
    pub active_transactions: Arc<Mutex<HashMap<String, TiKVTransaction>>>,
}

#[async_trait]
impl TitoEngine for TiKVBackend {
    type Transaction = TiKVTransaction;

    async fn begin_transaction(&self) -> Result<Self::Transaction, TitoError> {
        let no_flag = AtomicBool::new(false);
        let tx = self.client.begin_optimistic().await.map_err(|e| {
            classify_tikv_error(e, "Failed to begin transaction", &no_flag)
        })?;
        Ok(TiKVTransaction {
            id: DBUuid::new_v4().to_string(),
            inner: Arc::new(tokio::sync::Mutex::new(tx)),
            had_retryable_error: Arc::new(AtomicBool::new(false)),
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
                            let mut active_transactions = self.active_transactions.lock().await;
                            active_transactions.remove(&tx_id);
                            return Ok(value);
                        }
                        Err(e) => {
                            {
                                let mut active_transactions = self.active_transactions.lock().await;
                                active_transactions.remove(&tx_id);
                            }

                            if e.is_retryable() && retries < MAX_RETRIES {
                                retries += 1;
                                let jitter = rand::thread_rng().gen_range(0..base_delay_ms / 2);
                                let delay = base_delay_ms + jitter;
                                sleep(Duration::from_millis(delay)).await;
                                base_delay_ms = (base_delay_ms * 2).min(2000);
                                continue;
                            }
                            return Err(E::from(e));
                        }
                    }
                }
                Err(e) => {
                    let is_retryable = tx.had_retryable_error.load(Ordering::Relaxed);
                    let _ = tx.rollback().await;
                    {
                        let mut active_transactions = self.active_transactions.lock().await;
                        active_transactions.remove(&tx_id);
                    }

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

        for (_tx_id, tx) in transactions {
            let _ = tx.inner.lock().await.rollback().await;
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
    pub had_retryable_error: Arc<AtomicBool>,
}

#[async_trait]
impl TitoTransaction for TiKVTransaction {
    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<TitoValue>, TitoError> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();
        self.inner.lock().await.get(tikv_key).await
            .map_err(|e| classify_tikv_error(e, "Get operation failed", &self.had_retryable_error))
    }

    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), TitoError> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();
        let value_bytes = value.as_ref().to_vec();
        self.inner.lock().await.put(tikv_key, value_bytes).await
            .map_err(|e| classify_tikv_error(e, "Put operation failed", &self.had_retryable_error))
    }

    async fn delete<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<(), TitoError> {
        let tikv_key: tikv_client::Key = key.as_ref().to_vec().into();
        self.inner.lock().await.delete(tikv_key).await
            .map_err(|e| classify_tikv_error(e, "Delete operation failed", &self.had_retryable_error))
    }

    async fn scan<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let start = range.start.as_ref().to_vec();
        let end = range.end.as_ref().to_vec();
        let bound_range: tikv_client::BoundRange = (start..end).into();
        let result = self.inner.lock().await.scan(bound_range, limit).await
            .map_err(|e| classify_tikv_error(e, "Scan operation failed", &self.had_retryable_error))?;
        Ok(result.map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn scan_reverse<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let start = range.start.as_ref().to_vec();
        let end = range.end.as_ref().to_vec();
        let bound_range: tikv_client::BoundRange = (start..end).into();
        let result = self.inner.lock().await.scan_reverse(bound_range, limit).await
            .map_err(|e| classify_tikv_error(e, "Reverse scan operation failed", &self.had_retryable_error))?;
        Ok(result.map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn batch_get<K: AsRef<[u8]> + Send>(
        &self,
        keys: Vec<K>,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let tikv_keys: Vec<tikv_client::Key> =
            keys.iter().map(|k| k.as_ref().to_vec().into()).collect();
        let result = self.inner.lock().await.batch_get(tikv_keys).await
            .map_err(|e| classify_tikv_error(e, "Batch get operation failed", &self.had_retryable_error))?;
        Ok(result.map(|kv| (kv.0.into(), kv.1)).collect())
    }

    async fn commit(self) -> Result<(), TitoError> {
        self.inner.lock().await.commit().await
            .map(|_| ())
            .map_err(|e| classify_tikv_error(e, "Transaction commit failed", &self.had_retryable_error))
    }

    async fn rollback(self) -> Result<(), TitoError> {
        self.inner.lock().await.rollback().await
            .map_err(|e| classify_tikv_error(e, "Transaction rollback failed", &self.had_retryable_error))
    }
}

pub struct TiKV;

impl TiKV {
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
