use crate::types::{TitoEngine, TitoKvPair, TitoTransaction, TitoValue};
use crate::TitoError;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::BTreeMap;
use std::future::Future;
use std::ops::Range;
use std::sync::Arc;
use tokio::sync::Mutex;

#[derive(Clone, Default)]
pub(crate) struct MemoryEngine {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

#[derive(Clone)]
pub(crate) struct MemoryTransaction {
    data: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    local: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
}

impl MemoryEngine {
    pub(crate) async fn put_raw(&self, key: &str, value: Vec<u8>) {
        self.data
            .lock()
            .await
            .insert(key.as_bytes().to_vec(), value);
    }

    pub(crate) async fn put_json(&self, key: &str, value: &Value) {
        self.put_raw(key, serde_json::to_vec(value).unwrap()).await;
    }

    pub(crate) async fn raw_bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.data.lock().await.get(key.as_bytes()).cloned()
    }

    pub(crate) async fn raw_json(&self, key: &str) -> Option<Value> {
        self.raw_bytes(key)
            .await
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    }

    pub(crate) async fn contains_key(&self, key: &str) -> bool {
        self.data.lock().await.contains_key(key.as_bytes())
    }

    pub(crate) async fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.data
            .lock()
            .await
            .keys()
            .filter_map(|key| String::from_utf8(key.clone()).ok())
            .filter(|key| key.starts_with(prefix))
            .collect()
    }
}

#[async_trait]
impl TitoEngine for MemoryEngine {
    type Transaction = MemoryTransaction;

    async fn begin_transaction(&self) -> Result<Self::Transaction, TitoError> {
        let snapshot = self.data.lock().await.clone();
        Ok(MemoryTransaction {
            data: self.data.clone(),
            local: Arc::new(Mutex::new(snapshot)),
        })
    }

    async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(Self::Transaction) -> Fut + Clone + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: From<TitoError> + Send + std::fmt::Debug,
    {
        let tx = self.begin_transaction().await.map_err(E::from)?;
        match f(tx.clone()).await {
            Ok(value) => {
                tx.commit().await.map_err(E::from)?;
                Ok(value)
            }
            Err(error) => {
                let _ = tx.rollback().await;
                Err(error)
            }
        }
    }

    async fn clear_active_transactions(&self) -> Result<(), TitoError> {
        Ok(())
    }

    async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), TitoError> {
        let mut data = self.data.lock().await;
        let keys: Vec<Vec<u8>> = data
            .range(start.to_vec()..end.to_vec())
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            data.remove(&key);
        }
        Ok(())
    }
}

#[async_trait]
impl TitoTransaction for MemoryTransaction {
    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<TitoValue>, TitoError> {
        Ok(self.local.lock().await.get(key.as_ref()).cloned())
    }

    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), TitoError> {
        self.local
            .lock()
            .await
            .insert(key.as_ref().to_vec(), value.as_ref().to_vec());
        Ok(())
    }

    async fn delete<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<(), TitoError> {
        self.local.lock().await.remove(key.as_ref());
        Ok(())
    }

    async fn scan<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let start = range.start.as_ref().to_vec();
        let end = range.end.as_ref().to_vec();
        Ok(self
            .local
            .lock()
            .await
            .range(start..end)
            .take(limit as usize)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    async fn scan_reverse<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let start = range.start.as_ref().to_vec();
        let end = range.end.as_ref().to_vec();
        Ok(self
            .local
            .lock()
            .await
            .range(start..end)
            .rev()
            .take(limit as usize)
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect())
    }

    async fn batch_get<K: AsRef<[u8]> + Send>(
        &self,
        keys: Vec<K>,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let data = self.local.lock().await;
        Ok(keys
            .into_iter()
            .filter_map(|key| {
                let bytes = key.as_ref().to_vec();
                data.get(&bytes).map(|value| (bytes, value.clone()))
            })
            .collect())
    }

    async fn commit(self) -> Result<(), TitoError> {
        *self.data.lock().await = self.local.lock().await.clone();
        Ok(())
    }

    async fn rollback(self) -> Result<(), TitoError> {
        Ok(())
    }
}
