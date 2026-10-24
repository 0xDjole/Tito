use crate::backend::tikv::MAX_TRANSACTION_RETRIES;
use crate::types::{validate_delete_range, TitoEngine, TitoKvPair, TitoTransaction, TitoValue};
use crate::TitoError;
use async_trait::async_trait;
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};
use std::future::Future;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

type MemoryWrites = BTreeMap<Vec<u8>, Option<Vec<u8>>>;

#[derive(Default)]
struct MemoryStore {
    values: BTreeMap<Vec<u8>, Vec<u8>>,
    commits: u64,
    committed_at: HashMap<Vec<u8>, u64>,
}

impl MemoryStore {
    fn conflict(&self, writes: &MemoryWrites, snapshot: u64) -> Option<String> {
        writes
            .keys()
            .find(|key| {
                self.committed_at
                    .get(*key)
                    .is_some_and(|commit| *commit > snapshot)
            })
            .map(|key| String::from_utf8_lossy(key).into_owned())
    }

    fn apply(&mut self, writes: MemoryWrites) {
        if writes.is_empty() {
            return;
        }
        self.commits += 1;
        for (key, value) in writes {
            self.committed_at.insert(key.clone(), self.commits);
            match value {
                Some(value) => {
                    self.values.insert(key, value);
                }
                None => {
                    self.values.remove(&key);
                }
            }
        }
    }
}

#[derive(Clone, Default)]
pub(crate) struct MemoryEngine {
    store: Arc<Mutex<MemoryStore>>,
    next_get_error: Arc<Mutex<Option<String>>>,
    next_commit_unknown_after_apply: Arc<Mutex<Option<bool>>>,
    next_transaction_version: Arc<AtomicU64>,
    pending_queue_scans: Arc<AtomicU64>,
    recorded_reads: Arc<Mutex<Option<Vec<Vec<u8>>>>>,
    recorded_writes: Arc<Mutex<Option<Vec<RecordedWrite>>>>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum RecordedWrite {
    Put(String),
    Delete(String),
}

#[derive(Clone)]
pub(crate) struct MemoryTransaction {
    store: Arc<Mutex<MemoryStore>>,
    local: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    writes: Arc<Mutex<MemoryWrites>>,
    snapshot: u64,
    next_get_error: Arc<Mutex<Option<String>>>,
    next_commit_unknown_after_apply: Arc<Mutex<Option<bool>>>,
    start_version: u64,
    shared_fence_pick: u64,
    pending_queue_scans: Arc<AtomicU64>,
    recorded_reads: Arc<Mutex<Option<Vec<Vec<u8>>>>>,
    recorded_writes: Arc<Mutex<Option<Vec<RecordedWrite>>>>,
}

impl MemoryEngine {
    pub(crate) async fn start_recording_reads(&self) {
        *self.recorded_reads.lock().await = Some(Vec::new());
    }

    pub(crate) async fn take_recorded_reads(&self) -> Vec<Vec<u8>> {
        self.recorded_reads.lock().await.take().unwrap_or_default()
    }

    pub(crate) async fn start_recording_writes(&self) {
        *self.recorded_writes.lock().await = Some(Vec::new());
    }

    pub(crate) async fn take_recorded_writes(&self) -> Vec<RecordedWrite> {
        self.recorded_writes.lock().await.take().unwrap_or_default()
    }

    pub(crate) async fn put_raw_bytes(&self, key: Vec<u8>, value: Vec<u8>) {
        self.store.lock().await.values.insert(key, value);
    }

    pub(crate) async fn put_raw(&self, key: &str, value: Vec<u8>) {
        self.put_raw_bytes(key.as_bytes().to_vec(), value).await;
    }

    pub(crate) async fn put_json(&self, key: &str, value: &Value) {
        self.put_raw(key, serde_json::to_vec(value).unwrap()).await;
    }

    pub(crate) async fn fail_next_get(&self, message: &str) {
        *self.next_get_error.lock().await = Some(message.to_string());
    }

    pub(crate) async fn make_next_commit_outcome_unknown(&self, after_apply: bool) {
        *self.next_commit_unknown_after_apply.lock().await = Some(after_apply);
    }

    pub(crate) fn pending_queue_scan_count(&self) -> u64 {
        self.pending_queue_scans.load(Ordering::SeqCst)
    }

    pub(crate) async fn raw_bytes(&self, key: &str) -> Option<Vec<u8>> {
        self.store.lock().await.values.get(key.as_bytes()).cloned()
    }

    pub(crate) async fn raw_json(&self, key: &str) -> Option<Value> {
        self.raw_bytes(key)
            .await
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
    }

    pub(crate) async fn contains_key(&self, key: &str) -> bool {
        self.store.lock().await.values.contains_key(key.as_bytes())
    }

    pub(crate) async fn keys_with_prefix(&self, prefix: &str) -> Vec<String> {
        self.store
            .lock()
            .await
            .values
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
        let (snapshot, local) = {
            let store = self.store.lock().await;
            (store.commits, store.values.clone())
        };
        let start_version = self
            .next_transaction_version
            .fetch_add(1, Ordering::SeqCst)
            .saturating_add(1);
        Ok(MemoryTransaction {
            store: self.store.clone(),
            local: Arc::new(Mutex::new(local)),
            writes: Arc::new(Mutex::new(MemoryWrites::new())),
            snapshot,
            next_get_error: self.next_get_error.clone(),
            next_commit_unknown_after_apply: self.next_commit_unknown_after_apply.clone(),
            start_version,
            shared_fence_pick: rand::random(),
            pending_queue_scans: self.pending_queue_scans.clone(),
            recorded_reads: self.recorded_reads.clone(),
            recorded_writes: self.recorded_writes.clone(),
        })
    }

    async fn transaction<F, Fut, T, E>(&self, f: F) -> Result<T, E>
    where
        F: FnOnce(Self::Transaction) -> Fut + Clone + Send,
        Fut: Future<Output = Result<T, E>> + Send,
        T: Send,
        E: From<TitoError> + Send + std::fmt::Debug,
    {
        let mut retries = 0;
        loop {
            let tx = self.begin_transaction().await.map_err(E::from)?;
            match f.clone()(tx.clone()).await {
                Ok(value) => match tx.commit().await {
                    Ok(()) => return Ok(value),
                    Err(error) if error.is_retryable() && retries < MAX_TRANSACTION_RETRIES => {
                        retries += 1;
                    }
                    Err(error) => return Err(E::from(error)),
                },
                Err(error) => {
                    let _ = tx.rollback().await;
                    return Err(error);
                }
            }
        }
    }

    async fn clear_active_transactions(&self) -> Result<(), TitoError> {
        Ok(())
    }

    async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), TitoError> {
        validate_delete_range(start, end)?;
        let mut store = self.store.lock().await;
        let keys: Vec<Vec<u8>> = store
            .values
            .range(start.to_vec()..end.to_vec())
            .map(|(key, _)| key.clone())
            .collect();
        for key in keys {
            store.values.remove(&key);
        }
        Ok(())
    }
}

#[async_trait]
impl TitoTransaction for MemoryTransaction {
    fn start_version(&self) -> u64 {
        self.start_version
    }

    fn shared_fence_pick(&self) -> u64 {
        self.shared_fence_pick
    }

    async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<TitoValue>, TitoError> {
        if let Some(reads) = self.recorded_reads.lock().await.as_mut() {
            reads.push(key.as_ref().to_vec());
        }
        if let Some(message) = self.next_get_error.lock().await.take() {
            return Err(TitoError::QueryFailed(message));
        }
        Ok(self.local.lock().await.get(key.as_ref()).cloned())
    }

    async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
        &self,
        key: K,
        value: V,
    ) -> Result<(), TitoError> {
        if let Some(writes) = self.recorded_writes.lock().await.as_mut() {
            writes.push(RecordedWrite::Put(
                String::from_utf8_lossy(key.as_ref()).into_owned(),
            ));
        }
        self.local
            .lock()
            .await
            .insert(key.as_ref().to_vec(), value.as_ref().to_vec());
        self.writes
            .lock()
            .await
            .insert(key.as_ref().to_vec(), Some(value.as_ref().to_vec()));
        Ok(())
    }

    async fn delete<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<(), TitoError> {
        if let Some(writes) = self.recorded_writes.lock().await.as_mut() {
            writes.push(RecordedWrite::Delete(
                String::from_utf8_lossy(key.as_ref()).into_owned(),
            ));
        }
        self.local.lock().await.remove(key.as_ref());
        self.writes.lock().await.insert(key.as_ref().to_vec(), None);
        Ok(())
    }

    async fn scan<K: AsRef<[u8]> + Send>(
        &self,
        range: Range<K>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let start = range.start.as_ref().to_vec();
        let end = range.end.as_ref().to_vec();
        if let Some(reads) = self.recorded_reads.lock().await.as_mut() {
            reads.push([b"scan:", start.as_slice()].concat());
        }
        if start.starts_with(b"queue:pending:") {
            self.pending_queue_scans.fetch_add(1, Ordering::SeqCst);
        }
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
        if let Some(reads) = self.recorded_reads.lock().await.as_mut() {
            reads.push([b"scan-reverse:", start.as_slice()].concat());
        }
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
        if let Some(reads) = self.recorded_reads.lock().await.as_mut() {
            reads.extend(keys.iter().map(|key| key.as_ref().to_vec()));
        }
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
        let writes = std::mem::take(&mut *self.writes.lock().await);
        let mut store = self.store.lock().await;
        if let Some(key) = store.conflict(&writes, self.snapshot) {
            return Err(TitoError::Retryable(format!(
                "Write conflict: key '{key}' was committed by another transaction after this one started"
            )));
        }
        let unknown = self.next_commit_unknown_after_apply.lock().await.take();
        if unknown != Some(false) {
            store.apply(writes);
        }
        match unknown {
            Some(_) => Err(TitoError::CommitOutcomeUnknown(
                "forced indeterminate memory transaction commit".to_string(),
            )),
            None => Ok(()),
        }
    }

    async fn rollback(self) -> Result<(), TitoError> {
        Ok(())
    }
}
