mod types;
mod worker;

pub use types::{QueueConfig, QueueEvent, QueueEventState};
pub use worker::{run_worker, WorkerConfig};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::types::{TitoEngine, TitoTransaction, PARTITION_DIGITS};
use crate::TitoError;

const DEFAULT_LOCK_SECS: i64 = 300;

#[derive(Clone)]
pub struct Queue<E: TitoEngine> {
    pub engine: E,
    pub config: QueueConfig,
}

impl<E: TitoEngine> Queue<E> {
    pub fn new(engine: E, config: QueueConfig) -> Self {
        Self { engine, config }
    }

    fn partition_for_key(&self, key: &str) -> u32 {
        let partition_count = self.config.partition_count.max(1);
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() % partition_count as u64) as u32
    }

    fn canonical_key(event_id: &str) -> String {
        format!("queue:event:{}", event_id)
    }

    fn pending_key(partition: u32, timestamp: i64, event_id: &str) -> String {
        format!(
            "queue:pending:{:0pwidth$}:{}:{}",
            partition,
            timestamp,
            event_id,
            pwidth = PARTITION_DIGITS,
        )
    }

    fn processing_key(partition: u32, locked_until: i64, event_id: &str) -> String {
        format!(
            "queue:processing:{:0pwidth$}:{}:{}",
            partition,
            locked_until,
            event_id,
            pwidth = PARTITION_DIGITS,
        )
    }

    fn completed_key(processed_at: i64, event_id: &str) -> String {
        format!("queue:completed:{:020}:{}", processed_at, event_id)
    }

    fn dlq_key(partition: u32, failed_at: i64, event_id: &str) -> String {
        format!(
            "queue:dlq:{:0pwidth$}:{}:{}",
            partition,
            failed_at,
            event_id,
            pwidth = PARTITION_DIGITS,
        )
    }

    fn state_index_prefix(state: QueueEventState) -> &'static str {
        match state {
            QueueEventState::Pending => "queue:pending:",
            QueueEventState::Processing => "queue:processing:",
            QueueEventState::Completed => "queue:completed:",
            QueueEventState::DeadLetter => "queue:dlq:",
        }
    }

    fn state_value(state: QueueEventState) -> &'static str {
        match state {
            QueueEventState::Pending => "pending",
            QueueEventState::Processing => "processing",
            QueueEventState::Completed => "completed",
            QueueEventState::DeadLetter => "dead_letter",
        }
    }

    pub async fn publish_in_tx<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        mut event: QueueEvent<T>,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        let now = Utc::now().timestamp();
        let partition = self.partition_for_key(&event.key);
        let canonical_key = Self::canonical_key(&event.id);
        let pending_key = Self::pending_key(partition, event.timestamp, &event.id);

        event.state = QueueEventState::Pending;
        if event.created_at == 0 {
            event.created_at = now;
        }
        event.processed_at = None;
        event.locked_until = None;
        event.locked_by = None;

        let bytes = serde_json::to_vec(&event)
            .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

        tx.put(canonical_key.as_bytes(), bytes)
            .await
            .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

        tx.put(pending_key.as_bytes(), canonical_key.as_bytes())
            .await
            .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

        Ok(())
    }

    pub async fn publish<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        event: QueueEvent<T>,
    ) -> Result<(), TitoError> {
        self.engine
            .transaction(|tx| {
                let event = event.clone();
                async move { self.publish_in_tx(event, &tx).await }
            })
            .await
    }

    pub async fn pull<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        partition: u32,
        limit: u32,
    ) -> Result<Vec<(String, QueueEvent<T>)>, TitoError> {
        self.engine
            .transaction(|tx| async move {
                let now = Utc::now().timestamp();
                self.requeue_expired_processing::<T>(partition, now, limit.max(50), &tx)
                    .await?;

                let start = format!(
                    "queue:pending:{:0pwidth$}:{}",
                    partition,
                    0,
                    pwidth = PARTITION_DIGITS,
                );
                let end = format!(
                    "queue:pending:{:0pwidth$}:{}",
                    partition,
                    now + 1,
                    pwidth = PARTITION_DIGITS,
                );

                let events = tx
                    .scan(start.as_bytes()..end.as_bytes(), limit)
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

                let mut jobs: Vec<(String, QueueEvent<T>)> = Vec::new();
                for (pending_key, canonical_key_bytes) in events.into_iter() {
                    let canonical_key = String::from_utf8(canonical_key_bytes).map_err(|_| {
                        TitoError::DeserializationFailed("Invalid queue index".to_string())
                    })?;
                    let Some(value) = tx
                        .get(canonical_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Get queue event: {}", e)))?
                    else {
                        tx.delete(pending_key.as_slice()).await.map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete orphan queue index: {}", e))
                        })?;
                        continue;
                    };

                    let mut event: QueueEvent<T> = serde_json::from_slice(&value)
                        .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;

                    if event.state != QueueEventState::Pending || event.timestamp > now {
                        continue;
                    }

                    tx.delete(pending_key.as_slice()).await.map_err(|e| {
                        TitoError::DeleteFailed(format!("Delete pending queue index: {}", e))
                    })?;

                    let locked_until = now + DEFAULT_LOCK_SECS;
                    event.state = QueueEventState::Processing;
                    event.locked_until = Some(locked_until);
                    event.locked_by = Some(format!("partition:{}", partition));
                    event.processed_at = None;

                    let bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
                    tx.put(canonical_key.as_bytes(), bytes).await.map_err(|e| {
                        TitoError::UpdateFailed(format!("Claim queue event: {}", e))
                    })?;

                    let processing_key = Self::processing_key(partition, locked_until, &event.id);
                    tx.put(processing_key.as_bytes(), canonical_key.as_bytes())
                        .await
                        .map_err(|e| {
                            TitoError::UpdateFailed(format!("Create processing index: {}", e))
                        })?;

                    jobs.push((canonical_key, event));
                }

                Ok::<_, TitoError>(jobs)
            })
            .await
    }

    async fn requeue_expired_processing<
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    >(
        &self,
        partition: u32,
        now: i64,
        limit: u32,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        let start = format!(
            "queue:processing:{:0pwidth$}:{}",
            partition,
            0,
            pwidth = PARTITION_DIGITS,
        );
        let end = format!(
            "queue:processing:{:0pwidth$}:{}",
            partition,
            now + 1,
            pwidth = PARTITION_DIGITS,
        );

        let expired = tx
            .scan(start.as_bytes()..end.as_bytes(), limit)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Scan processing queue: {}", e)))?;

        for (processing_key, canonical_key_bytes) in expired {
            let canonical_key = String::from_utf8(canonical_key_bytes)
                .map_err(|_| TitoError::DeserializationFailed("Invalid queue index".to_string()))?;
            let Some(value) = tx
                .get(canonical_key.as_bytes())
                .await
                .map_err(|e| TitoError::QueryFailed(format!("Get queue event: {}", e)))?
            else {
                tx.delete(processing_key.as_slice()).await.map_err(|e| {
                    TitoError::DeleteFailed(format!("Delete orphan processing index: {}", e))
                })?;
                continue;
            };

            let mut event: QueueEvent<T> = serde_json::from_slice(&value)
                .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;

            if event.state != QueueEventState::Processing
                || event
                    .locked_until
                    .map_or(false, |locked_until| locked_until > now)
            {
                continue;
            }

            tx.delete(processing_key.as_slice()).await.map_err(|e| {
                TitoError::DeleteFailed(format!("Delete expired processing index: {}", e))
            })?;

            event.state = QueueEventState::Pending;
            event.locked_until = None;
            event.locked_by = None;
            event.timestamp = now;

            let bytes = serde_json::to_vec(&event)
                .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
            tx.put(canonical_key.as_bytes(), bytes)
                .await
                .map_err(|e| TitoError::UpdateFailed(format!("Requeue event: {}", e)))?;

            let pending_key = Self::pending_key(partition, event.timestamp, &event.id);
            tx.put(pending_key.as_bytes(), canonical_key.as_bytes())
                .await
                .map_err(|e| TitoError::UpdateFailed(format!("Create pending index: {}", e)))?;
        }

        Ok(())
    }

    pub async fn clear_by_key_in_tx<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        key: &str,
        tx: &E::Transaction,
    ) -> Result<u32, TitoError> {
        let mut deleted = 0u32;

        let events = tx
            .scan(
                b"queue:event:".as_slice()..b"queue:event:\xff".as_slice(),
                1000,
            )
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

        for (storage_key, value) in events {
            if let Ok(event) = serde_json::from_slice::<QueueEvent<T>>(&value) {
                if event.key == key {
                    self.delete_indexes_for_event(&event, tx).await?;
                    tx.delete(storage_key.as_slice())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete event: {}", e)))?;
                    deleted += 1;
                }
            }
        }
        Ok(deleted)
    }

    pub async fn clear_by_key<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        key: &str,
    ) -> Result<u32, TitoError> {
        let key_owned = key.to_string();
        self.engine
            .transaction(|tx| {
                let key_owned = key_owned.clone();
                async move { self.clear_by_key_in_tx::<T>(&key_owned, &tx).await }
            })
            .await
    }

    pub async fn ack(&self, key: &str) -> Result<(), TitoError> {
        self.engine
            .transaction(|tx| {
                let key = key.to_string();
                async move {
                    let Some(bytes) = tx
                        .get(key.as_bytes())
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Get queue event: {}", e)))?
                    else {
                        return Ok::<_, TitoError>(());
                    };

                    let mut value: Value = serde_json::from_slice(&bytes)
                        .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;
                    let event_id = value
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            TitoError::DeserializationFailed("Queue event missing id".to_string())
                        })?
                        .to_string();
                    let event_key = value
                        .get("key")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            TitoError::DeserializationFailed("Queue event missing key".to_string())
                        })?
                        .to_string();
                    let partition = self.partition_for_key(&event_key);
                    if let Some(locked_until) = value.get("lockedUntil").and_then(Value::as_i64) {
                        let processing_key =
                            Self::processing_key(partition, locked_until, &event_id);
                        tx.delete(processing_key.as_bytes()).await.map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete processing index: {}", e))
                        })?;
                    }

                    let processed_at = Utc::now().timestamp();
                    value["state"] = Value::String("completed".to_string());
                    value["processedAt"] = Value::Number(processed_at.into());
                    value["lockedUntil"] = Value::Null;
                    value["lockedBy"] = Value::Null;

                    let updated = serde_json::to_vec(&value)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
                    tx.put(key.as_bytes(), updated).await.map_err(|e| {
                        TitoError::UpdateFailed(format!("Complete queue event: {}", e))
                    })?;

                    let completed_key = Self::completed_key(processed_at, &event_id);
                    tx.put(completed_key.as_bytes(), key.as_bytes())
                        .await
                        .map_err(|e| {
                            TitoError::UpdateFailed(format!("Create completed index: {}", e))
                        })?;

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn reschedule<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        mut event: QueueEvent<T>,
        storage_key: &str,
        new_timestamp: i64,
    ) -> Result<(), TitoError> {
        let canonical_key = storage_key.to_string();

        self.engine
            .transaction(|tx| {
                let canonical_key = canonical_key.clone();

                async move {
                    let partition = self.partition_for_key(&event.key);
                    if let Some(locked_until) = event.locked_until {
                        let processing_key =
                            Self::processing_key(partition, locked_until, &event.id);
                        tx.delete(processing_key.as_bytes()).await.map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete processing index: {}", e))
                        })?;
                    }

                    event.timestamp = new_timestamp;
                    event.state = QueueEventState::Pending;
                    event.processed_at = None;
                    event.locked_until = None;
                    event.locked_by = None;

                    let new_key = format!(
                        "{}",
                        Self::pending_key(partition, event.timestamp, &event.id)
                    );

                    let bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

                    tx.put(canonical_key.as_bytes(), bytes)
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;
                    tx.put(new_key.as_bytes(), canonical_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn clear(&self) -> Result<(), TitoError> {
        Ok(())
    }

    pub async fn move_to_dlq<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        event: QueueEvent<T>,
        storage_key: &str,
    ) -> Result<(), TitoError> {
        let partition = self.partition_for_key(&event.key);
        let failed_at = Utc::now().timestamp();
        let dlq_key = Self::dlq_key(partition, failed_at, &event.id);

        self.engine
            .transaction(|tx| {
                let canonical_key = storage_key.to_string();
                let dlq_key = dlq_key.clone();
                let mut event = event.clone();
                async move {
                    if let Some(locked_until) = event.locked_until {
                        let processing_key =
                            Self::processing_key(partition, locked_until, &event.id);
                        tx.delete(processing_key.as_bytes()).await.map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete processing index: {}", e))
                        })?;
                    }

                    event.state = QueueEventState::DeadLetter;
                    event.processed_at = Some(failed_at);
                    event.locked_until = None;
                    event.locked_by = None;

                    let bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
                    tx.put(canonical_key.as_bytes(), bytes)
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;
                    tx.put(dlq_key.as_bytes(), canonical_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn delete_by_state_before(
        &self,
        state: QueueEventState,
        cutoff: i64,
        limit: u32,
    ) -> Result<usize, TitoError> {
        if !matches!(
            state,
            QueueEventState::Completed | QueueEventState::DeadLetter
        ) {
            return Err(TitoError::InvalidInput(format!(
                "Refusing to delete non-terminal queue state {}",
                Self::state_value(state)
            )));
        }

        self.engine
            .transaction(|tx| async move {
                let prefix = Self::state_index_prefix(state);
                let entries = if state == QueueEventState::Completed {
                    let start = "queue:completed:00000000000000000000".to_string();
                    let end = format!("queue:completed:{:020}:", cutoff.saturating_add(1));
                    tx.scan(start.as_bytes()..end.as_bytes(), limit)
                        .await
                        .map_err(|e| {
                            TitoError::QueryFailed(format!("Scan completed queue: {}", e))
                        })?
                } else {
                    let start = prefix.as_bytes();
                    let mut end = prefix.as_bytes().to_vec();
                    end.push(0xff);
                    tx.scan(start..end.as_slice(), limit).await.map_err(|e| {
                        TitoError::QueryFailed(format!(
                            "Scan {} queue: {}",
                            Self::state_value(state),
                            e
                        ))
                    })?
                };

                let mut deleted = 0usize;
                for (index_key, canonical_key_bytes) in entries {
                    let Some(bytes) =
                        tx.get(canonical_key_bytes.as_slice()).await.map_err(|e| {
                            TitoError::QueryFailed(format!("Get queue event for delete: {}", e))
                        })?
                    else {
                        tx.delete(index_key.as_slice()).await.map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete orphan queue index: {}", e))
                        })?;
                        continue;
                    };

                    let value: Value = serde_json::from_slice(&bytes)
                        .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;
                    let row_state = value.get("state").and_then(Value::as_str);
                    let processed_at = value.get("processedAt").and_then(Value::as_i64);
                    if row_state != Some(Self::state_value(state))
                        || !processed_at.map_or(false, |processed_at| processed_at <= cutoff)
                    {
                        continue;
                    }

                    tx.delete(index_key.as_slice()).await.map_err(|e| {
                        TitoError::DeleteFailed(format!("Delete queue index: {}", e))
                    })?;
                    tx.delete(canonical_key_bytes.as_slice())
                        .await
                        .map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete queue event: {}", e))
                        })?;
                    deleted += 1;
                }

                Ok::<_, TitoError>(deleted)
            })
            .await
    }

    pub async fn find_by_state_after<
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    >(
        &self,
        state: QueueEventState,
        cutoff: i64,
        limit: u32,
    ) -> Result<Vec<(String, QueueEvent<T>)>, TitoError> {
        self.engine
            .transaction(|tx| async move {
                let prefix = Self::state_index_prefix(state);
                let entries = if state == QueueEventState::Completed {
                    let start = format!("queue:completed:{:020}:", cutoff.saturating_add(1));
                    let end = b"queue:completed:\xff".to_vec();
                    tx.scan(start.as_bytes()..end.as_slice(), limit)
                        .await
                        .map_err(|e| {
                            TitoError::QueryFailed(format!("Scan completed queue: {}", e))
                        })?
                } else {
                    let start = prefix.as_bytes();
                    let mut end = prefix.as_bytes().to_vec();
                    end.push(0xff);
                    tx.scan(start..end.as_slice(), limit).await.map_err(|e| {
                        TitoError::QueryFailed(format!(
                            "Scan {} queue: {}",
                            Self::state_value(state),
                            e
                        ))
                    })?
                };

                let mut events = Vec::new();
                for (index_key, canonical_key_bytes) in entries {
                    let canonical_key = String::from_utf8(canonical_key_bytes).map_err(|_| {
                        TitoError::DeserializationFailed("Invalid queue index".to_string())
                    })?;
                    let Some(bytes) = tx
                        .get(canonical_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Get queue event: {}", e)))?
                    else {
                        tx.delete(index_key.as_slice()).await.map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete orphan queue index: {}", e))
                        })?;
                        continue;
                    };

                    let event: QueueEvent<T> = serde_json::from_slice(&bytes)
                        .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;
                    let state_timestamp = match state {
                        QueueEventState::Pending => Some(event.timestamp),
                        QueueEventState::Processing => event.locked_until,
                        QueueEventState::Completed | QueueEventState::DeadLetter => {
                            event.processed_at
                        }
                    };
                    if event.state == state
                        && state_timestamp.map_or(false, |timestamp| timestamp > cutoff)
                    {
                        events.push((canonical_key, event));
                    }
                }

                Ok::<_, TitoError>(events)
            })
            .await
    }

    async fn delete_indexes_for_event<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        event: &QueueEvent<T>,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        let partition = self.partition_for_key(&event.key);
        match event.state {
            QueueEventState::Pending => {
                let key = Self::pending_key(partition, event.timestamp, &event.id);
                tx.delete(key.as_bytes())
                    .await
                    .map_err(|e| TitoError::DeleteFailed(format!("Delete pending index: {}", e)))?;
            }
            QueueEventState::Processing => {
                if let Some(locked_until) = event.locked_until {
                    let key = Self::processing_key(partition, locked_until, &event.id);
                    tx.delete(key.as_bytes()).await.map_err(|e| {
                        TitoError::DeleteFailed(format!("Delete processing index: {}", e))
                    })?;
                }
            }
            QueueEventState::Completed => {
                if let Some(processed_at) = event.processed_at {
                    let key = Self::completed_key(processed_at, &event.id);
                    tx.delete(key.as_bytes()).await.map_err(|e| {
                        TitoError::DeleteFailed(format!("Delete completed index: {}", e))
                    })?;
                }
            }
            QueueEventState::DeadLetter => {
                if let Some(processed_at) = event.processed_at {
                    let key = Self::dlq_key(partition, processed_at, &event.id);
                    tx.delete(key.as_bytes())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete DLQ index: {}", e)))?;
                }
            }
        }
        Ok(())
    }
}

pub type TitoQueue<E> = Queue<E>;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{TitoKvPair, TitoValue};
    use async_trait::async_trait;
    use serde::{Deserialize, Serialize};
    use std::collections::BTreeMap;
    use std::future::Future;
    use std::ops::Range;
    use std::sync::Arc;
    use tokio::sync::Mutex;

    #[derive(Clone, Default)]
    struct TestEngine {
        store: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    }

    #[derive(Clone)]
    struct TestTransaction {
        store: Arc<Mutex<BTreeMap<Vec<u8>, Vec<u8>>>>,
    }

    #[async_trait]
    impl TitoEngine for TestEngine {
        type Transaction = TestTransaction;

        async fn begin_transaction(&self) -> Result<Self::Transaction, TitoError> {
            Ok(TestTransaction {
                store: self.store.clone(),
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
            let result = f(tx.clone()).await?;
            tx.commit().await.map_err(E::from)?;
            Ok(result)
        }

        async fn clear_active_transactions(&self) -> Result<(), TitoError> {
            Ok(())
        }

        async fn delete_range(&self, start: &[u8], end: &[u8]) -> Result<(), TitoError> {
            let mut store = self.store.lock().await;
            let keys: Vec<Vec<u8>> = store
                .keys()
                .filter(|key| key.as_slice() >= start && key.as_slice() < end)
                .cloned()
                .collect();
            for key in keys {
                store.remove(&key);
            }
            Ok(())
        }
    }

    #[async_trait]
    impl TitoTransaction for TestTransaction {
        async fn get<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<Option<TitoValue>, TitoError> {
            Ok(self.store.lock().await.get(key.as_ref()).cloned())
        }

        async fn put<K: AsRef<[u8]> + Send, V: AsRef<[u8]> + Send>(
            &self,
            key: K,
            value: V,
        ) -> Result<(), TitoError> {
            self.store
                .lock()
                .await
                .insert(key.as_ref().to_vec(), value.as_ref().to_vec());
            Ok(())
        }

        async fn delete<K: AsRef<[u8]> + Send>(&self, key: K) -> Result<(), TitoError> {
            self.store.lock().await.remove(key.as_ref());
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
                .store
                .lock()
                .await
                .iter()
                .filter(|(key, _)| **key >= start && **key < end)
                .take(limit as usize)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect())
        }

        async fn scan_reverse<K: AsRef<[u8]> + Send>(
            &self,
            range: Range<K>,
            limit: u32,
        ) -> Result<Vec<TitoKvPair>, TitoError> {
            let mut results = self.scan(range, u32::MAX).await?;
            results.reverse();
            results.truncate(limit as usize);
            Ok(results)
        }

        async fn batch_get<K: AsRef<[u8]> + Send>(
            &self,
            keys: Vec<K>,
        ) -> Result<Vec<TitoKvPair>, TitoError> {
            let store = self.store.lock().await;
            Ok(keys
                .into_iter()
                .filter_map(|key| {
                    store
                        .get(key.as_ref())
                        .map(|value| (key.as_ref().to_vec(), value.clone()))
                })
                .collect())
        }

        async fn commit(self) -> Result<(), TitoError> {
            Ok(())
        }

        async fn rollback(self) -> Result<(), TitoError> {
            Ok(())
        }
    }

    #[derive(Clone, Debug, Serialize, Deserialize)]
    struct TestPayload {
        value: String,
    }

    #[tokio::test]
    async fn queue_completion_is_durable() {
        let engine = TestEngine::default();
        let queue = Queue::new(engine.clone(), QueueConfig::new(4));
        let event = QueueEvent::new(
            "store:store-1",
            TestPayload {
                value: "created".to_string(),
            },
        );
        let event_id = event.id.clone();

        queue.publish(event).await.unwrap();

        let jobs = queue
            .pull::<TestPayload>(queue.partition_for_key("store:store-1"), 10)
            .await
            .unwrap();
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.state, QueueEventState::Processing);

        queue.ack(&jobs[0].0).await.unwrap();

        let store = engine.store.lock().await;
        let canonical_key = Queue::<TestEngine>::canonical_key(&event_id);
        let bytes = store
            .get(canonical_key.as_bytes())
            .expect("completed queue event should remain durable");
        let completed: QueueEvent<TestPayload> = serde_json::from_slice(bytes).unwrap();
        assert_eq!(completed.state, QueueEventState::Completed);
        assert!(completed.processed_at.is_some());
    }

    #[tokio::test]
    async fn terminal_queue_delete_is_explicit_and_state_based() {
        let engine = TestEngine::default();
        let queue = Queue::new(engine.clone(), QueueConfig::new(1));
        let event = QueueEvent::new(
            "store:store-1",
            TestPayload {
                value: "updated".to_string(),
            },
        );
        let event_id = event.id.clone();

        queue.publish(event).await.unwrap();
        let jobs = queue.pull::<TestPayload>(0, 10).await.unwrap();
        queue.ack(&jobs[0].0).await.unwrap();

        assert_eq!(
            queue
                .delete_by_state_before(QueueEventState::Completed, i64::MAX - 1, 100)
                .await
                .unwrap(),
            1
        );

        let store = engine.store.lock().await;
        let canonical_key = Queue::<TestEngine>::canonical_key(&event_id);
        assert!(!store.contains_key(canonical_key.as_bytes()));
    }

    #[tokio::test]
    async fn queue_events_can_be_scanned_by_state() {
        let engine = TestEngine::default();
        let queue = Queue::new(engine, QueueConfig::new(1));
        let event = QueueEvent::new(
            "store:store-1",
            TestPayload {
                value: "replay".to_string(),
            },
        );

        queue.publish(event).await.unwrap();
        let jobs = queue.pull::<TestPayload>(0, 10).await.unwrap();
        queue.ack(&jobs[0].0).await.unwrap();

        let completed = queue
            .find_by_state_after::<TestPayload>(QueueEventState::Completed, 0, 10)
            .await
            .unwrap();
        assert_eq!(completed.len(), 1);
        assert_eq!(completed[0].1.state, QueueEventState::Completed);
        assert_eq!(completed[0].1.payload.value, "replay");

        let completed = queue
            .find_by_state_after::<TestPayload>(QueueEventState::Completed, i64::MAX - 1, 10)
            .await
            .unwrap();
        assert!(completed.is_empty());
    }

    #[tokio::test]
    async fn non_terminal_queue_delete_is_rejected() {
        let engine = TestEngine::default();
        let queue = Queue::new(engine, QueueConfig::new(1));

        let err = queue
            .delete_by_state_before(QueueEventState::Pending, i64::MAX - 1, 100)
            .await
            .unwrap_err();

        assert!(matches!(err, TitoError::InvalidInput(_)));
    }
}
