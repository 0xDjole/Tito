mod cluster;
mod types;
mod worker;

pub use cluster::{
    run_cluster_worker, ClusterCoordinatorLease, ClusterPartitionAssignment, ClusterWorkerConfig,
    ClusterWorkerNode,
};
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

const LEGACY_EVENT_PREFIX: &str = "queue:event:";

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

    fn pending_key(partition: u32, timestamp: i64, event_id: &str) -> String {
        format!(
            "queue:pending:{:0pwidth$}:{}:{}",
            partition,
            timestamp,
            event_id,
            pwidth = PARTITION_DIGITS,
        )
    }

    fn completed_key(processed_at: i64, event_id: &str) -> String {
        format!("queue:completed:{:020}:{}", processed_at, event_id)
    }

    fn failed_key(partition: u32, failed_at: i64, event_id: &str) -> String {
        format!(
            "queue:failed:{:0pwidth$}:{}:{}",
            partition,
            failed_at,
            event_id,
            pwidth = PARTITION_DIGITS,
        )
    }

    fn state_prefixes(state: QueueEventState) -> Vec<&'static str> {
        match state {
            QueueEventState::Pending => vec!["queue:pending:"],
            QueueEventState::Completed => vec!["queue:completed:"],
            QueueEventState::Failed => vec!["queue:failed:", "queue:dlq:"],
        }
    }

    fn state_value(state: QueueEventState) -> &'static str {
        match state {
            QueueEventState::Pending => "pending",
            QueueEventState::Completed => "completed",
            QueueEventState::Failed => "failed",
        }
    }

    fn prefix_end(prefix: &str) -> Vec<u8> {
        let mut end = prefix.as_bytes().to_vec();
        end.push(0xff);
        end
    }

    fn state_timestamp<T>(event: &QueueEvent<T>, state: QueueEventState) -> Option<i64> {
        match state {
            QueueEventState::Pending => Some(event.timestamp),
            QueueEventState::Completed | QueueEventState::Failed => event.processed_at,
        }
    }

    async fn read_event_from_value<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        tx: &E::Transaction,
        value: &[u8],
    ) -> Result<Option<(QueueEvent<T>, Option<Vec<u8>>)>, TitoError> {
        if let Ok(event) = serde_json::from_slice::<QueueEvent<T>>(value) {
            return Ok(Some((event, None)));
        }

        let pointer = String::from_utf8(value.to_vec()).map_err(|_| {
            TitoError::DeserializationFailed("Invalid queue event bytes".to_string())
        })?;
        if !pointer.starts_with(LEGACY_EVENT_PREFIX) {
            return Err(TitoError::DeserializationFailed(
                "Invalid queue event bytes".to_string(),
            ));
        }

        let Some(bytes) = tx
            .get(pointer.as_bytes())
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get legacy queue event: {}", e)))?
        else {
            return Ok(None);
        };

        let event = serde_json::from_slice::<QueueEvent<T>>(&bytes)
            .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;
        Ok(Some((event, Some(pointer.into_bytes()))))
    }

    async fn read_value_from_entry(
        tx: &E::Transaction,
        value: &[u8],
    ) -> Result<Option<(Value, Option<Vec<u8>>)>, TitoError> {
        if let Ok(event) = serde_json::from_slice::<Value>(value) {
            if event.get("id").is_some() && event.get("key").is_some() {
                return Ok(Some((event, None)));
            }
        }

        let pointer = String::from_utf8(value.to_vec()).map_err(|_| {
            TitoError::DeserializationFailed("Invalid queue event bytes".to_string())
        })?;
        if !pointer.starts_with(LEGACY_EVENT_PREFIX) {
            return Err(TitoError::DeserializationFailed(
                "Invalid queue event bytes".to_string(),
            ));
        }

        let Some(bytes) = tx
            .get(pointer.as_bytes())
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get legacy queue event: {}", e)))?
        else {
            return Ok(None);
        };

        let event = serde_json::from_slice::<Value>(&bytes)
            .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;
        Ok(Some((event, Some(pointer.into_bytes()))))
    }

    async fn delete_entry(
        tx: &E::Transaction,
        storage_key: &[u8],
        pointer_key: Option<Vec<u8>>,
    ) -> Result<(), TitoError> {
        tx.delete(storage_key)
            .await
            .map_err(|e| TitoError::DeleteFailed(format!("Delete queue event: {}", e)))?;
        if let Some(pointer_key) = pointer_key {
            tx.delete(pointer_key.as_slice()).await.map_err(|e| {
                TitoError::DeleteFailed(format!("Delete legacy queue event: {}", e))
            })?;
        }
        Ok(())
    }

    pub async fn publish_in_tx<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        mut event: QueueEvent<T>,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        let partition = self.partition_for_key(&event.key);
        let pending_key = Self::pending_key(partition, event.timestamp, &event.id);

        event.state = QueueEventState::Pending;
        event.processed_at = None;

        let bytes = serde_json::to_vec(&event)
            .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

        tx.put(pending_key.as_bytes(), bytes)
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

    pub async fn pull<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        partition: u32,
        limit: u32,
    ) -> Result<Vec<(String, QueueEvent<T>)>, TitoError> {
        self.engine
            .transaction(|tx| async move {
                let now = Utc::now().timestamp();
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

                let entries = tx
                    .scan(start.as_bytes()..end.as_bytes(), limit)
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Scan pending queue: {}", e)))?;

                let mut jobs = Vec::new();
                for (storage_key, value) in entries {
                    let Some((event, _)) = Self::read_event_from_value::<T>(&tx, &value).await?
                    else {
                        tx.delete(storage_key.as_slice()).await.map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete orphan queue index: {}", e))
                        })?;
                        continue;
                    };

                    if event.state == QueueEventState::Pending && event.timestamp <= now {
                        let key = String::from_utf8(storage_key).map_err(|_| {
                            TitoError::DeserializationFailed("Invalid queue key".to_string())
                        })?;
                        jobs.push((key, event));
                    }
                }

                Ok::<_, TitoError>(jobs)
            })
            .await
    }

    pub async fn clear_by_key_in_tx<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        key: &str,
        tx: &E::Transaction,
    ) -> Result<u32, TitoError> {
        let mut deleted = 0u32;

        for prefix in [
            "queue:pending:",
            "queue:completed:",
            "queue:failed:",
            "queue:dlq:",
        ] {
            let entries = tx
                .scan(prefix.as_bytes()..Self::prefix_end(prefix).as_slice(), 1000)
                .await
                .map_err(|e| TitoError::QueryFailed(format!("Scan queue: {}", e)))?;

            for (storage_key, value) in entries {
                let Some((event, pointer_key)) =
                    Self::read_event_from_value::<T>(tx, &value).await?
                else {
                    tx.delete(storage_key.as_slice()).await.map_err(|e| {
                        TitoError::DeleteFailed(format!("Delete orphan queue index: {}", e))
                    })?;
                    continue;
                };

                if event.key == key {
                    Self::delete_entry(tx, storage_key.as_slice(), pointer_key).await?;
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

                    let Some((mut event, pointer_key)) =
                        Self::read_value_from_entry(&tx, &bytes).await?
                    else {
                        tx.delete(key.as_bytes()).await.map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete orphan queue index: {}", e))
                        })?;
                        return Ok::<_, TitoError>(());
                    };

                    let event_id = event
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            TitoError::DeserializationFailed("Queue event missing id".to_string())
                        })?
                        .to_string();
                    let processed_at = Utc::now().timestamp();
                    event["state"] =
                        Value::String(Self::state_value(QueueEventState::Completed).to_string());
                    event["processedAt"] = Value::Number(processed_at.into());

                    let completed_key = Self::completed_key(processed_at, &event_id);
                    let completed_bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

                    Self::delete_entry(&tx, key.as_bytes(), pointer_key).await?;
                    tx.put(completed_key.as_bytes(), completed_bytes)
                        .await
                        .map_err(|e| {
                            TitoError::UpdateFailed(format!("Create completed event: {}", e))
                        })?;

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn reschedule<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        event: QueueEvent<T>,
        storage_key: &str,
        new_timestamp: i64,
    ) -> Result<(), TitoError> {
        let storage_key = storage_key.to_string();

        self.engine
            .transaction(|tx| {
                let storage_key = storage_key.clone();
                let mut event = event.clone();

                async move {
                    let pointer_key = tx
                        .get(storage_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Get queue event: {}", e)))?
                        .and_then(|value| {
                            String::from_utf8(value)
                                .ok()
                                .filter(|key| key.starts_with(LEGACY_EVENT_PREFIX))
                                .map(String::into_bytes)
                        });

                    Self::delete_entry(&tx, storage_key.as_bytes(), pointer_key).await?;

                    event.timestamp = new_timestamp;
                    event.state = QueueEventState::Pending;
                    event.processed_at = None;

                    let partition = self.partition_for_key(&event.key);
                    let new_key = Self::pending_key(partition, event.timestamp, &event.id);
                    let bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

                    tx.put(new_key.as_bytes(), bytes)
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn clear(&self) -> Result<(), TitoError> {
        for prefix in [
            "queue:pending:",
            "queue:completed:",
            "queue:failed:",
            "queue:dlq:",
            LEGACY_EVENT_PREFIX,
        ] {
            loop {
                let deleted = self
                    .engine
                    .transaction(|tx| async move {
                        let entries = tx
                            .scan(prefix.as_bytes()..Self::prefix_end(prefix).as_slice(), 1000)
                            .await
                            .map_err(|e| {
                                TitoError::QueryFailed(format!("Scan queue prefix: {}", e))
                            })?;
                        let deleted = entries.len();

                        for (key, _) in entries {
                            tx.delete(key).await.map_err(|e| {
                                TitoError::DeleteFailed(format!("Delete queue entry: {}", e))
                            })?;
                        }

                        Ok::<_, TitoError>(deleted)
                    })
                    .await?;

                if deleted < 1000 {
                    break;
                }
            }
        }
        Ok(())
    }

    pub async fn move_to_failed<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        event: QueueEvent<T>,
        storage_key: &str,
    ) -> Result<(), TitoError> {
        let partition = self.partition_for_key(&event.key);
        let failed_at = Utc::now().timestamp();
        let failed_key = Self::failed_key(partition, failed_at, &event.id);

        self.engine
            .transaction(|tx| {
                let storage_key = storage_key.to_string();
                let failed_key = failed_key.clone();
                let mut event = event.clone();

                async move {
                    let pointer_key = tx
                        .get(storage_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Get queue event: {}", e)))?
                        .and_then(|value| {
                            String::from_utf8(value)
                                .ok()
                                .filter(|key| key.starts_with(LEGACY_EVENT_PREFIX))
                                .map(String::into_bytes)
                        });

                    Self::delete_entry(&tx, storage_key.as_bytes(), pointer_key).await?;

                    event.state = QueueEventState::Failed;
                    event.processed_at = Some(failed_at);

                    let bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
                    tx.put(failed_key.as_bytes(), bytes)
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn move_to_dlq<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        event: QueueEvent<T>,
        storage_key: &str,
    ) -> Result<(), TitoError> {
        self.move_to_failed(event, storage_key).await
    }

    pub async fn delete_by_state_before(
        &self,
        state: QueueEventState,
        cutoff: i64,
        limit: u32,
    ) -> Result<usize, TitoError> {
        if state == QueueEventState::Pending {
            return Err(TitoError::InvalidInput(format!(
                "Refusing to delete non-terminal queue state {}",
                Self::state_value(state)
            )));
        }

        self.engine
            .transaction(|tx| async move {
                let mut deleted = 0usize;
                for prefix in Self::state_prefixes(state) {
                    if deleted >= limit as usize {
                        break;
                    }

                    let entries = if state == QueueEventState::Completed {
                        let start = format!("queue:completed:{:020}:", cutoff.saturating_add(1));
                        tx.scan(
                            "queue:completed:00000000000000000000".as_bytes()..start.as_bytes(),
                            limit.saturating_sub(deleted as u32),
                        )
                        .await
                        .map_err(|e| {
                            TitoError::QueryFailed(format!("Scan completed queue: {}", e))
                        })?
                    } else {
                        tx.scan(
                            prefix.as_bytes()..Self::prefix_end(prefix).as_slice(),
                            limit.saturating_sub(deleted as u32),
                        )
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Scan failed queue: {}", e)))?
                    };

                    for (storage_key, value) in entries {
                        let Some((event, pointer_key)) =
                            Self::read_event_from_value::<Value>(&tx, &value).await?
                        else {
                            tx.delete(storage_key.as_slice()).await.map_err(|e| {
                                TitoError::DeleteFailed(format!("Delete orphan queue index: {}", e))
                            })?;
                            continue;
                        };

                        if event.state == state
                            && event
                                .processed_at
                                .map_or(false, |processed_at| processed_at <= cutoff)
                        {
                            Self::delete_entry(&tx, storage_key.as_slice(), pointer_key).await?;
                            deleted += 1;
                        }
                    }
                }

                Ok::<_, TitoError>(deleted)
            })
            .await
    }

    pub async fn find_by_state_after<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        state: QueueEventState,
        cutoff: i64,
        limit: u32,
    ) -> Result<Vec<(String, QueueEvent<T>)>, TitoError> {
        self.engine
            .transaction(|tx| async move {
                let mut events = Vec::new();
                for prefix in Self::state_prefixes(state) {
                    if events.len() >= limit as usize {
                        break;
                    }

                    let entries = if state == QueueEventState::Completed {
                        let start = format!("queue:completed:{:020}:", cutoff.saturating_add(1));
                        tx.scan(
                            start.as_bytes()..Self::prefix_end("queue:completed:").as_slice(),
                            limit.saturating_sub(events.len() as u32),
                        )
                        .await
                        .map_err(|e| {
                            TitoError::QueryFailed(format!("Scan completed queue: {}", e))
                        })?
                    } else {
                        tx.scan(
                            prefix.as_bytes()..Self::prefix_end(prefix).as_slice(),
                            limit.saturating_sub(events.len() as u32),
                        )
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Scan queue: {}", e)))?
                    };

                    for (storage_key, value) in entries {
                        let Some((event, _)) =
                            Self::read_event_from_value::<T>(&tx, &value).await?
                        else {
                            tx.delete(storage_key.as_slice()).await.map_err(|e| {
                                TitoError::DeleteFailed(format!("Delete orphan queue index: {}", e))
                            })?;
                            continue;
                        };

                        if event.state == state
                            && Self::state_timestamp(&event, state)
                                .map_or(false, |timestamp| timestamp > cutoff)
                        {
                            let key = String::from_utf8(storage_key).map_err(|_| {
                                TitoError::DeserializationFailed("Invalid queue key".to_string())
                            })?;
                            events.push((key, event));
                        }
                    }
                }

                Ok::<_, TitoError>(events)
            })
            .await
    }
}

pub type TitoQueue<E> = Queue<E>;
