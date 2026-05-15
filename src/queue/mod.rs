mod types;
mod worker;

pub use types::{QueueConfig, QueueEvent};
pub use worker::{run_worker, WorkerConfig};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::base::TitoModel;
use crate::event::TitoEvent;
use crate::types::{TitoEngine, TitoTransaction, PARTITION_DIGITS};
use crate::TitoError;

#[derive(Clone)]
pub struct Queue<E: TitoEngine> {
    pub engine: E,
    pub config: QueueConfig,
    event_model: Option<TitoModel<E, TitoEvent>>,
}

impl<E: TitoEngine> Queue<E> {
    pub fn new(engine: E, config: QueueConfig) -> Self {
        Self {
            engine,
            config,
            event_model: None,
        }
    }

    pub fn with_event_model(mut self, model: TitoModel<E, TitoEvent>) -> Self {
        self.event_model = Some(model);
        self
    }

    pub async fn publish_in_tx<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        event: QueueEvent<T>,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        let mut hasher = DefaultHasher::new();
        event.key.hash(&mut hasher);
        let partition = (hasher.finish() % self.config.partition_count as u64) as u32;

        let key = format!(
            "queue:{:0pwidth$}:{}:{}",
            partition,
            event.scheduled_at,
            event.id,
            pwidth = PARTITION_DIGITS,
        );

        let bytes = serde_json::to_vec(&event)
            .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

        tx.put(&key, bytes)
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
                    "queue:{:0pwidth$}:{}",
                    partition, 0,
                    pwidth = PARTITION_DIGITS,
                );
                let end = format!(
                    "queue:{:0pwidth$}:{}",
                    partition, now + 1,
                    pwidth = PARTITION_DIGITS,
                );

                let events = tx
                    .scan(start.as_bytes()..end.as_bytes(), limit)
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

                let mut jobs: Vec<(String, QueueEvent<T>)> = Vec::new();
                for (storage_key, value) in events.into_iter() {
                    if let Ok(event) = serde_json::from_slice::<QueueEvent<T>>(&value) {
                        if event.scheduled_at <= now {
                            let key_str = String::from_utf8_lossy(&storage_key).into_owned();
                            jobs.push((key_str, event));
                        }
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

        for partition in 0..self.config.partition_count {
            let start = format!(
                "queue:{:0pwidth$}:0",
                partition, pwidth = PARTITION_DIGITS,
            );
            let end = format!(
                "queue:{:0pwidth$}:9999999999",
                partition, pwidth = PARTITION_DIGITS,
            );

            let events = tx
                .scan(start.as_bytes()..end.as_bytes(), 1000)
                .await
                .map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

            for (storage_key, value) in events {
                if let Ok(event) = serde_json::from_slice::<QueueEvent<T>>(&value) {
                    if event.key == key {
                        let key_str = String::from_utf8_lossy(&storage_key);
                        tx.delete(key_str.as_bytes())
                            .await
                            .map_err(|e| TitoError::DeleteFailed(format!("Delete event: {}", e)))?;
                        deleted += 1;
                    }
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

    pub async fn ack(&self, key: &str, event_id: &str) -> Result<(), TitoError> {
        self.engine
            .transaction(|tx| {
                let key = key.to_string();
                let event_id = event_id.to_string();
                async move {
                    tx.delete(key.as_bytes())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete event: {}", e)))?;

                    if let Some(model) = &self.event_model {
                        if let Ok(mut tito_event) = model.get(&event_id).execute(Some(&tx)).await {
                            tito_event.status = "processed".to_string();
                            tito_event.processed_at = Utc::now().timestamp_millis();
                            let _ = model
                                .set(tito_event)
                                .changelog(false)
                                .timestamps(false)
                                .execute(&tx)
                                .await;
                        }
                    }

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn reschedule<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        mut event: QueueEvent<T>,
        storage_key: &str,
        new_scheduled_at: i64,
    ) -> Result<(), TitoError> {
        let old_key = storage_key.to_string();
        let event_id = event.id.clone();

        self.engine
            .transaction(|tx| {
                let old_key = old_key.clone();
                let event_id = event_id.clone();

                async move {
                    tx.delete(old_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete old event: {}", e)))?;

                    let mut hasher = DefaultHasher::new();
                    event.key.hash(&mut hasher);
                    let partition = (hasher.finish() % self.config.partition_count as u64) as u32;

                    event.scheduled_at = new_scheduled_at;

                    let new_key = format!(
                        "queue:{:0pwidth$}:{}:{}",
                        partition,
                        event.scheduled_at,
                        event.id,
                        pwidth = PARTITION_DIGITS,
                    );

                    let bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

                    tx.put(&new_key, bytes)
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

                    if let Some(model) = &self.event_model {
                        if let Ok(mut tito_event) = model.get(&event_id).execute(Some(&tx)).await {
                            tito_event.error = event.error.clone();
                            tito_event.retry_count = event.retry_count;
                            let _ = model
                                .set(tito_event)
                                .changelog(false)
                                .timestamps(false)
                                .execute(&tx)
                                .await;
                        }
                    }

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
        let mut hasher = DefaultHasher::new();
        event.key.hash(&mut hasher);
        let partition = (hasher.finish() % self.config.partition_count as u64) as u32;

        let dlq_key = format!(
            "dlq:{:0pwidth$}:{}:{}",
            partition,
            event.created_at,
            event.id,
            pwidth = PARTITION_DIGITS,
        );

        let event_id = event.id.clone();

        self.engine
            .transaction(|tx| {
                let old_key = storage_key.to_string();
                let dlq_key = dlq_key.clone();
                let event = event.clone();
                let event_id = event_id.clone();
                async move {
                    tx.delete(old_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete event: {}", e)))?;

                    let bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
                    tx.put(&dlq_key, bytes)
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

                    if let Some(model) = &self.event_model {
                        if let Ok(mut tito_event) = model.get(&event_id).execute(Some(&tx)).await {
                            tito_event.status = "dlq".to_string();
                            tito_event.error = event.error.clone();
                            tito_event.retry_count = event.retry_count;
                            let _ = model
                                .set(tito_event)
                                .changelog(false)
                                .timestamps(false)
                                .execute(&tx)
                                .await;
                        }
                    }

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }
}

pub type TitoQueue<E> = Queue<E>;
