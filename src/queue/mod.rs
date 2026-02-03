mod traits;
mod types;
mod worker;

pub use traits::{EventHandler, EventType};
pub use types::{QueueConfig, QueueEvent, RetryPolicy, WorkerConfig};
pub use worker::run_worker;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::types::{TitoEngine, TitoTransaction, PARTITION_DIGITS};
use crate::TitoError;

/// Generic queue backed by TitoEngine
#[derive(Clone)]
pub struct Queue<E: TitoEngine> {
    pub engine: E,
    pub config: QueueConfig,
}

impl<E: TitoEngine> Queue<E> {
    pub fn new(engine: E, config: QueueConfig) -> Self {
        Self { engine, config }
    }

    /// Publish an event to the queue within an existing transaction
    pub async fn publish_in_tx<T: EventType + Serialize>(
        &self,
        mut event: QueueEvent<T>,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        let event_type = T::event_type_name();

        // Calculate partition from entity_id
        let mut hasher = DefaultHasher::new();
        event.entity_id.hash(&mut hasher);
        let partition = (hasher.finish() % self.config.partition_count as u64) as u32;

        // Generate storage key
        let key = format!(
            "event:{}:{:0pwidth$}:{}:{}",
            event_type,
            partition,
            event.scheduled_at,
            event.id,
            pwidth = PARTITION_DIGITS,
        );

        event.key = key.clone();

        let bytes = serde_json::to_vec(&event)
            .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

        tx.put(&key, bytes)
            .await
            .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

        Ok(())
    }

    /// Publish an event to the queue (creates its own transaction)
    pub async fn publish<T: EventType + Serialize>(
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

    /// Pull events from a specific partition that are ready to process
    pub async fn pull<T: EventType + DeserializeOwned>(
        &self,
        event_type: &str,
        partition: u32,
        limit: u32,
    ) -> Result<Vec<QueueEvent<T>>, TitoError> {
        self.engine
            .transaction(|tx| {
                let event_type = event_type.to_string();
                async move {
                    let now = Utc::now().timestamp();

                    let event_start = format!(
                        "event:{}:{:0pwidth$}:{}",
                        event_type,
                        partition,
                        0,
                        pwidth = PARTITION_DIGITS,
                    );
                    let event_end = format!(
                        "event:{}:{:0pwidth$}:{}",
                        event_type,
                        partition,
                        now + 1,
                        pwidth = PARTITION_DIGITS,
                    );

                    let events = tx
                        .scan(event_start.as_bytes()..event_end.as_bytes(), limit)
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

                    let mut jobs: Vec<QueueEvent<T>> = Vec::new();
                    for (_key, value) in events.into_iter() {
                        if let Ok(event) = serde_json::from_slice::<QueueEvent<T>>(&value) {
                            if event.scheduled_at <= now {
                                jobs.push(event);
                            }
                        }
                    }

                    Ok::<_, TitoError>(jobs)
                }
            })
            .await
    }

    /// Acknowledge (delete) an event from the queue
    pub async fn ack(&self, key: &str) -> Result<(), TitoError> {
        self.engine
            .transaction(|tx| {
                let key = key.to_string();
                async move {
                    tx.delete(key.as_bytes())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete event: {}", e)))?;
                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    /// Reschedule an event with updated scheduled_at time
    pub async fn reschedule<T: EventType + Serialize>(
        &self,
        mut event: QueueEvent<T>,
        new_scheduled_at: i64,
    ) -> Result<(), TitoError> {
        let old_key = event.key.clone();

        self.engine
            .transaction(|tx| {
                let old_key = old_key.clone();
                let event_type = T::event_type_name();

                async move {
                    // Delete old event
                    tx.delete(old_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete old event: {}", e)))?;

                    // Calculate partition
                    let mut hasher = DefaultHasher::new();
                    event.entity_id.hash(&mut hasher);
                    let partition = (hasher.finish() % self.config.partition_count as u64) as u32;

                    // Update scheduled_at
                    event.scheduled_at = new_scheduled_at;

                    // Generate new key with updated timestamp
                    let new_key = format!(
                        "event:{}:{:0pwidth$}:{}:{}",
                        event_type,
                        partition,
                        event.scheduled_at,
                        event.id,
                        pwidth = PARTITION_DIGITS,
                    );

                    event.key = new_key.clone();

                    let bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

                    tx.put(&new_key, bytes)
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    /// Clear all events (for testing)
    pub async fn clear(&self) -> Result<(), TitoError> {
        Ok(())
    }
}

// Keep backward compatibility with old TitoQueue name
pub type TitoQueue<E> = Queue<E>;
