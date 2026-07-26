mod cluster;
mod types;
mod worker;

pub use cluster::{
    run_cluster_worker, ClusterCoordinatorLease, ClusterPartitionAssignment, ClusterWorkerConfig,
    ClusterWorkerNode,
};
pub use types::{
    QueueConfig, QueueEvent, QueueEventState, QueueHandlerOutcome, QueuePullCursor, QueuePullPage,
    QueueScanPage,
};
pub use worker::{run_worker, WorkerConfig};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::types::{TitoEngine, TitoTransaction, PARTITION_DIGITS};
use crate::TitoError;

pub(crate) const COMPLETED_EVENT_RETENTION_SECONDS: i64 = 3 * 24 * 60 * 60;
pub(crate) const COMPLETED_EVENT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const COMPLETED_EVENT_MAINTENANCE_BATCH_SIZE: u32 = 1_000;
pub(crate) const COMPLETED_EVENT_MAINTENANCE_MAX_BATCHES: usize = 4;

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

    fn completed_key(processed_at: i64, scheduled_at: i64, event_id: &str) -> String {
        format!(
            "queue:completed:{:020}:{:020}:{}",
            processed_at, scheduled_at, event_id
        )
    }

    fn state_prefix(state: QueueEventState) -> &'static str {
        match state {
            QueueEventState::Pending => "queue:pending:",
            QueueEventState::Completed => "queue:completed:",
        }
    }

    fn state_value(state: QueueEventState) -> &'static str {
        match state {
            QueueEventState::Pending => "pending",
            QueueEventState::Completed => "completed",
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
            QueueEventState::Completed => event.processed_at,
        }
    }

    async fn read_event_from_value<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        value: &[u8],
    ) -> Result<QueueEvent<T>, TitoError> {
        serde_json::from_slice::<QueueEvent<T>>(value)
            .map_err(|e| TitoError::DeserializationFailed(e.to_string()))
    }

    async fn read_value_from_entry(value: &[u8]) -> Result<Value, TitoError> {
        if let Ok(event) = serde_json::from_slice::<Value>(value) {
            if event.get("id").is_some() && event.get("key").is_some() {
                return Ok(event);
            }
        }

        Err(TitoError::DeserializationFailed(
            "Invalid queue event bytes".to_string(),
        ))
    }

    async fn delete_entry(tx: &E::Transaction, storage_key: &[u8]) -> Result<(), TitoError> {
        tx.delete(storage_key)
            .await
            .map_err(|e| TitoError::DeleteFailed(format!("Delete queue event: {}", e)))
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
        cursor: Option<QueuePullCursor>,
        limit: u32,
    ) -> Result<QueuePullPage<T>, TitoError> {
        let limit = limit.max(1);
        self.engine
            .transaction(|tx| {
                let cursor = cursor.clone();
                async move {
                let now = Utc::now().timestamp();
                let partition_start = format!(
                    "queue:pending:{:0pwidth$}:{}",
                    partition,
                    0,
                    pwidth = PARTITION_DIGITS,
                )
                .into_bytes();
                let due_end = format!(
                    "queue:pending:{:0pwidth$}:{}",
                    partition,
                    now.saturating_add(1),
                    pwidth = PARTITION_DIGITS,
                )
                .into_bytes();
                let (start, cycle_end) = cursor
                    .map(|cursor| (cursor.next_start, cursor.cycle_end))
                    .unwrap_or((partition_start, due_end));

                if start >= cycle_end {
                    return Ok::<_, TitoError>(QueuePullPage {
                        events: Vec::new(),
                        next_cursor: None,
                    });
                }

                let mut entries = tx
                    .scan(start..cycle_end.clone(), limit.saturating_add(1))
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Scan pending queue: {}", e)))?;

                // Advance by raw storage rows, not successfully decoded events.
                // A malformed row therefore remains available for inspection
                // without pinning every valid row behind it.
                let has_more = entries.len() > limit as usize;
                if has_more {
                    entries.truncate(limit as usize);
                }
                let next_cursor = if has_more {
                    entries.last().map(|(key, _)| {
                        let mut next_start = key.clone();
                        next_start.push(0);
                        QueuePullCursor {
                            next_start,
                            cycle_end,
                        }
                    })
                } else {
                    None
                };

                let mut events = Vec::new();
                for (storage_key, value) in entries {
                    let event = match Self::read_event_from_value::<T>(&value).await {
                        Ok(event) => event,
                        Err(error) => {
                            log::error!(
                                "Could not deserialize pending queue row {}; leaving it untouched: {}",
                                String::from_utf8_lossy(&storage_key),
                                error
                            );
                            continue;
                        }
                    };

                    if event.state == QueueEventState::Pending && event.timestamp <= now {
                        let key = match String::from_utf8(storage_key) {
                            Ok(key) => key,
                            Err(error) => {
                                log::error!(
                                    "Could not decode pending queue storage key; leaving row untouched: {}",
                                    error
                                );
                                continue;
                            }
                        };
                        events.push((key, event));
                    }
                }

                Ok::<_, TitoError>(QueuePullPage {
                    events,
                    next_cursor,
                })
                }
            })
            .await
    }

    pub async fn ack(&self, key: &str) -> Result<(), TitoError> {
        if !key.starts_with(Self::state_prefix(QueueEventState::Pending)) {
            return Err(TitoError::InvalidInput(
                "Only pending queue events can be acknowledged".to_string(),
            ));
        }

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

                    let mut event = Self::read_value_from_entry(&bytes).await?;

                    if event.get("state").and_then(Value::as_str) != Some("pending") {
                        return Ok::<_, TitoError>(());
                    }

                    let event_id = event
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            TitoError::DeserializationFailed("Queue event missing id".to_string())
                        })?
                        .to_string();
                    let scheduled_at =
                        event
                            .get("timestamp")
                            .and_then(Value::as_i64)
                            .ok_or_else(|| {
                                TitoError::DeserializationFailed(
                                    "Queue event missing timestamp".to_string(),
                                )
                            })?;
                    let processed_at = Utc::now().timestamp();
                    event["state"] =
                        Value::String(Self::state_value(QueueEventState::Completed).to_string());
                    event["processedAt"] = Value::Number(processed_at.into());

                    let completed_key = Self::completed_key(processed_at, scheduled_at, &event_id);
                    let completed_bytes = serde_json::to_vec(&event)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

                    Self::delete_entry(&tx, key.as_bytes()).await?;
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

    /// Atomically hands off the exact pending invocation at `storage_key` to a
    /// successor invocation at `timestamp`.
    ///
    /// The successor keeps the current event's key, payload, and original
    /// schedule and permanent logical event ID. Its new pending storage key
    /// identifies the scheduled invocation. The logical event becomes
    /// Completed only after a handler returns [`QueueHandlerOutcome::Acknowledge`].
    /// If another consumer has already moved or acknowledged the exact pending
    /// row, this is an idempotent no-op.
    pub(crate) async fn schedule_at<
        T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    >(
        &self,
        storage_key: &str,
        timestamp: i64,
    ) -> Result<Option<QueueEvent<T>>, TitoError> {
        if !storage_key.starts_with(Self::state_prefix(QueueEventState::Pending)) {
            return Err(TitoError::InvalidInput(
                "Only pending queue events can schedule a successor".to_string(),
            ));
        }

        let storage_key = storage_key.to_string();

        self.engine
            .transaction(|tx| {
                let storage_key = storage_key.clone();
                async move {
                    let Some(bytes) = tx
                        .get(storage_key.as_bytes())
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Get queue event: {}", e)))?
                    else {
                        return Ok::<_, TitoError>(None);
                    };

                    let current = Self::read_event_from_value::<T>(&bytes).await?;

                    if current.state != QueueEventState::Pending {
                        return Ok::<_, TitoError>(None);
                    }

                    let successor = current.successor_at(timestamp);
                    let successor_partition = self.partition_for_key(&successor.key);
                    let successor_key =
                        Self::pending_key(successor_partition, successor.timestamp, &successor.id);
                    if successor_key == storage_key {
                        return Err(TitoError::InvalidInput(
                            "ScheduleAt must move the event to a different due timestamp"
                                .to_string(),
                        ));
                    }
                    let successor_bytes = serde_json::to_vec(&successor)
                        .map_err(|e| TitoError::SerializationFailed(e.to_string()))?;

                    Self::delete_entry(&tx, storage_key.as_bytes()).await?;
                    tx.put(successor_key.as_bytes(), successor_bytes)
                        .await
                        .map_err(|e| TitoError::CreateFailed(e.to_string()))?;

                    Ok::<_, TitoError>(Some(successor))
                }
            })
            .await
    }

    pub async fn clear(&self) -> Result<(), TitoError> {
        for prefix in ["queue:pending:", "queue:completed:"] {
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
                let start = format!("queue:completed:{:020}:", cutoff.saturating_add(1));
                let entries = tx
                    .scan(
                        "queue:completed:00000000000000000000".as_bytes()..start.as_bytes(),
                        limit.max(1),
                    )
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Scan completed queue: {}", e)))?;

                let deleted = entries.len();
                for (storage_key, _) in entries {
                    Self::delete_entry(&tx, storage_key.as_slice()).await?;
                }

                Ok::<_, TitoError>(deleted)
            })
            .await
    }

    pub(crate) async fn maintain_completed_event_retention(
        &self,
        now: i64,
    ) -> Result<bool, TitoError> {
        let cutoff = now.saturating_sub(COMPLETED_EVENT_RETENTION_SECONDS);

        for _ in 0..COMPLETED_EVENT_MAINTENANCE_MAX_BATCHES {
            let batch = self
                .delete_by_state_before(
                    QueueEventState::Completed,
                    cutoff,
                    COMPLETED_EVENT_MAINTENANCE_BATCH_SIZE,
                )
                .await?;
            if batch < COMPLETED_EVENT_MAINTENANCE_BATCH_SIZE as usize {
                return Ok(false);
            }
        }

        Ok(true)
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
                let prefix = Self::state_prefix(state);
                let entries = if state == QueueEventState::Completed {
                    let start = format!("queue:completed:{:020}:", cutoff.saturating_add(1));
                    tx.scan(
                        start.as_bytes()..Self::prefix_end("queue:completed:").as_slice(),
                        limit,
                    )
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Scan completed queue: {}", e)))?
                } else {
                    tx.scan(
                        prefix.as_bytes()..Self::prefix_end(prefix).as_slice(),
                        limit,
                    )
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Scan queue: {}", e)))?
                };

                for (storage_key, value) in entries {
                    let event = Self::read_event_from_value::<T>(&value).await?;

                    if event.state == state
                        && Self::state_timestamp(&event, state)
                            .is_some_and(|timestamp| timestamp > cutoff)
                    {
                        let key = String::from_utf8(storage_key).map_err(|_| {
                            TitoError::DeserializationFailed("Invalid queue key".to_string())
                        })?;
                        events.push((key, event));
                    }
                }

                Ok::<_, TitoError>(events)
            })
            .await
    }

    pub async fn scan_by_state<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        state: QueueEventState,
        cursor: Option<Vec<u8>>,
        limit: u32,
    ) -> Result<QueueScanPage<T>, TitoError> {
        let prefix = Self::state_prefix(state);
        let limit = limit.max(1);

        self.engine
            .transaction(|tx| {
                let cursor = cursor.clone();
                async move {
                    let start = cursor.unwrap_or_else(|| prefix.as_bytes().to_vec());
                    let entries = tx
                        .scan(start..Self::prefix_end(prefix), limit)
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Scan queue: {}", e)))?;

                    let next_cursor = if entries.len() == limit as usize {
                        entries.last().map(|(key, _)| {
                            let mut cursor = key.clone();
                            cursor.push(0);
                            cursor
                        })
                    } else {
                        None
                    };

                    let mut events = Vec::new();
                    for (storage_key, value) in entries {
                        let event = Self::read_event_from_value::<T>(&value).await?;

                        if event.state == state {
                            let key = String::from_utf8(storage_key).map_err(|_| {
                                TitoError::DeserializationFailed("Invalid queue key".to_string())
                            })?;
                            events.push((key, event));
                        }
                    }

                    Ok::<_, TitoError>(QueueScanPage {
                        events,
                        next_cursor,
                    })
                }
            })
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::MemoryEngine;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Payload {
        name: String,
    }

    fn queue(engine: MemoryEngine) -> Queue<MemoryEngine> {
        Queue::new(engine, QueueConfig::new(1))
    }

    fn payload(name: &str) -> Payload {
        Payload {
            name: name.to_string(),
        }
    }

    fn event(
        id: &str,
        key: &str,
        state: QueueEventState,
        timestamp: i64,
        processed_at: Option<i64>,
    ) -> QueueEvent<Payload> {
        QueueEvent {
            id: id.to_string(),
            key: key.to_string(),
            payload: payload(id),
            timestamp,
            original_scheduled_at: Some(timestamp),
            state,
            processed_at,
        }
    }

    async fn put_event(engine: &MemoryEngine, storage_key: &str, event: QueueEvent<Payload>) {
        engine
            .put_raw(storage_key, serde_json::to_vec(&event).unwrap())
            .await;
    }

    #[tokio::test]
    async fn publish_and_pull_keeps_pending_rows_until_ack() {
        let engine = MemoryEngine::default();
        let queue = queue(engine);
        let now = chrono::Utc::now().timestamp();

        queue
            .publish(QueueEvent::new("entry:due", payload("due")).scheduled_for(now - 10))
            .await
            .unwrap();
        queue
            .publish(QueueEvent::new("entry:future", payload("future")).scheduled_for(now + 3600))
            .await
            .unwrap();

        let jobs = queue.pull::<Payload>(0, None, 10).await.unwrap().events;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.key, "entry:due");
        assert_eq!(jobs[0].1.state, QueueEventState::Pending);

        let pending_before_ack = queue
            .scan_by_state::<Payload>(QueueEventState::Pending, None, 10)
            .await
            .unwrap();
        assert_eq!(pending_before_ack.events.len(), 2);

        queue.ack(&jobs[0].0).await.unwrap();

        let pending_after_ack = queue
            .scan_by_state::<Payload>(QueueEventState::Pending, None, 10)
            .await
            .unwrap();
        assert_eq!(pending_after_ack.events.len(), 1);
        assert_eq!(pending_after_ack.events[0].1.key, "entry:future");

        let completed = queue
            .scan_by_state::<Payload>(QueueEventState::Completed, None, 10)
            .await
            .unwrap();
        assert_eq!(completed.events.len(), 1);
        assert_eq!(completed.events[0].1.key, "entry:due");
        assert_eq!(completed.events[0].1.state, QueueEventState::Completed);
        assert!(completed.events[0].1.processed_at.is_some());
    }

    #[tokio::test]
    async fn scan_by_state_pages_completed_queue_rows() {
        let queue = queue(MemoryEngine::default());

        for name in ["one", "two", "three"] {
            queue
                .publish(QueueEvent::new(format!("entry:{name}"), payload(name)))
                .await
                .unwrap();
        }

        let jobs = queue.pull::<Payload>(0, None, 10).await.unwrap().events;
        assert_eq!(jobs.len(), 3);
        for (storage_key, _) in jobs {
            queue.ack(&storage_key).await.unwrap();
        }

        let first = queue
            .scan_by_state::<Payload>(QueueEventState::Completed, None, 2)
            .await
            .unwrap();
        assert_eq!(first.events.len(), 2);
        assert!(first.next_cursor.is_some());

        let second = queue
            .scan_by_state::<Payload>(QueueEventState::Completed, first.next_cursor, 2)
            .await
            .unwrap();
        assert_eq!(second.events.len(), 1);
        assert!(second.next_cursor.is_none());
        assert!(second
            .events
            .iter()
            .all(|(_, event)| event.state == QueueEventState::Completed));
    }

    #[tokio::test]
    async fn scan_by_state_pages_pending_queue_rows() {
        let queue = queue(MemoryEngine::default());

        for name in ["one", "two", "three"] {
            queue
                .publish(QueueEvent::new(format!("entry:{name}"), payload(name)))
                .await
                .unwrap();
        }

        let first = queue
            .scan_by_state::<Payload>(QueueEventState::Pending, None, 2)
            .await
            .unwrap();
        assert_eq!(first.events.len(), 2);
        assert!(first.next_cursor.is_some());

        let second = queue
            .scan_by_state::<Payload>(QueueEventState::Pending, first.next_cursor, 2)
            .await
            .unwrap();
        assert_eq!(second.events.len(), 1);
        assert!(second.next_cursor.is_none());
        assert!(second
            .events
            .iter()
            .all(|(_, event)| event.state == QueueEventState::Pending));
    }

    #[tokio::test]
    async fn scan_by_state_surfaces_and_preserves_malformed_rows() {
        let engine = MemoryEngine::default();
        let queue = queue(engine.clone());
        let orphan_key = "queue:completed:00000000000000000009:orphan";
        let valid_key = Queue::<MemoryEngine>::completed_key(10, 1, "valid");
        put_event(
            &engine,
            &valid_key,
            event(
                "valid",
                "entry:valid",
                QueueEventState::Completed,
                1,
                Some(10),
            ),
        )
        .await;
        engine.put_raw(orphan_key, b"not-json".to_vec()).await;

        let error = queue
            .scan_by_state::<Payload>(QueueEventState::Completed, None, 10)
            .await
            .unwrap_err();

        assert!(matches!(error, TitoError::DeserializationFailed(_)));
        assert!(engine.contains_key(orphan_key).await);
        assert!(engine.contains_key(&valid_key).await);
    }

    #[tokio::test]
    async fn find_by_state_after_filters_by_state_timestamp() {
        let engine = MemoryEngine::default();
        let queue = queue(engine.clone());

        put_event(
            &engine,
            &Queue::<MemoryEngine>::completed_key(10, 1, "old"),
            event("old", "entry:old", QueueEventState::Completed, 1, Some(10)),
        )
        .await;
        put_event(
            &engine,
            &Queue::<MemoryEngine>::completed_key(20, 1, "new"),
            event("new", "entry:new", QueueEventState::Completed, 1, Some(20)),
        )
        .await;

        let events = queue
            .find_by_state_after::<Payload>(QueueEventState::Completed, 10, 10)
            .await
            .unwrap();

        assert_eq!(events.len(), 1);
        assert_eq!(events[0].1.key, "entry:new");
    }

    #[tokio::test]
    async fn delete_by_state_before_removes_only_terminal_rows_at_or_before_cutoff() {
        let engine = MemoryEngine::default();
        let queue = queue(engine.clone());
        let old_key = Queue::<MemoryEngine>::completed_key(10, 1, "old");
        let fresh_key = Queue::<MemoryEngine>::completed_key(20, 1, "fresh");

        put_event(
            &engine,
            &old_key,
            event("old", "entry:old", QueueEventState::Completed, 1, Some(10)),
        )
        .await;
        put_event(
            &engine,
            &fresh_key,
            event(
                "fresh",
                "entry:fresh",
                QueueEventState::Completed,
                1,
                Some(20),
            ),
        )
        .await;

        let deleted = queue
            .delete_by_state_before(QueueEventState::Completed, 10, 10)
            .await
            .unwrap();

        assert_eq!(deleted, 1);
        assert!(!engine.contains_key(&old_key).await);
        assert!(engine.contains_key(&fresh_key).await);

        let error = queue
            .delete_by_state_before(QueueEventState::Pending, 10, 10)
            .await
            .unwrap_err();
        assert!(matches!(error, TitoError::InvalidInput(_)));
    }
}
