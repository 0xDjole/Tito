mod cluster;
mod types;
mod worker;

pub use cluster::{
    run_cluster_worker, ClusterCoordinatorLease, ClusterPartitionAssignment, ClusterWorkerConfig,
    ClusterWorkerNode,
};
pub use types::{
    QueueConfig, QueueDeletePage, QueueEvent, QueueEventState, QueueHandlerOutcome,
    QueueHandlerResult, QueueOwner, QueuePullCursor, QueuePullPage, QueueScanPage,
};
pub use worker::{run_worker, WorkerConfig};

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::types::{TitoEngine, TitoKvPair, TitoTransaction, PARTITION_DIGITS};
use crate::TitoError;

pub(crate) const COMPLETED_EVENT_MAINTENANCE_INTERVAL: Duration = Duration::from_secs(30);
pub(crate) const COMPLETED_EVENT_MAINTENANCE_BATCH_SIZE: u32 = 1_000;
pub(crate) const COMPLETED_EVENT_MAINTENANCE_MAX_BATCHES: usize = 4;
pub const MAX_QUEUE_EVENT_BYTES: usize = 1024 * 1024;
pub(crate) const QUEUE_SCAN_RPC_LIMIT: u32 = 16;
const KEY_NUMBER_DIGITS: usize = 20;

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

    fn pending_key(partition: u32, timestamp: i64, enqueue_version: u64, event_id: &str) -> String {
        format!(
            "queue:pending:{:0pwidth$}:{:0twidth$}:{:0vwidth$}:{}",
            partition,
            timestamp,
            enqueue_version,
            event_id,
            pwidth = PARTITION_DIGITS,
            twidth = KEY_NUMBER_DIGITS,
            vwidth = KEY_NUMBER_DIGITS,
        )
    }

    fn pending_partition_start(partition: u32, timestamp: i64) -> Vec<u8> {
        format!(
            "queue:pending:{:0pwidth$}:{:0twidth$}",
            partition,
            timestamp,
            pwidth = PARTITION_DIGITS,
            twidth = KEY_NUMBER_DIGITS,
        )
        .into_bytes()
    }

    fn pending_key_generation(storage_key: &[u8], partition: u32) -> Option<(&str, u64)> {
        let storage_key = std::str::from_utf8(storage_key).ok()?;
        let prefix = format!(
            "queue:pending:{partition:0width$}:",
            width = PARTITION_DIGITS
        );
        let suffix = storage_key.strip_prefix(&prefix)?;
        let mut fields = suffix.splitn(3, ':');
        let timestamp = fields.next()?;
        let enqueue_version = fields.next()?;
        fields.next()?;

        if timestamp.len() != KEY_NUMBER_DIGITS
            || enqueue_version.len() != KEY_NUMBER_DIGITS
            || !timestamp.bytes().all(|byte| byte.is_ascii_digit())
            || !enqueue_version.bytes().all(|byte| byte.is_ascii_digit())
        {
            return None;
        }

        Some((timestamp, enqueue_version.parse().ok()?))
    }

    fn pending_timestamp_bucket_end(partition: u32, timestamp: &str) -> Vec<u8> {
        let prefix = format!(
            "queue:pending:{partition:0width$}:{timestamp}:",
            width = PARTITION_DIGITS
        );
        Self::prefix_end(&prefix)
    }

    fn completed_key(processed_at: i64, event_timestamp: i64, event_id: &str) -> String {
        format!(
            "queue:completed:{:0width$}:{:0width$}:{}",
            processed_at,
            event_timestamp,
            event_id,
            width = KEY_NUMBER_DIGITS,
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

    fn validate_timestamp(timestamp: i64) -> Result<(), TitoError> {
        if timestamp < 0 {
            return Err(TitoError::InvalidInput(
                "Queue timestamps must be non-negative Unix seconds".to_string(),
            ));
        }
        Ok(())
    }

    fn prefix_end(prefix: &str) -> Vec<u8> {
        let mut end = prefix.as_bytes().to_vec();
        end.push(0xff);
        end
    }

    fn read_event_from_value<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        value: &[u8],
    ) -> Result<QueueEvent<T>, TitoError> {
        serde_json::from_slice::<QueueEvent<T>>(value)
            .map_err(|e| TitoError::DeserializationFailed(e.to_string()))
    }

    fn read_value_from_entry(value: &[u8]) -> Result<Value, TitoError> {
        if let Ok(event) = serde_json::from_slice::<Value>(value) {
            if event.get("id").is_some() && event.get("key").is_some() {
                return Ok(event);
            }
        }

        Err(TitoError::DeserializationFailed(
            "Invalid queue event bytes".to_string(),
        ))
    }

    fn serialize_new_event<T: Serialize>(event: &QueueEvent<T>) -> Result<Vec<u8>, TitoError> {
        let bytes =
            serde_json::to_vec(event).map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
        if bytes.len() > MAX_QUEUE_EVENT_BYTES {
            return Err(TitoError::InvalidInput(format!(
                "Serialized queue event exceeds the {MAX_QUEUE_EVENT_BYTES}-byte limit"
            )));
        }
        Ok(bytes)
    }

    async fn scan_queue_entries(
        tx: &E::Transaction,
        start: Vec<u8>,
        end: Vec<u8>,
        limit: u32,
    ) -> Result<Vec<TitoKvPair>, TitoError> {
        let limit = limit.max(1);
        let mut entries = Vec::new();
        let mut next_start = start;

        while next_start < end && entries.len() < limit as usize {
            let remaining = limit.saturating_sub(entries.len() as u32);
            let rpc_limit = remaining.min(QUEUE_SCAN_RPC_LIMIT);
            let page = tx.scan(next_start.clone()..end.clone(), rpc_limit).await?;
            let scanned_full_page = page.len() == rpc_limit as usize;
            let Some((last_key, _)) = page.last() else {
                break;
            };
            let mut following_start = last_key.clone();
            following_start.push(0);
            if following_start <= next_start {
                return Err(TitoError::QueryFailed(
                    "Queue scan did not advance".to_string(),
                ));
            }

            entries.extend(page);
            next_start = following_start;
            if !scanned_full_page {
                break;
            }
        }

        Ok(entries)
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
        Self::validate_timestamp(event.timestamp)?;
        let partition = self.partition_for_key(&event.key);
        let pending_key =
            Self::pending_key(partition, event.timestamp, tx.start_version(), &event.id);

        event.state = QueueEventState::Pending;
        event.processed_at = None;

        let bytes = Self::serialize_new_event(&event)?;

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
                let (start, cycle_end, enqueue_horizon) = cursor
                    .map(|cursor| {
                        (
                            cursor.next_start,
                            cursor.cycle_end,
                            cursor.enqueue_horizon,
                        )
                    })
                    .unwrap_or_else(|| {
                        (
                            Self::pending_partition_start(partition, 0),
                            Self::pending_partition_start(partition, now.saturating_add(1)),
                            tx.start_version(),
                        )
                    });

                if start >= cycle_end {
                    return Ok::<_, TitoError>(QueuePullPage {
                        events: Vec::new(),
                        next_cursor: None,
                    });
                }

                let entries = Self::scan_queue_entries(
                    &tx,
                    start.clone(),
                    cycle_end.clone(),
                    limit,
                )
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Scan pending queue: {}", e)))?;
                let scanned_full_page = entries.len() == limit as usize;

                let mut events = Vec::new();
                let mut next_start = start.clone();
                for (storage_key, value) in entries {
                    if storage_key < next_start {
                        continue;
                    }

                    if let Some((timestamp, enqueue_version)) =
                        Self::pending_key_generation(&storage_key, partition)
                    {
                        if enqueue_version >= enqueue_horizon {
                            next_start = Self::pending_timestamp_bucket_end(partition, timestamp);
                            continue;
                        }
                    }

                    next_start = storage_key.clone();
                    next_start.push(0);

                    let event = match Self::read_event_from_value::<T>(&value) {
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

                let next_cursor =
                    (scanned_full_page && next_start > start && next_start < cycle_end).then_some(
                        QueuePullCursor {
                            next_start,
                            cycle_end,
                            enqueue_horizon,
                        },
                    );

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

                    let mut event = Self::read_value_from_entry(&bytes)?;

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
                    let event_timestamp = event
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
                    let completed_key =
                        Self::completed_key(processed_at, event_timestamp, &event_id);
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

    pub async fn reschedule<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        storage_key: &str,
        mut next: QueueEvent<T>,
    ) -> Result<(), TitoError> {
        Self::validate_timestamp(next.timestamp)?;
        if !storage_key.starts_with(Self::state_prefix(QueueEventState::Pending)) {
            return Err(TitoError::InvalidInput(
                "Only pending queue events can be rescheduled".to_string(),
            ));
        }

        next.state = QueueEventState::Pending;
        next.processed_at = None;
        let storage_key = storage_key.to_string();

        self.engine
            .transaction(|tx| {
                let storage_key = storage_key.clone();
                let next = next.clone();
                async move {
                    let Some(bytes) = tx.get(storage_key.as_bytes()).await.map_err(|error| {
                        TitoError::QueryFailed(format!("Get queue event for reschedule: {error}"))
                    })?
                    else {
                        return Ok::<_, TitoError>(());
                    };

                    let mut current = Self::read_value_from_entry(&bytes)?;
                    if current.get("state").and_then(Value::as_str) != Some("pending") {
                        return Ok(());
                    }

                    let current_id = current
                        .get("id")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            TitoError::DeserializationFailed("Queue event missing id".to_string())
                        })?
                        .to_string();
                    if current_id != next.id {
                        return Err(TitoError::InvalidInput(
                            "A rescheduled row must preserve the logical event id".to_string(),
                        ));
                    }
                    let current_key =
                        current.get("key").and_then(Value::as_str).ok_or_else(|| {
                            TitoError::DeserializationFailed(
                                "Queue event missing partition key".to_string(),
                            )
                        })?;
                    if current_key != next.key {
                        return Err(TitoError::InvalidInput(
                            "A rescheduled row must preserve the event partition key".to_string(),
                        ));
                    }
                    let next_payload = serde_json::to_value(&next.payload)
                        .map_err(|error| TitoError::SerializationFailed(error.to_string()))?;
                    if current.get("payload") != Some(&next_payload) {
                        return Err(TitoError::InvalidInput(
                            "A rescheduled row must preserve the event payload".to_string(),
                        ));
                    }
                    let current_timestamp = current
                        .get("timestamp")
                        .and_then(Value::as_i64)
                        .ok_or_else(|| {
                            TitoError::DeserializationFailed(
                                "Queue event missing timestamp".to_string(),
                            )
                        })?;

                    let processed_at = Utc::now().timestamp();
                    current["state"] =
                        Value::String(Self::state_value(QueueEventState::Completed).to_string());
                    current["processedAt"] = Value::Number(processed_at.into());
                    let completed_key =
                        Self::completed_key(processed_at, current_timestamp, &current_id);
                    let completed_bytes = serde_json::to_vec(&current)
                        .map_err(|error| TitoError::SerializationFailed(error.to_string()))?;

                    let partition = self.partition_for_key(&next.key);
                    let pending_key =
                        Self::pending_key(partition, next.timestamp, tx.start_version(), &next.id);
                    let pending_bytes = Self::serialize_new_event(&next)?;

                    Self::delete_entry(&tx, storage_key.as_bytes()).await?;
                    tx.put(completed_key.as_bytes(), completed_bytes)
                        .await
                        .map_err(|error| {
                            TitoError::UpdateFailed(format!(
                                "Complete rescheduled queue event: {error}"
                            ))
                        })?;
                    tx.put(pending_key.as_bytes(), pending_bytes)
                        .await
                        .map_err(|error| TitoError::CreateFailed(error.to_string()))?;
                    Ok(())
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
                        let entries = Self::scan_queue_entries(
                            &tx,
                            prefix.as_bytes().to_vec(),
                            Self::prefix_end(prefix),
                            1000,
                        )
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Scan queue prefix: {}", e)))?;
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
                let start = format!(
                    "queue:completed:{:0width$}:",
                    cutoff.saturating_add(1),
                    width = KEY_NUMBER_DIGITS,
                );
                let entries = Self::scan_queue_entries(
                    &tx,
                    "queue:completed:00000000000000000000".as_bytes().to_vec(),
                    start.as_bytes().to_vec(),
                    limit,
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
        let retention_seconds =
            i64::try_from(self.config.completed_retention.as_secs()).unwrap_or(i64::MAX);
        let cutoff = now.saturating_sub(retention_seconds);

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
                    let entries =
                        Self::scan_queue_entries(&tx, start, Self::prefix_end(prefix), limit)
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
                        let event = Self::read_event_from_value::<T>(&value)?;

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

    pub async fn delete_matching_in_tx<T, F>(
        &self,
        state: QueueEventState,
        cursor: Option<Vec<u8>>,
        limit: u32,
        tx: &E::Transaction,
        matches: F,
    ) -> Result<QueueDeletePage, TitoError>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
        F: Fn(&QueueEvent<T>) -> Result<bool, TitoError> + Send + Sync,
    {
        let prefix = Self::state_prefix(state);
        let limit = limit.max(1);
        let start = cursor.unwrap_or_else(|| prefix.as_bytes().to_vec());
        let entries = Self::scan_queue_entries(tx, start, Self::prefix_end(prefix), limit)
            .await
            .map_err(|error| TitoError::QueryFailed(format!("Scan queue for deletion: {error}")))?;
        let next_cursor = if entries.len() == limit as usize {
            entries.last().map(|(key, _)| {
                let mut cursor = key.clone();
                cursor.push(0);
                cursor
            })
        } else {
            None
        };
        let mut deleted_event_ids = Vec::new();
        for (storage_key, value) in entries {
            let event = Self::read_event_from_value::<T>(&value)?;
            if event.state == state && matches(&event)? {
                deleted_event_ids.push(event.id);
                Self::delete_entry(tx, &storage_key).await?;
            }
        }
        deleted_event_ids.sort();
        deleted_event_ids.dedup();
        Ok(QueueDeletePage {
            deleted_event_ids,
            next_cursor,
        })
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
        Queue::new(
            engine,
            QueueConfig::new(1, Duration::from_secs(3 * 24 * 60 * 60)),
        )
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
            owner: None,
            payload: payload(id),
            timestamp,
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
            .publish(QueueEvent::new("entry:due", payload("due"), now - 10))
            .await
            .unwrap();
        queue
            .publish(QueueEvent::new(
                "entry:future",
                payload("future"),
                now + 3600,
            ))
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
        assert!(
            serde_json::to_value(&completed.events[0].1).unwrap()["completionReason"].is_null()
        );
    }

    #[tokio::test]
    async fn publish_rejects_an_invocation_larger_than_the_transport_safe_limit() {
        let engine = MemoryEngine::default();
        let queue = queue(engine.clone());
        let oversized = Payload {
            name: "x".repeat(MAX_QUEUE_EVENT_BYTES),
        };

        let error = queue
            .publish(QueueEvent::new(
                "entry:oversized",
                oversized,
                Utc::now().timestamp(),
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, TitoError::InvalidInput(message)
            if message.contains(&MAX_QUEUE_EVENT_BYTES.to_string())));
        assert!(engine
            .keys_with_prefix(Queue::<MemoryEngine>::state_prefix(
                QueueEventState::Pending
            ))
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn queue_reads_fulfill_logical_limits_through_bounded_rpc_pages() {
        let engine = MemoryEngine::default();
        let queue = queue(engine.clone());
        let now = Utc::now().timestamp();

        for index in 0..18 {
            queue
                .publish(QueueEvent::new(
                    format!("entry:bounded-{index}"),
                    payload(&format!("bounded-{index}")),
                    now,
                ))
                .await
                .unwrap();
        }

        let page = queue.pull::<Payload>(0, None, 17).await.unwrap();

        assert_eq!(page.events.len(), 17);
        assert!(page.next_cursor.is_some());
        assert_eq!(engine.pending_queue_scan_count(), 2);

        let scanned = queue
            .scan_by_state::<Payload>(QueueEventState::Pending, None, 17)
            .await
            .unwrap();
        assert_eq!(scanned.events.len(), 17);
        assert!(scanned.next_cursor.is_some());
        assert_eq!(engine.pending_queue_scan_count(), 4);
    }

    #[tokio::test]
    async fn pull_reaches_event_timestamps_with_fewer_decimal_digits_than_now() {
        let queue = queue(MemoryEngine::default());
        let old_timestamp = 999_999_999;

        queue
            .publish(QueueEvent::new(
                "entry:old-due",
                payload("old-due"),
                old_timestamp,
            ))
            .await
            .unwrap();

        let jobs = queue.pull::<Payload>(0, None, 10).await.unwrap().events;

        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.key, "entry:old-due");
        assert_eq!(jobs[0].1.timestamp, old_timestamp);
    }

    #[tokio::test]
    async fn scan_by_state_pages_completed_queue_rows() {
        let queue = queue(MemoryEngine::default());

        for name in ["one", "two", "three"] {
            queue
                .publish(QueueEvent::new(
                    format!("entry:{name}"),
                    payload(name),
                    Utc::now().timestamp(),
                ))
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
                .publish(QueueEvent::new(
                    format!("entry:{name}"),
                    payload(name),
                    Utc::now().timestamp(),
                ))
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

    #[tokio::test]
    async fn delete_matching_in_tx_is_bounded_selective_and_owner_aware() {
        let engine = MemoryEngine::default();
        let queue = queue(engine.clone());
        let now = Utc::now().timestamp();
        let target = QueueOwner::new("store", "target").unwrap();
        let foreign = QueueOwner::new("store", "foreign").unwrap();
        for index in 0..5 {
            let owner = if index < 3 {
                target.clone()
            } else {
                foreign.clone()
            };
            queue
                .publish(
                    QueueEvent::new(
                        format!("entry:owned-{index}"),
                        payload(&format!("owned-{index}")),
                        now,
                    )
                    .with_owner(owner),
                )
                .await
                .unwrap();
        }

        let mut cursor = None;
        let mut deleted = Vec::new();
        loop {
            let queue_for_tx = queue.clone();
            let target_for_tx = target.clone();
            let page = engine
                .transaction(move |tx| {
                    let cursor = cursor.clone();
                    let target = target_for_tx.clone();
                    let queue = queue_for_tx.clone();
                    async move {
                        queue
                            .delete_matching_in_tx::<Payload, _>(
                                QueueEventState::Pending,
                                cursor,
                                2,
                                &tx,
                                |event| Ok(event.owner.as_ref() == Some(&target)),
                            )
                            .await
                    }
                })
                .await
                .unwrap();
            deleted.extend(page.deleted_event_ids);
            let Some(next) = page.next_cursor else { break };
            cursor = Some(next);
        }

        assert_eq!(deleted.len(), 3);
        let remaining = queue
            .scan_by_state::<Payload>(QueueEventState::Pending, None, 10)
            .await
            .unwrap();
        assert_eq!(remaining.events.len(), 2);
        assert!(remaining
            .events
            .iter()
            .all(|(_, event)| event.owner.as_ref() == Some(&foreign)));
    }
}
