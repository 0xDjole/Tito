mod cluster;
mod partition;
mod types;
mod worker;

#[cfg(test)]
mod named_tests;
#[cfg(test)]
pub(crate) mod test_engine;

pub use cluster::{
    run_cluster_worker, ClusterPartitionAssignment, ClusterPartitions, ClusterWorkerConfig,
    ClusterWorkerNode,
};
pub use partition::queue_partition_for_key;
pub use types::{
    QueueConfig, QueueDeletePage, QueueEvent, QueueHandlerOutcome, QueueHandlerResult, QueueOwner,
    QueuePullCursor, QueuePullPage, QueueScanPage, MAX_QUEUE_OWNER_COMPONENT_BYTES,
};

use std::collections::HashMap;
use std::time::Duration;

use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine as _};
use chrono::Utc;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value;

use crate::types::{Sealed, TitoEngine, TitoKvPair, TitoTransaction, PARTITION_DIGITS};
use crate::{encode_index_integer, TitoError};

pub const MAX_QUEUE_EVENT_BYTES: usize = 1024 * 1024;
pub(crate) const QUEUE_SCAN_RPC_LIMIT: u32 = 16;
const KEY_NUMBER_DIGITS: usize = 20;

fn duration_millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_nanos().div_ceil(1_000_000)).unwrap_or(i64::MAX)
}

#[derive(Clone, Copy)]
enum QueueReplacementType {
    Reschedule,
    Advance,
}

impl QueueReplacementType {
    fn action(self) -> &'static str {
        match self {
            Self::Reschedule => "reschedule",
            Self::Advance => "advance",
        }
    }

    fn replacement(self) -> &'static str {
        match self {
            Self::Reschedule => "rescheduled",
            Self::Advance => "advanced",
        }
    }

    fn validate_payload(self, current: &Value, next: &Value) -> Result<(), TitoError> {
        match self {
            Self::Reschedule if current != next => Err(TitoError::InvalidInput(
                "A rescheduled row must preserve the event payload".to_string(),
            )),
            Self::Advance if current == next => Err(TitoError::InvalidInput(
                "An advanced row must change the event payload".to_string(),
            )),
            _ => Ok(()),
        }
    }
}

struct PendingQueueEntry {
    value: Value,
    id: String,
    owner: Option<QueueOwner>,
}

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
        queue_partition_for_key(key, self.config.partition_count)
    }

    fn pending_key(
        &self,
        partition: u32,
        timestamp: i64,
        enqueue_version: u64,
        event_id: &str,
    ) -> String {
        format!(
            "{}:pending:{:0pwidth$}:{}:{:0vwidth$}:{}",
            self.config.name(),
            partition,
            encode_index_integer(timestamp),
            enqueue_version,
            event_id,
            pwidth = PARTITION_DIGITS,
            vwidth = KEY_NUMBER_DIGITS,
        )
    }

    fn pending_partition_start(&self, partition: u32, timestamp: i64) -> Vec<u8> {
        format!(
            "{}:pending:{:0pwidth$}:{}",
            self.config.name(),
            partition,
            encode_index_integer(timestamp),
            pwidth = PARTITION_DIGITS,
        )
        .into_bytes()
    }

    fn pending_key_generation<'a>(
        &self,
        storage_key: &'a [u8],
        partition: u32,
    ) -> Option<(&'a str, u64)> {
        let storage_key = std::str::from_utf8(storage_key).ok()?;
        let prefix = format!(
            "{}:pending:{partition:0width$}:",
            self.config.name(),
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

    fn pending_timestamp_bucket_end(&self, partition: u32, timestamp: &str) -> Vec<u8> {
        let prefix = format!(
            "{}:pending:{partition:0width$}:{timestamp}:",
            self.config.name(),
            width = PARTITION_DIGITS
        );
        Self::prefix_end(&prefix)
    }

    pub(crate) fn pending_prefix(&self) -> String {
        format!("{}:pending:", self.config.name())
    }

    pub(crate) fn owner_prefix(&self, owner: &QueueOwner) -> Result<String, TitoError> {
        owner.validate()?;
        Ok(format!(
            "{}:owner:{}:{}:",
            self.config.name(),
            URL_SAFE_NO_PAD.encode(owner.r#type.as_bytes()),
            URL_SAFE_NO_PAD.encode(owner.id.as_bytes()),
        ))
    }

    pub(crate) fn owner_index_key(
        &self,
        owner: &QueueOwner,
        storage_key: &[u8],
    ) -> Result<String, TitoError> {
        Ok(format!(
            "{}{}",
            self.owner_prefix(owner)?,
            URL_SAFE_NO_PAD.encode(storage_key),
        ))
    }

    fn indexed_storage_key(index_key: &[u8], prefix: &str) -> Result<Vec<u8>, TitoError> {
        let index_key = std::str::from_utf8(index_key).map_err(|_| {
            TitoError::DeserializationFailed("Invalid queue owner index key".to_string())
        })?;
        let encoded = index_key.strip_prefix(prefix).ok_or_else(|| {
            TitoError::DeserializationFailed(
                "Queue owner index escaped its requested prefix".to_string(),
            )
        })?;
        URL_SAFE_NO_PAD.decode(encoded).map_err(|error| {
            TitoError::DeserializationFailed(format!(
                "Invalid queue owner index storage key: {error}"
            ))
        })
    }

    async fn put_owner_index(
        &self,
        tx: &E::Transaction,
        owner: Option<&QueueOwner>,
        storage_key: &[u8],
    ) -> Result<(), TitoError> {
        let Some(owner) = owner else {
            return Ok(());
        };
        let index_key = self.owner_index_key(owner, storage_key)?;
        tx.put(index_key.as_bytes(), Vec::new())
            .await
            .map_err(|error| TitoError::CreateFailed(format!("Create queue owner index: {error}")))
    }

    async fn delete_owner_index(
        &self,
        tx: &E::Transaction,
        owner: Option<&QueueOwner>,
        storage_key: &[u8],
    ) -> Result<(), TitoError> {
        let Some(owner) = owner else {
            return Ok(());
        };
        let index_key = self.owner_index_key(owner, storage_key)?;
        tx.delete(index_key.as_bytes())
            .await
            .map_err(|error| TitoError::DeleteFailed(format!("Delete queue owner index: {error}")))
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

    fn read_pending_entry(value: &[u8]) -> Result<PendingQueueEntry, TitoError> {
        let event = Self::read_event_from_value::<Value>(value)?;
        if let Some(owner) = &event.owner {
            owner.validate()?;
        }
        let value = serde_json::to_value(&event)
            .map_err(|error| TitoError::SerializationFailed(error.to_string()))?;

        Ok(PendingQueueEntry {
            value,
            id: event.id,
            owner: event.owner,
        })
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

    async fn delete_pending_entry(
        &self,
        tx: &E::Transaction,
        storage_key: &[u8],
        owner: Option<&QueueOwner>,
    ) -> Result<(), TitoError> {
        Self::delete_entry(tx, storage_key).await?;
        self.delete_owner_index(tx, owner, storage_key).await
    }

    pub async fn publish_in_tx<T: Serialize + Clone + Send + Sync + 'static>(
        &self,
        event: QueueEvent<T>,
        tx: &E::Transaction,
    ) -> Result<(), TitoError> {
        if let Some(owner) = event.owner.as_ref() {
            owner.validate()?;
        }
        let partition = self.partition_for_key(&event.key);
        let pending_key =
            self.pending_key(partition, event.timestamp, tx.start_version(), &event.id);

        let bytes = Self::serialize_new_event(&event)?;

        tx.put(pending_key.as_bytes(), bytes)
            .await
            .map_err(|e| TitoError::CreateFailed(e.to_string()))?;
        self.put_owner_index(tx, event.owner.as_ref(), pending_key.as_bytes())
            .await?;

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
        let prefix = format!(
            "{}:pending:{partition:0width$}:",
            self.config.name(),
            width = PARTITION_DIGITS
        );
        if cursor.as_ref().is_some_and(|cursor| {
            !cursor.next_start.starts_with(prefix.as_bytes())
                || !cursor.cycle_end.starts_with(prefix.as_bytes())
        }) {
            return Err(TitoError::InvalidInput(
                "Queue cursor belongs to another queue or partition".into(),
            ));
        }
        let limit = limit.max(1);
        self.engine
            .transaction(|tx| {
                let cursor = cursor.clone();
                async move {
                let now = Utc::now().timestamp_millis();
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
                            self.pending_partition_start(partition, i64::MIN),
                            self.pending_timestamp_bucket_end(partition, &encode_index_integer(now)),
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
                        self.pending_key_generation(&storage_key, partition)
                    {
                        if enqueue_version >= enqueue_horizon {
                            next_start = self.pending_timestamp_bucket_end(partition, timestamp);
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

                    if event.timestamp <= now {
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
        if !key.starts_with(&self.pending_prefix()) {
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

                    let current = Self::read_pending_entry(&bytes)?;
                    self.delete_pending_entry(&tx, key.as_bytes(), current.owner.as_ref())
                        .await
                }
            })
            .await
    }

    pub async fn ack_in_tx<T>(
        &self,
        event: &QueueEvent<T>,
        tx: &E::Transaction,
    ) -> Result<bool, TitoError> {
        let storage_key = event.pending_key.as_deref().ok_or_else(|| {
            TitoError::InvalidInput(
                "Only an event delivered from its pending row can be acknowledged in a transaction"
                    .to_string(),
            )
        })?;
        if !storage_key.starts_with(&self.pending_prefix()) {
            return Err(TitoError::InvalidInput(
                "Only pending queue events can be acknowledged".to_string(),
            ));
        }

        let Some(bytes) = tx
            .get(storage_key.as_bytes())
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get queue event: {}", e)))?
        else {
            return Ok(false);
        };
        let current = Self::read_pending_entry(&bytes)?;
        if current.id != event.id {
            return Err(TitoError::InvalidInput(
                "A pending queue row can only be acknowledged by its own event".to_string(),
            ));
        }

        self.delete_pending_entry(tx, storage_key.as_bytes(), current.owner.as_ref())
            .await?;
        Ok(true)
    }

    pub async fn reschedule<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        storage_key: &str,
        next: QueueEvent<T>,
    ) -> Result<(), TitoError> {
        self.replace_pending(storage_key, next, QueueReplacementType::Reschedule)
            .await
    }

    pub async fn advance<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        storage_key: &str,
        next: QueueEvent<T>,
    ) -> Result<(), TitoError> {
        self.replace_pending(storage_key, next, QueueReplacementType::Advance)
            .await
    }

    async fn replace_pending<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        storage_key: &str,
        next: QueueEvent<T>,
        replacement_type: QueueReplacementType,
    ) -> Result<(), TitoError> {
        if let Some(owner) = next.owner.as_ref() {
            owner.validate()?;
        }
        if !storage_key.starts_with(&self.pending_prefix()) {
            return Err(TitoError::InvalidInput(format!(
                "Only pending queue events can be {}",
                replacement_type.replacement()
            )));
        }

        let storage_key = storage_key.to_string();

        self.engine
            .transaction(|tx| {
                let storage_key = storage_key.clone();
                let next = next.clone();
                async move {
                    let Some(bytes) = tx.get(storage_key.as_bytes()).await.map_err(|error| {
                        TitoError::QueryFailed(format!(
                            "Get queue event to {}: {error}",
                            replacement_type.action()
                        ))
                    })?
                    else {
                        return Ok::<_, TitoError>(());
                    };

                    let current = Self::read_pending_entry(&bytes)?;

                    if current.id != next.id {
                        return Err(TitoError::InvalidInput(format!(
                            "A {} row must preserve the logical event id",
                            replacement_type.replacement()
                        )));
                    }
                    let current_key = current
                        .value
                        .get("key")
                        .and_then(Value::as_str)
                        .ok_or_else(|| {
                            TitoError::DeserializationFailed(
                                "Queue event missing partition key".to_string(),
                            )
                        })?;
                    if current_key != next.key {
                        return Err(TitoError::InvalidInput(format!(
                            "A {} row must preserve the event partition key",
                            replacement_type.replacement()
                        )));
                    }
                    if current.owner.as_ref() != next.owner.as_ref() {
                        return Err(TitoError::InvalidInput(format!(
                            "A {} row must preserve the event owner",
                            replacement_type.replacement()
                        )));
                    }
                    let next_payload = serde_json::to_value(&next.payload)
                        .map_err(|error| TitoError::SerializationFailed(error.to_string()))?;
                    let current_payload = current.value.get("payload").ok_or_else(|| {
                        TitoError::DeserializationFailed("Queue event missing payload".to_string())
                    })?;
                    replacement_type.validate_payload(current_payload, &next_payload)?;

                    let partition = self.partition_for_key(&next.key);
                    let pending_key =
                        self.pending_key(partition, next.timestamp, tx.start_version(), &next.id);
                    let pending_bytes = Self::serialize_new_event(&next)?;

                    self.delete_pending_entry(&tx, storage_key.as_bytes(), current.owner.as_ref())
                        .await?;
                    tx.put(pending_key.as_bytes(), pending_bytes)
                        .await
                        .map_err(|error| TitoError::CreateFailed(error.to_string()))?;
                    self.put_owner_index(&tx, next.owner.as_ref(), pending_key.as_bytes())
                        .await?;
                    Ok(())
                }
            })
            .await
    }

    pub async fn clear(&self) -> Result<(), TitoError> {
        for prefix in [
            self.pending_prefix(),
            format!("{}:owner:", self.config.name()),
        ] {
            let prefix = prefix.as_str();
            loop {
                let deleted = self
                    .engine
                    .transaction(|tx| async move {
                        let entries = Self::scan_queue_entries(
                            &tx,
                            prefix.as_bytes().to_vec(),
                            Self::prefix_end(&prefix),
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

    pub async fn scan_by_status<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        cursor: Option<Vec<u8>>,
        limit: u32,
    ) -> Result<QueueScanPage<T>, TitoError> {
        let prefix = self.pending_prefix();
        Self::validate_scan_cursor(&prefix, cursor.as_deref())?;
        let limit = limit.max(1);

        self.engine
            .transaction(|tx| {
                let cursor = cursor.clone();
                async move {
                    let start = cursor.unwrap_or_else(|| prefix.as_bytes().to_vec());
                    let entries =
                        Self::scan_queue_entries(&tx, start, Self::prefix_end(&prefix), limit)
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
                        let key = String::from_utf8(storage_key).map_err(|_| {
                            TitoError::DeserializationFailed("Invalid queue key".to_string())
                        })?;
                        events.push((key, event));
                    }

                    Ok::<_, TitoError>(QueueScanPage {
                        events,
                        next_cursor,
                    })
                }
            })
            .await
    }

    pub async fn scan_by_owner<T: DeserializeOwned + Clone + Send + Sync + 'static>(
        &self,
        owner: &QueueOwner,
        cursor: Option<Vec<u8>>,
        limit: u32,
    ) -> Result<QueueScanPage<T>, TitoError> {
        let prefix = self.owner_prefix(owner)?;
        if cursor
            .as_ref()
            .is_some_and(|value| !value.starts_with(prefix.as_bytes()))
        {
            return Err(TitoError::InvalidInput(
                "Queue cursor belongs to another owner".into(),
            ));
        }
        let limit = limit.max(1);
        self.engine
            .transaction(|tx| {
                let start = cursor.clone().unwrap_or_else(|| prefix.as_bytes().to_vec());
                let prefix = prefix.clone();
                async move {
                    let entries =
                        Self::scan_queue_entries(&tx, start, Self::prefix_end(&prefix), limit)
                            .await?;
                    let next_cursor = if entries.len() == limit as usize {
                        entries.last().map(|(key, _)| {
                            let mut cursor = key.clone();
                            cursor.push(0);
                            cursor
                        })
                    } else {
                        None
                    };
                    let mut storage_keys = Vec::with_capacity(entries.len());
                    for (index_key, _) in &entries {
                        let storage_key =
                            Self::indexed_storage_key(index_key, &prefix).and_then(|storage_key| {
                                if storage_key.starts_with(self.pending_prefix().as_bytes()) {
                                    Ok(storage_key)
                                } else {
                                    Err(TitoError::DeserializationFailed(
                                        "Queue owner index points outside its queue".into(),
                                    ))
                                }
                            });
                        let valid = storage_key.is_ok();
                        storage_keys.push(storage_key);
                        if !valid {
                            break;
                        }
                    }
                    let readable = storage_keys
                        .iter()
                        .filter_map(|storage_key| storage_key.as_ref().ok())
                        .collect::<Vec<_>>();
                    let found: HashMap<_, _> = if readable.is_empty() {
                        HashMap::new()
                    } else {
                        tx.get_keys(readable, Sealed).await?.into_iter().collect()
                    };
                    let mut events = Vec::with_capacity(storage_keys.len());
                    for storage_key in storage_keys {
                        let storage_key = storage_key?;
                        let value = found.get(&storage_key).ok_or_else(|| {
                            TitoError::DeserializationFailed(
                                "Queue owner index points to a missing event".into(),
                            )
                        })?;
                        let event = Self::read_event_from_value::<T>(value)?;
                        if event.owner.as_ref() != Some(owner) {
                            return Err(TitoError::DeserializationFailed(
                                "Queue owner index conflicts with its event".into(),
                            ));
                        }
                        let key = String::from_utf8(storage_key).map_err(|_| {
                            TitoError::DeserializationFailed("Invalid queue key".into())
                        })?;
                        events.push((key, event));
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
        cursor: Option<Vec<u8>>,
        limit: u32,
        tx: &E::Transaction,
        matches: F,
    ) -> Result<QueueDeletePage, TitoError>
    where
        T: DeserializeOwned + Clone + Send + Sync + 'static,
        F: Fn(&QueueEvent<T>) -> Result<bool, TitoError> + Send + Sync,
    {
        let prefix = self.pending_prefix();
        Self::validate_scan_cursor(&prefix, cursor.as_deref())?;
        let limit = limit.max(1);
        let start = cursor.unwrap_or_else(|| prefix.as_bytes().to_vec());
        let entries = Self::scan_queue_entries(tx, start, Self::prefix_end(&prefix), limit)
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
            if matches(&event)? {
                self.delete_pending_entry(tx, &storage_key, event.owner.as_ref())
                    .await?;
                deleted_event_ids.push(event.id);
            }
        }
        deleted_event_ids.sort();
        deleted_event_ids.dedup();
        Ok(QueueDeletePage {
            deleted_event_ids,
            next_cursor,
        })
    }

    fn validate_scan_cursor(prefix: &str, cursor: Option<&[u8]>) -> Result<(), TitoError> {
        if cursor.is_some_and(|value| !value.starts_with(prefix.as_bytes())) {
            return Err(TitoError::InvalidInput(
                "Queue cursor belongs to another queue".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::test_engine::{
        get_raw, keys_with_prefix, native_engine, put_raw, unique_queue_name,
    };
    use super::*;
    use crate::backend::tikv::TiKVBackend;
    use serde::{Deserialize, Serialize};
    use std::collections::{BTreeSet, HashSet};

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
    struct Payload {
        name: String,
    }

    async fn test_queue() -> Queue<TiKVBackend> {
        Queue::new(
            native_engine().await,
            QueueConfig::new(1)
                .with_name(unique_queue_name("queue-test"))
                .unwrap(),
        )
    }

    fn payload(name: &str) -> Payload {
        Payload {
            name: name.to_string(),
        }
    }

    fn queue_prefix(queue: &Queue<TiKVBackend>) -> String {
        format!("{}:", queue.config.name())
    }

    #[tokio::test]
    async fn publish_and_pull_keeps_pending_rows_until_ack() {
        let queue = test_queue().await;
        let now = chrono::Utc::now().timestamp_millis();

        queue
            .publish(QueueEvent::new("entry:due", payload("due"), now - 10_000))
            .await
            .unwrap();
        queue
            .publish(QueueEvent::new(
                "entry:future",
                payload("future"),
                now + 3_600_000,
            ))
            .await
            .unwrap();

        let jobs = queue.pull::<Payload>(0, None, 10).await.unwrap().events;
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].1.key, "entry:due");

        let pending_before_ack = queue.scan_by_status::<Payload>(None, 10).await.unwrap();
        assert_eq!(pending_before_ack.events.len(), 2);

        queue.ack(&jobs[0].0).await.unwrap();

        let pending_after_ack = queue.scan_by_status::<Payload>(None, 10).await.unwrap();
        assert_eq!(pending_after_ack.events.len(), 1);
        assert_eq!(pending_after_ack.events[0].1.key, "entry:future");
        assert_eq!(
            keys_with_prefix(&queue.engine, &queue_prefix(&queue)).await,
            vec![pending_after_ack.events[0].0.clone()]
        );
        let stored: Value = serde_json::from_slice(
            &get_raw(&queue.engine, &pending_after_ack.events[0].0)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(stored.get("status").is_none());
        assert!(stored.get("processedAt").is_none());
    }

    #[tokio::test]
    async fn acknowledged_rescheduled_and_advanced_rows_leave_no_finished_row() {
        let queue = test_queue().await;
        let owner = QueueOwner::new("store", "finished").unwrap();
        let now = Utc::now().timestamp_millis();
        for name in ["ack", "reschedule", "advance"] {
            queue
                .publish(
                    QueueEvent::new(format!("entry:{name}"), payload(name), now - 1_000)
                        .with_owner(owner.clone()),
                )
                .await
                .unwrap();
        }

        let jobs = queue.pull::<Payload>(0, None, 10).await.unwrap().events;
        assert_eq!(jobs.len(), 3);
        for (storage_key, event) in &jobs {
            match event.payload.name.as_str() {
                "ack" => queue.ack(storage_key).await.unwrap(),
                "reschedule" => queue
                    .reschedule(storage_key, event.rescheduled(now + 60_000))
                    .await
                    .unwrap(),
                _ => {
                    let mut next = event.rescheduled(now + 60_000);
                    next.payload = payload("advanced");
                    queue.advance(storage_key, next).await.unwrap();
                }
            }
        }

        let remaining = queue
            .scan_by_owner::<Payload>(&owner, None, 10)
            .await
            .unwrap();
        assert_eq!(
            remaining
                .events
                .iter()
                .map(|(_, event)| event.payload.name.clone())
                .collect::<BTreeSet<_>>(),
            BTreeSet::from(["advanced".to_string(), "reschedule".to_string()])
        );
        let mut expected = remaining
            .events
            .iter()
            .map(|(storage_key, _)| storage_key.clone())
            .collect::<Vec<_>>();
        for (storage_key, _) in &remaining.events {
            expected.push(
                queue
                    .owner_index_key(&owner, storage_key.as_bytes())
                    .unwrap(),
            );
        }
        expected.sort();
        assert_eq!(
            keys_with_prefix(&queue.engine, &queue_prefix(&queue)).await,
            expected
        );
    }

    #[tokio::test]
    async fn publish_rejects_an_invocation_larger_than_the_transport_safe_limit() {
        let queue = test_queue().await;
        let oversized = Payload {
            name: "x".repeat(MAX_QUEUE_EVENT_BYTES),
        };

        let error = queue
            .publish(QueueEvent::new(
                "entry:oversized",
                oversized,
                Utc::now().timestamp_millis(),
            ))
            .await
            .unwrap_err();

        assert!(matches!(error, TitoError::InvalidInput(message)
            if message.contains(&MAX_QUEUE_EVENT_BYTES.to_string())));
        assert!(keys_with_prefix(&queue.engine, &queue.pending_prefix())
            .await
            .is_empty());
    }

    #[tokio::test]
    async fn queue_reads_fulfill_logical_limits_through_bounded_rpc_pages() {
        let queue = test_queue().await;
        let now = Utc::now().timestamp_millis();

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
        let rest = queue
            .pull::<Payload>(0, page.next_cursor, 17)
            .await
            .unwrap();
        assert_eq!(rest.events.len(), 1);

        let scanned = queue.scan_by_status::<Payload>(None, 17).await.unwrap();
        assert_eq!(scanned.events.len(), 17);
        assert!(scanned.next_cursor.is_some());
    }

    #[tokio::test]
    async fn pull_reaches_event_timestamps_with_fewer_decimal_digits_than_now() {
        let queue = test_queue().await;
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
    async fn scan_by_status_pages_pending_queue_rows() {
        let queue = test_queue().await;

        for name in ["one", "two", "three"] {
            queue
                .publish(QueueEvent::new(
                    format!("entry:{name}"),
                    payload(name),
                    Utc::now().timestamp_millis(),
                ))
                .await
                .unwrap();
        }

        let first = queue.scan_by_status::<Payload>(None, 2).await.unwrap();
        assert_eq!(first.events.len(), 2);
        assert!(first.next_cursor.is_some());

        let second = queue
            .scan_by_status::<Payload>(first.next_cursor, 2)
            .await
            .unwrap();
        assert_eq!(second.events.len(), 1);
        assert!(second.next_cursor.is_none());
    }

    #[tokio::test]
    async fn scan_by_status_surfaces_and_preserves_malformed_rows() {
        let queue = test_queue().await;
        queue
            .publish(QueueEvent::new("entry:valid", payload("valid"), 1))
            .await
            .unwrap();
        let valid_key = queue
            .scan_by_status::<Payload>(None, 10)
            .await
            .unwrap()
            .events[0]
            .0
            .clone();
        let orphan_key = queue.pending_key(0, 9, 0, "orphan");
        put_raw(&queue.engine, &orphan_key, b"not-json".to_vec()).await;

        let error = queue.scan_by_status::<Payload>(None, 10).await.unwrap_err();

        assert!(matches!(error, TitoError::DeserializationFailed(_)));
        assert_eq!(
            get_raw(&queue.engine, &orphan_key).await,
            Some(b"not-json".to_vec())
        );
        assert!(get_raw(&queue.engine, &valid_key).await.is_some());
    }

    #[tokio::test]
    async fn delete_matching_in_tx_is_bounded_selective_and_owner_aware() {
        let queue = test_queue().await;
        let engine = queue.engine.clone();
        let now = Utc::now().timestamp_millis();
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
                            .delete_matching_in_tx::<Payload, _>(cursor, 2, &tx, |event| {
                                Ok(event.owner.as_ref() == Some(&target))
                            })
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
        let remaining = queue.scan_by_status::<Payload>(None, 10).await.unwrap();
        assert_eq!(remaining.events.len(), 2);
        assert!(remaining
            .events
            .iter()
            .all(|(_, event)| event.owner.as_ref() == Some(&foreign)));
        assert!(
            keys_with_prefix(&engine, &queue.owner_prefix(&target).unwrap())
                .await
                .is_empty()
        );
    }

    #[tokio::test]
    async fn owner_reader_pages_only_its_index_and_preserves_queue_transitions() {
        let queue = test_queue().await;
        let owner = QueueOwner::new("store", "a:with/slashes").unwrap();
        let foreign = QueueOwner::new("store", "a:with/slashes/other").unwrap();
        for i in 0..35 {
            queue
                .publish(
                    QueueEvent::new(format!("entry:{i}"), payload(&i.to_string()), 0)
                        .with_owner(owner.clone()),
                )
                .await
                .unwrap();
        }
        queue
            .publish(
                QueueEvent::new("entry:foreign", payload("foreign"), 0).with_owner(foreign.clone()),
            )
            .await
            .unwrap();
        let foreign_key = queue
            .scan_by_owner::<Payload>(&foreign, None, 1)
            .await
            .unwrap()
            .events[0]
            .0
            .clone();
        put_raw(
            &queue.engine,
            &foreign_key,
            b"invalid unrelated payload".to_vec(),
        )
        .await;
        let mut cursor = None;
        let mut rows = Vec::new();
        loop {
            let page = queue
                .scan_by_owner::<Payload>(&owner, cursor.clone(), 17)
                .await
                .unwrap();
            assert!(page.events.len() <= 17);
            if let Some(next) = &page.next_cursor {
                assert!(cursor.as_ref().is_none_or(|previous| next > previous));
            }
            rows.extend(page.events);
            cursor = page.next_cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(rows.len(), 35);
        assert_eq!(
            rows.iter()
                .map(|(_, event)| &event.id)
                .collect::<HashSet<_>>()
                .len(),
            35
        );
        queue.ack(&rows[0].0).await.unwrap();
        queue
            .reschedule(&rows[1].0, rows[1].1.rescheduled(60_000))
            .await
            .unwrap();
        let pending = queue
            .scan_by_owner::<Payload>(&owner, None, 100)
            .await
            .unwrap();
        assert_eq!(pending.events.len(), 34);
        assert!(pending
            .events
            .iter()
            .any(|(_, event)| event.id == rows[1].1.id && event.timestamp == 60_000));
        for (storage_key, _) in &rows[..2] {
            assert!(get_raw(&queue.engine, storage_key).await.is_none());
            assert!(get_raw(
                &queue.engine,
                &queue
                    .owner_index_key(&owner, storage_key.as_bytes())
                    .unwrap()
            )
            .await
            .is_none());
        }
    }

    #[tokio::test]
    async fn owner_reader_rejects_foreign_cursor_and_corrupt_index_without_writes() {
        let queue = test_queue().await;
        let owner = QueueOwner::new("store", "target").unwrap();
        let foreign = QueueOwner::new("store", "foreign").unwrap();
        assert!(matches!(
            queue
                .scan_by_owner::<Payload>(
                    &owner,
                    Some(queue.owner_prefix(&foreign).unwrap().into_bytes()),
                    10
                )
                .await,
            Err(TitoError::InvalidInput(_))
        ));
        queue
            .publish(
                QueueEvent::new("entry:target", payload("target"), 0).with_owner(owner.clone()),
            )
            .await
            .unwrap();
        let row = queue
            .scan_by_owner::<Payload>(&owner, None, 10)
            .await
            .unwrap()
            .events
            .pop()
            .unwrap();
        let mut mismatched = row.1;
        mismatched.owner = Some(foreign);
        let bytes = serde_json::to_vec(&mismatched).unwrap();
        put_raw(&queue.engine, &row.0, bytes.clone()).await;
        assert!(matches!(
            queue.scan_by_owner::<Payload>(&owner, None, 10).await,
            Err(TitoError::DeserializationFailed(_))
        ));
        assert_eq!(get_raw(&queue.engine, &row.0).await, Some(bytes));

        let isolated = test_queue().await;
        let dangling_index = isolated
            .owner_index_key(&owner, isolated.pending_key(0, 0, 0, "missing").as_bytes())
            .unwrap();
        put_raw(&isolated.engine, &dangling_index, Vec::new()).await;
        assert!(matches!(
            isolated.scan_by_owner::<Payload>(&owner, None, 10).await,
            Err(TitoError::DeserializationFailed(_))
        ));
        assert!(get_raw(&isolated.engine, &dangling_index).await.is_some());
    }

    #[tokio::test]
    async fn owner_reader_returns_every_event_its_index_names() {
        let queue = test_queue().await;
        let owner = QueueOwner::new("store", "target").unwrap();
        for i in 0..3 {
            queue
                .publish(
                    QueueEvent::new(format!("entry:{i}"), payload(&i.to_string()), 0)
                        .with_owner(owner.clone()),
                )
                .await
                .unwrap();
        }

        let page = queue
            .scan_by_owner::<Payload>(&owner, None, 10)
            .await
            .unwrap();
        assert_eq!(
            page.events
                .iter()
                .map(|(_, event)| event.payload.name.clone())
                .collect::<HashSet<_>>()
                .len(),
            3
        );
    }

    #[tokio::test]
    async fn owner_reader_reports_the_first_corrupt_entry_in_index_order() {
        let queue = test_queue().await;
        let owner = QueueOwner::new("store", "target").unwrap();
        let missing = queue
            .owner_index_key(&owner, queue.pending_key(0, 0, 0, "missing").as_bytes())
            .unwrap();
        let outside = queue
            .owner_index_key(
                &owner,
                format!("{}:other:outside", queue.config.name()).as_bytes(),
            )
            .unwrap();
        put_raw(&queue.engine, &missing, Vec::new()).await;
        put_raw(&queue.engine, &outside, Vec::new()).await;
        let first = if missing < outside {
            "Queue owner index points to a missing event"
        } else {
            "Queue owner index points outside its queue"
        };

        assert_eq!(
            queue
                .scan_by_owner::<Payload>(&owner, None, 10)
                .await
                .unwrap_err(),
            TitoError::DeserializationFailed(first.to_string())
        );
        assert!(get_raw(&queue.engine, &missing).await.is_some());
        assert!(get_raw(&queue.engine, &outside).await.is_some());
    }
}
