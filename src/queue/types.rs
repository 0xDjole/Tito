use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueEventState {
    #[default]
    Pending,
    Completed,
}

/// The durable action Tito takes after a queue handler finishes successfully.
///
/// Returning an error performs no queue mutation, so the current pending
/// invocation remains eligible for redelivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueHandlerOutcome {
    /// Complete the current invocation.
    Acknowledge,
    /// Atomically replace the current pending invocation with a successor
    /// pending invocation at the exact Unix timestamp.
    ///
    /// The successor preserves the logical queue event ID. Its exact pending
    /// storage key identifies the new scheduled invocation.
    ScheduleAt(i64),
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct QueueEvent<T> {
    pub id: String,
    pub key: String,
    pub payload: T,
    pub timestamp: i64,
    #[serde(default)]
    pub(crate) original_scheduled_at: Option<i64>,
    #[serde(default)]
    pub state: QueueEventState,
    #[serde(default)]
    pub processed_at: Option<i64>,
}

#[derive(Debug, Clone)]
pub struct QueueScanPage<T> {
    pub events: Vec<(String, QueueEvent<T>)>,
    pub next_cursor: Option<Vec<u8>>,
}

/// An opaque, in-memory cursor for one fair pass over a partition's due rows.
///
/// The cursor is deliberately not serializable or durable queue state. Pass it
/// back to [`Queue::pull`](super::Queue::pull) to continue the same bounded
/// pass; pass `None` to begin a new pass from the oldest due storage key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueuePullCursor {
    pub(crate) next_start: Vec<u8>,
    pub(crate) cycle_end: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct QueuePullPage<T> {
    pub events: Vec<(String, QueueEvent<T>)>,
    pub next_cursor: Option<QueuePullCursor>,
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> QueueEvent<T> {
    pub fn new(key: impl Into<String>, payload: T) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: queue_event_id(),
            key: key.into(),
            payload,
            timestamp: now,
            original_scheduled_at: Some(now),
            state: QueueEventState::Pending,
            processed_at: None,
        }
    }

    pub fn scheduled_for(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self.original_scheduled_at = Some(timestamp);
        self
    }

    pub fn original_scheduled_at(&self) -> i64 {
        self.original_scheduled_at.unwrap_or(self.timestamp)
    }

    pub fn key_type(&self) -> &str {
        self.key.split(':').next().unwrap_or(&self.key)
    }

    pub fn key_value(&self) -> &str {
        self.key.split(':').nth(1).unwrap_or(&self.key)
    }

    pub fn event(&self) -> &T {
        &self.payload
    }

    pub(crate) fn successor_at(&self, timestamp: i64) -> Self {
        Self {
            id: self.id.clone(),
            key: self.key.clone(),
            payload: self.payload.clone(),
            timestamp,
            original_scheduled_at: Some(self.original_scheduled_at()),
            state: QueueEventState::Pending,
            processed_at: None,
        }
    }
}

fn queue_event_id() -> String {
    let micros = chrono::Utc::now().timestamp_micros();
    format!("{micros:020}-{}", uuid::Uuid::new_v4())
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub partition_count: u32,
}

impl QueueConfig {
    pub fn new(partition_count: u32) -> Self {
        Self { partition_count }
    }
}
