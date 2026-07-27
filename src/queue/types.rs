use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueEventState {
    #[default]
    Pending,
    Completed,
}

/// Why a queue invocation entered the terminal Completed state.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueCompletionReason {
    /// The handler completed the invocation without scheduling more queue work.
    #[default]
    Acknowledged,
    /// The handler completed the invocation and atomically scheduled a new one.
    ScheduledNext,
}

/// The durable action Tito takes after a queue handler finishes successfully.
///
/// Returning an error performs no queue mutation, so the current pending
/// invocation remains eligible for redelivery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueHandlerOutcome {
    /// Complete the current invocation.
    Acknowledge,
    /// Atomically complete the current invocation and create a new pending
    /// invocation at the exact Unix timestamp.
    ///
    /// The successor receives a new queue event ID while preserving the
    /// current invocation's business key and payload.
    ScheduleNextAt(i64),
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
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) completion_reason: Option<QueueCompletionReason>,
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
    pub(crate) enqueue_horizon: u64,
}

#[derive(Debug, Clone)]
pub struct QueuePullPage<T> {
    pub events: Vec<(String, QueueEvent<T>)>,
    pub next_cursor: Option<QueuePullCursor>,
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> QueueEvent<T> {
    pub fn new(key: impl Into<String>, payload: T, due_at: i64) -> Self {
        Self {
            id: queue_event_id(),
            key: key.into(),
            payload,
            timestamp: due_at,
            original_scheduled_at: Some(due_at),
            state: QueueEventState::Pending,
            processed_at: None,
            completion_reason: None,
        }
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

    /// Returns the terminal reason for a Completed invocation.
    ///
    /// Completed rows written before completion reasons were introduced are
    /// interpreted as acknowledged. Pending rows never have a completion
    /// reason.
    pub fn completion_reason(&self) -> Option<QueueCompletionReason> {
        match self.state {
            QueueEventState::Pending => None,
            QueueEventState::Completed => Some(self.completion_reason.unwrap_or_default()),
        }
    }

    pub(crate) fn successor_at(&self, timestamp: i64) -> Self {
        Self {
            id: queue_event_id(),
            key: self.key.clone(),
            payload: self.payload.clone(),
            timestamp,
            original_scheduled_at: Some(self.original_scheduled_at()),
            state: QueueEventState::Pending,
            processed_at: None,
            completion_reason: None,
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
