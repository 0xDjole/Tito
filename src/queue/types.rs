use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueEventState {
    #[default]
    Pending,
    Completed,
}

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueCompletionReason {
    #[default]
    Acknowledged,
    ScheduledNext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueueHandlerOutcome {
    Acknowledge,
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
            state: QueueEventState::Pending,
            processed_at: None,
            completion_reason: None,
        }
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
