use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::time::Duration;

use crate::TitoError;

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueEventState {
    #[default]
    Pending,
    Completed,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QueueHandlerOutcome<T> {
    Acknowledge,
    Reschedule(QueueEvent<T>),
}

/// The result of handling one queue event.
///
/// An error leaves the current pending event unchanged for later redelivery.
pub type QueueHandlerResult<T> = Result<QueueHandlerOutcome<T>, TitoError>;

#[derive(Serialize, Deserialize, Debug, Clone, PartialEq, Eq)]
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
    pub fn new(key: impl Into<String>, payload: T, timestamp: i64) -> Self {
        Self {
            id: queue_event_id(),
            key: key.into(),
            payload,
            timestamp,
            state: QueueEventState::Pending,
            processed_at: None,
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

    pub fn created_at_millis(&self) -> i64 {
        self.id
            .split_once('-')
            .and_then(|(micros, _)| micros.parse::<i64>().ok())
            .map(|micros| micros / 1_000)
            .unwrap_or_else(|| self.timestamp.saturating_mul(1_000))
    }

    pub fn rescheduled(&self, timestamp: i64) -> Self {
        Self {
            id: self.id.clone(),
            key: self.key.clone(),
            payload: self.payload.clone(),
            timestamp,
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
    pub completed_retention: Duration,
}

impl QueueConfig {
    pub fn new(partition_count: u32, completed_retention: Duration) -> Self {
        Self {
            partition_count,
            completed_retention,
        }
    }
}
