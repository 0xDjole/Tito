use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq, Default)]
#[serde(rename_all = "snake_case")]
pub enum QueueEventState {
    #[serde(alias = "processing")]
    #[default]
    Pending,
    Completed,
    #[serde(alias = "dead_letter")]
    Failed,
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
    pub retry_count: u32,
    pub max_retries: u32,
    pub errors: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct QueueScanPage<T> {
    pub events: Vec<(String, QueueEvent<T>)>,
    pub next_cursor: Option<Vec<u8>>,
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
            retry_count: 0,
            max_retries: 0,
            errors: Vec::new(),
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

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
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
