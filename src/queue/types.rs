use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum QueueEventState {
    Pending,
    Processing,
    Completed,
    DeadLetter,
}

impl Default for QueueEventState {
    fn default() -> Self {
        Self::Pending
    }
}

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueueEvent<T> {
    pub id: String,
    pub key: String,
    pub payload: T,
    pub timestamp: i64,
    #[serde(default)]
    pub state: QueueEventState,
    #[serde(default)]
    pub created_at: i64,
    #[serde(default)]
    pub processed_at: Option<i64>,
    #[serde(default)]
    pub locked_until: Option<i64>,
    #[serde(default)]
    pub locked_by: Option<String>,
    pub retry_count: u32,
    pub max_retries: u32,
    pub errors: Vec<String>,
}

impl<T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static> QueueEvent<T> {
    pub fn new(key: impl Into<String>, payload: T) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: queue_event_id(),
            key: key.into(),
            payload,
            timestamp: now,
            state: QueueEventState::Pending,
            created_at: now,
            processed_at: None,
            locked_until: None,
            locked_by: None,
            retry_count: 0,
            max_retries: 0,
            errors: Vec::new(),
        }
    }

    pub fn scheduled_for(mut self, timestamp: i64) -> Self {
        self.timestamp = timestamp;
        self
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
