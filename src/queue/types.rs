use serde::{Deserialize, Serialize};

use super::traits::EventType;

#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueueEvent<T> {
    pub id: String,
    #[serde(default)]
    pub key: String,
    pub entity_id: String,
    pub payload: T,
    pub created_at: i64,
    pub scheduled_at: i64,
    pub retry_count: u32,
    pub max_retries: u32,
    pub error: Option<String>,
}

impl<T: EventType> QueueEvent<T> {
    pub fn new(entity_id: impl Into<String>, payload: T) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            key: String::new(),
            entity_id: entity_id.into(),
            payload,
            created_at: now,
            scheduled_at: now,
            retry_count: 0,
            max_retries: 0,
            error: None,
        }
    }

    pub fn scheduled_for(mut self, timestamp: i64) -> Self {
        self.scheduled_at = timestamp;
        self
    }

    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    pub fn entity_type(&self) -> &str {
        self.entity_id.split(':').next().unwrap_or(&self.entity_id)
    }

    pub fn entity_value(&self) -> &str {
        self.entity_id.split(':').nth(1).unwrap_or(&self.entity_id)
    }

    pub fn entity(&self) -> &str {
        &self.entity_id
    }

    pub fn event(&self) -> &T {
        &self.payload
    }
}

#[derive(Debug, Clone)]
pub struct QueueConfig {
    pub partition_count: u32,
}

impl Default for QueueConfig {
    fn default() -> Self {
        Self { partition_count: 1 }
    }
}

impl QueueConfig {
    pub fn with_partitions(partition_count: u32) -> Self {
        Self { partition_count }
    }
}
