use serde::{Deserialize, Serialize};
use std::ops::Range;

use super::traits::EventType;

/// Generic queue event that wraps any EventType
#[derive(Serialize, Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct QueueEvent<T> {
    /// Unique event ID
    pub id: String,
    /// Storage key in Tito (set by queue on publish)
    #[serde(default)]
    pub key: String,
    /// Entity reference (e.g., "order:123")
    pub entity_id: String,
    /// The typed event payload
    pub payload: T,
    /// Creation timestamp
    pub created_at: i64,
    /// When to process (for scheduling/retry)
    pub scheduled_at: i64,
    /// Who triggered this event
    pub actor: Option<String>,
    /// Current retry count
    pub retry_count: u32,
    /// Maximum retries allowed (0 = no retries)
    pub max_retries: u32,
    /// Last error message if failed
    pub error: Option<String>,
}

impl<T: EventType> QueueEvent<T> {
    /// Create a new queue event
    pub fn new(entity_id: impl Into<String>, payload: T) -> Self {
        let now = chrono::Utc::now().timestamp();
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            key: String::new(),
            entity_id: entity_id.into(),
            payload,
            created_at: now,
            scheduled_at: now,
            actor: None,
            retry_count: 0,
            max_retries: 0,
            error: None,
        }
    }

    /// Set the actor
    pub fn with_actor(mut self, actor: impl Into<String>) -> Self {
        self.actor = Some(actor.into());
        self
    }

    /// Set scheduled time
    pub fn scheduled_for(mut self, timestamp: i64) -> Self {
        self.scheduled_at = timestamp;
        self
    }

    /// Set max retries
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }

    /// Get entity type from entity_id (e.g., "order" from "order:123")
    pub fn entity_type(&self) -> &str {
        self.entity_id.split(':').next().unwrap_or(&self.entity_id)
    }

    /// Get entity value from entity_id (e.g., "123" from "order:123")
    pub fn entity_value(&self) -> &str {
        self.entity_id.split(':').nth(1).unwrap_or(&self.entity_id)
    }

    /// Backward compatibility: alias for entity_id
    /// Returns the entity reference (e.g., "order:123")
    pub fn entity(&self) -> &str {
        &self.entity_id
    }

    /// Backward compatibility: access the payload as `event`
    pub fn event(&self) -> &T {
        &self.payload
    }
}

/// Configuration for the queue
#[derive(Debug, Clone)]
pub struct QueueConfig {
    /// Number of partitions for the queue
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

/// Configuration for running a worker
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Event type to process (e.g., "events")
    pub event_type: String,
    /// Consumer name
    pub consumer: String,
    /// Partition range to process
    pub partition_range: Range<u32>,
}

/// Retry policy configuration
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    /// Maximum number of retries
    pub max_retries: u32,
    /// Base backoff in seconds (exponential: base^retry_count)
    pub backoff_base: u32,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_retries: 3,
            backoff_base: 2,
        }
    }
}

impl RetryPolicy {
    pub fn no_retry() -> Self {
        Self {
            max_retries: 0,
            backoff_base: 2,
        }
    }

    pub fn with_retries(max_retries: u32) -> Self {
        Self {
            max_retries,
            backoff_base: 2,
        }
    }

    /// Calculate backoff duration for a given retry count
    pub fn backoff_seconds(&self, retry_count: u32) -> i64 {
        (self.backoff_base as i64).pow(retry_count)
    }
}
