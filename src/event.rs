use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TitoEvent {
    pub id: String,
    pub name: String,
    pub payload: String,
    pub created_at: i64,
    pub status: String,
    pub event_key: String,
    pub event_data: String,
    pub scheduled_at: i64,
    pub processed_at: i64,
    pub retry_count: u32,
    pub max_retries: u32,
    pub error: Option<String>,
}
