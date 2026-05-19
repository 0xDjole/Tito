use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct TitoEvent {
    pub id: String,
    pub name: String,
    pub payload: String,
    pub created_at: i64,
}
