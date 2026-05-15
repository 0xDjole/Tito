use serde::{Deserialize, Serialize};

use crate::types::{
    TitoEmbeddedRelationshipConfig, TitoIndexBlockType, TitoIndexConfig, TitoIndexField,
    TitoModelTrait,
};

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

impl TitoModelTrait for TitoEvent {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn table() -> String {
        "event".to_string()
    }

    fn key_prefix() -> String {
        "event".to_string()
    }

    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            TitoIndexConfig {
                condition: true,
                name: "event-by-name".to_string(),
                fields: vec![TitoIndexField {
                    name: "name".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
            TitoIndexConfig {
                condition: !self.status.is_empty(),
                name: "event-by-status".to_string(),
                fields: vec![TitoIndexField {
                    name: "status".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
        ]
    }

    fn relationships() -> Vec<TitoEmbeddedRelationshipConfig> {
        vec![]
    }
}
