use crate::key_encoder::safe_encode;
use crate::queue::{
    retry_backoff_seconds, run_worker, Queue, QueueConfig, QueueEvent, QueueEventState,
    WorkerConfig,
};
use crate::test_support::MemoryEngine;
use crate::types::{
    PartitionConfig, TitoCursor, TitoEngine, TitoFindByIndexPayload, TitoFindOneByIndexPayload,
    TitoFindPayload, TitoId, TitoIndexBlockType, TitoIndexConfig, TitoIndexField, TitoModelOptions,
    TitoModelTrait, TitoPaginated, TitoRelationshipConfig, TitoScanPayload,
};
use crate::utils::{next_string_lexicographically, previous_string_lexicographically};
use crate::{ClusterCoordinatorLease, ClusterWorkerConfig, TitoError};
use chrono::Utc;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{broadcast, Notify};
use tokio::time::{sleep, timeout};

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Author {
    id: String,
    name: String,
    email: String,
    age: i64,
    org_id: String,
    role: String,
    #[serde(default)]
    optional: Option<String>,
    #[serde(default)]
    created_at: i64,
    #[serde(default)]
    updated_at: i64,
}

impl TitoModelTrait for Author {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            TitoIndexConfig {
                condition: true,
                name: "author-by-email".to_string(),
                fields: vec![TitoIndexField {
                    name: "email".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
            TitoIndexConfig {
                condition: true,
                name: "author-by-age".to_string(),
                fields: vec![TitoIndexField {
                    name: "age".to_string(),
                    r#type: TitoIndexBlockType::Number,
                }],
            },
            TitoIndexConfig {
                condition: true,
                name: "author-by-org-email".to_string(),
                fields: vec![
                    TitoIndexField {
                        name: "org_id".to_string(),
                        r#type: TitoIndexBlockType::String,
                    },
                    TitoIndexField {
                        name: "email".to_string(),
                        r#type: TitoIndexBlockType::String,
                    },
                ],
            },
            TitoIndexConfig {
                condition: true,
                name: "author-by-kind-org".to_string(),
                fields: vec![
                    TitoIndexField {
                        name: "kind".to_string(),
                        r#type: TitoIndexBlockType::Custom("author".to_string()),
                    },
                    TitoIndexField {
                        name: "org_id".to_string(),
                        r#type: TitoIndexBlockType::String,
                    },
                ],
            },
            TitoIndexConfig {
                condition: true,
                name: "author-by-optional".to_string(),
                fields: vec![TitoIndexField {
                    name: "optional".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
            TitoIndexConfig {
                condition: false,
                name: "author-disabled".to_string(),
                fields: vec![TitoIndexField {
                    name: "role".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
        ]
    }

    fn table() -> String {
        "authors".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Tag {
    id: String,
    name: String,
}

impl TitoModelTrait for Tag {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "tag-by-name".to_string(),
            fields: vec![TitoIndexField {
                name: "name".to_string(),
                r#type: TitoIndexBlockType::String,
            }],
        }]
    }

    fn table() -> String {
        "tags".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Block {
    id: String,
    author_id: String,
    #[serde(default)]
    author: Option<Author>,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Comment {
    author_id: String,
    body: String,
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Post {
    id: String,
    title: String,
    author_id: String,
    tag_ids: Vec<String>,
    comments: Vec<Comment>,
    blocks: Vec<Block>,
    metadata: HashMap<String, String>,
    #[serde(default)]
    author: Option<Author>,
    #[serde(default)]
    tags: Vec<Tag>,
}

impl TitoModelTrait for Post {
    fn relationships() -> Vec<TitoRelationshipConfig> {
        vec![
            TitoRelationshipConfig {
                source_field_name: "author_id".to_string(),
                destination_field_name: "author".to_string(),
                model: "authors".to_string(),
            },
            TitoRelationshipConfig {
                source_field_name: "tag_ids".to_string(),
                destination_field_name: "tags".to_string(),
                model: "tags".to_string(),
            },
            TitoRelationshipConfig {
                source_field_name: "blocks.author_id".to_string(),
                destination_field_name: "blocks.author".to_string(),
                model: "authors".to_string(),
            },
        ]
    }

    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            TitoIndexConfig {
                condition: true,
                name: "post-by-author".to_string(),
                fields: vec![TitoIndexField {
                    name: "author_id".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
            TitoIndexConfig {
                condition: true,
                name: "post-by-tag".to_string(),
                fields: vec![TitoIndexField {
                    name: "tag_ids".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
            TitoIndexConfig {
                condition: true,
                name: "post-by-comment-author".to_string(),
                fields: vec![TitoIndexField {
                    name: "comments.author_id".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
            TitoIndexConfig {
                condition: true,
                name: "post-by-metadata".to_string(),
                fields: vec![TitoIndexField {
                    name: "metadata".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
        ]
    }

    fn table() -> String {
        "posts".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn engine() -> MemoryEngine {
    MemoryEngine::default()
}

fn author(id: &str, email: &str, age: i64, org_id: &str) -> Author {
    Author {
        id: id.to_string(),
        name: format!("Author {id}"),
        email: email.to_string(),
        age,
        org_id: org_id.to_string(),
        role: "writer".to_string(),
        optional: None,
        created_at: 0,
        updated_at: 0,
    }
}

fn tag(id: &str, name: &str) -> Tag {
    Tag {
        id: id.to_string(),
        name: name.to_string(),
    }
}

fn post(id: &str, author_id: &str, tag_ids: Vec<String>) -> Post {
    Post {
        id: id.to_string(),
        title: format!("Post {id}"),
        author_id: author_id.to_string(),
        tag_ids,
        comments: vec![Comment {
            author_id: author_id.to_string(),
            body: "hello".to_string(),
        }],
        blocks: vec![Block {
            id: "block-1".to_string(),
            author_id: author_id.to_string(),
            author: None,
        }],
        metadata: HashMap::from([
            ("locale".to_string(), "en".to_string()),
            ("channel".to_string(), "web".to_string()),
        ]),
        author: None,
        tags: Vec::new(),
    }
}

async fn save_author(engine: &MemoryEngine, item: Author) -> Author {
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .transaction(|tx| {
            let model = model.clone();
            let item = item.clone();
            async move { model.set(item).execute(&tx).await }
        })
        .await
        .unwrap()
}

async fn save_tag(engine: &MemoryEngine, item: Tag) -> Tag {
    let model = engine.clone().model::<Tag>(TitoModelOptions::default());
    engine
        .transaction(|tx| {
            let model = model.clone();
            let item = item.clone();
            async move { model.set(item).execute(&tx).await }
        })
        .await
        .unwrap()
}

async fn save_post(engine: &MemoryEngine, item: Post) -> Post {
    let model = engine.clone().model::<Post>(TitoModelOptions::default());
    engine
        .transaction(|tx| {
            let model = model.clone();
            let item = item.clone();
            async move { model.set(item).execute(&tx).await }
        })
        .await
        .unwrap()
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct QueuePayload {
    name: String,
}

fn queue_payload(name: &str) -> QueuePayload {
    QueuePayload {
        name: name.to_string(),
    }
}

fn queue_event(id: &str, key: &str, timestamp: i64) -> QueueEvent<QueuePayload> {
    QueueEvent {
        id: id.to_string(),
        key: key.to_string(),
        payload: queue_payload(id),
        timestamp,
        original_scheduled_at: Some(timestamp),
        state: QueueEventState::Pending,
        processed_at: None,
        retry_count: 0,
        max_retries: 3,
        errors: Vec::new(),
    }
}

fn queue(engine: MemoryEngine, partitions: u32) -> Queue<MemoryEngine> {
    Queue::new(engine, QueueConfig::new(partitions))
}

fn cluster_config(node_id: &str) -> ClusterWorkerConfig {
    ClusterWorkerConfig {
        node_id: node_id.to_string(),
        heartbeat_interval: Duration::from_millis(10),
        poll_interval: Duration::from_millis(10),
        lease_ttl: Duration::from_secs(60),
        stale_node_ttl: Duration::from_secs(120),
    }
}

mod cluster;
mod index;
mod model;
mod queue;
mod relationship;
