use crate::key_encoder::safe_encode;
use crate::queue::{run_worker, Queue, QueueConfig, QueueEvent, QueueEventState, WorkerConfig};
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

#[tokio::test]
async fn model_set_get_and_get_many_round_trip() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    save_author(&engine, author("a1", "Ada@Example.com", 36, "org-a")).await;
    save_author(&engine, author("a2", "grace@example.com", 42, "org-a")).await;

    assert_eq!(
        model.get("a1").execute(None).await.unwrap().email,
        "Ada@Example.com"
    );
    let many = model
        .get_many(vec!["a2".to_string(), "a1".to_string()])
        .execute(None)
        .await
        .unwrap();
    assert_eq!(
        many.into_iter().map(|item| item.id).collect::<Vec<_>>(),
        vec!["a2", "a1"]
    );
}

#[tokio::test]
async fn model_set_adds_timestamps_by_default() {
    let engine = engine();

    let saved = save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    assert!(saved.created_at > 0);
    assert!(saved.updated_at >= saved.created_at);
}

#[tokio::test]
async fn model_set_can_skip_timestamps() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    let saved = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "ada@example.com", 36, "org-a"))
                    .timestamps(false)
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap();

    assert_eq!(saved.created_at, 0);
    assert_eq!(saved.updated_at, 0);
}

#[tokio::test]
async fn transaction_commits_successful_writes_and_rolls_back_errors() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("committed", "ok@example.com", 1, "org"))
                    .execute(&tx)
                    .await?;
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();

    let result = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("rolled-back", "no@example.com", 1, "org"))
                    .execute(&tx)
                    .await?;
                Err::<(), TitoError>(TitoError::Internal("boom".to_string()))
            }
        })
        .await;

    assert!(result.is_err());
    assert!(model.get("committed").execute(None).await.is_ok());
    assert!(model.get("rolled-back").execute(None).await.is_err());
}

#[tokio::test]
async fn transaction_reads_own_writes_before_commit() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    let found = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "ada@example.com", 1, "org"))
                    .execute(&tx)
                    .await?;
                model.get("a1").execute(Some(&tx)).await
            }
        })
        .await
        .unwrap();

    assert_eq!(found.id, "a1");
}

#[tokio::test]
async fn get_missing_record_returns_not_found() {
    let model = engine().model::<Author>(TitoModelOptions::default());

    assert!(matches!(
        model.get("missing").execute(None).await.unwrap_err(),
        TitoError::NotFound(_)
    ));
}

#[tokio::test]
async fn string_index_queries_are_case_normalized_and_escape_colons() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "Ada:Admin@Example.com", 36, "org-a")).await;

    let mut query = model.query_by_index("author-by-email");
    let found = query
        .value("ada:admin@example.com")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a1");
    assert!(
        engine
            .contains_key("index:author-by-email:email:ada\\:admin@example.com:table:authors:a1")
            .await
    );
}

#[tokio::test]
async fn number_index_queries_sort_and_match_padded_values() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a9", "nine@example.com", 9, "org-a")).await;
    save_author(&engine, author("a12", "twelve@example.com", 12, "org-a")).await;

    let mut query = model.query_by_index("author-by-age");
    let found = query.value("9").execute(None).await.unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a9");
    assert!(
        engine
            .contains_key("index:author-by-age:age:0000000009:table:authors:a9")
            .await
    );
}

#[tokio::test]
async fn compound_index_queries_match_all_values() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;
    save_author(&engine, author("a2", "ada@example.com", 42, "org-b")).await;

    let mut query = model.query_by_index("author-by-org-email");
    let found = query
        .value("org-a")
        .value("ada@example.com")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a1");
}

#[tokio::test]
async fn custom_index_values_are_queryable() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    let mut query = model.query_by_index("author-by-kind-org");
    let found = query
        .value("author")
        .value("org-a")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a1");
}

#[tokio::test]
async fn missing_or_null_index_values_use_null_sentinel() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    let mut query = model.query_by_index("author-by-optional");
    let found = query.value("__null__").execute(None).await.unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a1");
}

#[tokio::test]
async fn disabled_indexes_are_not_written() {
    let engine = engine();
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    assert!(engine
        .keys_with_prefix("index:author-disabled:")
        .await
        .is_empty());
}

#[tokio::test]
async fn updating_record_replaces_old_index_keys() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "old@example.com", 36, "org-a")).await;
    save_author(&engine, author("a1", "new@example.com", 36, "org-a")).await;

    let mut old_query = model.query_by_index("author-by-email");
    let old = old_query
        .value("old@example.com")
        .execute(None)
        .await
        .unwrap();
    let mut new_query = model.query_by_index("author-by-email");
    let new = new_query
        .value("new@example.com")
        .execute(None)
        .await
        .unwrap();

    assert!(old.items.is_empty());
    assert_eq!(new.items.len(), 1);
    assert!(
        !engine
            .contains_key("index:author-by-email:email:old@example.com:table:authors:a1")
            .await
    );
}

#[tokio::test]
async fn removing_record_deletes_row_reverse_index_and_index_keys() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove("a1", &tx).await.map(|_| ()) }
        })
        .await
        .unwrap();

    assert!(model.get("a1").execute(None).await.is_err());
    assert!(!engine.contains_key("table:authors:a1").await);
    assert!(!engine.contains_key("reverse-index:table:authors:a1").await);
    assert!(engine
        .keys_with_prefix("index:author-by-email:")
        .await
        .is_empty());
}

#[tokio::test]
async fn remove_by_index_deletes_matching_batch() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "a1@example.com", 36, "org-a")).await;
    save_author(&engine, author("a2", "a2@example.com", 42, "org-a")).await;
    save_author(&engine, author("a3", "a3@example.com", 42, "org-b")).await;

    let removed = engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove_by_index("author-by-age", "42", 10, &tx).await }
        })
        .await
        .unwrap();

    assert_eq!(removed.len(), 2);
    assert!(model.get("a2").execute(None).await.is_err());
    assert!(model.get("a3").execute(None).await.is_err());
    assert!(model.get("a1").execute(None).await.is_ok());
}

#[tokio::test]
async fn find_one_by_index_returns_first_match_or_not_found() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    let found = model
        .find_one_by_index(
            TitoFindOneByIndexPayload {
                index: "author-by-email".to_string(),
                values: vec!["ada@example.com".to_string()],
                rels: Vec::new(),
            },
            None,
        )
        .await
        .unwrap();

    assert_eq!(found.id, "a1");
    assert!(model
        .find_one_by_index(
            TitoFindOneByIndexPayload {
                index: "author-by-email".to_string(),
                values: vec!["missing@example.com".to_string()],
                rels: Vec::new(),
            },
            None,
        )
        .await
        .is_err());
}

#[tokio::test]
async fn unknown_index_and_extra_index_values_return_errors() {
    let model = engine().model::<Author>(TitoModelOptions::default());

    assert!(matches!(
        model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "missing".to_string(),
                    values: vec!["x".to_string()],
                    rels: Vec::new(),
                    limit: None,
                    cursor: None,
                },
                None,
            )
            .await
            .unwrap_err(),
        TitoError::IndexError(_)
    ));

    assert!(matches!(
        model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "author-by-email".to_string(),
                    values: vec!["x".to_string(), "y".to_string()],
                    rels: Vec::new(),
                    limit: None,
                    cursor: None,
                },
                None,
            )
            .await
            .unwrap_err(),
        TitoError::IndexError(_)
    ));
}

#[tokio::test]
async fn find_paginates_with_cursor() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    for id in ["a1", "a2", "a3"] {
        save_author(&engine, author(id, &format!("{id}@example.com"), 1, "org")).await;
    }

    let first = model
        .find(TitoFindPayload {
            start: "".to_string(),
            end: None,
            limit: Some(2),
            cursor: None,
            rels: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(first.items.len(), 2);
    assert!(first.cursor.is_some());

    let second = model
        .find(TitoFindPayload {
            start: "".to_string(),
            end: None,
            limit: Some(2),
            cursor: first.cursor,
            rels: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].id, "a3");
}

#[tokio::test]
async fn scan_reverse_returns_items_in_reverse_key_order() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    for id in ["a1", "a2", "a3"] {
        save_author(&engine, author(id, &format!("{id}@example.com"), 1, "org")).await;
    }

    let tx = engine.begin_transaction().await.unwrap();
    let (items, has_more) = model
        .scan_reverse(
            TitoScanPayload {
                start: "table:authors:".to_string(),
                end: None,
                limit: Some(2),
                cursor: None,
            },
            &tx,
        )
        .await
        .unwrap();

    assert!(has_more);
    assert_eq!(
        items
            .iter()
            .map(|(_, value)| value["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["a3", "a2"]
    );
}

#[tokio::test]
async fn index_reverse_query_returns_reverse_index_order() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "a@example.com", 7, "org")).await;
    save_author(&engine, author("a2", "b@example.com", 7, "org")).await;
    save_author(&engine, author("a3", "c@example.com", 7, "org")).await;

    let mut query = model.query_by_index("author-by-age");
    let page = query
        .value("7")
        .limit(Some(2))
        .execute_reverse(None)
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].id, "a3");
    assert_eq!(page.items[1].id, "a2");
    assert!(page.cursor.is_some());
}

#[tokio::test]
async fn array_indexes_write_one_key_per_value() {
    let engine = engine();
    let model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_tag(&engine, tag("t1", "Tech")).await;
    save_tag(&engine, tag("t2", "Rust")).await;
    save_post(
        &engine,
        post("p1", "a1", vec!["t1".to_string(), "t2".to_string()]),
    )
    .await;

    let mut query = model.query_by_index("post-by-tag");
    let found = query.value("t2").execute(None).await.unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "p1");
    assert!(
        engine
            .contains_key("index:post-by-tag:tag_ids:t1:table:posts:p1")
            .await
    );
    assert!(
        engine
            .contains_key("index:post-by-tag:tag_ids:t2:table:posts:p1")
            .await
    );
}

#[tokio::test]
async fn nested_array_indexes_write_one_key_per_nested_value() {
    let engine = engine();
    save_post(
        &engine,
        Post {
            comments: vec![
                Comment {
                    author_id: "a1".to_string(),
                    body: "one".to_string(),
                },
                Comment {
                    author_id: "a2".to_string(),
                    body: "two".to_string(),
                },
            ],
            ..post("p1", "a1", vec![])
        },
    )
    .await;
    let model = engine.clone().model::<Post>(TitoModelOptions::default());

    let mut query = model.query_by_index("post-by-comment-author");
    let found = query.value("a2").execute(None).await.unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "p1");
}

#[tokio::test]
async fn map_indexes_include_map_entry_keys_in_index_key() {
    let engine = engine();
    save_post(&engine, post("p1", "a1", vec![])).await;

    let keys = engine.keys_with_prefix("index:post-by-metadata:").await;

    assert!(keys
        .iter()
        .any(|key| key.contains("metadata:locale.en:table:posts:p1")));
    assert!(keys
        .iter()
        .any(|key| key.contains("metadata:channel.web:table:posts:p1")));
}

#[tokio::test]
async fn relationship_stitches_single_and_array_relationships_when_requested() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_tag(&engine, tag("t1", "Tech")).await;
    save_tag(&engine, tag("t2", "Rust")).await;
    save_post(
        &engine,
        post("p1", "a1", vec!["t1".to_string(), "t2".to_string()]),
    )
    .await;

    let found = post_model
        .get("p1")
        .relationship("author")
        .relationship("tags")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.author.unwrap().id, "a1");
    assert_eq!(
        found.tags.into_iter().map(|tag| tag.id).collect::<Vec<_>>(),
        vec!["t1", "t2"]
    );
}

#[tokio::test]
async fn relationship_stitches_nested_array_relationships() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_post(&engine, post("p1", "a1", vec![])).await;

    let found = post_model
        .get("p1")
        .relationship("blocks.author")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.blocks[0].author.as_ref().unwrap().id, "a1");
}

#[tokio::test]
async fn relationship_is_not_stitched_when_not_requested() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_post(&engine, post("p1", "a1", vec![])).await;

    let found = post_model.get("p1").execute(None).await.unwrap();

    assert!(found.author.is_none());
}

#[tokio::test]
async fn get_many_can_stitch_relationships() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_author(&engine, author("a2", "grace@example.com", 42, "org")).await;
    save_post(&engine, post("p1", "a1", vec![])).await;
    save_post(&engine, post("p2", "a2", vec![])).await;

    let found = post_model
        .get_many(vec!["p2".to_string(), "p1".to_string()])
        .relationship("author")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(
        found
            .iter()
            .map(|post| post.author.as_ref().unwrap().id.clone())
            .collect::<Vec<_>>(),
        vec!["a2", "a1"]
    );
}

#[tokio::test]
async fn index_query_can_stitch_relationships() {
    let engine = engine();
    let post_model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org")).await;
    save_post(&engine, post("p1", "a1", vec![])).await;

    let mut query = post_model.query_by_index("post-by-author");
    let found = query
        .value("a1")
        .relationship("author")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].author.as_ref().unwrap().id, "a1");
}

#[tokio::test]
async fn batch_get_returns_only_existing_keys_in_requested_order() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "a1@example.com", 1, "org")).await;
    save_author(&engine, author("a2", "a2@example.com", 1, "org")).await;
    let tx = engine.begin_transaction().await.unwrap();

    let items = model
        .batch_get(
            vec![
                "table:authors:a2".to_string(),
                "table:authors:missing".to_string(),
                "table:authors:a1".to_string(),
            ],
            &tx,
        )
        .await
        .unwrap();

    assert_eq!(
        items
            .into_iter()
            .map(|(_, value)| value["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["a2", "a1"]
    );
}

#[tokio::test]
async fn get_key_reads_raw_json_by_storage_key() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 1, "org")).await;
    let tx = engine.begin_transaction().await.unwrap();

    let value = model.get_key("table:authors:a1", &tx).await.unwrap();

    assert_eq!(value["id"], "a1");
    assert_eq!(
        engine.raw_json("table:authors:a1").await.unwrap()["id"],
        "a1"
    );
}

#[tokio::test]
async fn delete_range_removes_keys_inside_range_only() {
    let engine = engine();
    engine
        .put_json("table:authors:a1", &json!({"id": "a1"}))
        .await;
    engine
        .put_json("table:authors:a2", &json!({"id": "a2"}))
        .await;
    engine
        .put_json("table:posts:p1", &json!({"id": "p1"}))
        .await;

    engine
        .delete_range(b"table:authors:", b"table:authors;")
        .await
        .unwrap();

    assert!(!engine.contains_key("table:authors:a1").await);
    assert!(!engine.contains_key("table:authors:a2").await);
    assert!(engine.contains_key("table:posts:p1").await);
}

#[tokio::test]
async fn model_tx_runs_through_engine_transaction() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    let saved = model
        .tx(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "ada@example.com", 1, "org"))
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap();

    assert_eq!(saved.id, "a1");
    assert!(model.get("a1").execute(None).await.is_ok());
}

#[tokio::test]
async fn invalid_find_cursor_returns_deserialization_error() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "a1@example.com", 1, "org")).await;

    let error = model
        .find(TitoFindPayload {
            start: "".to_string(),
            end: None,
            limit: Some(1),
            cursor: Some("not-base64".to_string()),
            rels: Vec::new(),
        })
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::DeserializationFailed(_)));
}

#[test]
fn model_options_partition_config_cursor_and_id_helpers_are_stable() {
    let model = engine().model::<Author>(TitoModelOptions::with_partitions(12));
    let partition = PartitionConfig::new(7);
    let paginated = TitoPaginated::new(vec!["a".to_string()], Some("cursor".to_string()));
    let id = TitoId::new("a1", "author");
    let cursor = TitoCursor { ids: vec![None] };

    assert_eq!(model.partition_count, 12);
    assert_eq!(partition.partition, 7);
    assert_eq!(paginated.items, vec!["a"]);
    assert_eq!(paginated.cursor.as_deref(), Some("cursor"));
    assert_eq!(id.to_string(), "author:a1");
    assert!(cursor.first_id().is_err());
}

#[test]
fn model_key_helpers_extract_last_key_segment() {
    let model = engine().model::<Author>(TitoModelOptions::default());

    assert_eq!(
        model.get_id_from_table("table:authors:a1".to_string()),
        "a1"
    );
    assert_eq!(
        model.get_last_id("index:author-by-email:email:x:table:authors:a1".to_string()),
        Some("a1".to_string())
    );
}

#[test]
fn safe_encode_snake_cases_and_escapes_key_separators() {
    assert_eq!(safe_encode("Ada:Admin\\User"), "ada\\:admin\\\\user");
}

#[test]
fn lexicographic_helpers_move_string_bounds() {
    assert_eq!(next_string_lexicographically("abc".to_string()), "abd");
    assert_eq!(previous_string_lexicographically("abd".to_string()), "abc");
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

#[test]
fn queue_event_helpers_parse_key_and_update_builder_fields() {
    let event = QueueEvent::new("entry:entry-1", queue_payload("payload"))
        .scheduled_for(123)
        .with_max_retries(5);

    assert_eq!(event.key_type(), "entry");
    assert_eq!(event.key_value(), "entry-1");
    assert_eq!(event.event().name, "payload");
    assert_eq!(event.timestamp, 123);
    assert_eq!(event.max_retries, 5);
}

#[tokio::test]
async fn queue_ack_missing_key_is_noop() {
    let engine = engine();
    let queue = queue(engine, 1);

    queue.ack("queue:pending:0000:1:missing").await.unwrap();
}

#[tokio::test]
async fn queue_ack_deletes_orphan_event_bytes() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let key = "queue:pending:0000:1:bad";
    engine.put_raw(key, b"not-json".to_vec()).await;

    queue.ack(key).await.unwrap();

    assert!(!engine.contains_key(key).await);
}

#[tokio::test]
async fn queue_reschedule_moves_pulled_event_to_new_pending_timestamp() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let now = Utc::now().timestamp();
    queue
        .publish(queue_event("event-1", "entry:1", now - 10))
        .await
        .unwrap();

    let pulled = queue.pull::<QueuePayload>(0, 10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    let (old_storage_key, event) = pulled.into_iter().next().unwrap();
    let future_timestamp = now + 3_600;

    queue
        .reschedule(event, &old_storage_key, future_timestamp)
        .await
        .unwrap();

    assert!(!engine.contains_key(&old_storage_key).await);
    assert!(queue.pull::<QueuePayload>(0, 10).await.unwrap().is_empty());
    let page = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].1.timestamp, future_timestamp);
}

#[tokio::test]
async fn queue_move_to_dlq_is_failed_alias() {
    let engine = engine();
    let queue = queue(engine, 1);
    queue
        .publish(queue_event(
            "event-1",
            "entry:1",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let (storage_key, event) = queue
        .pull::<QueuePayload>(0, 10)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    queue.move_to_dlq(event, &storage_key).await.unwrap();

    let failed = queue
        .find_by_state_after::<QueuePayload>(QueueEventState::Failed, 0, 10)
        .await
        .unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].1.state, QueueEventState::Failed);
}

#[tokio::test]
async fn queue_clear_removes_pending_completed_failed_and_dlq_rows() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let now = Utc::now().timestamp();
    let pending = queue_event("pending", "entry:pending", now);
    let mut completed = queue_event("completed", "entry:completed", now);
    completed.state = QueueEventState::Completed;
    completed.processed_at = Some(now);
    let mut failed = queue_event("failed", "entry:failed", now);
    failed.state = QueueEventState::Failed;
    failed.processed_at = Some(now);
    let mut dlq = queue_event("dlq", "entry:dlq", now);
    dlq.state = QueueEventState::Failed;
    dlq.processed_at = Some(now);

    engine
        .put_raw(
            "queue:pending:0000:1:pending",
            serde_json::to_vec(&pending).unwrap(),
        )
        .await;
    engine
        .put_raw(
            "queue:completed:00000000000000000001:completed",
            serde_json::to_vec(&completed).unwrap(),
        )
        .await;
    engine
        .put_raw(
            "queue:failed:0000:1:failed",
            serde_json::to_vec(&failed).unwrap(),
        )
        .await;
    engine
        .put_raw("queue:dlq:0000:1:dlq", serde_json::to_vec(&dlq).unwrap())
        .await;

    queue.clear().await.unwrap();

    assert!(engine.keys_with_prefix("queue:").await.is_empty());
}

#[tokio::test]
async fn queue_delete_by_state_before_rejects_pending_state() {
    let engine = engine();
    let queue = queue(engine, 1);

    let error = queue
        .delete_by_state_before(QueueEventState::Pending, Utc::now().timestamp(), 10)
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::InvalidInput(_)));
}

#[tokio::test]
async fn queue_scan_cursor_continues_after_previous_page() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp();
    for id in ["event-1", "event-2", "event-3"] {
        queue.publish(queue_event(id, id, now - 10)).await.unwrap();
    }

    let first = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 2)
        .await
        .unwrap();
    assert_eq!(first.events.len(), 2);
    assert!(first.next_cursor.is_some());

    let second = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, first.next_cursor, 2)
        .await
        .unwrap();
    assert_eq!(second.events.len(), 1);
}

#[tokio::test]
async fn queue_worker_acks_successful_jobs() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    queue
        .publish(queue_event(
            "worker-success",
            "entry:worker-success",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let processed = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handler_processed = processed.clone();

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig {
            partition_range: 0..1,
        },
        move |event| {
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
                assert_eq!(event.id, "worker-success");
                handler_processed.notify_one();
                Ok(())
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(2), processed.notified())
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let completed = queue
                .find_by_state_after::<QueuePayload>(QueueEventState::Completed, 0, 10)
                .await
                .unwrap();
            if completed
                .iter()
                .any(|(_, event)| event.id == "worker-success")
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn queue_worker_moves_exhausted_retries_to_failed() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    let mut event = queue_event(
        "worker-failed",
        "entry:worker-failed",
        Utc::now().timestamp() - 10,
    );
    event.max_retries = 0;
    queue.publish(event).await.unwrap();
    let processed = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handler_processed = processed.clone();

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig {
            partition_range: 0..1,
        },
        move |_event| {
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
                handler_processed.notify_one();
                Err(TitoError::Internal("handler failed".to_string()))
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(2), processed.notified())
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let failed = queue
                .find_by_state_after::<QueuePayload>(QueueEventState::Failed, 0, 10)
                .await
                .unwrap();
            if failed.iter().any(|(_, event)| {
                event.id == "worker-failed"
                    && event.retry_count == 1
                    && event.errors == vec!["Unexpected error: handler failed".to_string()]
            }) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
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

#[tokio::test]
async fn cluster_coordinator_lease_blocks_other_nodes_until_expired() {
    let engine = engine();
    let queue = queue(engine.clone(), 2);
    let node_a = cluster_config("node-a");
    let node_b = cluster_config("node-b");

    assert!(queue
        .try_acquire_cluster_coordinator(&node_a)
        .await
        .unwrap());
    assert!(queue
        .try_acquire_cluster_coordinator(&node_a)
        .await
        .unwrap());
    assert!(!queue
        .try_acquire_cluster_coordinator(&node_b)
        .await
        .unwrap());

    let expired = ClusterCoordinatorLease {
        owner_node_id: "node-a".to_string(),
        lease_until: Utc::now().timestamp() - 1,
        updated_at: Utc::now().timestamp() - 2,
    };
    engine
        .put_json("tito:queue:cluster:coordinator", &json!(expired))
        .await;

    assert!(queue
        .try_acquire_cluster_coordinator(&node_b)
        .await
        .unwrap());
}

#[tokio::test]
async fn cluster_rebalance_assigns_and_syncs_partitions_to_active_nodes() {
    let engine = engine();
    let queue = queue(engine, 4);
    let node_a = cluster_config("node-a");
    let node_b = cluster_config("node-b");

    queue.heartbeat_cluster_worker(&node_b).await.unwrap();
    queue.heartbeat_cluster_worker(&node_a).await.unwrap();

    let active = queue.active_cluster_workers(&node_a).await.unwrap();
    assert_eq!(
        active
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["node-a", "node-b"]
    );

    let assignments = queue.rebalance_cluster_partitions(&node_a).await.unwrap();
    assert_eq!(assignments.len(), 4);
    assert_eq!(assignments[0].desired_node_id.as_deref(), Some("node-a"));
    assert_eq!(assignments[1].desired_node_id.as_deref(), Some("node-a"));
    assert_eq!(assignments[2].desired_node_id.as_deref(), Some("node-b"));
    assert_eq!(assignments[3].desired_node_id.as_deref(), Some("node-b"));

    let owned_a = queue.sync_cluster_partition_leases(&node_a).await.unwrap();
    let owned_b = queue.sync_cluster_partition_leases(&node_b).await.unwrap();
    assert_eq!(
        owned_a
            .iter()
            .map(|assignment| assignment.partition)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        owned_b
            .iter()
            .map(|assignment| assignment.partition)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        queue.owned_cluster_partitions(&node_a).await.unwrap(),
        vec![0, 1]
    );
}
