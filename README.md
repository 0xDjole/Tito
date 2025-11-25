# Tito

A database layer on TiKV with indexing, relationships, transactions, and a built-in transactional outbox with partitioned scheduled pub/sub.

## Features

- **Data Storage**: Models with CRUD operations
- **Indexing**: Conditional and composite indexes for efficient queries
- **Relationships**: Embedded relationship hydration
- **Transactions**: Full ACID transactions
- **Query Builder**: Fluent API for querying by index
- **Transactional Outbox**: Events written atomically with data - no dual-write problem
- **Partitioned Pub/Sub**: Horizontal scaling via partitions
- **Scheduled Events**: Set timestamp for when events should fire

## Model Definition

```rust
#[derive(Default, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    name: String,
    email: String,
    business_id: String,
}

impl TitoModelTrait for User {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn table(&self) -> String {
        "user".to_string()
    }

    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            TitoIndexConfig {
                condition: true,
                name: "by_email".to_string(),
                fields: vec![TitoIndexField {
                    name: "email".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
            TitoIndexConfig {
                condition: true,
                name: "by_business".to_string(),
                fields: vec![TitoIndexField {
                    name: "business_id".to_string(),
                    r#type: TitoIndexBlockType::String,
                }],
            },
        ]
    }

    fn events(&self) -> Vec<TitoEventConfig> {
        let now = chrono::Utc::now().timestamp();
        vec![
            TitoEventConfig { name: "user".to_string(), timestamp: now },
            TitoEventConfig { name: "analytics".to_string(), timestamp: now },
        ]
    }
}
```

## CRUD Operations

```rust
let db = TiKVBackend::connect(vec!["127.0.0.1:2379"]).await?;
let users = db.clone().model::<User>();

// Create
db.transaction(|tx| async move {
    users.build_with_options(user, TitoOptions::with_events(TitoOperation::Insert), &tx).await
}).await?;

// Read
let user = users.find_by_id(&id, vec![]).await?;

// Query by index
let mut query = users.query_by_index("by_business");
let results = query.value(&business_id).limit(Some(10)).execute().await?;

// Update
db.transaction(|tx| async move {
    users.update_with_options(user, TitoOptions::with_events(TitoOperation::Update), &tx).await
}).await?;

// Delete
db.transaction(|tx| async move {
    users.delete_by_id_with_options(&id, TitoOptions::with_events(TitoOperation::Delete), &tx).await
}).await?;
```

## Relationships

```rust
#[derive(Default, Clone, Serialize, Deserialize)]
struct Post {
    id: String,
    title: String,
    tag_ids: Vec<String>,
    #[serde(default)]
    tags: Vec<Tag>,
}

impl TitoModelTrait for Post {
    fn relationships(&self) -> Vec<TitoRelationshipConfig> {
        vec![TitoRelationshipConfig {
            source_field_name: "tag_ids".to_string(),
            destination_field_name: "tags".to_string(),
            model: "tag".to_string(),
        }]
    }

    fn references(&self) -> Vec<String> {
        self.tag_ids.clone()
    }

    // ... other methods
}

// Query with relationship hydration
let mut query = posts.query_by_index("by_author");
let results = query.value(&author_id).relationship("tags").execute().await?;
```

## Event Processing

Events are written in the same transaction as data. Workers process them later.

```rust
let queue = Arc::new(TitoQueue { engine: db });
let partition_config = PartitionConfig::new(4, 0); // 4 partitions, this worker owns partition 0

run_worker(
    queue,
    "user".to_string(),
    |event| async move {
        match event.action.as_str() {
            "INSERT" => send_welcome_email(&event.entity_id()).await,
            "UPDATE" => sync_to_search_index(&event.entity_id()).await,
            "DELETE" => cleanup_related_data(&event.entity_id()).await,
            _ => Ok(()),
        }
    }.boxed(),
    partition_config,
    is_leader,
    shutdown_rx,
).await;
```

## Scaling

```rust
// Worker 1: partition 0
PartitionConfig::new(4, 0)

// Worker 2: partition 1
PartitionConfig::new(4, 1)

// Worker 3: partition 2
PartitionConfig::new(4, 2)

// Worker 4: partition 3
PartitionConfig::new(4, 3)

// Need more throughput? Add more partitions and workers.
```

## Scheduled Events

```rust
fn events(&self) -> Vec<TitoEventConfig> {
    let in_one_hour = chrono::Utc::now().timestamp() + 3600;
    vec![TitoEventConfig {
        name: "reminder".to_string(),
        timestamp: in_one_hour,
    }]
}
```

## Event Key Format

```
event:{type}:{partition}:{timestamp}:{uuid}
```

## Requirements

- Rust 2021+
- TiKV cluster

## License

Apache-2.0
