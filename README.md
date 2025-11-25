# Tito

A database layer on TiKV with indexing, relationships, transactions, and a built-in transactional outbox with partitioned scheduled pub/sub.

## Features

- **Data Storage**: Models with CRUD operations
- **Indexing**: Conditional and composite indexes for efficient queries
- **Relationships**: Embedded relationship hydration
- **Transactions**: Full ACID transactions
- **Query Builder**: Fluent API for querying by index
- **Transactional Outbox**: Events written atomically with data
- **Partitioned Pub/Sub**: Horizontal scaling via partitions
- **Scheduled Events**: Set timestamp for when events should fire

## Connection

```rust
use tito::backend::tikv::TiKV;

let db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;

let db = TiKV::connect_with_partitions(vec!["127.0.0.1:2379"], 1024).await?;
```

## Model Definition

```rust
#[derive(Default, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    name: String,
    email: String,
}

impl TitoModelTrait for User {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn table(&self) -> String {
        "user".to_string()
    }

    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "by_email".to_string(),
            fields: vec![TitoIndexField {
                name: "email".to_string(),
                r#type: TitoIndexBlockType::String,
            }],
        }]
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
let users = db.clone().model::<User>();

db.transaction(|tx| async move {
    users.build_with_options(user, TitoOptions::with_events(TitoOperation::Insert), &tx).await
}).await?;

let user = users.find_by_id(&id, vec![]).await?;

let mut query = users.query_by_index("by_email");
let results = query.value(&email).limit(Some(10)).execute().await?;
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
}

let mut query = posts.query_by_index("by_author");
let results = query.value(&author_id).relationship("tags").execute().await?;
```

## Event Processing

Events are written in the same transaction as data. Workers process them later.

```rust
let queue = Arc::new(TitoQueue { engine: db });

run_worker(
    queue,
    "user".to_string(),
    |event| async move {
        match event.action.as_str() {
            "INSERT" => handle_create(&event.entity_id()).await,
            "UPDATE" => handle_update(&event.entity_id()).await,
            "DELETE" => handle_delete(&event.entity_id()).await,
            _ => Ok(()),
        }
    }.boxed(),
    PartitionConfig::new(0),
    is_leader,
    shutdown_rx,
).await;
```

## Scaling

```rust
PartitionConfig::new(0)
PartitionConfig::new(1)
PartitionConfig::new(2)
PartitionConfig::new(3)
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

## License

Apache-2.0
