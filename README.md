# Tito

A database layer on TiKV with indexing, relationships, transactions, and a built-in partitioned scheduled queue.

## Features

- **Data Storage**: Models with CRUD operations
- **Indexing**: Conditional and composite indexes for efficient queries
- **Relationships**: Embedded relationship hydration
- **Transactions**: Full ACID transactions
- **Query Builder**: Fluent API for querying by index
- **Transactional Publication**: Queue events can be written atomically with application data
- **Partitioned Queue**: Horizontal scaling via stable business-key partitions
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

## Queue Processing

Queue events are partitioned by their business key, can be scheduled for a future timestamp, and remain pending until the handler explicitly acknowledges them. Tito has no automatic retry policy, retry counter, backoff, failed state, or DLQ:

- `Ok(QueueHandlerOutcome::Acknowledge)` completes the current invocation.
- `Ok(QueueHandlerOutcome::ScheduleAt(timestamp))` atomically replaces the current pending invocation with a pending successor carrying the same logical event ID, key, and payload at that exact timestamp.
- `Err(_)`, a handler panic, or the executor timeout leaves the current invocation unchanged and pending for redelivery at the worker's normal poll cadence.

```rust
use std::sync::Arc;
use futures::FutureExt;
use tito::{Queue, QueueConfig, QueueEvent, QueueHandlerOutcome, WorkerConfig};
use tito::queue::run_worker;

let queue = Arc::new(Queue::new(db.clone(), QueueConfig::new(4)));

queue
    .publish(QueueEvent::new("user:123", UserCreated { id: "123".into() }))
    .await?;

run_worker(
    queue,
    WorkerConfig::new(0..4),
    |event: QueueEvent<UserCreated>| async move {
        handle_user_created(event.payload).await?;
        Ok(QueueHandlerOutcome::Acknowledge)
    }
    .boxed(),
    shutdown_rx,
).await;
```

`ScheduleAt` is an atomic pending-to-pending handoff, not a queue retry policy. Tito checks that the exact current pending storage key still exists, then deletes that key and writes one successor in the same transaction. The timestamp must produce a different due key. The event ID remains the permanent logical identity; the due timestamp in each pending storage key distinguishes scheduled invocations. Only `Acknowledge` creates a completed row. If concurrent consumers process the same invocation, only the winner can create the successor. Application handlers should choose one of these two outcomes rather than mutating queue state directly.

Workers supervise each handler with a ten-minute timeout by default. Configure `handler_timeout` when a workload has a different bounded execution contract; the timeout is executor protection and never changes queue state or provider policy.

Partition polling is fair across due rows. Each worker keeps an in-memory cursor for one bounded pass, advances that cursor by raw storage row (including malformed rows), and then wraps to the oldest due key. The pass keeps a fixed due-time horizon, so continuously arriving work cannot prevent the wrap. The cursor is executor state only: it is not persisted, does not lease a queue row, and does not encode retry policy.

Completed invocation history has one fixed retention policy: standalone workers and the elected
cluster coordinator delete rows older than three days in bounded passes. A full pass yields and
continues immediately until the expired range is caught up; the 30-second interval applies only
after a short pass. Maintenance uses the completed-state/processed-time key range directly, never
inspects pending work, and has no retry, recovery, or provider semantics. Malformed values inside an
expired completed-row key are also removed so corrupt terminal history cannot pin newer cleanup
work.

### Transaction retry safety

Tito may replay a transaction closure after an explicitly retryable, determined datastore failure. TiKV's `UndeterminedError` is different: the commit may already be durable. Tito returns `TitoError::CommitOutcomeUnknown` and never replays that closure. The caller must reconcile against authoritative stored state; queue acknowledgement and `ScheduleAt` operations naturally converge when a still-pending row is pulled again.

### Upgrade contract

This release intentionally removes the former retry/DLQ metadata and is not wire-compatible with workers using that queue protocol. Do not run the old and new queue protocols together.

For the prelaunch cutover, stop publishers and workers, use the old release to drain Pending and clear its Failed/DLQ keyspaces, verify Pending is empty, deploy the replacement environment, and then restart publication and processing. If a future nonempty production environment requires migration, build and deploy a separately named bridge release first; compatibility scaffolding is not part of this queue contract.

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
