# Tito

A database layer on TiKV with indexing, transactions, and a built-in partitioned scheduled queue.

## Features

- **Data Storage**: Models with CRUD operations
- **Indexing**: Conditional and composite indexes for efficient queries
- **Transactions**: Full ACID transactions
- **Query Builder**: Fluent API for querying by index

Indexes are sparse. Tito writes an index key only when every configured field contains a value of
the configured type. Missing fields, JSON `null`, empty strings, empty collections, and values of a
different type produce no key for that index. A model can use `condition` to apply an additional
domain-specific inclusion rule.
- **Transactional Publication**: Queue events can be written atomically with application data
- **Partitioned Queue**: Horizontal scaling via stable business-key partitions
- **Event Timestamps**: Each event says when it becomes runnable

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

Tito reads and writes one model at a time. Applications load related records explicitly so their
domain and API boundaries determine when an additional read is required.

## Storage Maintenance

```rust
use std::time::Duration;
use tito::TitoEngine;

db.garbage_collect(Duration::from_secs(24 * 60 * 60))
    .await?;
```

`garbage_collect` derives an MVCC safe point from TiKV's current timestamp minus the supplied
nonzero retention window and asks TiKV to apply it. A successful call also succeeds when PD already
has a newer safe point or the engine has no historical versions to collect. The application owns
the cluster-wide retention policy and must choose a window older than every transaction or
historical read it still needs.

`delete_range(start, end)` is an offline destructive operation over the start-inclusive,
end-exclusive range. The TiKV engine uses the transactional client's unsafe range destruction, so
all MVCC data in the range is removed. Use model transactions for ordinary deletes and reserve
range destruction for reset, restore, or drop-style maintenance with application traffic stopped.

## Queue Processing

Queue events are partitioned by their business key, carry their own non-negative Unix timestamp, and remain pending until the handler explicitly acknowledges them. Tito rejects negative timestamps instead of storing an event that polling cannot reach. Tito has no automatic retry policy, retry counter, backoff, failed state, or DLQ:

Handlers return `QueueHandlerResult<T>`, an alias for `Result<QueueHandlerOutcome<T>, TitoError>`.

- `Ok(QueueHandlerOutcome::Acknowledge)` completes the current invocation.
- `Ok(QueueHandlerOutcome::Reschedule(next_event))` atomically completes the current queue row and inserts the supplied replacement row.
- `Err(_)`, a handler panic, executor timeout, lost worker, or queue-commit failure produces no persisted outcome, so the exact current invocation remains pending.

Each event's own timestamp determines when it becomes runnable. Tito indexes that timestamp but never chooses or changes it. A domain that needs another invocation supplies a replacement event carrying the desired timestamp through `Reschedule`; Tito only commits the acknowledge-and-insert transaction. A replacement must preserve the logical event ID, partition key, and payload and may change only the timestamp. `QueueEvent::rescheduled` constructs that replacement, so replay-safe projections keep one identity across queue rows. Provider leases and processing deadlines belong only to domain records.

Every new invocation must serialize to at most `MAX_QUEUE_EVENT_BYTES` (1 MiB). Publication rejects
larger events before writing anything. Queue range reads preserve their public logical page size while fetching at most 16
invocations per datastore scan. Tito configures both TiKV clients with a 32 MiB decoding budget, so
the maximum 16 MiB of valid invocation bytes has at least 2x transport headroom for completed-state
metadata, keys, and protobuf framing. This keeps one logical page from becoming one unbounded RPC
without turning a 50-event worker pull into dozens of sequential datastore calls.

```rust
use std::sync::Arc;
use std::time::Duration;
use futures::FutureExt;
use tito::{Queue, QueueConfig, QueueEvent, QueueHandlerOutcome, WorkerConfig};
use tito::queue::run_worker;

let queue = Arc::new(Queue::new(
    db.clone(),
    QueueConfig::new(4, Duration::from_secs(3 * 24 * 60 * 60)),
));

queue
    .publish(QueueEvent::new(
        "user:123",
        UserCreated { id: "123".into() },
        chrono::Utc::now().timestamp(),
    ))
    .await?;

run_worker(
    queue,
    WorkerConfig::new(0..4),
    |event: QueueEvent<UserCreated>| async move {
        handle_user_created(event).await?;
        Ok(QueueHandlerOutcome::Acknowledge)
    }.boxed(),
    shutdown_rx,
).await;
```

`completed_retention` is an application policy supplied at queue construction. Tito's standalone
worker or elected cluster coordinator removes bounded batches of older completed rows during its
normal maintenance tick. Tito does not hardcode the duration, publish cleanup events, or delegate
queue cleanup to the application's backup process.

`Reschedule` is not an automatic retry policy. The application decides whether another event exists and supplies the complete event, including its timestamp. Tito creates no successor on its own and never interprets provider or domain state.

An optional `QueueOwner` gives an application a bounded ownership index without changing due-time
ordering. Tito writes that secondary key in the same transaction as publication, moves it atomically
on acknowledge/reschedule, and removes it with the queue row. Applications that must erase one
owner's work can call `delete_by_owner_matching_in_tx`; the scan touches only that owner's Pending
or Completed keys and the predicate can preserve a currently executing lifecycle invocation. Owner
kind and ID are opaque, non-empty strings bounded to 512 bytes each. They are routing/erasure
metadata, not provider identity or domain state.

Workers supervise each handler with a ten-minute timeout by default. Configure `handler_timeout` when a workload has a different bounded execution contract; the timeout is executor protection and never changes queue state or provider policy.
Worker shutdown stops new pulls and handler starts, then drains every handler that already started
and applies its outcome before joining. The configured handler timeout bounds that drain. A handler
error, panic, timeout, lost worker, lost cluster partition lease, or queue-outcome storage failure
leaves the exact invocation Pending.

Partition polling is fair across due rows. Pending storage keys are ordered as
`queue:pending:{partition:04}:{timestamp:020}:{enqueue_version:020}:{event_id}`. The enqueue version is
the datastore transaction's globally ordered start version; it is internal ordering metadata, not
an invocation ID, provider identity, lease, or domain state. Each worker keeps an in-memory cursor
for one bounded pass. The first pull freezes both the runnable timestamp boundary and transaction-version
horizon. Later pulls advance by raw storage row (including malformed rows) and jump over an entire
due-time bucket when they encounter a row enqueued at or after that horizon. A handler can therefore
create immediate same-time events without keeping later timestamp buckets behind them forever.
After the pass is exhausted, polling wraps to the oldest runnable key and those events become
eligible. The cursor is executor state only: it is not persisted and does not encode retry policy.

Completed invocation history uses the retention supplied by the application in `QueueConfig`.
Standalone workers and the elected cluster coordinator delete older rows in bounded passes. A full
pass yields and continues immediately until the expired range is caught up; the 30-second interval
applies only after a short pass. Maintenance uses the completed-state/processed-time key range
directly, never inspects pending work, and has no retry, recovery, or provider semantics. Malformed
values inside an expired completed-row key are also removed so corrupt terminal history cannot pin
newer cleanup work.

### Transaction retry safety

Tito may replay a transaction closure after an explicitly retryable, determined datastore failure. TiKV's `UndeterminedError` is different: the commit may already be durable. Tito returns `TitoError::CommitOutcomeUnknown` and never replays that closure. The caller reconciles against authoritative domain state; an acknowledgement either committed or the unchanged Pending invocation is delivered again.

### Upgrade contract

This queue protocol removes the former retry/DLQ metadata, changes Pending storage keys to include
the fixed-width event timestamp and enqueue-version fields, and writes owner secondary keys for
newly published invocations. It is not wire-compatible with workers using an older queue protocol.
Do not run old and new queue protocols together. Owner indexes are not inferred from pre-upgrade
rows; a deployment that intends to use owner-bounded erasure must reset/drain those rows or ship an
explicit one-time bridge before enabling that operation.

For the prelaunch cutover, stop publishers and workers, use the source release to drain Pending and clear its Failed/DLQ keyspaces, verify Pending is empty, deploy the replacement environment, and then restart publication and processing. A future nonempty production environment requires an explicit, separately named bridge release before this queue contract is enabled.

## Future Events

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
queue:pending:{partition:04}:{timestamp:020}:{enqueue_version:020}:{event_id}
queue:completed:{processed_at:020}:{event_timestamp:020}:{event_id}
queue:owner:{base64url(kind)}:{base64url(id)}:{state}:{base64url(queue_storage_key)}
```

## License

Apache-2.0
