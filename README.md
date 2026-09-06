# Tito

A database layer on TiKV with indexing, transactions, and a built-in partitioned scheduled queue.

## Features

- **Data Storage**: Models with CRUD operations
- **Indexing**: Conditional ordinary and unique composite indexes for efficient queries
- **Transactions**: Full ACID transactions
- **Query Builder**: Fluent API for querying by index

Indexes are sparse. Tito writes an index key only when every configured field contains a value of
the configured type. Missing fields, JSON `null`, empty strings, empty collections, and values of a
different type produce no key for that index. A model can use `condition` to apply an additional
domain-specific inclusion rule.

`Number` indexes accept signed 64-bit integers. JSON unsigned integers through `i64::MAX` have the
same exact indexed identity; larger unsigned values and all floating-point numbers (including
`1.0`) fail the write instead of silently losing the index. Non-number values retain sparse
behavior, and a disabled index condition omits the index without validating its values. Numeric
query values must be canonical signed decimal strings: no leading plus, leading zeros, negative
zero, fractional or exponent notation, whitespace, or out-of-range values.

`encode_index_integer(i64)` encodes the full signed range by flipping the sign bit and writing the
result as exactly 20 decimal digits. Byte ordering therefore matches signed integer ordering,
including negative values, zero, and decimal-width boundaries. Use this exported encoder for
manual numeric range endpoints; never pad the original integer directly. Half-open scans and exact
cursor continuation retain their existing semantics. String index encoding and opaque record IDs
are unchanged.

Ordinary indexes are declared by `indexes`; their keys include the primary record identity and may
have many owners. Exclusive value ownership is declared separately by `unique_indexes`; its key
includes the model, index name, and complete indexed value but not the claimant ID. A competing
claim therefore conflicts on one real ownership key and returns `TitoError::UniqueViolation`
without exposing the indexed value. `find_one_by_unique_index` performs an exact key read and then
loads the authoritative primary record. Conditional unique indexes are omitted when their model
instance sets `condition` to false.
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
        let now = chrono::Utc::now().timestamp_millis();
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
domain and API boundaries determine when an additional read is required. When a transaction
depends on an existing record remaining unchanged, `model.assert_current(id, &tx)` reads and stages
the exact primary bytes without deserializing, changing timestamps, touching indexes, or creating a
second lock record.

## Storage integrity and pagination

Each persisted model row has a matching `reverse-index:{primary-key}` manifest, including models
with no secondary indexes. The manifest may name ordinary `index:` keys ending in that exact
primary key and model-scoped `unique-index:` keys whose stored owner matches that primary record.
Updates and removals validate the pair and every unique owner before mutating either side. A
missing, orphaned, malformed, or syntactically cross-record manifest is an integrity error; Tito
does not reinterpret it as a missing entity or follow it to an unrelated key.

The unchanged manifest format does not contain an index-schema version, so Tito cannot prove that
a syntactically valid manifest still enumerates every index created by an older application schema.
Recomputing against the current model would incorrectly reject legitimate index additions,
removals, and condition changes. Detecting or repairing a valid-shaped but semantically incomplete
historical manifest therefore requires an application-owned audited rebuild; 0.16.2 deliberately
does not overstate that guarantee.

Scans fail on malformed JSON, non-UTF-8 keys, and values that do not deserialize into the requested
model. They never silently shorten a page by dropping corrupt rows. Forward cursors continue from
the exact key plus a NUL byte; reverse cursors use the exact key as the exclusive upper bound. Both
directions reject a cursor outside the requested half-open range. `find` applies its optional `end`
as an exclusive model-key suffix, and a scan limit of zero is invalid.

Prefix endpoints and exact-key continuation are separate operations:

```rust
let end = tito::prefix_end("index:by_store:store_id:abc:".to_string());
let after = tito::key_after("index:by_store:store_id:abc:table:item:42".to_string());
```

`next_string_lexicographically` remains an alias for `prefix_end` for source compatibility. It must
not be used to continue after an exact key because advancing the final character can skip keys such
as `a20` after `a2`.

Secondary index values intentionally remain complete clones of the primary JSON document in
0.16.2. This preserves the existing storage wire format and query behavior. Removing that
redundancy requires a separately designed release that rehydrates primary rows and migrates every
existing index; it is not part of this correctness patch.

### 0.18.0 development cutover

Version 0.18.0 changes every Tito-owned wall-clock instant to signed UTC Unix epoch milliseconds:
automatic model `created_at` and `updated_at`, queue `timestamp` and `processedAt`, cluster
heartbeats, lease deadlines, and assignment timestamps. Timestamp-bearing models receive automatic
millisecond stamps unless a write explicitly uses `.timestamps(false)` to preserve application-owned
values. No undeclared timestamp fields are added.

Queue scheduling still accepts only non-negative `i64` instants, including zero and early-epoch
millisecond values. The requested due time is stored exactly, including its millisecond component;
Tito never guesses units, scales caller values, or rounds them to whole seconds. Completed-retention
cutoffs and cluster leases convert their configured `Duration` to milliseconds. Oversized durations
saturate rather than wrapping into an earlier deadline. Fractional milliseconds round up so a
configured positive span never becomes zero or expires early. Runtime sleeps, handler budgets, and monotonic measurements remain
`Duration`/`Instant` values.

TiKV transaction versions and native physical/logical timestamps do not change. Queue IDs keep their
opaque microsecond prefix and UUID suffix; `created_at_millis()` converts that prefix to milliseconds
and otherwise returns the exact millisecond queue timestamp without another scale conversion.

The same candidate applies the canonical queue lifecycle vocabulary: `QueueEvent.status` uses
`QueueEventStatus`, serialized as `{"type":"pending"}` or `{"type":"completed"}`. The field is
required; neither the old `state` field nor a string-valued status is accepted. `QueueOwner.r#type`
serializes as `type`, never `kind`. Owner records and queue envelopes reject unknown fields.
Query methods are `scan_by_status` and `delete_by_status_before`; derived owner index segments use
the scalar status type while the event itself retains its tagged status object. There are no old
API aliases or dual readers.

The same candidate replaces the old minimum-width decimal numeric-index keys with the signed-sortable
encoding above. Ordinary, unique, array, and map numeric index writes and numeric queries use the
same integer encoding. The previous implementation silently omitted floating-point and oversized
unsigned values; these unsupported numbers now fail explicitly before any model/index mutation.
Numeric secondary index keys must be rebuilt through the authorized reset/reseed. Queue keys and
opaque microsecond queue IDs retain their existing key layouts.

This is an incompatible data and worker contract: instant units, numeric index keys, and queue JSON
fields change. Do not mix 0.17.x and 0.18.x publishers, workers, model data, queue rows,
cluster records, or backup artifacts. The pre-production cutover stops traffic and workers, verifies
the authorized reset, and reseeds the complete affected state. There is no mixed-unit decoder,
magnitude heuristic, migration, or legacy compatibility mode in 0.18.0.

The current 0.18.0 candidate is a development change, not a registry publication or release tag.
Applications can verify it through a reviewed exact Git revision and lockfile; a sibling checkout or
a moving branch name is not a reproducible dependency pin. Publication is a separate authorized step.

### 0.17.0 rollout

Version 0.17.0 adds conditional unique indexes, exact unique lookup, and `assert_current`. Existing
ordinary index keys remain unchanged. Models adopting a unique index must rewrite or reset their
records before relying on the new key because Tito never invents index entries for stored rows.
The release also includes 0.16.4's queue `Advance` outcome without changing persisted queue rows.

Tito updates `created_at` and `updated_at` only when those fields are present in the model's
serialized shape. Timestamp-bearing models keep automatic create/update timestamps. Models that do
not declare the fields no longer acquire invisible JSON metadata that their Rust type cannot read
back. Applications resetting or migrating from 0.16.2 should rewrite affected rows so primary and
index values exactly match the declared model shape.

### 0.16.3 rollout

Version 0.16.3 preserves timestamp behavior for models that declare `created_at` or `updated_at`
and stops adding undeclared timestamp fields. The primary and index key formats are unchanged. An
application enforcing exact typed payloads must reset, migrate, or rewrite older rows that contain
timestamp fields absent from their model before enabling that enforcement.

### 0.16.2 rollout

Publish and tag Tito 0.16.2, update the application dependency and lockfile to exactly that patch,
and replace application-side ambiguous helper calls with `prefix_end` for prefix ranges or
`key_after` for exact-row continuation. The primary, reverse-manifest, index-value, and cursor wire
formats are unchanged, so a rolling binary deployment needs no data migration. Existing missing,
orphaned, malformed, or unsafe manifest pairs now fail closed and must be repaired by an audited
rebuild or removed by the planned prelaunch reset before those records can be updated or deleted.
A clean reset/reseed recreates every manifest from the current index schema.

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

Queue events are partitioned by their business key, carry their own non-negative Unix epoch-millisecond timestamp, and remain pending until the handler explicitly acknowledges them. Tito rejects negative timestamps instead of storing an event that polling cannot reach. Tito has no automatic retry policy, retry counter, backoff, failed state, or DLQ:

Handlers return `QueueHandlerResult<T>`, an alias for `Result<QueueHandlerOutcome<T>, TitoError>`.

- `Ok(QueueHandlerOutcome::Acknowledge)` completes the current invocation.
- `Ok(QueueHandlerOutcome::Reschedule(next_event))` atomically completes the current queue row and inserts the supplied replacement row.
- `Ok(QueueHandlerOutcome::Advance(next_event))` atomically preserves the current row as completed history and inserts the supplied next typed payload.
- `Err(_)`, a handler panic, executor timeout, lost worker, or queue-commit failure produces no persisted outcome, so the exact current invocation remains pending.

Each event's own timestamp determines when it becomes runnable. Tito indexes that timestamp but never chooses or changes it. A domain that needs another invocation supplies a replacement event carrying the desired timestamp; Tito only commits the complete-and-insert transaction. `Reschedule` requires the logical event ID, partition key, owner, and payload to remain identical, so only the timestamp may change. `QueueEvent::rescheduled` constructs that replacement. `Advance` also preserves the ID, key, and owner, but requires the typed payload to change; its replacement timestamp remains application-owned. In both cases the completed row retains the exact prior payload while the new Pending row carries the replacement. Provider leases and processing deadlines belong only to domain records.

Every new invocation must serialize to at most `MAX_QUEUE_EVENT_BYTES` (1 MiB). Publication rejects
larger events before writing anything. Queue range reads preserve their public logical page size while fetching at most 16
invocations per datastore scan. Tito configures both TiKV clients with a 32 MiB decoding budget, so
the maximum 16 MiB of valid invocation bytes has at least 2x transport headroom for completed-status
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
        chrono::Utc::now().timestamp_millis(),
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

`Reschedule` is not an automatic retry policy. The application decides whether another event exists and supplies the complete event, including its timestamp. `Advance` is likewise an explicit application decision to move one logical event to a different typed payload. Tito creates no successor on its own and never interprets provider or domain state.

An optional `QueueOwner` gives an application a bounded ownership index without changing due-time
ordering. Tito writes that secondary key in the same transaction as publication, moves it atomically
on acknowledge/reschedule/advance, and removes it with the queue row. Applications that must erase one
owner's work can call `delete_by_owner_matching_in_tx`; the scan touches only that owner's Pending
or Completed keys and the predicate can preserve a currently executing lifecycle invocation. Owner
type and ID are opaque, non-empty strings bounded to 512 bytes each. They are routing/erasure
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
applies only after a short pass. Maintenance uses the completed-status/processed-time key range
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

Tito 0.16.4 adds `Advance` without changing the persisted `QueueEvent` JSON, queue key formats,
state values, or owner indexes used by 0.16.3. The handler outcome itself is not stored. Upgrading
from 0.16.3 therefore requires no queue storage migration.

For the prelaunch cutover, stop publishers and workers, use the source release to drain Pending and clear its Failed/DLQ keyspaces, verify Pending is empty, deploy the replacement environment, and then restart publication and processing. A future nonempty production environment requires an explicit, separately named bridge release before this queue contract is enabled.

## Future Events

```rust
fn events(&self) -> Vec<TitoEventConfig> {
    let in_one_hour = chrono::Utc::now().timestamp_millis() + 3_600_000;
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
queue:owner:{base64url(type)}:{base64url(id)}:{status_type}:{base64url(queue_storage_key)}
```

## Verification

Run the complete crate checks from this repository:

```sh
cargo test --all-targets
```

The tests use the in-memory engine and cover model/index writes, queue transitions, exact
millisecond scheduling and completion, configured retention, cluster ownership and leases, worker
shutdown, and ambiguous transaction outcomes. Examples are compiled but not run against a live
database. Application-level TiKV, provider-effect, backup, and restore evidence belongs to the
application's complete suite against this exact dependency candidate.

## License

Apache-2.0
