# Tito

A database layer on TiKV with indexing, transactions, and a built-in partitioned scheduled queue.

## Features

- **Data Storage**: Models with CRUD operations
- **Indexing**: Conditional ordinary and unique composite indexes for efficient queries
- **Guards**: A read by id in a transaction that writes guards its record, so a concurrent save or
  remove of that record makes one of the two transactions retry
- **Transactions**: Full ACID transactions
- **Query Builder**: Fluent API for querying by index
- **Transactional Publication**: Queue events can be written atomically with application data
- **Partitioned Queue**: Horizontal scaling via fixed business-key partitions
- **Event Timestamps**: Each event says when it becomes runnable

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
manual numeric range endpoints; never pad the original integer directly.

String index values are stored exactly. `encode_index_string(&str)` escapes `\` as `\\` and `:` as
`\:` and keeps every other character as it is: no case folding, trimming or word splitting. Values
that differ only by case or spacing, such as `ABC`, `abc` and ` abc `, are different values in
ordinary and unique indexes, and a lookup finds only the exact value. A map field's entry is
encoded as `{key}.{value}` and a numeric map entry's key goes through the same encoder. Use this
exported encoder for manual string range endpoints. Opaque record IDs are unchanged.

Ordinary indexes are declared by `indexes`; their keys include the primary record identity and may
have many owners. Identical keys of one record, such as a list that holds the same value twice, are
written and listed in its manifest once. Exclusive value ownership is declared separately by
`unique_indexes`; its key includes the model, index name, and complete indexed value but not the
claimant ID. A competing claim therefore conflicts on one real ownership key and returns
`TitoError::UniqueViolation` without exposing the indexed value. `find_one_by_unique_index` performs an exact key read and
resolves the record according to the index's value mode; in a transaction that writes, a hit guards that record like
`get` (see [Guards](#guards)). Conditional unique indexes are omitted when their model instance sets
`condition` to false.

Every ordinary and unique `TitoIndexConfig` requires an explicit `value`. There is no default:

```rust
pub enum TitoIndexValue {
    RecordCopy,
    RecordReference,
}
```

`RecordCopy` stores the full serialized record in each index entry. A record read uses that copy
without another primary lookup. Every write refreshes the copy, including changes to fields that
are not indexed. Copies use more stored and written bytes; they remove the extra record fetch.

`RecordReference` keeps entries small. An ordinary entry,
`index:{index}:{field}:{value}…:{primary key}`, stores an empty value because its key already ends
with `table:{table}:{id}`. A unique entry,
`unique-index:{table}:{index}:{field}:{value}…`, stores its owner's id as UTF-8 bytes. Record reads
resolve the original in the same transaction. `get_index_keys` returns each key with the bytes
chosen by its declaration.

The mode belongs to the index schema and must be the same for every record declaring that index.
This schema is installed on a fresh database. There is no migration or old-format compatibility
path: Tito reads exactly the declared format, so a restore brings back primary records, index
entries and manifests of this same schema together. Writers with different declarations must not
run together.

- An index list (`find_by_index`, `find_by_index_reverse`, `find_one_by_index`,
  `query_by_index(..).execute` and `.execute_reverse`, and their `_raw` forms) uses copied records
  directly and fetches referenced records in one batch in the same transaction. Order, limits,
  cursors and has-more come from the keys. A missing referenced record, a copied record that
  disagrees with its key, or a key outside the model fails with `TitoError::IndexError` naming the
  key; nothing is skipped. A copy read does not separately check whether its primary exists.
- `query_by_index(..).execute_ids(tx)` reads the entry keys only and returns
  `TitoPaginated<String>` of record ids, with the same limits and cursors as `execute`. A check
  that only needs to know whether any record holds a value uses it with `limit(Some(1))`.
- `find_one_by_unique_index` reads a copy directly or resolves a referenced owner by id. It fails
  with `IndexError` when a referenced owner is missing or the returned record no longer declares
  that unique value. Both modes take the same owner guard.
- `scan` and `scan_reverse` over an index range return each entry's key with its record's JSON,
  read the same way; a primary range returns the stored records.

## TiKV client dependency

Tito vendors its TiKV client in `vendor/tikv-client` and depends on it by path with default features
disabled. The vendored tree is [getwyrd/client-rust
55cd29b2ec9ad9f04d312e18a5ed8f6393fe219e](https://github.com/getwyrd/client-rust/commit/55cd29b2ec9ad9f04d312e18a5ed8f6393fe219e)
with one changed file, `src/pd/retry.rs` (a failed PD request waits for a replaced connection after
the reconnect throttle while keeping the existing request and connection budgets), and one added
test file; `vendor/tikv-client/UPSTREAM-PROVENANCE.json` records every file hash. The base merge
retains both [upstream primary-commit
ambiguity handling](https://github.com/tikv/client-rust/commit/337c710451e4078b8014dd6661fb84f2335c5eec)
and [expired missing-primary lock recovery](https://github.com/getwyrd/client-rust/commit/a7fba928e5712922fa95a497c52552e141afbd41),
including both native per-key error wrappers. The client uses Tonic 0.12 and the HTTP/2 0.4
dependency chain. A consumer receives the vendored client through Tito's own path dependency,
including when it patches Tito to a local checkout; it needs no separate client patch.

Primary-commit transport/status loss and native undetermined region responses remain ambiguous.
Tito returns `CommitOutcomeUnknown` without replaying or rolling back that transaction. An expired
secondary whose transaction primary was never written is resolved by a normal native read; it
does not require an application repair command.

This dependency is distributed through an immutable Tito Git release. [Cargo registry
publication](https://doc.rust-lang.org/cargo/reference/specifying-dependencies.html#multiple-locations)
would replace a Git dependency with its declared registry version, so no registry fallback is
declared for an older client that lacks these fixes.

## Connection

```rust
use tito::backend::tikv::TiKV;

let db = TiKV::connect(vec!["127.0.0.1:2379"]).await?;
```

Queue partitions are configured on the queue (`QueueConfig::new(partitions, retention)`), not on
the connection.

## Model Definition

```rust
#[derive(Default, Clone, Serialize, Deserialize)]
struct User {
    id: String,
    name: String,
    email: String,
    avatar_id: Option<String>,
}

impl TitoModelTrait for User {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn table() -> String {
        "user".to_string()
    }

    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            TitoIndexConfig {
                condition: true,
                name: "user-by-email".to_string(),
                value: TitoIndexValue::RecordCopy,
                fields: vec![TitoIndexField {
                    name: "email".to_string(),
                    r#type: TitoIndexFieldType::String,
                }],
            },
            TitoIndexConfig {
                condition: true,
                name: "user-by-avatar".to_string(),
                value: TitoIndexValue::RecordReference,
                fields: vec![TitoIndexField {
                    name: "avatar_id".to_string(),
                    r#type: TitoIndexFieldType::String,
                }],
            },
        ]
    }
}
```

A model declares its indexes and, when it has any, its unique indexes. It declares nothing about
fences: every record of every model has the same 17 fence keys (see [Guards](#guards)). A record
that holds another record's id, like `avatar_id`, keeps an index on that field so the other
record's delete can find it (see [Ids held by other records](#ids-held-by-other-records)).

## CRUD Operations

```rust
use tito::{TitoEngine, TitoModelOptions};

let users = db.clone().model::<User>(TitoModelOptions::default());

db.transaction(|tx| {
    let users = users.clone();
    let user = user.clone();
    async move { users.set(user).timestamps(false).execute(&tx).await }
})
.await?;

let user = users.get(&id).execute(None).await?;

let mut query = users.query_by_index("user-by-email");
let results = query.value(email.as_str()).limit(Some(10)).execute(None).await?;
let ids = query.execute_ids(None).await?;
```

`execute` reads copied records directly or fetches referenced records in one batch; `execute_ids`
reads the keys only and returns the record ids in either mode.

A save sets `created_at` on create and `updated_at` on every write to the current signed UTC epoch
milliseconds, but only when the model's serialized shape has those fields; `.timestamps(false)`
keeps the values the application set. Tito adds no field the model doesn't declare.

Tito reads and writes one model at a time. Applications load related records explicitly so their
domain and API boundaries determine when an additional read is required.

In a transaction that writes, every read by id guards the record it returns: `get`, `get_many`,
`get_versioned` and a hit of `find_one_by_unique_index` take a shared guard, and
`model.get_exclusive(id, &tx)` reads the record and takes the exclusive guard. A save or remove of a
guarded record by another transaction then makes one of the two retry (see [Guards](#guards)). A
read without a transaction adds no guard, and neither does a shared read in a transaction that
writes nothing. `get_exclusive` counts as a write, so its guard is taken even when its transaction
writes nothing else.

`model.get_versioned(id, transaction)` returns `TitoVersioned<T>` with the model in `value` and
an opaque `TitoRecordVersion` in `version`. The version is the positive transaction version saved
by the last model write, not the current reader's transaction version or a business timestamp. To
act only on a version read earlier, read it again with `get_versioned` in the acting transaction
and compare: the read's guard makes a concurrent save conflict at commit. A missing record is
`NotFound`; invalid storage metadata is an error.

Reads and guards do not advance the record version. Every model write installs its write-transaction
version, including same-timestamp writes; delete/recreate obtains a new version. Versions serialize
as exact opaque strings, so clients do not truncate TiKV's integers to JavaScript numbers. They
are equality tokens, not domain revisions, provider-operation fences or permission to retry an
external effect. The transaction engine must supply unique positive transaction versions and
detect write conflicts.

## Ids held by other records

Tito has no foreign keys and keeps no references. A record that holds another record's id, such as
a variant's `tax_category_id`, holds an ordinary field. The application keeps such ids honest with
two rules:

1. **A save reads every id it holds.** In the save's transaction, read each held record by id. A
   missing record fails the save with `NotFound`, and the read guards the record.
2. **A delete checks every model that can hold its id.** In the delete's transaction, read each
   holder model's index for the id and refuse while one finds a holder.

```rust
let mut holders = variants.query_by_index("product-variant-by-tax-category");
holders.value(tax_category_id.clone()).limit(Some(1));
if holders.execute_ids(Some(&tx)).await?.items.is_empty() {
    tax_categories.remove(&tax_category_id, &tx).await?;
}
```

The check reads at most two entry keys, the second only to tell whether more follow, and no
record. A save and a delete that overlap conflict: the save's read deletes one of the held record's
shared fence keys and the delete's remove deletes all 17. Tito retries one of them. The retried
delete finds the new holder, or the retried save finds the record gone. The index read has to run
in the delete's own transaction: TiKV doesn't check reads, and the retried delete reads a new
snapshot, which includes the holder that committed first. An index read is a list and adds no
guard.

`remove` deletes a record, its indexes and its unique keys, whoever holds its id; the check above is
the application's. A record whose delete takes many steps keeps its own deleting state as an
ordinary field: Tito stores no deleting marker.

## Guards

Tito's TiKV transactions are optimistic, and TiKV checks only writes at commit. A transaction that
read record A and wrote record B commits even if another transaction changed A in between. A guard
turns the read into a write that both transactions make, so TiKV cancels one of them, Tito retries
it at most 10 times, and the retry sees the other's result.

**Fence keys.** Every record of every model has 17 fence keys, its 16 shared keys and its exclusive
key:

```
fence:{table}:{id}:0 … fence:{table}:{id}:15
fence:{table}:{id}:exclusive
```

The table and id are escaped with `encode_index_string`. Tito only ever deletes a fence key and
never puts one. In TiKV a delete is a write: it conflicts with any other write of the key, and TiKV
keeps it as a Delete entry in the key's history until garbage collection removes it. Reads never
see that entry, so to every reader a fence key doesn't exist. No value is stored at a fence key, so
none is listed in a manifest, and a manifest that lists one is an integrity error.

| Operation | Fence keys deleted |
| --- | --- |
| `get`, `get_many`, `get_versioned`, a hit of `find_one_by_unique_index`, in a transaction that writes | one shared key of each record read, at commit |
| `get_exclusive` | the record's exclusive key, at commit |
| `set`, `replace_many`, `remove`, `remove_by_index` | all 17 keys of each record written or removed |
| a shared read in a transaction that writes nothing, and any read without a transaction | nothing |
| index reads (`find_by_index`, `find_by_index_reverse`, `query_by_index` with `execute`, `execute_reverse` and `execute_ids`, `find_one_by_index`) and any batched read of referenced records behind them, `scan`, `scan_reverse`, and a read that finds nothing | nothing |

So:

- two shared guards of one record conflict only when their transactions picked the same shared
  key, about one pair in 16;
- two exclusive guards of one record always conflict;
- a shared and an exclusive guard of one record don't conflict;
- a save or remove of a record conflicts with every guard of it that it overlaps.

**The shared key.** A transaction draws one random number when it begins, and every shared guard
it adds uses key `number % 16`. Two transactions that read the same twenty records conflict about
one time in 16, the same as two that read one: either they drew one key or they didn't. A retry is
a new transaction and draws again.

**When guards are added.** A transaction notes each record it reads by id. At commit, if it wrote
anything or took an exclusive guard, Tito deletes one key per noted record: the exclusive key for a
record read with `get_exclusive`, a shared key otherwise. A record read both ways holds only the
exclusive key. Any write counts: a save or remove, a queue publication or acknowledgement, a raw put
or delete. `get_exclusive` counts as a write itself: its key is deleted even when the transaction
writes nothing else, and the transaction's shared guards then apply too. A record the transaction
saved or removed itself gets nothing more, since its save already deleted all 17. A transaction that
only reads with `get`, `get_many`, `get_versioned` or a unique hit adds nothing, and neither does a
read without a transaction: there is no write to protect. Each attempt of a retried transaction
starts with nothing noted.

**Lists.** A guard covers records read one by one, not a list: an index can gain a row while no
record read changes. A list reads copies directly or fetches referenced records by id in one batched
read; neither notes a guard, so a save of a listed record never makes the list's transaction retry.
A rule over a list, such as "a company keeps at least one manager", takes `get_exclusive` on the list's parent in every
transaction that changes the list.

**Engines.** `TitoTransaction::guard(table, id, guard)` notes a record as `TitoGuard::Shared`,
`Exclusive` or `Written`, and the strongest note wins. An engine draws a new number for every
transaction it begins, and its `commit` deletes the noted keys before committing when the
transaction wrote or holds an exclusive guard. The TiKV engine and the in-memory test engine do
both.

## Storage integrity and pagination

`model.replace_many(Vec<(expected, next)>).timestamps(false).execute(&tx)` replaces existing
records of one model in the caller's transaction. Every source must still equal its complete
expected serialized record, and each replacement keeps its source ID. The method validates and
serializes the complete batch before staging any write. Unique keys may move between included
sources, including swap cycles; a final duplicate key or an owner outside the batch is refused.
Each replaced record's 17 fence keys are deleted, as a save deletes them. Each source primary is
replaced, fencing concurrent source changes, and all manifests receive the transaction's record
version. Timestamp handling matches `set`; pass `timestamps(false)` to retain application-authored
times.

A batch contains 1–100 distinct existing records. Its combined old and prepared storage keys
must fit 10,000 keys, and its prepared writes and deletion keys must fit 8 MiB. Invalid input,
stale expected records, ownership conflicts and serialization errors are returned before the batch
writes anything. A later transaction failure rolls back the whole batch with its caller's other
changes. This operation does not create records or assign temporary unique values.

Each persisted model row has a matching `reverse-index:{primary-key}` manifest containing its
required record `version` and index-key `value` array, including models with no secondary indexes.
Primary, metadata and secondary indexes commit atomically. The manifest may name ordinary `index:`
keys ending in that exact primary key and model-scoped `unique-index:` keys whose stored owner
matches that primary record. It never names a `fence:` key. A save fails with `InvalidInput` when
its manifest would hold more than 10,000 keys or more than one MiB. Updates and removals validate
the pair and every unique owner before mutating either side. A missing, orphaned, malformed, or
syntactically cross-record manifest is an integrity error; Tito does not reinterpret it as a missing
entity or follow it to an unrelated key.

The record version is not an index-schema version: Tito cannot tell whether a valid manifest lists
every index of an older schema, and it never fills a missing record version from the current read,
clock or model timestamp. A changed index schema needs a reset and reseed.

Scans fail on malformed JSON, non-UTF-8 keys, and values that do not deserialize into the requested
model. A scanned index entry follows its declared value mode. An ordinary reference entry with a
non-empty value fails with `IndexError`. A reference whose record is missing
or a copied record that disagrees with its key fails with `IndexError` naming the entry's key. Scans never silently shorten a page by
dropping corrupt rows. Forward cursors continue from
the exact key plus a NUL byte; reverse cursors use the exact key as the exclusive upper bound. Both
directions reject a cursor outside the requested half-open range, and a scan limit of zero is
invalid.

Prefix endpoints and exact-key continuation are separate operations:

```rust
let end = tito::prefix_end("index:by_store:store_id:abc:".to_string());
let after = tito::key_after("index:by_store:store_id:abc:table:item:42".to_string());
```

`prefix_end` closes a prefix range; it must not be used to continue after an exact key because
advancing the final character can skip keys such as `a20` after `a2`. Use `key_after` for that.

Secondary index values follow their explicit `TitoIndexValue` declaration (see the index
paragraphs at the top). Record copies are maintained atomically with their primary; references
resolve the primary on a record read.

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

`QueueConfig::new(partitions, retention)` selects the default queue named `queue`.
Use `.with_name("test-example")?` for another queue on the same database. Names contain 1–128
ASCII letters, digits, hyphens or underscores. Pending/completed rows, owner indexes, worker
coordination and retention are scoped to that name. Publishers and workers for one application
must use the same configuration. Cursors and mutation keys from another queue are rejected;
clearing one queue leaves the others untouched. Named queues do not isolate application models.

Queue events are partitioned by their business key, carry their own signed Unix epoch-millisecond timestamp, and remain pending until the handler explicitly acknowledges them. The full `i64` range is stored and ordered exactly; negative timestamps are reachable due times, not invalid values. Tito has no automatic retry policy, retry counter, backoff, failed state, or DLQ:

Handlers return `QueueHandlerResult<T>`, an alias for `Result<QueueHandlerOutcome<T>, TitoError>`.

- `Ok(QueueHandlerOutcome::Acknowledge)` completes the current invocation.
- `Ok(QueueHandlerOutcome::Reschedule(next_event))` atomically completes the current queue row and inserts the supplied replacement row.
- `Ok(QueueHandlerOutcome::Advance(next_event))` atomically preserves the current row as completed history and inserts the supplied next typed payload.
- `Err(_)`, a handler panic, executor timeout, lost worker, or queue-commit failure produces no persisted outcome, so the exact current invocation remains pending.

A handler whose work is one Tito transaction can complete its own invocation inside that
transaction, so the work applies exactly once. Workers hand every delivered event the storage key
of the pending row it came from in `QueueEvent::pending_key`. The field is never serialized and is
`None` on published, pulled, scanned and constructed events; code that hands such an event to a
handler itself sets it from the row's storage key. `queue.ack_in_tx(&event, &tx)` completes that
row in the caller's transaction and returns `true`, or returns `false` when the row is already
gone, and the handler then skips its work. Two deliveries of one row both write its pending key:
the later commit conflicts, and its retry finds the row gone and skips. A rolled-back transaction
leaves the row pending for redelivery. Such a handler returns `Acknowledge`; the worker's separate
acknowledgement then finds no pending row and changes nothing, while a `Reschedule` or `Advance`
returned after an in-transaction acknowledgement writes no successor. `ack_in_tx` refuses an event
without a pending key, a key outside the pending rows of its queue, and a pending row of another
event.

Each event's own timestamp determines when it becomes runnable. Tito indexes that timestamp but never chooses or changes it. A domain that needs another invocation supplies a replacement event carrying the desired timestamp; Tito only commits the complete-and-insert transaction. `Reschedule` requires the logical event ID, partition key, owner, and payload to remain identical, so only the timestamp may change. `QueueEvent::rescheduled` constructs that replacement. `Advance` also preserves the ID, key, and owner, but requires the typed payload to change; its replacement timestamp remains application-owned. In both cases the completed row retains the exact prior payload while the new Pending row carries the replacement. Provider leases and processing deadlines belong only to domain records.

Every new invocation must serialize to at most `MAX_QUEUE_EVENT_BYTES` (1 MiB). Publication rejects
larger events before writing anything. Queue range reads preserve their public logical page size while
asking the engine for at most 16 invocations per scan.

The TiKV engine bounds every read by bytes as well as rows, for models, indexes and the queue
alike. A scan is fetched as a sequence of datastore requests:

- the first request asks for at most `SCAN_RPC_FIRST_ROWS` (16) rows;
- each later request asks for as many rows as fit `SCAN_RPC_BYTES` (4 MiB) at the size of the
  largest row of the previous request, between 1 and `SCAN_RPC_MAX_ROWS` (4,096);
- a request that TiKV or the client refuses for its message size (gRPC `RESOURCE_EXHAUSTED` or
  `OUT_OF_RANGE`) is repeated with half the rows; a single row larger than the transport limit
  fails the read with the transport error.

A batch get that is refused for its size is split in halves the same way. Callers still receive
exactly the rows they asked for, in order. Tito's client decodes responses up to 32 MiB. A TiKV that
limits its responses below that (`server.max-grpc-send-msg-len`) only costs refused round trips
before a request is halved; setting the limit to 32 MiB keeps a first request of 16 rows of 1 MiB
in one round trip.

```rust
use std::sync::Arc;
use std::time::Duration;
use futures::FutureExt;
use tito::{ClusterWorkerConfig, Queue, QueueConfig, QueueEvent, QueueHandlerOutcome};
use tito::queue::run_cluster_worker;

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

run_cluster_worker(
    queue,
    ClusterWorkerConfig::from_env(),
    |event: QueueEvent<UserCreated>| async move {
        handle_user_created(event).await?;
        Ok(QueueHandlerOutcome::Acknowledge)
    }.boxed(),
    shutdown_rx,
).await;
```

Every node runs `run_cluster_worker`. Each node records a heartbeat, one node holds the coordinator
lease and assigns the partitions to the live nodes, and a node works only the partitions whose
lease it holds. `ClusterWorkerConfig::from_env()` names the node from `POD_NAME` and `POD_UID`, or
from the host name and process id, followed by a boot id.

`completed_retention` is an application policy supplied at queue construction. The elected cluster
coordinator removes bounded batches of older completed rows during its normal maintenance tick.
Tito does not hardcode the duration, publish cleanup events, or delegate queue cleanup to the
application's backup process.

`Reschedule` is not an automatic retry policy. The application decides whether another event exists and supplies the complete event, including its timestamp. `Advance` is likewise an explicit application decision to move one logical event to a different typed payload. Tito creates no successor on its own and never interprets provider or domain state.

An optional `QueueOwner` gives an application a bounded ownership index without changing due-time
ordering. Tito writes that secondary key in the same transaction as publication, moves it atomically
on acknowledge/reschedule/advance, and removes it with the queue row. `scan_by_owner` pages one
owner's Pending or Completed rows through that index. `delete_matching_in_tx` deletes, in the
caller's transaction, the rows of one status that its predicate selects, a bounded page at a time,
together with their owner keys. Owner type and ID are opaque, non-empty strings bounded to 512
bytes each. They are routing metadata, not provider identity or domain state.

A partition worker keeps pulling while its pass has more rows: after a full page it pulls the next
page at once. It waits its poll interval only after a pass ends or a pull fails.
`ClusterWorkerConfig::new` polls every 500 ms and exposes `poll_interval`.

Workers supervise each handler with a ten-minute timeout by default. Configure `handler_timeout` when a workload has a different bounded execution contract; the timeout is executor protection and never changes queue state or provider policy.
Worker shutdown stops new pulls and handler starts, then drains every handler that already started
and applies its outcome before joining. The configured handler timeout bounds that drain. A handler
error, panic, timeout, lost worker, lost cluster partition lease, or queue-outcome storage failure
leaves the exact invocation Pending.

Each event's partition is `queue_partition_for_key(&event.key, partition_count)`: the 64-bit FNV-1a
hash of the key's UTF-8 bytes (offset basis `0xcbf29ce484222325`, prime `0x100000001b3`) modulo
the partition count. The hash is part of the storage contract and does not depend on the Rust
release, so a key's events stay in one partition across upgrades while the partition count is
unchanged. Publishers and workers must use the same partition count.

Partition polling is fair across due rows. Pending storage keys are ordered as
`queue:pending:{partition:04}:{encode_index_integer(timestamp)}:{enqueue_version:020}:{event_id}`. The enqueue version is
the datastore transaction's globally ordered start version; it is internal ordering metadata, not
an invocation ID, provider identity, lease, or domain state. Each worker keeps an in-memory cursor
for one bounded pass. The first pull freezes both the runnable timestamp boundary and transaction-version
horizon. Later pulls advance by raw storage row (including malformed rows) and jump over an entire
due-time bucket when they encounter a row enqueued at or after that horizon. A handler can therefore
create immediate same-time events without keeping later timestamp buckets behind them forever.
After the pass is exhausted, polling wraps to the oldest runnable key and those events become
eligible. The cursor is executor state only: it is not persisted and does not encode retry policy.

Completed invocation history uses the retention supplied by the application in `QueueConfig`.
Retention and cluster lease durations convert to milliseconds, rounding a fractional millisecond up
and saturating instead of wrapping. The elected cluster coordinator deletes older rows in bounded
passes. A full pass yields and continues immediately until the expired range is caught up; the
30-second interval applies only after a short pass. Maintenance uses the
completed-status/processed-time key range directly, never inspects pending work, and has no retry,
recovery, or provider semantics. Malformed values inside an expired completed-row key are also
removed so corrupt terminal history cannot pin newer cleanup work.

### Transaction retry safety

Tito may replay a transaction closure after an explicitly retryable, determined datastore failure. TiKV's `UndeterminedError` is different: the commit may already be durable. Tito returns `TitoError::CommitOutcomeUnknown` and never replays that closure. The caller reconciles against authoritative domain state; an acknowledgement either committed or the unchanged Pending invocation is delivered again.

Cancellation removes a transaction from the active registry. Before native commit starts, cleanup
rolls back pending work. Once native commit has started, cancellation only releases the retained
handle so its heartbeat can stop; cleanup never replays the closure or rolls back an uncertain
commit. Callers still reconcile any interrupted command against its authoritative identity.

## Event Key Format

```
queue:pending:{partition:04}:{encode_index_integer(timestamp)}:{enqueue_version:020}:{event_id}
queue:completed:{encode_index_integer(processed_at)}:{encode_index_integer(event_timestamp)}:{event_id}
queue:owner:{base64url(type)}:{base64url(id)}:{status_type}:{base64url(queue_storage_key)}
```

A queue event is stored as JSON with camelCase field names. Its `status` is `{"type":"pending"}` or
`{"type":"completed"}`, its optional owner is `{"type": …, "id": …}`, and events and owners reject
unknown fields. `timestamp`, `processedAt`, cluster heartbeats and lease deadlines are signed UTC
epoch milliseconds. An event id is its publication time in microseconds, zero-padded to 20 digits,
then a hyphen and a UUID; `created_at_millis()` reads that prefix as milliseconds.

## Verification

Run the complete verification command from this repository. It prepares an explicitly owned
disposable native PD/TiKV runtime and supplies `TITO_NATIVE_PD_URI` as its loopback `host:port` and
`TITO_NATIVE_RUN_ID` as its `run-` identity; the native cases still refuse undeclared infrastructure.

```sh
python3 scripts/verify.py
```

The wrapper runs the complete `cargo test --locked --all-targets` phase, the vendored PD owner's
eight reconnect regressions and two retained controls, and the actual three-member PD leadership
transfer witness. Every phase is required. See [complete verification](docs/verification.md) for
prerequisites, source holds, native ownership, cleanup and retained evidence.

The library cases use the in-memory engine and cover model/index writes, exact index values, index
reference entries that hold no record data (an empty ordinary entry, an owner id in a unique entry), lists
that read copies directly or fetch referenced records in one batched read in index order, `execute_ids` reading keys only,
cursors and limits across pages, a missing record behind an entry, moved and removed entries, the
fence keys each save, remove and batch deletes and each guard a commit adds, queue
transitions, fixed partitions, exact signed millisecond scheduling and completion, exact inclusive
cutoff buckets and signed extrema, configured retention, cluster ownership and leases, worker paging
and shutdown, and ambiguous transaction outcomes. `tikv_native_orphan_lock_test` creates a real secondary lock after a definite primary
prewrite conflict, waits for its observed native TTL, proves the missing-primary branch through a
normal Tito read, and verifies recovery preserves the winning primary. It removes only its own
two UUID-scoped keys. `tikv_native_large_rows_test` reads 40 records of 1 MiB, more than the
client's 32 MiB decoding budget, through scans, index pages (entry keys, then one batch get of
their records) and a batch get, and pulls 16 queue events of almost 1 MiB. It also saves a record
of 1 MiB with five ordinary index entries and one unique entry and reads every entry back by get
and each ordinary entry also by scan and batch get: every ordinary entry stores an empty value and
the unique entry its owner's id, while lists and the unique lookup return the record and
`execute_ids` returns its id. `tikv_native_index_values_test` covers both explicit modes together:
stored bytes, ordinary and unique ranges, forward/reverse pages, copied reads without a primary
fetch, key-only ID reads, validation of malformed copies, non-indexed updates and timestamps,
rollback, changed and removed entries, unique claims and batch ownership swaps, shared guards and
unguarded lists. Ordinary reference record reads reject non-empty entry values, while key-only ID
reads retain their value-free behavior. `tikv_native_queue_test` proves that a cluster-worker
handler completes its own pending row inside its transaction, that a duplicate delivery conflicts
on that row and its retry skips, that a rolled-back handler leaves the row pending, and that
`ack_in_tx` refuses events and keys it does not own without writing. `tikv_native_fence_test`
proves on real TiKV:

- every read by id in a transaction that writes conflicts with a save of its record, while index
  reads (records and `execute_ids`), scans and reads that find nothing don't;
- two transactions reading the same twenty records conflict in 4 to 75 of 480 pairs, about one time
  in 16;
- two exclusive reads of one record always conflict, and a shared and an exclusive read commit
  together;
- a save and a remove conflict with shared and exclusive reads in either commit order;
- a transaction that only reads by id adds no guard, `get_exclusive` counts as a write, and a retry
  starts without the guards of the attempt before;
- a new holder and a delete of the record it names conflict in either order: the retried delete
  refuses, or the retried save finds the record gone;
- unique claims conflict at commit, and unique values differing only by case or spacing coexist.

The in-memory engine fails a commit with `TitoError::Retryable` when another transaction committed
one of its keys after it started, as TiKV does, so the library cases cover these conflicts too; the
native cases prove them on TiKV. Examples are compiled but not run against a live database.

Application-level primary-commit response loss, provider effects, backup and restore remain the
application's complete-suite responsibility against the exact released Tito revision. The native
client proof must cover both connection loss and gRPC status loss after an actual primary
acknowledgement, with an unknown outcome, no rollback, and all accepted domain writes intact.

## Release notes

0.18.8 removes `get_for_update`, `TitoGuard::ForUpdate`, `rebuild_indexes_for_restore`,
`delete_by_owner_matching_in_tx`, the standalone worker (`run_worker`, `WorkerConfig`) and unused
exports, and stops storing `updated_at` on queue cluster lease and partition records.

## License

Apache-2.0
