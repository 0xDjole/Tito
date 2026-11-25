# Tito

A database layer on TiKV with indexing, transactions, and a built-in partitioned scheduled queue.

## Features

- **Models**: `create`, `update` and `remove` write a record with its index entries in the caller's transaction,
  and read nothing
- **Reads are notes**: a `Read<T>` names what it reads and its guard; `tx.batch_get` sends a tuple of up to eight of
  them, or a `Vec` of any number, keys and index pages together, in one trip, and answers in the order asked
- **One kind of index**: `indexes()` returns the record's `TitoIndex { name, fields, value, unique }` entries, which
  hold the whole record (`Full`), nothing (`KeysOnly`) or named fields (`Part`)
- **Guards**: a read for share or for update in a writing transaction conflicts with any save of its record; a plain
  read guards nothing
- **Transactions**: TiKV optimistic transactions, retried on conflicts
- **Partitioned queue**: scheduled invocations partitioned by business key, published in the caller's transaction and
  deleted when their outcome commits

This shape is installed on a fresh database. There is no migration or old-format compatibility path: Tito reads
exactly this format, so data written by earlier revisions (reverse indexes, `unique-index:` and `holder-index:` keys,
record versions) can't be read. Writers with different declarations must not run together.

## TiKV client dependency

Tito vendors its TiKV client in `vendor/tikv-client` and depends on it by path with default features
disabled. The vendored tree is [getwyrd/client-rust
55cd29b2ec9ad9f04d312e18a5ed8f6393fe219e](https://github.com/getwyrd/client-rust/commit/55cd29b2ec9ad9f04d312e18a5ed8f6393fe219e)
with five changed files and one added test file: `src/pd/retry.rs` (a failed PD request waits for a replaced
connection after the reconnect throttle while keeping the existing request and connection budgets);
`src/transaction/buffer.rs` (a batch read remembers the keys it did not find, as a single read does, and leaves
every key its transaction wrote or locked as it was); and `src/transaction/transaction.rs` with its exports in
`src/transaction/mod.rs` and `src/lib.rs` (`batch_get_and_scan`: a batch read of keys and the first page of several
scans, sent at the same moment and each answered through the transaction's buffer as a lone batch read or scan is).
`vendor/tikv-client/UPSTREAM-PROVENANCE.json` records every file hash. The base merge
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

Queue partitions are configured on the queue (`QueueConfig::new(partitions)`), not on the connection. Model records
are not partitioned.

## Model Definition

```rust
use serde::{Deserialize, Serialize};
use tito::{TitoIndex, TitoIndexField, TitoIndexValue, TitoModelTrait};

#[derive(Clone, Serialize, Deserialize)]
struct User {
    id: String,
    org_id: String,
    email: String,
    name: String,
    created_at: i64,
    role_ids: Vec<String>,
}

impl TitoModelTrait for User {
    fn table() -> String {
        "user".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }

    fn indexes(&self) -> Vec<TitoIndex> {
        let mut indexes = vec![
            TitoIndex {
                name: "user-by-email",
                fields: vec![TitoIndexField::Text(self.email.clone())],
                value: TitoIndexValue::Full,
                unique: true,
            },
            TitoIndex {
                name: "user-by-org",
                fields: vec![
                    TitoIndexField::Text(self.org_id.clone()),
                    TitoIndexField::Number(self.created_at),
                ],
                value: TitoIndexValue::Part(&["id", "name"]),
                unique: false,
            },
        ];
        for role_id in &self.role_ids {
            indexes.push(TitoIndex {
                name: "user-by-role",
                fields: vec![TitoIndexField::Text(role_id.clone())],
                value: TitoIndexValue::KeysOnly,
                unique: false,
            });
        }
        indexes
    }
}
```

A model type implements `TitoModelTrait` and is `Clone + Serialize + DeserializeOwned + Unpin + Send + Sync +
'static` (`TitoModelConstraints`). `TitoModel::new(engine)` and the engine's `.model::<T>()` make a model; neither
takes options.

`indexes()` returns this record's entries:

- An entry that doesn't apply to the record is not pushed; a field that holds a list is a loop that pushes one entry
  per element. Two entries of one record that make the same key are written once.
- `fields` are values the model hands over, `TitoIndexField::Text(…)` or `TitoIndexField::Number(i64)`, never field
  names, so a renamed field is a compile error. No index lists the id: Tito adds it.
- Every index has at least one field, a `Text` value is never empty, a `unique` index holds `Full` entries, a `Part`
  index names at least one path, and an index name is non-empty and holds neither `:` nor `\`. A write that breaks a
  rule fails with `TitoError::IndexError` before it writes anything.
- Index names are global: keys carry no table, so two models never share a name.

```
table:{table}:{id}                              the record
index:{name}:{value}:…:{value}:{id}:            an index entry
index:{name}:{value}:…:{value}:                 an entry of a unique index
fence:{table}:{id}:0 … fence:{table}:{id}:15    the shared fence keys
fence:{table}:{id}:exclusive                    the exclusive fence key
```

- A `Text` value is stored exactly: `encode_index_string` escapes `\` as `\\` and `:` as `\:` and keeps every other
  character, with no case folding, trimming or word splitting, so `ABC`, `abc` and ` abc ` are three values. The id
  at the end of an entry's key and the table and id of a fence key are escaped the same way. Every segment of an
  index key, the last one too, is followed by `:`.
- A `Number` is `encode_index_integer(i64)`: the sign bit flipped, written as exactly 20 decimal digits, so byte
  order is signed integer order across negatives, zero and both extremes. Hand-built range endpoints use these two
  exported encoders.
- A `Full` entry holds the record's bytes, the same as its row; such a record must serialize to a JSON object. A
  `KeysOnly` entry holds nothing. A `Part(&[…])` entry holds `[id, {…}]`: each named path copied with its parents
  (`"contact.email"` gives `{"contact":{"email":…}}`), a path through a list copying that field of each element, and a
  missing field left out.

## Reads

```rust
use tito::{Guard, TitoError};

let users = db.clone().model::<User>();

let (user, team) = db
    .transaction(|tx| {
        let users = users.clone();
        let id = id.clone();
        let team_ids = team_ids.clone();
        async move {
            tx.batch_get((users.read(&id, Guard::None), users.read_many(team_ids, Guard::Share)))
                .await
        }
    })
    .await?;
```

A read is a note, `Read<T>`, that the model makes and `tx.batch_get` fetches. Making a note fetches nothing.

| Note | Answer |
| --- | --- |
| `read(id, guard)` | the record; `TitoError::NotFound` when it is missing |
| `read_maybe(id, guard)` | `Option<T>` |
| `read_many(ids, guard)` | the records found, in the order asked; missing ids are left out |
| `read_unique(index, values, guard)` | the record holding that unique value, read from its entry's copy; `NotFound` when none does |
| `read_unique_maybe(index, values, guard)` | `Option<T>` |
| `query_by_index(…).read()`, `.read_part::<P>()`, `.read_ids()` | a page of an index ([Index queries](#index-queries)) |

`Read::map` converts an answer, for a repository that turns its stored type into its domain type; a conversion that
fails is corrupt storage and returns `TitoError::DeserializationFailed`.

`tx.batch_get(batch)` takes a note, a tuple of 2 to 8 batches, or a `Vec` of batches, and answers in the order given.
A `Vec<B>` answers `Vec<B::Output>` with `B::Error`, for a list whose length the request decides (one
`read_unique_maybe` note per audience or per stock location); an empty `Vec` answers an empty `Vec` and adds nothing,
so a batch of nothing else makes no trip. A `Vec` can sit inside a tuple and hold tuples. Every key of every note goes
in one TiKV batch read, and the first request of every index page goes out at the same moment, so a batch costs one
trip. A key this transaction already read or wrote is answered from its memory, so a batch whose keys are
all known sends nothing, and a key a batch read did not find stays known as missing. A read that depends on another
goes in the next call. A tuple's error is its first element's, and every other element's error converts into it
(`B::Error: Into<A::Error>`). The `Batch` trait (`requests`, `answer`) lets an application type be one element of a
batch: its `requests` add its own notes, and its `answer` reads them and may make a second trip of its own.

Tito checks what it decodes: a record's id equals the id asked, or the id in its key, and a copy agrees with its key,
which means the record it holds makes that exact entry under its own declarations. A failure is
`TitoError::DeserializationFailed` or `TitoError::IndexError` naming the key; nothing is skipped.

## Writes

```rust
db.transaction(|tx| {
    let users = users.clone();
    let user = user.clone();
    async move { users.create(&user, &tx).await }
})
.await?;

db.transaction(|tx| {
    let users = users.clone();
    let id = id.clone();
    async move {
        let old = tx.batch_get(users.read(&id, Guard::None)).await?;
        let new = User {
            name: "Ana".to_string(),
            ..old.clone()
        };
        users.update(&old, &new, &tx).await
    }
})
.await?;
```

- `create(&record, &tx)` writes the record key and each unique entry as an insert, and every other entry as a put. An
  id or a unique value already taken refuses the commit: the transaction ends, after its closure returned `Ok`, in
  `TitoError::RecordExists { model, id }` or `TitoError::UniqueViolation { model, index, values }`, never retried.
  Of two creates of one value at once, one commits and the other is refused: TiKV first reports the write conflict,
  so the second retries, and its retry is refused. A second create of one id or one unique value in one transaction
  is refused at once with the same errors. A refusal stays with its transaction: the commit ends in that same error
  even when the closure goes on and returns `Ok`, so no part of a refused create or update is stored.
- `update(&old, &new, &tx)` writes `new` and its entries. It deletes the entries that are gone, inserts each new unique
  entry (so it can end in `UniqueViolation`), rewrites every `Full` copy and a changed `Part`, and leaves an unchanged
  `KeysOnly` key alone. `old` names the record by its id, and `new` keeps that id
  (`TitoError::UpdateFailed("An update of record '{key}' must keep its id")`).
- `remove(&record, &tx)` removes the record and every entry.
- None of the three reads TiKV. A transaction remembers, for every record it read or wrote, the record's index
  entries as they are stored (computed from the record as read, or as a create or an update wrote it). An update and
  a remove take the entries to replace or delete from that memory, never from the copy handed over, so a copy rebuilt
  from a domain type that is not byte-exact saves right and can't leave a stale entry behind; a writer that changed
  the record since the read is caught by the commit conflict. A record this transaction did not read or write is
  refused (`TitoError::UpdateFailed` or `TitoError::DeleteFailed`, "Record '{key}' was not read in this
  transaction"). A record counts as read when a note or an index page returned it whole (by id, by a unique value, a
  `Full` page); a `Part` or id page doesn't count. A create or an update in the same transaction is its latest state,
  and a remove forgets it.
- Each of the three deletes the record's 17 fence keys ([Guards](#guards)).
- Tito keeps no reverse index, no record versions and no timestamps: a record is stored as it is handed over.
- In one transaction a unique value passes from one record to another when the record giving it up is updated
  first. A swap of two records' values needs a temporary value: whichever update runs first inserts a value the other
  still holds, and the commit ends in `UniqueViolation`.

One save writes at most `MAX_SAVE_BYTES` (64 MiB): its record key and bytes and every entry's key and value. A larger
save fails with `TitoError::InvalidInput` and the message `A save exceeds the 67108864-byte limit` before it writes
anything.

## Index queries

```rust
use tito::{TitoIndexBound, TitoIndexField};

let page = users
    .query_by_index("user-by-org")
    .value(TitoIndexField::Text(org_id.clone()))
    .lower(TitoIndexBound::Included(vec![TitoIndexField::Number(since)]))
    .descending()
    .limit(Some(20))
    .cursor(cursor)
    .execute_part::<UserCard>(None)
    .await?;
```

- `value(…)` adds an equal value, field by field from the first. The values are a prefix of the index's fields; a
  query with none reads the whole index in key order. A unique index read with all its values is a note
  (`read_unique`), never a query.
- `lower` and `upper` bound the fields after the values, `Included` or `Excluded`. A bound may end with the record id
  as `Text`, for pages before or after one record. No bound leaves that side open. `Text` values and ids order by
  their bytes followed by `:`, so a value that extends another with a character before `:` in byte order (a space, a
  digit, most punctuation) sorts before it: `Item 2` comes before `Item`, and the id `a20` before `a2`. Bounds follow
  that same order: an `Included` lower bound of `Item` starts at `Item` and leaves `Item 2` out, an `Excluded` upper
  bound of `Item` takes `Item 2`. Numbers and fixed-length ids (UUIDs) are unaffected.
- `descending()` reads the keys from the end. `limit(Some(n))` returns at most `n` items, `None` every item, and
  `Some(0)` is refused. A page's `cursor` is set when more items follow; it is opaque, fits only the query that made
  it, and a cursor outside the query's range is refused.
- `execute` reads whole records, from `Full` entries only; `execute_part::<P>` reads `Part` entries into `P`, a strict
  struct with no defaults and `deny_unknown_fields`, so a field renamed on the record but not in the list fails
  loudly; `execute_ids` reads the ids from all three. Anything else fails with `TitoError::IndexError` naming the key.
- `execute*(None)` runs in a transaction of its own and `execute*(Some(&tx))` in the caller's. Inside a batch the same
  pages are notes: `read`, `read_part::<P>` and `read_ids`.
- Order, limits, cursors and has-more come from the keys. Index pages add no guard.

`model.scan_records(limit, cursor, &tx)` pages every record of the model in key order, for rebuilds and refills; a
limit of zero is refused.

## Rebuilding an index

`model.rebuild_index("user-by-role")` removes every entry under `index:user-by-role:`, in transactions of 1,000 keys,
then writes the index again from every record's `indexes()`, a page of at most 100 records or 8 MiB of entries in
each transaction. A unique value that two records claim fails it with `UniqueViolation`, and a record whose bytes
can't be read fails it, as any read does. Lists through the index miss entries while it runs. A changed index
definition is followed by a rebuild of that index; a stored type that no longer deserializes can't have its entries
computed and needs a reset.

## Ids held by other records

Tito has no foreign keys and keeps no references. A record that holds another record's id, such as a variant's
`tax_category_id`, holds an ordinary field and declares an index entry on it, usually `KeysOnly`. The application
keeps such ids honest with two rules:

1. **A save reads every id it holds**, with `Guard::Share`, in the save's transaction. A missing record fails the
   save, and the read guards the record.
2. **A delete checks every model that can hold its id**, in the delete's transaction, and refuses while one finds a
   holder.

```rust
let holders = variants
    .query_by_index("product-variant-by-tax-category")
    .value(TitoIndexField::Text(tax_category.id.clone()))
    .limit(Some(1))
    .execute_ids(Some(&tx))
    .await?;
if holders.items.is_empty() {
    tax_categories.remove(&tax_category, &tx).await?;
}
```

The check reads at most two entry keys, the second only to tell whether more follow, and no record. A save and a
delete that overlap conflict: the save's read deletes one of the held record's shared fence keys and the remove
deletes all 17. Tito retries one of them: the retried delete finds the new holder, or the retried save finds the
record gone. The index read has to run in the delete's own transaction, since TiKV doesn't check reads and the
retried delete reads a new snapshot. `remove` checks no holders itself.

## Guards

Tito's TiKV transactions are optimistic, and TiKV checks only writes at commit. A transaction that
read record A and wrote record B commits even if another transaction changed A in between. A guard
turns the read into a write that both transactions make, so TiKV cancels one of them, Tito retries
it at most 10 times, and the retry sees the other's result.

**Fence keys.** Every record of every model has 17 fence keys, its 16 shared keys and its exclusive
key (above). Tito only ever deletes a fence key and never puts one. In TiKV a delete is a write: it conflicts with
any other write of the key, and TiKV keeps it as a Delete entry in the key's history until garbage collection
removes it. Reads never see that entry, so to every reader a fence key doesn't exist.

| Operation | Fence keys deleted |
| --- | --- |
| a note with `Guard::Share` that finds its record, in a transaction that writes | one shared key of that record, at commit |
| a note with `Guard::Update` that finds its record | the record's exclusive key, at commit |
| `create`, `update`, `remove` | all 17 keys of the record |
| a note with `Guard::Share` in a transaction that writes nothing | nothing |
| a note with `Guard::None`, an index page, `scan_records`, a note that finds nothing | nothing |

So:

- two shared guards of one record conflict only when their transactions picked the same shared
  key, about one pair in 16;
- two exclusive guards of one record always conflict;
- a shared and an exclusive guard of one record don't conflict;
- a save or remove of a record conflicts with every guard of it that it overlaps.

Unlike SQL, where `FOR UPDATE` and `FOR SHARE` locks of one row block each other, a read for update
and a read for share of one record don't conflict here; only a save conflicts with both. Nothing
waits: a conflict shows at commit, and Tito retries the loser.

**The shared key.** A transaction draws one random number when it begins, and every shared guard
it adds uses key `number % 16`. Two transactions that read the same twenty records conflict about
one time in 16, the same as two that read one: either they drew one key or they didn't. A retry is
a new transaction and draws again.

**When guards are added.** A transaction notes each record a `Share` or `Update` note returns; a `None` note notes
nothing and cancels no guard. At commit, if the transaction wrote anything or holds an exclusive guard, Tito deletes
one key per noted record: the exclusive key for a record read with `Guard::Update`, a shared key otherwise. A record
read both ways holds only the exclusive key. Any write counts: a create, update or remove, a queue publication or
acknowledgement, a raw put or delete. `Guard::Update` counts as a write itself: its key is deleted even when the
transaction writes nothing else, and the transaction's shared guards then apply too. A fence key the transaction
already deleted itself, as a save of that record does, is not deleted again. A transaction that only reads for share
adds nothing: there is no write to protect. Each attempt of a retried transaction starts with nothing noted.

**Lists.** A guard covers records read one by one, not a list: an index can gain a row while no
record read changes. Index pages note no guard, so a save of a listed record never makes the list's transaction
retry. A rule over a list, such as "a company keeps at least one manager", reads the list's parent with
`Guard::Update` in every transaction that changes the list.

## Storage integrity and pagination

Reads fail on malformed JSON, non-UTF-8 keys and values that don't deserialize into the requested model; a page is
never shortened by dropping a corrupt row. Forward cursors continue after the exact key; descending
cursors use the exact key as the exclusive upper bound; both refuse a cursor outside the requested range.

Prefix endpoints and exact-key continuation are separate operations:

```rust
let end = tito::prefix_end("index:product-by-store:abc:".to_string());
let after = tito::key_after("index:product-by-store:abc:42:".to_string());
```

`prefix_end` closes a prefix range; it must not be used to continue after an exact key because
advancing the final character can skip keys such as `a20` after `a2`. Use `key_after` for that.

The TiKV engine bounds every read by bytes as well as rows, for models, indexes and the queue
alike. A scan is fetched as a sequence of datastore requests:

- the first request asks for at most `SCAN_RPC_FIRST_ROWS` (16) rows;
- each later request asks for as many rows as fit `SCAN_RPC_BYTES` (4 MiB) at the size of the
  largest row of the previous request, between 1 and `SCAN_RPC_MAX_ROWS` (4,096);
- a request that TiKV or the client refuses for its message size (gRPC `RESOURCE_EXHAUSTED` or
  `OUT_OF_RANGE`) is repeated with half the rows; a single row larger than the transport limit
  fails the read with the transport error.

A batch read that is refused for its size is split in halves the same way. Tito's client decodes responses up to
32 MiB. A TiKV that limits its responses below that (`server.max-grpc-send-msg-len`) only costs refused round trips
before a request is halved; setting the limit to 32 MiB keeps a first request of 16 rows of 1 MiB in one round trip.

A read is retried only for an error that can go away: a key that is locked, a write conflict, or a key error TiKV
marks retryable. A batch read or a scan gets its key errors back together, and each is judged the same way; any
other key error fails the read at once.

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

`QueueConfig::new(partitions)` selects the default queue named `queue`. Use `.with_name("test-example")?` for another
queue on the same database. Names contain 1–128 ASCII letters, digits, hyphens or underscores. Pending rows, owner
indexes and worker coordination are scoped to that name. Publishers and workers for one application must use the
same configuration. Cursors and mutation keys from another queue are rejected; clearing one queue leaves the others
untouched. Named queues do not isolate application models.

Queue events are partitioned by their business key, carry their own signed Unix epoch-millisecond timestamp, and
stay in the queue until the outcome of a handler commits. The full `i64` range is stored and ordered exactly;
negative timestamps are reachable due times, not invalid values. Tito has no automatic retry policy, retry counter,
backoff, failed state or DLQ, and the queue keeps no finished row: it holds only work still to run, so it needs no
retention setting or cleanup.

Handlers return `QueueHandlerResult<T>`, an alias for `Result<QueueHandlerOutcome<T>, TitoError>`.

- `Ok(QueueHandlerOutcome::Acknowledge)` deletes the current row and its owner-index row.
- `Ok(QueueHandlerOutcome::Reschedule(next_event))` deletes them and, in the same transaction, inserts the supplied
  replacement row.
- `Ok(QueueHandlerOutcome::Advance(next_event))` deletes them and, in the same transaction, inserts the supplied next
  typed payload.
- `Err(_)`, a handler panic, executor timeout, lost worker, or queue-commit failure produces no persisted outcome, so
  the exact current invocation remains pending.

A handler whose work is one Tito transaction can complete its own invocation inside that
transaction, so the work applies exactly once. Workers hand every delivered event the storage key
of the pending row it came from in `QueueEvent::pending_key`. The field is never serialized and is
`None` on published, pulled, scanned and constructed events; code that hands such an event to a
handler itself sets it from the row's storage key. `queue.ack_in_tx(&event, &tx)` deletes that
row in the caller's transaction and returns `true`, or returns `false` when the row is already
gone, and the handler then skips its work. Two deliveries of one row both write its pending key:
the later commit conflicts, and its retry finds the row gone and skips. A rolled-back transaction
leaves the row pending for redelivery. Such a handler returns `Acknowledge`; the worker's separate
acknowledgement then finds no pending row and changes nothing, while a `Reschedule` or `Advance`
returned after an in-transaction acknowledgement writes no successor. `ack_in_tx` refuses an event
without a pending key, a key outside the pending rows of its queue, and a pending row of another
event.

Each event's own timestamp determines when it becomes runnable. Tito indexes that timestamp but never chooses or
changes it. A domain that needs another invocation supplies a replacement event carrying the desired timestamp; Tito
only commits the delete-and-insert transaction. `Reschedule` requires the logical event ID, partition key, owner, and
payload to remain identical, so only the timestamp may change. `QueueEvent::rescheduled` constructs that replacement.
`Advance` also preserves the ID, key, and owner, but requires the typed payload to change; its replacement timestamp
remains application-owned. Nothing keeps the prior payload. Provider leases and processing deadlines belong only to
domain records.

Every new invocation must serialize to at most `MAX_QUEUE_EVENT_BYTES` (1 MiB). Publication rejects
larger events before writing anything. Queue range reads preserve their public logical page size while
asking the engine for at most 16 invocations per scan.

```rust
use std::sync::Arc;
use futures::FutureExt;
use tito::{ClusterWorkerConfig, Queue, QueueConfig, QueueEvent, QueueHandlerOutcome};
use tito::queue::run_cluster_worker;

let queue = Arc::new(Queue::new(db.clone(), QueueConfig::new(4)));

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

Every node runs `run_cluster_worker`, and nothing is elected. A partition's row names only its holder, or nobody,
and is written only when the holder changes. A node's heartbeat row is its lease on every partition it holds: a node
is alive while its heartbeat is less than `lease_ttl` (30 seconds) old. Every `heartbeat_interval` (5 seconds) each
node, by itself, writes its heartbeat, reads all heartbeats, works out its own share among the live nodes (sorted by
node id, a contiguous range of partitions each), takes each row of its share that is missing, names nobody or names a
node that isn't alive, and stops the worker of each row it holds outside its share and of each row that now names
another node. A row is given back only after its worker has finished its current event and written that event's
outcome. Any node deletes heartbeat rows older than `stale_node_ttl` (5 minutes). A node that stops cleanly gives its
partitions back and deletes its heartbeat row in one transaction; a node that crashes writes nothing, so its rows
wait until its heartbeat is `lease_ttl` old. A worker knows it holds its partition from its node's memory alone:
before each event it checks that its node's step hasn't told it to stop and that its node's last successful
heartbeat is less than `lease_ttl` minus two heartbeat intervals old. After the handler there is no check: its
outcome is applied even when the partition moved while it ran, and when two nodes ran the same event, the second
outcome finds the row gone and changes nothing. `ClusterWorkerConfig::from_env()` names the node from `POD_NAME` and
`POD_UID`, or from the host name and process id, followed by a boot id.

An optional `QueueOwner` gives an application a bounded ownership index without changing due-time
ordering. Tito writes that secondary key in the same transaction as publication, moves it atomically
on reschedule and advance, and removes it with the queue row. `scan_by_owner` pages one owner's rows through that
index. `delete_matching_in_tx` deletes, in the caller's transaction, the rows that its predicate selects, a bounded
page at a time, together with their owner keys. Owner type and ID are opaque, non-empty strings bounded to 512
bytes each. They are routing metadata, not provider identity or domain state.

A partition worker keeps pulling while its pass has more rows: after a full page it pulls the next page at once. It
starts its next pass at once after a pass that finished at least one row (acknowledged, rescheduled or advanced), and
waits its poll interval only after a pass that finished none or a failed pull. `ClusterWorkerConfig::new` polls every
500 ms and exposes `poll_interval`.

Workers supervise each handler with a ten-minute timeout by default. Configure `handler_timeout` when a workload has a different bounded execution contract; the timeout is executor protection and never changes queue state or provider policy.
Worker shutdown stops new pulls and handler starts, then drains every handler that already started
and applies its outcome before joining. The configured handler timeout bounds that drain. A handler
error, panic, timeout, lost worker, lost partition, or queue-outcome storage failure leaves the exact invocation
pending.

Each event's partition is `queue_partition_for_key(&event.key, partition_count)`: the 64-bit FNV-1a
hash of the key's UTF-8 bytes (offset basis `0xcbf29ce484222325`, prime `0x100000001b3`) modulo
the partition count. The hash is part of the storage contract and does not depend on the Rust
release, so a key's events stay in one partition across upgrades while the partition count is
unchanged. Publishers and workers must use the same partition count.

Partition polling is fair across due rows. Pending storage keys are ordered by partition, signed-sortable due time,
enqueue version and event id. The enqueue version is the datastore transaction's globally ordered start version; it
is internal ordering metadata, not an invocation ID, provider identity, lease, or domain state. Each worker keeps an
in-memory cursor for one bounded pass. The first pull freezes both the runnable timestamp boundary and
transaction-version horizon. Later pulls advance by raw storage row (including malformed rows) and jump over an
entire due-time bucket when they encounter a row enqueued at or after that horizon. A handler can therefore create
immediate same-time events without keeping later timestamp buckets behind them forever. After the pass is exhausted,
polling wraps to the oldest runnable key and those events become eligible. The cursor is executor state only: it is
not persisted and does not encode retry policy.

Heartbeat ages convert their configured durations to milliseconds, rounding a fractional millisecond up and
saturating instead of wrapping. Exact supplied due instants are never rounded.

### Transaction retry safety

Tito may replay a transaction closure after an explicitly retryable, determined datastore failure. TiKV's `UndeterminedError` is different: the commit may already be durable. Tito returns `TitoError::CommitOutcomeUnknown` and never replays that closure. The caller reconciles against authoritative domain state; an acknowledgement either committed or the unchanged Pending invocation is delivered again.

Cancellation removes a transaction from the active registry. Before native commit starts, cleanup
rolls back pending work. Once native commit has started, cancellation only releases the retained
handle so its heartbeat can stop; cleanup never replays the closure or rolls back an uncertain
commit. Callers still reconcile any interrupted command against its authoritative identity.

## Event Key Format

```
{queue}:pending:{partition:04}:{encode_index_integer(timestamp)}:{enqueue_version:020}:{event_id}
{queue}:owner:{base64url(type)}:{base64url(id)}:{base64url(pending storage key)}
tito:{queue}:cluster:nodes:{encode_index_string(node_id)}
tito:{queue}:cluster:partitions:{partition:04}
```

A queue event is stored as JSON with camelCase field names: `id`, `key`, an optional `owner` (`{"type": …, "id": …}`),
`payload` and `timestamp`. Events and owners reject unknown fields, so a row with a `status` or `processedAt` field
fails to read. `timestamp`, heartbeats and lease ages are signed UTC epoch milliseconds. An event id is its
publication time in microseconds, zero-padded to 20 digits, then a hyphen and a UUID; `created_at_millis()` reads
that prefix as milliseconds.

## Verification

Run the complete verification command from this repository with the PD and TiKV v7.1.6 binaries of
this machine. It starts its own PD and TiKV as plain processes on loopback ports it reserves, with
their data in a temporary folder, supplies `TITO_NATIVE_PD_URI` as the PD's loopback `host:port`
and `TITO_NATIVE_RUN_ID` as its `run-` identity, and stops them and removes the folder when it ends;
the tests refuse undeclared infrastructure. It needs no Docker and downloads nothing.

```sh
python3 scripts/verify.py --pd /path/to/pd-server --tikv /path/to/tikv-server
```

`TITO_VERIFY_PD` and `TITO_VERIFY_TIKV` name the binaries instead of the arguments. A missing,
non-executable or other-version binary refuses the run before anything starts. The wrapper runs the
complete `cargo test --locked --all-targets` phase, the vendored PD owner's eight reconnect
regressions and two retained controls, the vendored transaction buffer's eight cases, and the actual
three-member PD leadership transfer witness. Every phase is required. See
[complete verification](docs/verification.md) for prerequisites, the cases, source holds, native ownership, cleanup
and retained evidence.

Every test runs on that PD and TiKV; there is no in-memory stand-in. A test checks results: it reads TiKV after the
commit (the record, its index entries, its fence keys, the queue's rows), never how many trips a path took or which
keys it read. Tests run four at a time on one TiKV whose data stays across test binaries, so every id and indexed
value carries a fresh UUID, every test file declares table, index and queue names of its own, and a test that scans a
whole table or rebuilds an index uses a model of its own. Examples are compiled but not run against a live database.

Application-level primary-commit response loss, provider effects, backup and restore remain the
application's complete-suite responsibility against the exact released Tito revision. The native
client proof must cover both connection loss and gRPC status loss after an actual primary
acknowledgement, with an unknown outcome, no rollback, and all accepted domain writes intact.

## Release notes

The revision after 0.18.8 keeps its version number; the Server pins Tito's Git revision. It replaces the read and
write surface: notes and `tx.batch_get` replace `get`, `get_for_share`, `get_for_update`, `get_many*`, `find*`,
`unique_key`, `save_keys` and the raw key batch read; `create`, `update` and `remove(&record)` replace `set`,
`create_many`, `replace_many` and `remove(id)`; one `TitoIndex` list with `Full`, `KeysOnly` or `Part` entries replaces
`TitoIndexConfig` with its ordinary, unique and holder indexes; the reverse index, record versions, timestamps,
`TitoModelOptions` and the in-memory test engine are gone; `rebuild_index` rebuilds one index. The queue keeps no
finished rows, has no retention and no coordinator. Data written before this revision can't be read.

## License

Apache-2.0
