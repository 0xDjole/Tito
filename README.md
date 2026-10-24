# Tito

A database layer on TiKV with indexing, transactions, and a built-in partitioned scheduled queue.

## Features

- **Data Storage**: Models with CRUD operations
- **Indexing**: Conditional ordinary and unique composite indexes for efficient queries
- **References**: Records declare what they point at; Tito checks every pointer on save, keeps one
  key per pointer, refuses to remove a record while anything points at it, and lists who does
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
manual numeric range endpoints; never pad the original integer directly. Half-open scans and exact
cursor continuation retain their existing semantics.

String index values are stored exactly. `encode_index_string(&str)` escapes `\` as `\\` and `:` as
`\:` and keeps every other character as it is: no case folding, trimming or word splitting. Values
that differ only by case or spacing, such as `ABC`, `abc` and ` abc `, are different values in
ordinary and unique indexes, and a lookup finds only the exact value. A map field's entry is
encoded as `{key}.{value}` and a numeric map entry's key goes through the same encoder. Use this
exported encoder for manual string range endpoints. Opaque record IDs are unchanged.

Ordinary indexes are declared by `indexes`; their keys include the primary record identity and may
have many owners. Exclusive value ownership is declared separately by `unique_indexes`; its key
includes the model, index name, and complete indexed value but not the claimant ID. A competing
claim therefore conflicts on one real ownership key and returns `TitoError::UniqueViolation`
without exposing the indexed value. `find_one_by_unique_index` performs an exact key read and then
loads the authoritative primary record. Conditional unique indexes are omitted when their model
instance sets `condition` to false.

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
    deleting: bool,
}

impl TitoModelTrait for User {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn table() -> String {
        "user".to_string()
    }

    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "by_email".to_string(),
            fields: vec![TitoIndexField {
                name: "email".to_string(),
                r#type: TitoIndexFieldType::String,
            }],
        }]
    }

    fn references(&self) -> Vec<TitoReference> {
        self.avatar_id
            .iter()
            .map(|avatar_id| TitoReference::new("media", avatar_id, "avatar_id"))
            .collect()
    }

    fn is_deleting(&self) -> bool {
        self.deleting
    }
}
```

`references` and `is_deleting` have no default, so a model can't forget them. A model that points
at nothing returns an empty list, and a model with no deleting state returns `false`.
`fence_stripes()` defaults to 1, which means the record itself is its fence. A model whose records
many transactions check with `assert_current_shared` at the same time returns more, such as 16, and
0 is refused (see [Fences](#fences)).

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

let mut query = users.query_by_index("by_email");
let results = query.value(email.as_str()).limit(Some(10)).execute(None).await?;
```

Tito reads and writes one model at a time. Applications load related records explicitly so their
domain and API boundaries determine when an additional read is required. When a transaction
depends on an existing record remaining unchanged, `model.assert_current(id, &tx)` reads and stages
the exact primary bytes after validating its storage metadata, without deserializing the model,
changing timestamps, touching indexes, or creating a second lock record. Two such fences on one
record conflict with each other. When a transaction only needs the record to still exist and be
current, it uses `model.assert_current_shared(id, &tx)`. On a model with several fence stripes
the checks of two transactions take turns only when the transactions pick the same stripe; see
[Fences](#fences).

`model.get_versioned(id, transaction)` returns `TitoVersioned<T>` with the model in `value` and
an opaque `TitoRecordVersion` in `version`. The version is the positive transaction version saved
by the last model write, not the current reader's transaction version or a business timestamp.
`model.assert_version(id, expected_version, &tx)` returns `true` only for that stored version and
stages the primary write fence in the caller's transaction. Check the result before performing
dependent writes. Concurrent model changes then conflict at commit. A missing or changed record
returns `false`; invalid storage metadata returns an error.

Reads and fences do not advance the record version. Every model write installs its write-transaction
version, including same-timestamp writes; delete/recreate obtains a new version. Versions serialize
as exact opaque strings, so clients do not truncate TiKV's integers to JavaScript numbers. They
are equality tokens, not domain revisions, provider-operation fences or permission to retry an
external effect. The transaction engine must supply unique positive transaction versions and
detect write conflicts.

For a single-valued ordinary index, `model.assert_index_match(id, payload, &tx)` checks the exact
declared field values against the owner's reverse-index manifest and stages the same manifest bytes
on a match. It does not fetch the primary row or an index's copied owner payload. A missing manifest
or mismatched value returns `false`; corrupt, duplicate, ambiguous or oversized metadata errors.
Assertions accept at most one MiB of metadata and 10,000 keys, record IDs up to 512 bytes and field
values up to 4,096 bytes. Numeric comparisons use exact signed integer encoding.

This asserts indexed values, not the whole record or permission to access it. Applications using
it as a projection-revision fence must advance an indexed revision on every content change and
preserve atomic set/remove maintenance. Raw primary writes/deletes bypass that contract. Adding a
revision index requires an explicit offline index rebuild before dependent reads; the assertion
does not repair old manifests or fall back to fetching content. An engine must detect conflicting
writes to the manifest for the assertion to fence concurrent updates/removals.

## References

Tito has no foreign keys, so a record says what it points at, the way it lists its indexes.
`references()` returns one `TitoReference { table, id, path }` per pointer:
- `table` is the target model's `table()`;
- `id` is the target's record id;
- `path` says where the pointer sits in the source, such as `gallery.0`.

Table and id are 1 to 512 bytes and the path is 1 to 2,048 bytes. Identical triples count once. A
pointer to the record itself is ignored, because it goes away with the record.

Tito finds each target at `table:{table}:{id}`, the default `key_prefix`. A model that overrides
`key_prefix` can't be a reference target.

**On save**, in the caller's transaction:
1. Tito reads every target. A missing target fails the save with
   `TitoError::ReferenceMissing { table, id, path }`.
2. A target the record didn't point at before, under any path, is new. If a new target is marked
   deleting, the save fails with `TitoError::ReferenceDeleting { table, id, path }`.
3. A target the record already pointed at may stay while it is deleting, so a cleanup can still edit
   it away. Moving it to another path keeps it.
4. Tito writes one key per new pointer and deletes the keys of pointers that went away, in the same
   transaction. Kept keys are not rewritten.

Each key looks like this:

```
ref:{target table}:{target id}:{source table}:{source id}:{path}
```

Its value is the source's `TitoIncomingReference { table, id, path }`. Each segment escapes `\` and
`:` with a backslash, so ids that contain separators never collide.

**Deleting.** While `is_deleting()` is true, the record also owns the marker key
`deleting:{table}:{id}`, with the value `true`. That is how Tito knows a target is deleting without
knowing its Rust type.

**The manifest.** Reference keys and the marker are listed in the record's manifest beside its
index keys. A save fails with `InvalidInput` when the manifest would hold more than 10,000 keys or
more than one MiB. Index repair and index assertions enforce the same two limits.

**Remove.** `model.remove(id, &tx)` fails with `TitoError::Referenced { table, id, by }` while any
other record points at the record. `by` names up to 20 sources in key order. When it succeeds, the
remove deletes the record's own reference keys and marker with its indexes.

There is no cascade and no unset. A cleanup over many records runs as the application's own batched
job, which marks the target deleting first.

**Who points.** `model.referenced_by(id, &tx)` returns every other record pointing at a record, in
key order, paging through the keys internally.

**Who else points.** `model.referenced_by_except(id, &source_tables, limit, &tx)` returns up to
`limit` sources whose table is not in `source_tables`, in key order. A record that is about to clean
up its own dependants asks it before it is marked deleting: a category with a hundred thousand
products, or a collection with its entries, learns whether anything else points at it without
reading the pointers it will clear itself.

```rust
let others = categories
    .referenced_by_except(&category_id, &["product", "customer"], 20, &tx)
    .await?;
```

- Every key of one source table sits in one contiguous range under the target, so Tito seeks past
  each skipped range instead of reading it.
- `limit` must be at least 1. An empty or over-512-byte `id` or source table is `InvalidInput`.
- Listing a table nothing points from, or the same table twice, is allowed.
- Keys are checked as in `referenced_by`: a key that disagrees with its value is an `IndexError`.

**Erase.** `model.erase(id, &tx)` removes a record, its indexes, its own reference keys and its
marker without checking who points at it. It exists for erasing a whole tenant, where every source
goes too. Keys that other records hold pointing at the erased record stay until those records go.

**Index repair.** `rebuild_indexes_for_restore` rebuilds reference keys and the marker from the
record, like index keys. It doesn't read the targets, so records can be restored in any order.

**Concurrency.** A save that adds a pointer writes one of its target's 16 link stripes, and every
remove and deleting mark of the target writes all 16, so a pointer added while its target is
removed or marked deleting conflicts at commit and the retry sees the new state. Two pointers added
to one target at the same moment conflict only when their sources fall in the same link stripe,
whatever the target's own stripes. See [Fences](#fences).

## Fences

Tito's TiKV transactions are optimistic and reads aren't checked at commit, so a transaction that
depends on a record writes a key that every transaction it must not overlap also writes. TiKV
cancels one of two overlapping transactions that write the same key, Tito retries it at most 10
times, and the retry sees the new state.

**Stripe keys.** A record's fence stripes are keys of this form:

```
fence:{table}:{id}:{stripe}
```

The table and id are escaped like reference keys. A stripe key is only ever written as a delete. No
value is stored at it, so it is never listed in a manifest and erase and index repair have nothing
to clean up. A manifest that lists one is an integrity error. A record has two sets of stripes, both
counted from 0:
- **Record stripes.** A model says how many fence stripes its records have, the way a hot lock is
  striped: `fence_stripes() -> u16`. The default, 1, means the record itself is its only stripe, so
  it has no record stripe keys. A model with N stripes, N of at least 2, gives each record stripes
  0 to N - 1.
- **Link stripes.** Every record has 16, stripes 0 to 15, whatever its model's N. They fence new
  pointers to the record against its removal and deleting mark.

With N = 16 both sets are the same 16 keys. An operation that writes both writes each key once:
stripes 0 to max(N, 16) - 1.

Each operation writes these fence keys of a record, besides its ordinary keys:

| What happens to a record | Fence keys written |
| --- | --- |
| `assert_current` or `assert_version` on it | its primary key, rewritten unchanged |
| `assert_current_shared` on it | with 1 stripe, its primary key, rewritten unchanged as `assert_current` does; with N, its record stripe `tx.shared_fence_pick() % N` |
| another record's save adds a pointer to it | one of its 16 link stripes, picked from the source's table and id |
| another record's save keeps its pointer to it | nothing |
| a save, `replace_many`, `erase` or `rebuild_indexes_for_restore` of it that doesn't write its deleting marker | its N record stripes, none with 1 stripe |
| an operation that writes its deleting marker: every `remove`, a save or `replace_many` that marks it deleting or clears the mark, and an `erase` or `rebuild_indexes_for_restore` while it is deleting | its record stripes and its 16 link stripes, each once |

So:
- a new pointer conflicts with every removal or deleting mark of its target that it overlaps,
  whatever the target's stripes; the retry fails with `ReferenceMissing` or `ReferenceDeleting`, or
  the remove's retry fails with `Referenced`;
- two new pointers to one target conflict only when their sources fall in the same link stripe, and
  then one retries;
- a new pointer commits beside a plain save of a target with 1 stripe; a plain save of a target with
  N stripes writes stripes 0 to N - 1, so it conflicts with the new pointers whose link stripe is
  below N, which with 16 is every new pointer;
- on a record with 1 stripe, shared checks conflict with each other, with `assert_current` and with
  every save of the record, exactly like `assert_current`;
- on a record with N stripes, the shared checks of two transactions conflict only when the
  transactions pick the same stripe, every save, deleting mark, remove and erase of the record
  conflicts with every shared check it overlaps, and `assert_current` and `assert_version` don't
  conflict with shared checks, because neither kind changes the record.

**Stripe choice.** A pointer's link stripe is FNV-1a 64 of `{source table}:{source id}`, escaped as
in keys, modulo 16, so one source always fences the same link stripe of a target, pointers from many
sources spread over the 16, and Tito needs nothing from the target's model to pick it. A
transaction draws one random 64-bit number when it begins, `tx.shared_fence_pick()`, and every
shared check in it writes stripe `pick % N` of its record. So one transaction writes the same stripe
number on every record with N stripes it checks, and checking a record twice writes one key. Two
transactions take turns on their shared checks only when their picks fall in the same stripe: with
16, one pair in 16, however many such records both check. A retry is a new transaction, so it draws
again. An engine that implements `TitoTransaction` draws a new pick for each transaction it begins.

**Choosing N.** 1 costs nothing and suits most models, and pointers to their records already spread
over the 16 link stripes. A model whose records many concurrent transactions check with
`assert_current_shared`, such as a store during a sale, returns more: with 16, the checks of two
transactions take turns only when the transactions pick the same stripe, and every save, remove,
erase and repair of the record pays 16 stripe deletes. Busy models that the same transactions check
should share one N, because the record with the fewest stripes among those two transactions both
check sets how often they take turns. Changing a model's count is like a Tito upgrade: don't run
writers that disagree on it together, because a save by one doesn't write the stripe a shared check
by the other writes.

**Shared checks.** `model.assert_current_shared(id, &tx)` works on every model. It reads and
validates the record like `assert_current` and fails with `NotFound` when it is missing. On a model
with 1 stripe it is `assert_current`; with N stripes it writes the stripe key of its transaction's
pick and changes no stored byte and no record version. Use it where a transaction only needs the
record to still exist and be current, such as a store that must not be closing while orders are
written in it. Keep `assert_current` or `assert_version` where the fence is a compare-and-set.

**Zero stripes.** A model whose `fence_stripes()` is 0 is refused with `TitoError::Configuration`,
never rounded up: `set`, `replace_many`, `remove`, `erase`, `rebuild_indexes_for_restore` and
`assert_current_shared` fail before they read or write.

## Storage integrity and pagination

`model.replace_many(Vec<(expected, next)>).timestamps(false).execute(&tx)` replaces existing
records of one model in the caller's transaction. Every source must still equal its complete
expected serialized record, and each replacement keeps its source ID. The method validates and
serializes the complete batch before staging any write. Unique keys may move between included
sources, including swap cycles; a final duplicate key or an owner outside the batch is refused.
References use the batch's final target states: existing links may remain while a target becomes
Deleting, but a new link cannot point at a final Deleting target. External target checks keep the
normal reference fence: a new link writes one of its target's 16 link stripes, a replaced record of
a model with several fence stripes has all of them written, and a replacement that marks a record
deleting or clears the mark also writes its 16 link stripes. Each source primary is replaced,
fencing concurrent source changes, and all manifests receive the transaction's record version.
Timestamp handling matches `set`; pass `timestamps(false)` to retain application-authored times.

A batch contains 1–100 distinct existing records. Its combined old and prepared storage keys
must fit 10,000 keys, and its prepared writes and deletion keys must fit 8 MiB. Invalid input,
stale expected records, ownership conflicts, missing or deleting targets, and serialization errors
are returned before the batch writes anything. A later transaction failure rolls back the whole
batch with its caller's other changes. This operation does not create records or assign temporary
unique values.

Each persisted model row has a matching `reverse-index:{primary-key}` manifest containing its
required record `version` and index-key `value` array, including models with no secondary indexes.
Primary, metadata and secondary indexes commit atomically. The manifest may name ordinary `index:` keys ending in that exact
primary key, model-scoped `unique-index:` keys whose stored owner matches that primary record, the
record's own `ref:` keys and its own `deleting:` marker. It never names a `fence:` stripe key.
Updates and removals validate the pair and every unique owner before mutating either side. A
missing, orphaned, malformed, or syntactically cross-record manifest is an integrity error; Tito
does not reinterpret it as a missing entity or follow it to an unrelated key.

The record version is not an index-schema version, so Tito cannot prove that
a syntactically valid manifest still enumerates every index created by an older application schema.
Recomputing against the current model would incorrectly reject legitimate index additions,
removals, and condition changes. Detecting or repairing a valid-shaped but semantically incomplete
historical manifest therefore requires an application-owned audited rebuild. Missing record versions
are not backfilled from the current read, clock or model timestamp; older metadata requires the
application's explicit pre-production reset and backup-format cutover.

For offline repair of lost or stale secondary indexes under an unchanged model,
`model.rebuild_indexes_for_restore(id, &tx)` validates the retained primary and reverse manifest,
then rewrites only their declared index values, reference keys and deleting marker. A record of a
model with several fence stripes also has all its stripe keys written, as a save would, and a
deleting record also has its 16 link stripes written with its marker. The primary bytes, manifest
bytes and original record version are preserved and fenced in the same transaction.
Missing secondary keys are allowed; a unique key owned by another record is not. Ordinary writes
still reject missing unique keys. Missing versions, duplicate or foreign manifest keys, a
primary/key mismatch, or a manifest that differs from the current model's indexes, references or
deleting state reject. This is not schema migration, a new record write or permission to invent
missing authoritative metadata. The caller owns offline operation, bounded primary enumeration and
domain relationship validation; Tito does not scan related records or read reference targets.

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

`prefix_end` closes a prefix range; it must not be used to continue after an exact key because
advancing the final character can skip keys such as `a20` after `a2`. Use `key_after` for that.

Secondary index values intentionally remain complete clones of the primary JSON document. This
preserves the existing storage wire format and query behavior. Removing that redundancy requires a
separately designed release that rehydrates primary rows and migrates every existing index.

### 0.18.4 fence stripes

- `TitoModelTrait` gains `fence_stripes() -> u16`, default 1, and `TitoModel` gains
  `assert_current_shared(id, &tx)` for every model. On a model with 1 stripe the shared check
  rewrites the record like `assert_current`, and saves of its records write no record stripe.
- `TitoTransaction` gains `shared_fence_pick() -> u64` beside `start_version()`: a random number the
  transaction draws when it begins. `TiKVBackend::begin_transaction` and the in-memory test engine
  draw one for every transaction, and an engine that implements `TitoTransaction` must too.
- A model with N stripes, N of at least 2, has N record stripe keys per record,
  `fence:{table}:{id}:{stripe}`. A shared check writes stripe `pick % N`, where the pick is its
  transaction's `shared_fence_pick()`, so all shared checks of one transaction write one stripe
  number and a retry draws again; every save, `replace_many`, remove, erase and repair of the record
  writes all N. Shared checks of transactions whose picks fall in different stripes stop taking
  turns, while every change of the record still conflicts with each of them.
- Every record has 16 link stripes under the same key format, whatever its model's N. A new pointer
  writes one of its target's, picked from the source's table and id, instead of the target's
  `deleting:` key. Every write of a record's deleting marker (a remove, a deleting mark or its
  clearing, and an erase or repair of a deleting record) also writes all 16. So a new pointer still
  conflicts with every removal and deleting mark of its target, while new pointers to one target
  conflict only when their sources share a link stripe.
- A model whose `fence_stripes()` is 0 is refused with `TitoError::Configuration`.
- Stripe keys are write-only deletes: stored records, manifests and key formats are unchanged, and
  nothing needs migrating. Don't run 0.18.3 writers beside 0.18.4 writers: a remove or mark by a
  0.18.3 writer doesn't write the link stripe a new pointer by a 0.18.4 writer writes. Don't run
  writers that disagree on a model's stripes together either: a save by one doesn't write the stripe
  a shared check by the other writes.
- The in-memory test engine detects write-write conflicts at commit, applies only the keys a
  transaction wrote, and retries a conflicting commit like `TiKVBackend::transaction`.

### 0.18.3 exact index values, fixed partitions and bounded reads

- String index values are encoded exactly by `encode_index_string`: only `\` and `:` are escaped.
  Before, values were lowercased, trimmed and split at camel-case humps, so `ABC` and `abc` shared
  one unique value and a lookup of one returned the other. Index keys of values with capitals,
  surrounding whitespace or camel-case humps change, and so do keys of queue cluster nodes; reset
  and reseed, or save those records again. Numeric map-entry keys are now escaped too.
- Queue partitions come from `queue_partition_for_key`, a fixed FNV-1a hash, instead of the Rust
  standard library's hasher, which may change between Rust releases. Pending rows published by
  0.18.2 can sit in another partition than their key's new events; drain or reset the queue before
  upgrading.
- `next_string_lexicographically` is removed; call `prefix_end`.
- TiKV scans and batch gets are bounded by bytes as well as rows (see Queue Processing).
- Workers pull the next page at once while a pass has more rows. `WorkerConfig` gains
  `poll_interval`.

### 0.18.2 references

`TitoModelTrait` gains two required methods, `references` and `is_deleting`. Every model must
implement both before it compiles. There are three new errors: `TitoError::Referenced`,
`ReferenceMissing` and `ReferenceDeleting`. `TitoModel` gains `referenced_by`,
`referenced_by_except` and `erase`.

A map field's index entry is now encoded whole, as `{field}:{key}.{value}` passed through the same
encoding a lookup applies to its value. Before, the map key was written as it was, so a lookup for a
key with capitals, such as `en-US.spring-menu`, never matched. Index keys of maps whose keys have
capitals change; reset and reseed, or save those records again. Identical index keys of one record,
such as a list that holds the same value twice, are written and listed in the manifest once, so
index repair accepts the record.

Manifests now list the record's `ref:` keys and `deleting:` marker. Records written by 0.18.1 have
neither: `remove` doesn't see their pointers, and index repair rejects the manifest of any record
that holds a pointer or is deleting. Reset and reseed, or save every such record again, before
relying on the guard. Key formats for primaries, indexes and the queue are unchanged.

### 0.18.0 development cutover

Version 0.18.0 changes every Tito-owned wall-clock instant to signed UTC Unix epoch milliseconds:
automatic model `created_at` and `updated_at`, queue `timestamp` and `processedAt`, cluster
heartbeats, lease deadlines, and assignment timestamps. Timestamp-bearing models receive automatic
millisecond stamps unless a write explicitly uses `.timestamps(false)` to preserve application-owned
values. No undeclared timestamp fields are added.

Queue scheduling accepts the full signed `i64` instant range, including negative, zero, and early-epoch
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
Numeric secondary index keys and queue timestamp key segments use the same signed-sortable encoder
and must be rebuilt through the authorized reset/reseed. Pending queue keys encode the due time;
completed keys encode both the completion time and original due time. Due and completed-retention
scans include their exact cutoff bucket across the full signed range, without adding one to the
timestamp or overflowing at `i64::MAX`. Queue partition and unsigned transaction-version segments,
opaque microsecond queue IDs, and handler dispatch/claim/redelivery policy are unchanged.

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

The TiKV engine bounds every read by bytes as well as rows, for models, indexes, references and the
queue alike. A scan is fetched as a sequence of datastore requests:

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

A worker keeps pulling while its pass has more rows: after a full page it pulls the next page at once.
It waits its poll interval only after a pass ends or a pull fails. `WorkerConfig::new` polls every
second and `ClusterWorkerConfig::new` every 500 ms; both expose `poll_interval`.

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
Standalone workers and the elected cluster coordinator delete older rows in bounded passes. A full
pass yields and continues immediately until the expired range is caught up; the 30-second interval
applies only after a short pass. Maintenance uses the completed-status/processed-time key range
directly, never inspects pending work, and has no retry, recovery, or provider semantics. Malformed
values inside an expired completed-row key are also removed so corrupt terminal history cannot pin
newer cleanup work.

### Transaction retry safety

Tito may replay a transaction closure after an explicitly retryable, determined datastore failure. TiKV's `UndeterminedError` is different: the commit may already be durable. Tito returns `TitoError::CommitOutcomeUnknown` and never replays that closure. The caller reconciles against authoritative domain state; an acknowledgement either committed or the unchanged Pending invocation is delivered again.

Cancellation removes a transaction from the active registry. Before native commit starts, cleanup
rolls back pending work. Once native commit has started, cancellation only releases the retained
handle so its heartbeat can stop; cleanup never replays the closure or rolls back an uncertain
commit. Callers still reconcile any interrupted command against its authoritative identity.

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

## Event Key Format

```
queue:pending:{partition:04}:{encode_index_integer(timestamp)}:{enqueue_version:020}:{event_id}
queue:completed:{encode_index_integer(processed_at)}:{encode_index_integer(event_timestamp)}:{event_id}
queue:owner:{base64url(type)}:{base64url(id)}:{status_type}:{base64url(queue_storage_key)}
```

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

The library cases use the in-memory engine and cover model/index writes, exact index values,
reference keys, refusals, staged fence writes and fence stripe conflicts, queue transitions, fixed partitions, exact
signed millisecond scheduling and completion, exact inclusive cutoff buckets and signed extrema,
configured retention, cluster ownership and leases, worker paging and shutdown, and ambiguous
transaction outcomes. `tikv_native_orphan_lock_test` creates a real secondary lock after a definite primary
prewrite conflict, waits for its observed native TTL, proves the missing-primary branch through a
normal Tito read, and verifies recovery preserves the winning primary. It removes only its own
two UUID-scoped keys. `tikv_native_large_rows_test` reads 40 records of 1 MiB, more than the
client's 32 MiB decoding budget, through scans, index pages and a batch get, and pulls 16 queue
events of almost 1 MiB. `tikv_native_queue_test` proves that cluster-worker and standalone-worker
handlers complete their own pending row inside their transaction, that a duplicate delivery
conflicts on that row and its retry skips, that a rolled-back handler leaves the row pending, and
that `ack_in_tx` refuses events and keys it does not own without writing. `tikv_native_fence_test`
proves on real TiKV that a new pointer conflicts with a removal or deleting mark of its target,
whether the target has 1 fence stripe or 16; that new pointers to a target with 1 stripe or 16
commit together from sources in different link stripes and conflict from sources in one; that
shared checks on a record with 1 stripe conflict with each other and with an update, while on
records with 16, two transactions whose picks fall in different stripes check twenty records and
commit together, two whose picks fall in one stripe conflict and the retry passes, and a check
conflicts with an update in either commit order; that `assert_current`, `assert_version` and unique
claims conflict at commit; and that unique values differing only by case or spacing coexist. The in-memory engine fails a commit with
`TitoError::Retryable` when another transaction committed one of its keys after it started, as TiKV
does, so the library cases cover these conflicts too; the native cases prove them on TiKV. Examples
are compiled but not run against a live database.

Application-level primary-commit response loss, provider effects, backup and restore remain the
application's complete-suite responsibility against the exact released Tito revision. The native
client proof must cover both connection loss and gRPC status loss after an actual primary
acknowledgement, with an unknown outcome, no rollback, and all accepted domain writes intact.

## License

Apache-2.0
