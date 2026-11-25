mod native;

use native::{create, native_endpoint, native_engine, read, remove};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tito::backend::tikv::{TiKVBackend, TiKVTransaction};
use tito::types::TitoTransaction;
use tito::{
    Guard, TitoEngine, TitoError, TitoIndex, TitoIndexField, TitoIndexValue, TitoModel,
    TitoModelTrait,
};
use uuid::Uuid;

const BY_SCOPE: &str = "native-fence-record-by-scope";
const BY_VALUE: &str = "native-fence-record-value";
const HOLDER_BY_RECORD: &str = "native-fence-holder-by-record";
const CLAIM_VALUE: &str = "native-fence-claim-value";

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeRecord {
    id: String,
    scope: String,
    value: String,
    name: String,
}

impl TitoModelTrait for NativeRecord {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![
            TitoIndex {
                name: BY_SCOPE,
                fields: vec![TitoIndexField::Text(self.scope.clone())],
                value: TitoIndexValue::Full,
                unique: false,
            },
            TitoIndex {
                name: BY_VALUE,
                fields: vec![
                    TitoIndexField::Text(self.scope.clone()),
                    TitoIndexField::Text(self.value.clone()),
                ],
                value: TitoIndexValue::Full,
                unique: true,
            },
        ]
    }

    fn table() -> String {
        "native-fence-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeHolder {
    id: String,
    record_id: String,
}

impl TitoModelTrait for NativeHolder {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![TitoIndex {
            name: HOLDER_BY_RECORD,
            fields: vec![TitoIndexField::Text(self.record_id.clone())],
            value: TitoIndexValue::KeysOnly,
            unique: false,
        }]
    }

    fn table() -> String {
        "native-fence-holders".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeClaim {
    id: String,
    scope: String,
    value: String,
}

impl TitoModelTrait for NativeClaim {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![TitoIndex {
            name: CLAIM_VALUE,
            fields: vec![
                TitoIndexField::Text(self.scope.clone()),
                TitoIndexField::Text(self.value.clone()),
            ],
            value: TitoIndexValue::Full,
            unique: true,
        }]
    }

    fn table() -> String {
        "native-fence-claims".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn record_id(run_id: &str) -> String {
    format!("{run_id}-{}", Uuid::new_v4())
}

fn new_record(run_id: &str, scope: &str) -> NativeRecord {
    NativeRecord {
        id: record_id(run_id),
        scope: scope.into(),
        value: Uuid::new_v4().to_string(),
        name: "record".into(),
    }
}

fn with_name(record: &NativeRecord, name: &str) -> NativeRecord {
    NativeRecord {
        name: name.into(),
        ..record.clone()
    }
}

fn value_of(record: &NativeRecord) -> Vec<TitoIndexField> {
    vec![
        TitoIndexField::Text(record.scope.clone()),
        TitoIndexField::Text(record.value.clone()),
    ]
}

fn claim_value(scope: &str, value: &str) -> Vec<TitoIndexField> {
    vec![
        TitoIndexField::Text(scope.to_string()),
        TitoIndexField::Text(value.to_string()),
    ]
}

fn records_of(engine: &TiKVBackend) -> TitoModel<TiKVBackend, NativeRecord> {
    engine.clone().model::<NativeRecord>()
}

async fn created(
    engine: &TiKVBackend,
    records: &TitoModel<TiKVBackend, NativeRecord>,
    record: NativeRecord,
) -> NativeRecord {
    create(engine, records, &record).await.unwrap();
    record
}

async fn rename(
    engine: &TiKVBackend,
    records: &TitoModel<TiKVBackend, NativeRecord>,
    id: &str,
    name: &str,
) -> Result<NativeRecord, TitoError> {
    let note = records.read(id, Guard::None);
    engine
        .transaction(|tx| {
            let records = records.clone();
            let note = note.clone();
            async move {
                let old = tx.batch_get(note).await?;
                let new = with_name(&old, name);
                records.update(&old, &new, &tx).await?;
                Ok::<_, TitoError>(new)
            }
        })
        .await
}

async fn unique_claim(
    engine: &TiKVBackend,
    claims: &TitoModel<TiKVBackend, NativeClaim>,
    scope: &str,
    value: &str,
) -> Result<NativeClaim, TitoError> {
    let note = claims.read_unique(CLAIM_VALUE, claim_value(scope, value), Guard::None);
    engine
        .transaction(|tx| async move { tx.batch_get(note).await })
        .await
}

async fn write_scratch(tx: &TiKVTransaction, run_id: &str) {
    tx.delete(format!("native-fence-scratch:{}", record_id(run_id)))
        .await
        .unwrap();
}

async fn committed(tx: TiKVTransaction) -> bool {
    let kept = tx.clone();
    match tx.commit().await {
        Ok(()) => true,
        Err(TitoError::Retryable(_)) => {
            let _ = kept.rollback().await;
            false
        }
        Err(error) => panic!("A native guard commit failed without a conflict: {error}"),
    }
}

#[derive(Clone, Copy, Debug)]
enum NativeRead {
    ById,
    MaybeById,
    Many,
    Unique,
    MaybeUnique,
}

const NATIVE_READS: [NativeRead; 5] = [
    NativeRead::ById,
    NativeRead::MaybeById,
    NativeRead::Many,
    NativeRead::Unique,
    NativeRead::MaybeUnique,
];

async fn read_with(
    records: &TitoModel<TiKVBackend, NativeRecord>,
    form: NativeRead,
    guard: Guard,
    record: &NativeRecord,
    tx: &TiKVTransaction,
) {
    match form {
        NativeRead::ById => {
            assert_eq!(
                tx.batch_get(records.read(&record.id, guard)).await.unwrap(),
                *record
            );
        }
        NativeRead::MaybeById => {
            assert_eq!(
                tx.batch_get(records.read_maybe(&record.id, guard))
                    .await
                    .unwrap(),
                Some(record.clone())
            );
        }
        NativeRead::Many => {
            assert_eq!(
                tx.batch_get(records.read_many(vec![record.id.clone()], guard))
                    .await
                    .unwrap(),
                vec![record.clone()]
            );
        }
        NativeRead::Unique => {
            assert_eq!(
                tx.batch_get(records.read_unique(BY_VALUE, value_of(record), guard))
                    .await
                    .unwrap(),
                *record
            );
        }
        NativeRead::MaybeUnique => {
            assert_eq!(
                tx.batch_get(records.read_unique_maybe(BY_VALUE, value_of(record), guard))
                    .await
                    .unwrap(),
                Some(record.clone())
            );
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum NativeWriter {
    Update,
    Remove,
}

async fn stage_writer(
    records: &TitoModel<TiKVBackend, NativeRecord>,
    writer: NativeWriter,
    record: &NativeRecord,
    tx: &TiKVTransaction,
) {
    let old = tx
        .batch_get(records.read(&record.id, Guard::None))
        .await
        .unwrap();
    match writer {
        NativeWriter::Update => {
            records
                .update(&old, &with_name(&old, "updated by a writer"), tx)
                .await
                .unwrap();
        }
        NativeWriter::Remove => {
            records.remove(&old, tx).await.unwrap();
        }
    }
}

async fn delete_unless_held(
    records: &TitoModel<TiKVBackend, NativeRecord>,
    holders: &TitoModel<TiKVBackend, NativeHolder>,
    id: &str,
    tx: &TiKVTransaction,
) -> Result<Vec<String>, TitoError> {
    let mut query = holders.query_by_index(HOLDER_BY_RECORD);
    query
        .value(TitoIndexField::Text(id.to_string()))
        .limit(Some(20));
    let (held, record) = tx
        .batch_get((query.read_ids(), records.read(id, Guard::None)))
        .await?;
    if held.items.is_empty() {
        records.remove(&record, tx).await?;
    }
    Ok(held.items)
}

#[tokio::test]
async fn native_every_read_for_share_or_for_update_in_a_writing_transaction_conflicts_with_an_update_of_its_record(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        for guard in [Guard::Share, Guard::Update] {
            for form in NATIVE_READS {
                let record = created(&engine, &records, new_record(&run_id, &scope)).await;
                let reader = engine.begin_transaction().await.unwrap();
                read_with(&records, form, guard, &record, &reader).await;
                write_scratch(&reader, &run_id).await;
                let renamed = rename(
                    &engine,
                    &records,
                    &record.id,
                    "renamed by a committed writer",
                )
                .await
                .unwrap();
                assert!(
                    !committed(reader).await,
                    "{form:?} with {guard:?} in a writing transaction must conflict with an update of its record"
                );
                assert_eq!(
                    read(&engine, &records, &record.id).await.unwrap(),
                    Some(renamed)
                );
                remove(&engine, &records, &record.id).await.unwrap();
            }
        }
    })
    .await
    .expect("The native guarded reads must finish within their owned run");
}

#[tokio::test]
async fn native_index_reads_scans_and_misses_add_no_guard() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let record = created(&engine, &records, new_record(&run_id, &scope)).await;

        let reader = engine.begin_transaction().await.unwrap();
        let mut query = records.query_by_index(BY_SCOPE);
        query
            .value(TitoIndexField::Text(scope.clone()))
            .limit(Some(10));
        assert_eq!(
            query.execute(Some(&reader)).await.unwrap().items,
            vec![record.clone()]
        );
        assert_eq!(
            query.execute_ids(Some(&reader)).await.unwrap().items,
            vec![record.id.clone()]
        );
        let (page, ids) = reader
            .batch_get((query.read(), query.read_ids()))
            .await
            .unwrap();
        assert_eq!(page.items, vec![record.clone()]);
        assert_eq!(ids.items, vec![record.id.clone()]);
        query.descending();
        assert_eq!(
            query.execute(Some(&reader)).await.unwrap().items,
            vec![record.clone()]
        );
        let scanned = records.scan_records(1_000, None, &reader).await.unwrap();
        assert!(scanned.items.contains(&record));
        let missing = record_id(&run_id);
        for guard in [Guard::Share, Guard::Update] {
            assert_eq!(
                reader
                    .batch_get(records.read_maybe(&missing, guard))
                    .await
                    .unwrap(),
                None
            );
            assert!(matches!(
                reader.batch_get(records.read(&missing, guard)).await,
                Err(TitoError::NotFound(_))
            ));
            assert_eq!(
                reader
                    .batch_get(records.read_many(vec![missing.clone()], guard))
                    .await
                    .unwrap(),
                Vec::new()
            );
            assert_eq!(
                reader
                    .batch_get(records.read_unique_maybe(
                        BY_VALUE,
                        claim_value(&scope, "missing"),
                        guard
                    ))
                    .await
                    .unwrap(),
                None
            );
        }
        let late = NativeRecord {
            id: missing.clone(),
            scope: scope.clone(),
            value: "missing".into(),
            name: "created after the misses".into(),
        };
        create(&engine, &records, &late).await.unwrap();
        write_scratch(&reader, &run_id).await;
        let renamed = rename(
            &engine,
            &records,
            &record.id,
            "renamed by a committed writer",
        )
        .await
        .unwrap();
        assert!(committed(reader).await);

        assert_eq!(
            read(&engine, &records, &record.id).await.unwrap(),
            Some(renamed)
        );
        remove(&engine, &records, &record.id).await.unwrap();
        remove(&engine, &records, &late.id).await.unwrap();
    })
    .await
    .expect("The native index reads, scans and misses must finish within their owned run");
}

#[tokio::test]
async fn native_two_transactions_reading_the_same_twenty_records_conflict_about_one_time_in_sixteen(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(120), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let mut ids = Vec::new();
        for _ in 0..20 {
            ids.push(
                created(&engine, &records, new_record(&run_id, &scope))
                    .await
                    .id,
            );
        }

        let mut conflicts = 0;
        for _ in 0..480 {
            let first = engine.begin_transaction().await.unwrap();
            let second = engine.begin_transaction().await.unwrap();
            for tx in [&first, &second] {
                assert_eq!(
                    tx.batch_get(records.read_many(ids.clone(), Guard::Share))
                        .await
                        .unwrap()
                        .len(),
                    ids.len()
                );
                write_scratch(tx, &run_id).await;
            }
            assert!(committed(first).await);
            if !committed(second).await {
                conflicts += 1;
            }
        }
        assert!(
            (4..=75).contains(&conflicts),
            "{conflicts} of 480 pairs reading the same twenty records conflicted"
        );

        for id in &ids {
            remove(&engine, &records, id).await.unwrap();
        }
    })
    .await
    .expect("The native shared read pairs must finish within their owned run");
}

#[tokio::test]
async fn native_two_reads_for_update_of_one_record_always_conflict_and_the_retry_passes() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let record = created(&engine, &records, new_record(&run_id, &scope)).await;

        for _ in 0..16 {
            let first = engine.begin_transaction().await.unwrap();
            let second = engine.begin_transaction().await.unwrap();
            for tx in [&first, &second] {
                assert_eq!(
                    tx.batch_get(records.read(&record.id, Guard::Update))
                        .await
                        .unwrap(),
                    record
                );
                write_scratch(tx, &run_id).await;
            }
            assert!(committed(first).await);
            assert!(!committed(second).await);
        }

        let note = records.read(&record.id, Guard::Update);
        let found = engine
            .transaction(|tx| {
                let note = note.clone();
                let run_id = run_id.clone();
                async move {
                    let found = tx.batch_get(note).await?;
                    write_scratch(&tx, &run_id).await;
                    Ok::<_, TitoError>(found)
                }
            })
            .await
            .unwrap();
        assert_eq!(found, record);
        remove(&engine, &records, &record.id).await.unwrap();
    })
    .await
    .expect("The native exclusive read pairs must finish within their owned run");
}

#[tokio::test]
async fn native_a_read_for_share_and_a_read_for_update_of_one_record_commit_together_in_either_order(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let record = created(&engine, &records, new_record(&run_id, &scope)).await;

        for pair in 0..16 {
            let shared = engine.begin_transaction().await.unwrap();
            let exclusive = engine.begin_transaction().await.unwrap();
            read_with(&records, NativeRead::ById, Guard::Share, &record, &shared).await;
            read_with(
                &records,
                NativeRead::ById,
                Guard::Update,
                &record,
                &exclusive,
            )
            .await;
            write_scratch(&shared, &run_id).await;
            write_scratch(&exclusive, &run_id).await;
            if pair < 8 {
                assert!(committed(shared).await);
                assert!(committed(exclusive).await);
            } else {
                assert!(committed(exclusive).await);
                assert!(committed(shared).await);
            }
        }

        remove(&engine, &records, &record.id).await.unwrap();
    })
    .await
    .expect("The native shared and exclusive read pairs must finish within their owned run");
}

#[tokio::test]
async fn native_writers_that_read_one_record_unguarded_never_conflict() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let record = created(&engine, &records, new_record(&run_id, &scope)).await;

        for round in 0..128 {
            let form = NATIVE_READS[round % NATIVE_READS.len()];
            let first = engine.begin_transaction().await.unwrap();
            let second = engine.begin_transaction().await.unwrap();
            for tx in [&first, &second] {
                read_with(&records, form, Guard::None, &record, tx).await;
                write_scratch(tx, &run_id).await;
            }
            assert!(committed(first).await);
            assert!(
                committed(second).await,
                "Two writers that read one record with {form:?} and no guard must never conflict"
            );
        }

        remove(&engine, &records, &record.id).await.unwrap();
    })
    .await
    .expect("The native unguarded read pairs must finish within their owned run");
}

#[tokio::test]
async fn native_an_unguarded_read_conflicts_with_no_update_or_remove_of_its_record() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        for form in NATIVE_READS {
            for writer in [NativeWriter::Update, NativeWriter::Remove] {
                let record = created(&engine, &records, new_record(&run_id, &scope)).await;
                let looking = engine.begin_transaction().await.unwrap();
                read_with(&records, form, Guard::None, &record, &looking).await;
                write_scratch(&looking, &run_id).await;
                let writing = engine.begin_transaction().await.unwrap();
                stage_writer(&records, writer, &record, &writing).await;
                assert!(committed(writing).await);
                assert!(
                    committed(looking).await,
                    "A {form:?} read with no guard must not conflict with a committed {writer:?} of its record"
                );
                match writer {
                    NativeWriter::Update => {
                        assert_eq!(
                            read(&engine, &records, &record.id).await.unwrap(),
                            Some(with_name(&record, "updated by a writer"))
                        );
                        remove(&engine, &records, &record.id).await.unwrap();
                    }
                    NativeWriter::Remove => {
                        assert_eq!(read(&engine, &records, &record.id).await.unwrap(), None);
                    }
                }
            }
        }
    })
    .await
    .expect("The native unguarded reads beside writers must finish within their owned run");
}

#[tokio::test]
async fn native_an_unguarded_read_cancels_no_guard_of_its_transaction() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        for form in NATIVE_READS {
            for plain_first in [true, false] {
                let record = created(&engine, &records, new_record(&run_id, &scope)).await;
                let reader = engine.begin_transaction().await.unwrap();
                if plain_first {
                    read_with(&records, form, Guard::None, &record, &reader).await;
                    read_with(&records, NativeRead::ById, Guard::Share, &record, &reader).await;
                } else {
                    read_with(&records, NativeRead::ById, Guard::Share, &record, &reader).await;
                    read_with(&records, form, Guard::None, &record, &reader).await;
                }
                write_scratch(&reader, &run_id).await;
                let renamed = rename(&engine, &records, &record.id, "renamed while guarded")
                    .await
                    .unwrap();
                assert!(
                    !committed(reader).await,
                    "A record read for share keeps its guard beside a {form:?} read with no guard"
                );

                let held = engine.begin_transaction().await.unwrap();
                let competing = engine.begin_transaction().await.unwrap();
                if plain_first {
                    read_with(&records, form, Guard::None, &renamed, &held).await;
                    read_with(&records, NativeRead::ById, Guard::Update, &renamed, &held).await;
                } else {
                    read_with(&records, NativeRead::ById, Guard::Update, &renamed, &held).await;
                    read_with(&records, form, Guard::None, &renamed, &held).await;
                }
                read_with(
                    &records,
                    NativeRead::ById,
                    Guard::Update,
                    &renamed,
                    &competing,
                )
                .await;
                assert!(committed(competing).await);
                assert!(
                    !committed(held).await,
                    "A record read for update keeps its guard beside a {form:?} read with no guard"
                );
                remove(&engine, &records, &record.id).await.unwrap();
            }
        }
    })
    .await
    .expect("The native mixed reads must finish within their owned run");
}

#[tokio::test]
async fn native_an_update_and_a_remove_conflict_with_reads_for_share_and_for_update_in_either_order(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        for guard in [Guard::Share, Guard::Update] {
            for writer in [NativeWriter::Update, NativeWriter::Remove] {
                let record = created(&engine, &records, new_record(&run_id, &scope)).await;
                let guarded = engine.begin_transaction().await.unwrap();
                read_with(&records, NativeRead::ById, guard, &record, &guarded).await;
                write_scratch(&guarded, &run_id).await;
                let writing = engine.begin_transaction().await.unwrap();
                stage_writer(&records, writer, &record, &writing).await;
                assert!(committed(writing).await);
                assert!(
                    !committed(guarded).await,
                    "A read with {guard:?} must conflict with a committed {writer:?} of its record"
                );
                let retried = engine.begin_transaction().await.unwrap();
                let reread = retried.batch_get(records.read(&record.id, guard)).await;
                retried.rollback().await.unwrap();
                match writer {
                    NativeWriter::Update => {
                        assert_eq!(reread.unwrap(), with_name(&record, "updated by a writer"));
                        remove(&engine, &records, &record.id).await.unwrap();
                    }
                    NativeWriter::Remove => {
                        assert!(matches!(reread, Err(TitoError::NotFound(_))));
                    }
                }

                let record = created(&engine, &records, new_record(&run_id, &scope)).await;
                let guarded = engine.begin_transaction().await.unwrap();
                read_with(&records, NativeRead::ById, guard, &record, &guarded).await;
                write_scratch(&guarded, &run_id).await;
                let writing = engine.begin_transaction().await.unwrap();
                stage_writer(&records, writer, &record, &writing).await;
                assert!(committed(guarded).await);
                assert!(
                    !committed(writing).await,
                    "A {writer:?} must conflict with a committed read with {guard:?} of its record"
                );
                assert_eq!(
                    read(&engine, &records, &record.id).await.unwrap(),
                    Some(record.clone())
                );
                remove(&engine, &records, &record.id).await.unwrap();
            }
        }
    })
    .await
    .expect("The native writer and guard races must finish within their owned run");
}

#[tokio::test]
async fn native_a_transaction_that_writes_nothing_adds_no_guard_and_a_read_for_update_counts_as_a_write(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let mut saved = Vec::new();
        for _ in 0..4 {
            saved.push(created(&engine, &records, new_record(&run_id, &scope)).await);
        }

        let reader = engine.begin_transaction().await.unwrap();
        for (form, record) in [
            NativeRead::ById,
            NativeRead::MaybeById,
            NativeRead::Many,
            NativeRead::Unique,
        ]
        .into_iter()
        .zip(&saved)
        {
            read_with(&records, form, Guard::Share, record, &reader).await;
        }
        for record in &saved {
            rename(&engine, &records, &record.id, "renamed while read")
                .await
                .unwrap();
        }
        assert!(committed(reader).await);

        let held = engine.begin_transaction().await.unwrap();
        let competing = engine.begin_transaction().await.unwrap();
        held.batch_get(records.read(&saved[0].id, Guard::Update))
            .await
            .unwrap();
        competing
            .batch_get(records.read(&saved[0].id, Guard::Update))
            .await
            .unwrap();
        assert!(committed(competing).await);
        assert!(!committed(held).await);

        let exclusive = engine.begin_transaction().await.unwrap();
        exclusive
            .batch_get(records.read(&saved[0].id, Guard::Update))
            .await
            .unwrap();
        exclusive
            .batch_get(records.read(&saved[1].id, Guard::Share))
            .await
            .unwrap();
        rename(&engine, &records, &saved[1].id, "renamed while held")
            .await
            .unwrap();
        assert!(!committed(exclusive).await);

        for record in &saved {
            remove(&engine, &records, &record.id).await.unwrap();
        }
    })
    .await
    .expect("The native reads of transactions that write nothing must finish within their run");
}

#[tokio::test]
async fn native_a_retry_starts_without_the_guards_of_the_attempt_before() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let first = created(&engine, &records, new_record(&run_id, &scope)).await;
        let second = created(&engine, &records, new_record(&run_id, &scope)).await;
        let attempts = Arc::new(AtomicUsize::new(0));

        engine
            .transaction(|tx| {
                let engine = engine.clone();
                let records = records.clone();
                let attempts = attempts.clone();
                let first = first.clone();
                let second = second.clone();
                let run_id = run_id.clone();
                async move {
                    let attempt = attempts.fetch_add(1, Ordering::SeqCst) + 1;
                    let guarded = if attempt == 1 { &first } else { &second };
                    tx.batch_get(records.read(&guarded.id, Guard::Share))
                        .await?;
                    write_scratch(&tx, &run_id).await;
                    rename(
                        &engine,
                        &records,
                        &first.id,
                        &format!("renamed during attempt {attempt}"),
                    )
                    .await?;
                    Ok::<_, TitoError>(())
                }
            })
            .await
            .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            read(&engine, &records, &first.id)
                .await
                .unwrap()
                .expect("The first record stays")
                .name,
            "renamed during attempt 2"
        );
        remove(&engine, &records, &first.id).await.unwrap();
        remove(&engine, &records, &second.id).await.unwrap();
    })
    .await
    .expect("The native retried transaction must finish within its owned run");
}

#[tokio::test]
async fn native_a_new_holder_and_a_delete_of_the_record_it_names_conflict_in_either_order() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let holders = engine.clone().model::<NativeHolder>();
        let scope = record_id(&run_id);

        let record = created(&engine, &records, new_record(&run_id, &scope)).await;
        let holder = NativeHolder {
            id: record_id(&run_id),
            record_id: record.id.clone(),
        };
        let holding = engine.begin_transaction().await.unwrap();
        holding
            .batch_get(records.read(&record.id, Guard::Share))
            .await
            .unwrap();
        holders.create(&holder, &holding).await.unwrap();
        let deleting = engine.begin_transaction().await.unwrap();
        assert!(
            delete_unless_held(&records, &holders, &record.id, &deleting)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(committed(holding).await);
        assert!(!committed(deleting).await);
        let refused = engine
            .transaction(|tx| {
                let records = records.clone();
                let holders = holders.clone();
                let id = record.id.clone();
                async move { delete_unless_held(&records, &holders, &id, &tx).await }
            })
            .await
            .unwrap();
        assert_eq!(refused, vec![holder.id.clone()]);
        assert_eq!(
            read(&engine, &records, &record.id).await.unwrap(),
            Some(record.clone())
        );

        let target = created(&engine, &records, new_record(&run_id, &scope)).await;
        let late = NativeHolder {
            id: record_id(&run_id),
            record_id: target.id.clone(),
        };
        let holding = engine.begin_transaction().await.unwrap();
        holding
            .batch_get(records.read(&target.id, Guard::Share))
            .await
            .unwrap();
        holders.create(&late, &holding).await.unwrap();
        let deleting = engine.begin_transaction().await.unwrap();
        assert!(
            delete_unless_held(&records, &holders, &target.id, &deleting)
                .await
                .unwrap()
                .is_empty()
        );
        assert!(committed(deleting).await);
        assert!(!committed(holding).await);
        let retried = engine
            .transaction(|tx| {
                let records = records.clone();
                let holders = holders.clone();
                let late = late.clone();
                async move {
                    tx.batch_get(records.read(&late.record_id, Guard::Share))
                        .await?;
                    holders.create(&late, &tx).await
                }
            })
            .await;
        assert!(matches!(retried, Err(TitoError::NotFound(_))));
        assert_eq!(read(&engine, &holders, &late.id).await.unwrap(), None);
        assert_eq!(read(&engine, &records, &target.id).await.unwrap(), None);
        let mut held = holders.query_by_index(HOLDER_BY_RECORD);
        held.value(TitoIndexField::Text(target.id.clone()));
        assert!(held.execute_ids(None).await.unwrap().items.is_empty());

        remove(&engine, &holders, &holder.id).await.unwrap();
        remove(&engine, &records, &record.id).await.unwrap();
    })
    .await
    .expect("The native holder and delete races must finish within their owned run");
}

#[tokio::test]
async fn native_concurrent_claims_of_one_unique_value_conflict_and_the_retry_is_refused() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let claims = engine.clone().model::<NativeClaim>();
        let scope = record_id(&run_id);
        let claim = |value: &str| NativeClaim {
            id: record_id(&run_id),
            scope: scope.clone(),
            value: value.into(),
        };
        let taken = |value: &str| TitoError::UniqueViolation {
            model: "native-fence-claims".into(),
            index: CLAIM_VALUE.into(),
            values: claim_value(&scope, value),
        };

        let first = claim("asset-7");
        let second = claim("asset-7");
        let first_writer = engine.begin_transaction().await.unwrap();
        let second_writer = engine.begin_transaction().await.unwrap();
        claims.create(&first, &first_writer).await.unwrap();
        claims.create(&second, &second_writer).await.unwrap();
        first_writer.commit().await.unwrap();
        let kept = second_writer.clone();
        assert!(matches!(
            second_writer.commit().await,
            Err(TitoError::Retryable(_))
        ));
        let _ = kept.rollback().await;
        assert_eq!(
            create(&engine, &claims, &second).await,
            Err(taken("asset-7"))
        );
        assert_eq!(
            unique_claim(&engine, &claims, &scope, "asset-7")
                .await
                .unwrap(),
            first
        );
        assert_eq!(read(&engine, &claims, &second.id).await.unwrap(), None);

        let third = claim("asset-8");
        let fourth = claim("asset-8");
        let attempts = Arc::new(AtomicUsize::new(0));
        let refused = engine
            .transaction(|tx| {
                let engine = engine.clone();
                let claims = claims.clone();
                let third = third.clone();
                let fourth = fourth.clone();
                let attempts = attempts.clone();
                async move {
                    claims.create(&fourth, &tx).await?;
                    if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                        create(&engine, &claims, &third).await?;
                    }
                    Ok::<_, TitoError>(())
                }
            })
            .await;
        assert_eq!(refused, Err(taken("asset-8")));
        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            unique_claim(&engine, &claims, &scope, "asset-8")
                .await
                .unwrap(),
            third
        );
        assert_eq!(read(&engine, &claims, &fourth.id).await.unwrap(), None);

        remove(&engine, &claims, &first.id).await.unwrap();
        remove(&engine, &claims, &third.id).await.unwrap();
    })
    .await
    .expect("The native unique claim race must finish within its owned run");
}

#[tokio::test]
async fn native_unique_values_differing_only_by_case_or_spacing_coexist_and_are_found_exactly() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let claims = engine.clone().model::<NativeClaim>();
        let scope = record_id(&run_id);
        let claimed = [
            "Asset-7",
            "asset-7",
            "ASSET-7",
            " asset-7 ",
            "asset:7",
            "asset\\7",
        ]
        .into_iter()
        .map(|value| NativeClaim {
            id: record_id(&run_id),
            scope: scope.clone(),
            value: value.into(),
        })
        .collect::<Vec<_>>();
        for claim in &claimed {
            create(&engine, &claims, claim).await.unwrap();
        }

        for claim in &claimed {
            assert_eq!(
                unique_claim(&engine, &claims, &scope, &claim.value)
                    .await
                    .unwrap(),
                *claim
            );
        }
        assert!(matches!(
            unique_claim(&engine, &claims, &scope, "aSSET-7").await,
            Err(TitoError::NotFound(_))
        ));
        for claim in &claimed {
            remove(&engine, &claims, &claim.id).await.unwrap();
        }
    })
    .await
    .expect("The native exact unique values must finish within their owned run");
}
