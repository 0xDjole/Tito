use tito::types::TitoIndexValue;
mod native;

use native::{native_endpoint, native_engine, remove, save};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tito::backend::tikv::{TiKVBackend, TiKVTransaction};
use tito::types::{
    TitoFindByIndexPayload, TitoFindOneByIndexPayload, TitoIndexConfig, TitoIndexField,
    TitoIndexFieldType, TitoRecordVersion, TitoScanPayload, TitoTransaction,
};
use tito::{TitoEngine, TitoError, TitoModel, TitoModelOptions, TitoModelTrait};
use uuid::Uuid;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeRecord {
    id: String,
    scope: String,
    value: String,
    name: String,
}

impl TitoModelTrait for NativeRecord {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            value: TitoIndexValue::RecordReference,
            condition: true,
            name: "native-guard-record-by-scope".into(),
            fields: vec![TitoIndexField {
                name: "scope".into(),
                r#type: TitoIndexFieldType::String,
            }],
        }]
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            value: TitoIndexValue::RecordReference,
            condition: true,
            name: "native-guard-record-value".into(),
            fields: vec![
                TitoIndexField {
                    name: "scope".into(),
                    r#type: TitoIndexFieldType::String,
                },
                TitoIndexField {
                    name: "value".into(),
                    r#type: TitoIndexFieldType::String,
                },
            ],
        }]
    }

    fn table() -> String {
        "native-guard-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeHolder {
    id: String,
    record_id: String,
}

impl TitoModelTrait for NativeHolder {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            value: TitoIndexValue::RecordReference,
            condition: true,
            name: "native-guard-holder-by-record".into(),
            fields: vec![TitoIndexField {
                name: "record_id".into(),
                r#type: TitoIndexFieldType::String,
            }],
        }]
    }

    fn table() -> String {
        "native-guard-holders".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeClaim {
    id: String,
    scope: String,
    value: String,
}

impl TitoModelTrait for NativeClaim {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            value: TitoIndexValue::RecordReference,
            condition: true,
            name: "native-claim-value".into(),
            fields: vec![
                TitoIndexField {
                    name: "scope".into(),
                    r#type: TitoIndexFieldType::String,
                },
                TitoIndexField {
                    name: "value".into(),
                    r#type: TitoIndexFieldType::String,
                },
            ],
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

fn by_value(record: &NativeRecord) -> TitoFindOneByIndexPayload {
    TitoFindOneByIndexPayload {
        index: "native-guard-record-value".into(),
        values: vec![record.scope.clone(), record.value.clone()],
    }
}

fn records_of(engine: &TiKVBackend) -> TitoModel<TiKVBackend, NativeRecord> {
    engine
        .clone()
        .model::<NativeRecord>(TitoModelOptions::default())
}

async fn write_scratch(tx: &TiKVTransaction, run_id: &str) {
    tx.delete(format!("native-guard-scratch:{}", record_id(run_id)))
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
    Get,
    GetMany,
    GetVersioned,
    UniqueHit,
    Exclusive,
}

async fn read_by_id(
    records: &TitoModel<TiKVBackend, NativeRecord>,
    read: NativeRead,
    record: &NativeRecord,
    tx: &TiKVTransaction,
) -> Option<TitoRecordVersion> {
    match read {
        NativeRead::Get => {
            assert_eq!(
                records.get(&record.id).execute(Some(tx)).await.unwrap(),
                *record
            );
            None
        }
        NativeRead::GetMany => {
            assert_eq!(
                records
                    .get_many(vec![record.id.clone()])
                    .execute(Some(tx))
                    .await
                    .unwrap(),
                vec![record.clone()]
            );
            None
        }
        NativeRead::GetVersioned => {
            let versioned = records.get_versioned(&record.id, Some(tx)).await.unwrap();
            assert_eq!(versioned.value, *record);
            Some(versioned.version)
        }
        NativeRead::UniqueHit => {
            assert_eq!(
                records
                    .find_one_by_unique_index(by_value(record), Some(tx))
                    .await
                    .unwrap(),
                *record
            );
            None
        }
        NativeRead::Exclusive => {
            assert_eq!(
                records.get_exclusive(&record.id, tx).await.unwrap(),
                *record
            );
            None
        }
    }
}

#[derive(Clone, Copy, Debug)]
enum NativeWriter {
    Rename,
    Remove,
}

async fn stage_writer(
    records: &TitoModel<TiKVBackend, NativeRecord>,
    writer: NativeWriter,
    record: &NativeRecord,
    tx: &TiKVTransaction,
) {
    match writer {
        NativeWriter::Rename => {
            records
                .set(with_name(record, "renamed by a writer"))
                .timestamps(false)
                .execute(tx)
                .await
                .unwrap();
        }
        NativeWriter::Remove => {
            assert!(records.remove(&record.id, tx).await.unwrap());
        }
    }
}

async fn delete_unless_held(
    records: &TitoModel<TiKVBackend, NativeRecord>,
    holders: &TitoModel<TiKVBackend, NativeHolder>,
    id: &str,
    tx: &TiKVTransaction,
) -> Result<Vec<String>, TitoError> {
    let held = holders
        .find_by_index(
            TitoFindByIndexPayload {
                index: "native-guard-holder-by-record".into(),
                values: vec![id.to_string()],
                limit: Some(20),
                cursor: None,
            },
            Some(tx),
        )
        .await?
        .items
        .into_iter()
        .map(|holder| holder.id)
        .collect::<Vec<_>>();
    if held.is_empty() {
        records.remove(id, tx).await?;
    }
    Ok(held)
}

#[tokio::test]
async fn native_every_read_by_id_in_a_writing_transaction_conflicts_with_a_save_of_its_record() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        for read in [
            NativeRead::Get,
            NativeRead::GetMany,
            NativeRead::GetVersioned,
            NativeRead::UniqueHit,
            NativeRead::Exclusive,
        ] {
            let record = save(&engine, &records, new_record(&run_id, &scope))
                .await
                .unwrap();
            let reader = engine.begin_transaction().await.unwrap();
            let version = read_by_id(&records, read, &record, &reader).await;
            write_scratch(&reader, &run_id).await;
            let renamed = save(
                &engine,
                &records,
                with_name(&record, "renamed by a committed writer"),
            )
            .await
            .unwrap();
            assert!(
                !committed(reader).await,
                "{read:?} in a writing transaction must conflict with a save of its record"
            );
            if let Some(version) = version {
                assert_ne!(
                    records
                        .get_versioned(&record.id, None)
                        .await
                        .unwrap()
                        .version,
                    version
                );
            }
            assert_eq!(
                records.get(&record.id).execute(None).await.unwrap(),
                renamed
            );
            assert!(remove(&engine, &records, &record.id).await.unwrap());
        }
    })
    .await
    .expect("The native reads by id must finish within their owned run");
}

#[tokio::test]
async fn native_index_reads_scans_and_misses_add_no_guard() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let record = save(&engine, &records, new_record(&run_id, &scope))
            .await
            .unwrap();

        let reader = engine.begin_transaction().await.unwrap();
        assert_eq!(
            records
                .find_by_index(
                    TitoFindByIndexPayload {
                        index: "native-guard-record-by-scope".into(),
                        values: vec![scope.clone()],
                        limit: Some(10),
                        cursor: None,
                    },
                    Some(&reader),
                )
                .await
                .unwrap()
                .items,
            vec![record.clone()]
        );
        let mut query = records.query_by_index("native-guard-record-by-scope");
        query.value(scope.clone());
        assert_eq!(
            query.execute_reverse(Some(&reader)).await.unwrap().items,
            vec![record.clone()]
        );
        assert_eq!(
            query.execute_ids(Some(&reader)).await.unwrap().items,
            vec![record.id.clone()]
        );
        assert_eq!(
            records
                .find_one_by_index(
                    TitoFindOneByIndexPayload {
                        index: "native-guard-record-by-scope".into(),
                        values: vec![scope.clone()],
                    },
                    Some(&reader),
                )
                .await
                .unwrap(),
            record
        );
        let (scanned, _) = records
            .scan(
                TitoScanPayload {
                    start: format!("table:native-guard-records:{}", record.id),
                    end: None,
                    limit: Some(10),
                    cursor: None,
                },
                &reader,
            )
            .await
            .unwrap();
        assert_eq!(scanned.len(), 1);
        assert!(matches!(
            records
                .get(&record_id(&run_id))
                .execute(Some(&reader))
                .await,
            Err(TitoError::NotFound(_))
        ));
        assert!(matches!(
            records
                .find_one_by_unique_index(
                    TitoFindOneByIndexPayload {
                        index: "native-guard-record-value".into(),
                        values: vec![scope.clone(), "missing".into()],
                    },
                    Some(&reader),
                )
                .await,
            Err(TitoError::NotFound(_))
        ));
        write_scratch(&reader, &run_id).await;
        let renamed = save(
            &engine,
            &records,
            with_name(&record, "renamed by a committed writer"),
        )
        .await
        .unwrap();
        assert!(committed(reader).await);

        assert_eq!(
            records.get(&record.id).execute(None).await.unwrap(),
            renamed
        );
        assert!(remove(&engine, &records, &record.id).await.unwrap());
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
                save(&engine, &records, new_record(&run_id, &scope))
                    .await
                    .unwrap()
                    .id,
            );
        }

        let mut conflicts = 0;
        for _ in 0..480 {
            let first = engine.begin_transaction().await.unwrap();
            let second = engine.begin_transaction().await.unwrap();
            for tx in [&first, &second] {
                assert_eq!(
                    records
                        .get_many(ids.clone())
                        .execute(Some(tx))
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
            assert!(remove(&engine, &records, id).await.unwrap());
        }
    })
    .await
    .expect("The native shared read pairs must finish within their owned run");
}

#[tokio::test]
async fn native_two_exclusive_reads_of_one_record_always_conflict_and_the_retry_passes() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let record = save(&engine, &records, new_record(&run_id, &scope))
            .await
            .unwrap();

        for _ in 0..16 {
            let first = engine.begin_transaction().await.unwrap();
            let second = engine.begin_transaction().await.unwrap();
            for tx in [&first, &second] {
                assert_eq!(records.get_exclusive(&record.id, tx).await.unwrap(), record);
                write_scratch(tx, &run_id).await;
            }
            assert!(committed(first).await);
            assert!(!committed(second).await);
        }

        let read = engine
            .transaction(|tx| {
                let records = records.clone();
                let id = record.id.clone();
                let run_id = run_id.clone();
                async move {
                    let read = records.get_exclusive(&id, &tx).await?;
                    write_scratch(&tx, &run_id).await;
                    Ok::<_, TitoError>(read)
                }
            })
            .await
            .unwrap();
        assert_eq!(read, record);
        assert!(remove(&engine, &records, &record.id).await.unwrap());
    })
    .await
    .expect("The native exclusive read pairs must finish within their owned run");
}

#[tokio::test]
async fn native_a_shared_and_an_exclusive_read_of_one_record_commit_together_in_either_order() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let record = save(&engine, &records, new_record(&run_id, &scope))
            .await
            .unwrap();

        for pair in 0..16 {
            let shared = engine.begin_transaction().await.unwrap();
            let exclusive = engine.begin_transaction().await.unwrap();
            assert_eq!(
                records
                    .get(&record.id)
                    .execute(Some(&shared))
                    .await
                    .unwrap(),
                record
            );
            assert_eq!(
                records.get_exclusive(&record.id, &exclusive).await.unwrap(),
                record
            );
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

        assert!(remove(&engine, &records, &record.id).await.unwrap());
    })
    .await
    .expect("The native shared and exclusive read pairs must finish within their owned run");
}

#[tokio::test]
async fn native_a_save_and_a_remove_conflict_with_shared_and_exclusive_reads_in_either_order() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        for read in [NativeRead::Get, NativeRead::Exclusive] {
            for writer in [NativeWriter::Rename, NativeWriter::Remove] {
                let record = save(&engine, &records, new_record(&run_id, &scope))
                    .await
                    .unwrap();
                let guarded = engine.begin_transaction().await.unwrap();
                read_by_id(&records, read, &record, &guarded).await;
                write_scratch(&guarded, &run_id).await;
                let writing = engine.begin_transaction().await.unwrap();
                stage_writer(&records, writer, &record, &writing).await;
                assert!(committed(writing).await);
                assert!(
                    !committed(guarded).await,
                    "{read:?} must conflict with a committed {writer:?} of its record"
                );
                let retried = engine.begin_transaction().await.unwrap();
                let reread = match read {
                    NativeRead::Exclusive => records.get_exclusive(&record.id, &retried).await,
                    _ => records.get(&record.id).execute(Some(&retried)).await,
                };
                retried.rollback().await.unwrap();
                match writer {
                    NativeWriter::Rename => {
                        assert_eq!(reread.unwrap(), with_name(&record, "renamed by a writer"));
                        assert!(remove(&engine, &records, &record.id).await.unwrap());
                    }
                    NativeWriter::Remove => {
                        assert!(matches!(reread, Err(TitoError::NotFound(_))));
                    }
                }

                let record = save(&engine, &records, new_record(&run_id, &scope))
                    .await
                    .unwrap();
                let guarded = engine.begin_transaction().await.unwrap();
                read_by_id(&records, read, &record, &guarded).await;
                write_scratch(&guarded, &run_id).await;
                let writing = engine.begin_transaction().await.unwrap();
                stage_writer(&records, writer, &record, &writing).await;
                assert!(committed(guarded).await);
                assert!(
                    !committed(writing).await,
                    "A {writer:?} must conflict with a committed {read:?} of its record"
                );
                assert_eq!(records.get(&record.id).execute(None).await.unwrap(), record);
                assert!(remove(&engine, &records, &record.id).await.unwrap());
            }
        }
    })
    .await
    .expect("The native writer and guard races must finish within their owned run");
}

#[tokio::test]
async fn native_a_transaction_that_writes_nothing_adds_no_guard_and_get_exclusive_counts_as_a_write(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let records = records_of(&engine);
        let scope = record_id(&run_id);
        let mut saved = Vec::new();
        for _ in 0..4 {
            saved.push(
                save(&engine, &records, new_record(&run_id, &scope))
                    .await
                    .unwrap(),
            );
        }

        let reader = engine.begin_transaction().await.unwrap();
        for (read, record) in [
            NativeRead::Get,
            NativeRead::GetVersioned,
            NativeRead::GetMany,
            NativeRead::UniqueHit,
        ]
        .into_iter()
        .zip(&saved)
        {
            read_by_id(&records, read, record, &reader).await;
        }
        for record in &saved {
            save(&engine, &records, with_name(record, "renamed while read"))
                .await
                .unwrap();
        }
        assert!(committed(reader).await);

        let held = engine.begin_transaction().await.unwrap();
        let competing = engine.begin_transaction().await.unwrap();
        records.get_exclusive(&saved[0].id, &held).await.unwrap();
        records
            .get_exclusive(&saved[0].id, &competing)
            .await
            .unwrap();
        assert!(committed(competing).await);
        assert!(!committed(held).await);

        let exclusive = engine.begin_transaction().await.unwrap();
        records
            .get_exclusive(&saved[0].id, &exclusive)
            .await
            .unwrap();
        records
            .get(&saved[1].id)
            .execute(Some(&exclusive))
            .await
            .unwrap();
        save(
            &engine,
            &records,
            with_name(&saved[1], "renamed while held"),
        )
        .await
        .unwrap();
        assert!(!committed(exclusive).await);

        for record in &saved {
            assert!(remove(&engine, &records, &record.id).await.unwrap());
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
        let first = save(&engine, &records, new_record(&run_id, &scope))
            .await
            .unwrap();
        let second = save(&engine, &records, new_record(&run_id, &scope))
            .await
            .unwrap();
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
                    let read = if attempt == 1 { &first } else { &second };
                    records.get(&read.id).execute(Some(&tx)).await?;
                    write_scratch(&tx, &run_id).await;
                    save(
                        &engine,
                        &records,
                        with_name(&first, &format!("renamed during attempt {attempt}")),
                    )
                    .await?;
                    Ok::<_, TitoError>(())
                }
            })
            .await
            .unwrap();

        assert_eq!(attempts.load(Ordering::SeqCst), 2);
        assert_eq!(
            records.get(&first.id).execute(None).await.unwrap().name,
            "renamed during attempt 2"
        );
        assert!(remove(&engine, &records, &first.id).await.unwrap());
        assert!(remove(&engine, &records, &second.id).await.unwrap());
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
        let holders = engine
            .clone()
            .model::<NativeHolder>(TitoModelOptions::default());
        let scope = record_id(&run_id);

        let record = save(&engine, &records, new_record(&run_id, &scope))
            .await
            .unwrap();
        let holder = NativeHolder {
            id: record_id(&run_id),
            record_id: record.id.clone(),
        };
        let holding = engine.begin_transaction().await.unwrap();
        records
            .get(&record.id)
            .execute(Some(&holding))
            .await
            .unwrap();
        holders
            .set(holder.clone())
            .timestamps(false)
            .execute(&holding)
            .await
            .unwrap();
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
        assert_eq!(records.get(&record.id).execute(None).await.unwrap(), record);

        let target = save(&engine, &records, new_record(&run_id, &scope))
            .await
            .unwrap();
        let late = NativeHolder {
            id: record_id(&run_id),
            record_id: target.id.clone(),
        };
        let holding = engine.begin_transaction().await.unwrap();
        records
            .get(&target.id)
            .execute(Some(&holding))
            .await
            .unwrap();
        holders
            .set(late.clone())
            .timestamps(false)
            .execute(&holding)
            .await
            .unwrap();
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
                    records.get(&late.record_id).execute(Some(&tx)).await?;
                    holders.set(late).timestamps(false).execute(&tx).await
                }
            })
            .await;
        assert!(matches!(retried, Err(TitoError::NotFound(_))));
        assert!(matches!(
            holders.get(&late.id).execute(None).await,
            Err(TitoError::NotFound(_))
        ));
        assert!(matches!(
            records.get(&target.id).execute(None).await,
            Err(TitoError::NotFound(_))
        ));

        assert!(remove(&engine, &holders, &holder.id).await.unwrap());
        assert!(remove(&engine, &records, &record.id).await.unwrap());
    })
    .await
    .expect("The native holder and delete races must finish within their owned run");
}

#[tokio::test]
async fn native_concurrent_claims_of_one_unique_value_conflict_and_the_retry_is_refused() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let claims = engine
            .clone()
            .model::<NativeClaim>(TitoModelOptions::default());
        let scope = record_id(&run_id);
        let first = NativeClaim {
            id: record_id(&run_id),
            scope: scope.clone(),
            value: "asset-7".into(),
        };
        let second = NativeClaim {
            id: record_id(&run_id),
            scope: scope.clone(),
            value: "asset-7".into(),
        };

        let first_writer = engine.begin_transaction().await.unwrap();
        let second_writer = engine.begin_transaction().await.unwrap();
        claims
            .set(first.clone())
            .timestamps(false)
            .execute(&first_writer)
            .await
            .unwrap();
        claims
            .set(second.clone())
            .timestamps(false)
            .execute(&second_writer)
            .await
            .unwrap();
        first_writer.commit().await.unwrap();
        assert!(matches!(
            second_writer.commit().await,
            Err(TitoError::Retryable(_))
        ));

        assert!(matches!(
            save(&engine, &claims, second.clone()).await,
            Err(TitoError::UniqueViolation { .. })
        ));
        assert_eq!(
            claims
                .find_one_by_unique_index(
                    TitoFindOneByIndexPayload {
                        index: "native-claim-value".into(),
                        values: vec![scope.clone(), "asset-7".into()],
                    },
                    None,
                )
                .await
                .unwrap(),
            first
        );
        assert!(matches!(
            claims.get(&second.id).execute(None).await,
            Err(TitoError::NotFound(_))
        ));
        assert!(remove(&engine, &claims, &first.id).await.unwrap());
    })
    .await
    .expect("The native unique claim race must finish within its owned run");
}

#[tokio::test]
async fn native_unique_values_differing_only_by_case_or_spacing_coexist_and_are_found_exactly() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let claims = engine
            .clone()
            .model::<NativeClaim>(TitoModelOptions::default());
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
            save(&engine, &claims, claim.clone()).await.unwrap();
        }

        for claim in &claimed {
            assert_eq!(
                claims
                    .find_one_by_unique_index(
                        TitoFindOneByIndexPayload {
                            index: "native-claim-value".into(),
                            values: vec![scope.clone(), claim.value.clone()],
                        },
                        None,
                    )
                    .await
                    .unwrap(),
                *claim
            );
        }
        assert!(matches!(
            claims
                .find_one_by_unique_index(
                    TitoFindOneByIndexPayload {
                        index: "native-claim-value".into(),
                        values: vec![scope.clone(), "aSSET-7".into()],
                    },
                    None,
                )
                .await,
            Err(TitoError::NotFound(_))
        ));
        for claim in &claimed {
            assert!(remove(&engine, &claims, &claim.id).await.unwrap());
        }
    })
    .await
    .expect("The native exact unique values must finish within their owned run");
}
