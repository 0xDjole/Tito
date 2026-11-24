mod native;

use native::{native_endpoint, native_engine, remove, save};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tito::backend::tikv::{TiKVBackend, TiKVTransaction};
use tito::types::{
    TitoFindByIndexPayload, TitoFindOneByIndexPayload, TitoIndexConfig, TitoIndexField,
    TitoIndexFieldType, TitoScanPayload, TitoTransaction,
};
use tito::{TitoEngine, TitoError, TitoModel, TitoModelOptions, TitoModelTrait};
use uuid::Uuid;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
struct IndexValueRecord {
    id: String,
    scope: String,
    rank: i64,
    code: String,
    tags: Vec<String>,
    holds: Vec<String>,
    enabled: bool,
    body: String,
    created_at: i64,
    updated_at: i64,
}

fn index(name: &str, fields: Vec<(&str, TitoIndexFieldType)>, condition: bool) -> TitoIndexConfig {
    TitoIndexConfig {
        name: name.into(),
        condition,
        fields: fields
            .into_iter()
            .map(|(name, r#type)| TitoIndexField {
                name: name.into(),
                r#type,
            })
            .collect(),
    }
}

impl TitoModelTrait for IndexValueRecord {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            index(
                "native-value-rank",
                vec![
                    ("scope", TitoIndexFieldType::String),
                    ("rank", TitoIndexFieldType::Number),
                ],
                true,
            ),
            index(
                "native-value-tag",
                vec![
                    ("scope", TitoIndexFieldType::String),
                    ("tags", TitoIndexFieldType::String),
                ],
                self.enabled,
            ),
        ]
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![index(
            "native-value-code",
            vec![
                ("scope", TitoIndexFieldType::String),
                ("code", TitoIndexFieldType::String),
            ],
            true,
        )]
    }

    fn holder_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![index(
            "native-value-holder",
            vec![
                ("scope", TitoIndexFieldType::String),
                ("holds", TitoIndexFieldType::String),
            ],
            true,
        )]
    }

    fn table() -> String {
        "native-index-values".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

async fn fixture() -> (
    TiKVBackend,
    TitoModel<TiKVBackend, IndexValueRecord>,
    String,
) {
    let (endpoint, run_id) = native_endpoint();
    let engine = native_engine(endpoint).await;
    let model = engine
        .clone()
        .model::<IndexValueRecord>(TitoModelOptions::default());
    (engine, model, format!("{run_id}-{}", Uuid::new_v4()))
}

fn record(scope: &str, rank: i64) -> IndexValueRecord {
    IndexValueRecord {
        id: format!("{scope}:{rank}"),
        scope: scope.into(),
        rank,
        code: format!("code:{rank}"),
        tags: vec!["common".into(), format!("tag:{rank}")],
        holds: vec!["shared".into(), format!("held:{rank}")],
        enabled: true,
        body: format!("body {rank}"),
        created_at: 1,
        updated_at: 1,
    }
}

fn primary(record: &IndexValueRecord) -> String {
    format!("table:native-index-values:{}", record.id)
}

fn by_code(record: &IndexValueRecord) -> TitoFindOneByIndexPayload {
    TitoFindOneByIndexPayload {
        index: "native-value-code".into(),
        values: vec![record.scope.clone(), record.code.clone()],
    }
}

fn holder_key(record: &IndexValueRecord, held: &str) -> String {
    format!(
        "holder-index:native-value-holder:scope:{}:holds:{}:{}",
        tito::encode_index_string(&record.scope),
        tito::encode_index_string(held),
        primary(record)
    )
}

async fn assert_record_reads(
    model: &TitoModel<TiKVBackend, IndexValueRecord>,
    record: &IndexValueRecord,
) {
    assert_eq!(
        model
            .find_one_by_index(
                TitoFindOneByIndexPayload {
                    index: "native-value-rank".into(),
                    values: vec![record.scope.clone(), record.rank.to_string()],
                },
                None,
            )
            .await
            .unwrap(),
        *record
    );
    assert_eq!(
        model
            .find_one_by_unique_index_for_share(by_code(record), None)
            .await
            .unwrap(),
        *record
    );
}

async fn holder_ids(
    model: &TitoModel<TiKVBackend, IndexValueRecord>,
    scope: &str,
    held: &str,
) -> Vec<String> {
    let mut holders = model.query_by_index("native-value-holder");
    holders.value(scope).value(held).limit(Some(10));
    holders.execute_ids(None).await.unwrap().items
}

async fn assert_conflict(tx: TiKVTransaction) {
    let retained = tx.clone();
    let outcome = tx.commit().await;
    assert!(
        matches!(outcome, Err(TitoError::Retryable(_))),
        "{outcome:?}"
    );
    let _ = retained.rollback().await;
}

#[tokio::test]
async fn native_index_entries_hold_copies_and_holder_entries_hold_nothing_across_pages() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let (engine, model, scope) = fixture().await;
        let records = (-2..2).map(|rank| record(&scope, rank)).collect::<Vec<_>>();
        for record in &records {
            save(&engine, &model, record.clone()).await.unwrap();
        }
        let tx = engine.begin_transaction().await.unwrap();
        for record in &records {
            let json = serde_json::to_value(record).unwrap();
            let keys = model
                .get_index_keys(primary(record), record, &json)
                .unwrap();
            assert_eq!(keys.len(), 6);
            let stored_primary = tx.get(primary(record)).await.unwrap().unwrap();
            for (key, expected) in keys {
                let bytes = tx.get(&key).await.unwrap().unwrap();
                assert_eq!(bytes, expected);
                if key.starts_with("holder-index:") {
                    assert!(bytes.is_empty(), "{key}");
                } else {
                    assert_eq!(bytes, stored_primary, "{key}");
                }
            }
            for held in ["shared".to_string(), format!("held:{}", record.rank)] {
                assert_eq!(
                    tx.get(holder_key(record, &held)).await.unwrap(),
                    Some(Vec::new())
                );
            }
        }
        tx.rollback().await.unwrap();

        let mut query = model.query_by_index("native-value-rank");
        query.value(&scope).limit(Some(2));
        let first = query.execute(None).await.unwrap();
        assert_eq!(first.items, records[..2]);
        assert!(first.cursor.is_some());
        let ids = query.execute_ids(None).await.unwrap();
        assert_eq!(
            ids.items,
            records[..2]
                .iter()
                .map(|r| r.id.clone())
                .collect::<Vec<_>>()
        );
        assert_eq!(ids.cursor, first.cursor);
        query.cursor(first.cursor);
        let last = query.execute(None).await.unwrap();
        assert_eq!(last.items, records[2..]);
        assert!(last.cursor.is_none());
        query.cursor(None::<String>);
        let reverse = query.execute_reverse(None).await.unwrap();
        assert_eq!(
            reverse.items,
            records[2..].iter().rev().cloned().collect::<Vec<_>>()
        );
        query.cursor(reverse.cursor);
        let reverse_end = query.execute_reverse(None).await.unwrap();
        assert_eq!(
            reverse_end.items,
            records[..2].iter().rev().cloned().collect::<Vec<_>>()
        );
        assert!(reverse_end.cursor.is_none());

        let mut tags = model.query_by_index("native-value-tag");
        tags.value(&scope).value("common").limit(Some(10));
        let page = tags.execute(None).await.unwrap();
        assert_eq!(page.items.len(), records.len());
        assert!(page.cursor.is_none());
        for record in &records {
            assert!(page.items.contains(record));
        }

        let tx = engine.begin_transaction().await.unwrap();
        let start = format!("unique-index:native-index-values:native-value-code:scope:{scope}:");
        let payload = || TitoScanPayload {
            start: start.clone(),
            end: None,
            limit: Some(2),
            cursor: None,
        };
        let (first, more) = model.scan(payload(), &tx).await.unwrap();
        assert!(more);
        assert_eq!(first.len(), 2);
        let (last, more) = model.scan_reverse(payload(), &tx).await.unwrap();
        assert!(more);
        assert_eq!(last.len(), 2);
        assert_ne!(first[0].0, last[0].0);
        for (_, value) in first.into_iter().chain(last) {
            assert!(records.contains(&serde_json::from_value(value).unwrap()));
        }
        tx.rollback().await.unwrap();

        let mut holders = model.query_by_index("native-value-holder");
        holders.value(&scope).value("shared").limit(Some(3));
        let held = holders.execute_ids(None).await.unwrap();
        assert_eq!(held.items.len(), 3);
        assert!(held.cursor.is_some());
        holders.cursor(held.cursor);
        let rest = holders.execute_ids(None).await.unwrap();
        assert_eq!(rest.items.len(), 1);
        assert!(rest.cursor.is_none());
        let mut every = held.items;
        every.extend(rest.items);
        every.sort();
        let mut expected = records.iter().map(|r| r.id.clone()).collect::<Vec<_>>();
        expected.sort();
        assert_eq!(every, expected);
        assert_eq!(
            holder_ids(&model, &scope, "held:1").await,
            vec![records[3].id.clone()]
        );

        for record in &records {
            assert_record_reads(&model, record).await;
            assert!(remove(&engine, &model, &record.id).await.unwrap());
        }
        assert!(holder_ids(&model, &scope, "shared").await.is_empty());
    })
    .await
    .expect("Native copied index pages must finish within their owned run");
}

#[tokio::test]
async fn native_index_reads_come_from_the_copies_and_id_queries_skip_values() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let (engine, model, scope) = fixture().await;
        let record = IndexValueRecord {
            body: "x".repeat(1024 * 1024),
            ..record(&scope, 1)
        };
        save(&engine, &model, record.clone()).await.unwrap();
        let json = serde_json::to_value(&record).unwrap();
        let keys = model
            .get_index_keys(primary(&record), &record, &json)
            .unwrap();
        let (copy_key, copy_bytes) = keys
            .iter()
            .find(|(key, _)| key.starts_with("index:native-value-rank:"))
            .unwrap();
        let tx = engine.begin_transaction().await.unwrap();
        let primary_bytes = tx.get(primary(&record)).await.unwrap().unwrap();
        tx.delete(primary(&record)).await.unwrap();
        tx.commit().await.unwrap();

        let mut copies = model.query_by_index("native-value-rank");
        copies.value(&scope);
        assert_eq!(
            copies.execute(None).await.unwrap().items,
            vec![record.clone()]
        );
        assert_eq!(
            copies.execute_reverse(None).await.unwrap().items,
            vec![record.clone()]
        );
        assert_eq!(
            model
                .find_one_by_unique_index_for_share(by_code(&record), None)
                .await
                .unwrap(),
            record
        );

        let tx = engine.begin_transaction().await.unwrap();
        tx.put(copy_key, b"invalid JSON").await.unwrap();
        tx.commit().await.unwrap();
        assert_eq!(
            copies.execute_ids(None).await.unwrap().items,
            vec![record.id.clone()]
        );
        assert!(matches!(
            copies.execute(None).await,
            Err(TitoError::DeserializationFailed(_))
        ));
        let tx = engine.begin_transaction().await.unwrap();
        assert_eq!(
            tx.scan_keys(
                copy_key.as_bytes().to_vec()..tito::key_after(copy_key.clone()).into_bytes(),
                1
            )
            .await
            .unwrap(),
            vec![copy_key.as_bytes().to_vec()]
        );
        tx.put(primary(&record), primary_bytes).await.unwrap();
        let wrong = IndexValueRecord {
            id: format!("{scope}:other"),
            ..record.clone()
        };
        tx.put(copy_key, serde_json::to_vec(&wrong).unwrap())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(matches!(
            copies.execute(None).await,
            Err(TitoError::IndexError(_))
        ));
        let moved = IndexValueRecord {
            rank: 7,
            ..record.clone()
        };
        let tx = engine.begin_transaction().await.unwrap();
        tx.put(copy_key, serde_json::to_vec(&moved).unwrap())
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(matches!(
            copies.execute(None).await,
            Err(TitoError::IndexError(ref message)) if message.contains(copy_key.as_str())
        ));
        let tx = engine.begin_transaction().await.unwrap();
        tx.put(copy_key, copy_bytes).await.unwrap();
        tx.commit().await.unwrap();
        assert_record_reads(&model, &record).await;
        assert!(remove(&engine, &model, &record.id).await.unwrap());
    })
    .await
    .expect("Native copied reads must finish within their owned run");
}

#[tokio::test]
async fn native_every_save_refreshes_every_copy_and_removes_old_entries() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let (engine, model, scope) = fixture().await;
        let first = record(&scope, 1);
        save(&engine, &model, first.clone()).await.unwrap();
        let tx = engine.begin_transaction().await.unwrap();
        let updated = model
            .set(IndexValueRecord {
                body: "non-indexed edit".into(),
                ..first.clone()
            })
            .execute(&tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        assert!(updated.updated_at > first.updated_at);
        assert_eq!(updated.created_at, first.created_at);
        assert_record_reads(&model, &updated).await;
        let before = model
            .get_index_keys(
                primary(&updated),
                &updated,
                &serde_json::to_value(&updated).unwrap(),
            )
            .unwrap();
        let tx = engine.begin_transaction().await.unwrap();
        let stored = tx.get(primary(&updated)).await.unwrap().unwrap();
        for (key, _) in &before {
            let bytes = tx.get(key).await.unwrap().unwrap();
            if key.starts_with("holder-index:") {
                assert!(bytes.is_empty(), "{key}");
            } else {
                assert_eq!(bytes, stored, "{key}");
            }
        }
        tx.rollback().await.unwrap();
        let changed = IndexValueRecord {
            rank: 9,
            code: "changed".into(),
            tags: vec!["changed-tag".into()],
            holds: vec!["changed-held".into()],
            enabled: false,
            ..updated.clone()
        };
        let tx = engine.begin_transaction().await.unwrap();
        model
            .set(changed.clone())
            .timestamps(false)
            .execute(&tx)
            .await
            .unwrap();
        assert_eq!(
            model
                .find_one_by_unique_index_for_share(by_code(&changed), Some(&tx))
                .await
                .unwrap(),
            changed
        );
        tx.rollback().await.unwrap();
        assert_record_reads(&model, &updated).await;
        save(&engine, &model, changed.clone()).await.unwrap();
        assert_record_reads(&model, &changed).await;
        assert!(holder_ids(&model, &scope, "shared").await.is_empty());
        assert_eq!(
            holder_ids(&model, &scope, "changed-held").await,
            vec![changed.id.clone()]
        );
        let after = model
            .get_index_keys(
                primary(&changed),
                &changed,
                &serde_json::to_value(&changed).unwrap(),
            )
            .unwrap();
        assert_eq!(after.len(), 3);
        let tx = engine.begin_transaction().await.unwrap();
        for (key, _) in &before {
            assert!(tx.get(key).await.unwrap().is_none(), "old entry {key}");
        }
        for (key, bytes) in &after {
            assert_eq!(tx.get(key).await.unwrap().unwrap(), *bytes);
        }
        tx.rollback().await.unwrap();
        assert!(remove(&engine, &model, &changed.id).await.unwrap());
        let tx = engine.begin_transaction().await.unwrap();
        for (key, _) in after {
            assert!(tx.get(key).await.unwrap().is_none());
        }
        assert!(tx.get(primary(&changed)).await.unwrap().is_none());
        assert!(tx
            .get(format!("reverse-index:{}", primary(&changed)))
            .await
            .unwrap()
            .is_none());
        tx.rollback().await.unwrap();
    })
    .await
    .expect("Native copy writes must finish within their owned run");
}

#[tokio::test]
async fn native_copied_unique_entries_keep_claims_and_batch_swaps() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let (engine, model, scope) = fixture().await;
        let first = record(&scope, 1);
        let second = record(&scope, 2);
        save(&engine, &model, first.clone()).await.unwrap();
        save(&engine, &model, second.clone()).await.unwrap();
        let duplicate = IndexValueRecord {
            code: first.code.clone(),
            ..record(&scope, 3)
        };
        assert!(matches!(
            save(&engine, &model, duplicate.clone()).await,
            Err(TitoError::UniqueViolation { .. })
        ));
        assert!(matches!(
            model.get_for_share(&duplicate.id, None).await,
            Err(TitoError::NotFound(_))
        ));
        let first_next = IndexValueRecord {
            code: second.code.clone(),
            body: "first swapped".into(),
            ..first.clone()
        };
        let second_next = IndexValueRecord {
            code: first.code.clone(),
            body: "second swapped".into(),
            ..second.clone()
        };
        let tx = engine.begin_transaction().await.unwrap();
        assert_eq!(
            model
                .replace_many(vec![
                    (first.clone(), first_next.clone()),
                    (second.clone(), second_next.clone())
                ])
                .timestamps(false)
                .execute(&tx)
                .await
                .unwrap(),
            vec![first_next.clone(), second_next.clone()]
        );
        tx.commit().await.unwrap();
        assert_record_reads(&model, &first_next).await;
        assert_record_reads(&model, &second_next).await;

        let contender = record(&scope, 4);
        let losing = IndexValueRecord {
            code: contender.code.clone(),
            ..record(&scope, 5)
        };
        let winner_tx = engine.begin_transaction().await.unwrap();
        let loser_tx = engine.begin_transaction().await.unwrap();
        model
            .set(contender.clone())
            .timestamps(false)
            .execute(&winner_tx)
            .await
            .unwrap();
        model
            .set(losing.clone())
            .timestamps(false)
            .execute(&loser_tx)
            .await
            .unwrap();
        winner_tx.commit().await.unwrap();
        assert_conflict(loser_tx).await;
        assert!(matches!(
            save(&engine, &model, losing.clone()).await,
            Err(TitoError::UniqueViolation { .. })
        ));
        assert!(matches!(
            model.get_for_share(&losing.id, None).await,
            Err(TitoError::NotFound(_))
        ));
        assert_record_reads(&model, &contender).await;
        for record in [first_next, second_next, contender] {
            assert!(remove(&engine, &model, &record.id).await.unwrap());
        }
    })
    .await
    .expect("Native copy ownership must finish within its owned run");
}

#[tokio::test]
async fn native_copied_unique_hits_for_share_guard_the_owner_while_lists_remain_unguarded() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let (engine, model, scope) = fixture().await;
        let first = record(&scope, 1);
        save(&engine, &model, first.clone()).await.unwrap();
        let guarded = engine.begin_transaction().await.unwrap();
        assert_eq!(
            model
                .find_one_by_unique_index_for_share(by_code(&first), Some(&guarded))
                .await
                .unwrap(),
            first
        );
        guarded
            .delete(format!("native-value-probe:{scope}:guarded"))
            .await
            .unwrap();
        let next = IndexValueRecord {
            body: "winner".into(),
            ..first.clone()
        };
        save(&engine, &model, next.clone()).await.unwrap();
        assert_conflict(guarded).await;

        let list = engine.begin_transaction().await.unwrap();
        let mut query = model.query_by_index("native-value-rank");
        query.value(&scope);
        assert_eq!(
            query.execute(Some(&list)).await.unwrap().items,
            vec![next.clone()]
        );
        let mut holders = model.query_by_index("native-value-holder");
        holders.value(&scope).value("shared");
        assert_eq!(
            holders.execute_ids(Some(&list)).await.unwrap().items,
            vec![next.id.clone()]
        );
        list.delete(format!("native-value-probe:{scope}:list"))
            .await
            .unwrap();
        let current = IndexValueRecord {
            body: "second winner".into(),
            ..next.clone()
        };
        save(&engine, &model, current.clone()).await.unwrap();
        list.commit().await.unwrap();

        let read_only = engine.begin_transaction().await.unwrap();
        assert_eq!(
            model
                .find_one_by_unique_index_for_share(by_code(&current), Some(&read_only))
                .await
                .unwrap(),
            current
        );
        assert!(remove(&engine, &model, &current.id).await.unwrap());
        read_only.commit().await.unwrap();
    })
    .await
    .expect("Native copied guards must finish within their owned run");
}

#[tokio::test]
async fn native_holder_entries_are_read_only_by_key_and_refuse_record_reads() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let (engine, model, scope) = fixture().await;
        let record = record(&scope, 1);
        save(&engine, &model, record.clone()).await.unwrap();
        let key = holder_key(&record, "shared");
        let payload = TitoFindByIndexPayload {
            index: "native-value-holder".into(),
            values: vec![scope.clone(), "shared".into()],
            limit: Some(1),
            cursor: None,
        };
        let mut query = model.query_by_index("native-value-holder");
        query.value(&scope).value("shared");
        for stored in [Vec::new(), b"not JSON".to_vec(), serde_json::to_vec(&record).unwrap()] {
            let tx = engine.begin_transaction().await.unwrap();
            tx.put(&key, stored).await.unwrap();
            tx.commit().await.unwrap();
            assert_eq!(
                query.execute_ids(None).await.unwrap().items,
                vec![record.id.clone()]
            );
            let tx = engine.begin_transaction().await.unwrap();
            let scan = || TitoScanPayload {
                start: key.clone(),
                end: Some(tito::key_after(key.clone())),
                limit: Some(1),
                cursor: None,
            };
            let errors = [
                query.execute(None).await.unwrap_err(),
                query.execute_reverse(None).await.unwrap_err(),
                model.find_by_index(payload.clone(), None).await.unwrap_err(),
                model
                    .find_by_index_reverse(payload.clone(), None)
                    .await
                    .unwrap_err(),
                model
                    .find_one_by_index(
                        TitoFindOneByIndexPayload {
                            index: "native-value-holder".into(),
                            values: vec![scope.clone(), "shared".into()],
                        },
                        None,
                    )
                    .await
                    .unwrap_err(),
            ];
            for error in errors {
                assert!(
                    matches!(error, TitoError::IndexError(ref message) if message.contains("holder index")),
                    "{error:?}"
                );
            }
            for error in [
                model.scan(scan(), &tx).await.unwrap_err(),
                model.scan_reverse(scan(), &tx).await.unwrap_err(),
            ] {
                assert!(
                    matches!(error, TitoError::IndexError(ref message) if message.contains(key.as_str())),
                    "{error:?}"
                );
            }
            assert_eq!(
                tx.scan_keys(
                    key.as_bytes().to_vec()..tito::key_after(key.clone()).into_bytes(),
                    2
                )
                .await
                .unwrap(),
                vec![key.as_bytes().to_vec()]
            );
            tx.rollback().await.unwrap();
        }
        assert_record_reads(&model, &record).await;
        assert!(remove(&engine, &model, &record.id).await.unwrap());
        let tx = engine.begin_transaction().await.unwrap();
        assert!(tx.get(&key).await.unwrap().is_none());
        tx.rollback().await.unwrap();
    })
    .await
    .expect("Native holder entries must finish within their owned run");
}

#[tokio::test]
async fn native_batch_creation_commits_indexed_records_in_input_order_and_rolls_back_with_its_transaction(
) {
    let (engine, model, scope) = fixture().await;
    let records = vec![record(&scope, 2), record(&scope, 1)];
    let fence_prefix = format!(
        "fence:{}:{}",
        tito::encode_index_string(&IndexValueRecord::table()),
        tito::encode_index_string(&records[0].id)
    );
    let mut fences = (0..16)
        .map(|index| format!("{fence_prefix}:{index}"))
        .collect::<Vec<_>>();
    fences.push(format!("{fence_prefix}:exclusive"));
    let previous = engine.begin_transaction().await.unwrap();
    for key in &fences {
        previous.put(key, b"previous fence").await.unwrap();
    }
    previous.commit().await.unwrap();
    let tx = engine.begin_transaction().await.unwrap();
    let result = model
        .create_many(records.clone())
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(result, records);
    for key in &fences {
        assert!(tx.get(key).await.unwrap().is_none());
    }
    for value in &records {
        assert_eq!(
            model.get_for_share(&value.id, Some(&tx)).await.unwrap(),
            *value
        );
        assert!(matches!(
            model.get_for_share(&value.id, None).await,
            Err(TitoError::NotFound(_))
        ));
        let stored = tx.get(primary(value)).await.unwrap().unwrap();
        let json = serde_json::to_value(value).unwrap();
        for (key, _) in model.get_index_keys(primary(value), value, &json).unwrap() {
            let bytes = tx.get(&key).await.unwrap().unwrap();
            if key.starts_with("holder-index:") {
                assert!(bytes.is_empty(), "{key}");
            } else {
                assert_eq!(bytes, stored, "{key}");
            }
        }
    }
    tx.rollback().await.unwrap();
    let snapshot = engine.begin_transaction().await.unwrap();
    for key in &fences {
        assert_eq!(
            snapshot.get(key).await.unwrap().as_deref(),
            Some(b"previous fence".as_slice())
        );
    }
    snapshot.rollback().await.unwrap();
    for value in &records {
        assert!(matches!(
            model.get_for_share(&value.id, None).await,
            Err(TitoError::NotFound(_))
        ));
        assert!(matches!(
            model
                .find_one_by_unique_index_for_share(by_code(value), None)
                .await,
            Err(TitoError::NotFound(_))
        ));
    }

    let tx = engine.begin_transaction().await.unwrap();
    let stamped = model
        .create_many(records.clone())
        .execute(&tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
    for (value, original) in stamped.iter().zip(&records) {
        assert_eq!(value.id, original.id);
        assert!(value.created_at > original.created_at);
        assert_eq!(value.created_at, value.updated_at);
        assert_record_reads(&model, value).await;
        assert_eq!(
            model
                .get_versioned_for_share(&value.id, None)
                .await
                .unwrap()
                .value,
            *value
        );
        remove(&engine, &model, &value.id).await.unwrap();
        assert!(matches!(
            model
                .find_one_by_unique_index_for_share(by_code(value), None)
                .await,
            Err(TitoError::NotFound(_))
        ));
    }
}

#[tokio::test]
async fn native_batch_creation_rejects_conflicts_corruption_and_limits_before_staging_any_record() {
    let (engine, model, scope) = fixture().await;
    let first = record(&scope, 1);
    let existing = record(&scope, 2);
    save(&engine, &model, existing.clone()).await.unwrap();
    let mut rival = record(&scope, 3);
    rival.code = existing.code.clone();
    let mut twin = record(&scope, 4);
    twin.code = first.code.clone();
    let invalid = engine.begin_transaction().await.unwrap();
    assert!(matches!(
        model.create_many(vec![]).execute(&invalid).await,
        Err(TitoError::InvalidInput(_))
    ));
    let too_many = (0..=tito::MAX_BATCH_RECORDS)
        .map(|i| record(&scope, i as i64))
        .collect();
    assert!(matches!(
        model.create_many(too_many).execute(&invalid).await,
        Err(TitoError::InvalidInput(_))
    ));
    let mut large = record(&scope, 5);
    large.body = "x".repeat(tito::MAX_BATCH_BYTES);
    assert!(matches!(
        model.create_many(vec![large]).execute(&invalid).await,
        Err(TitoError::InvalidInput(_))
    ));
    let mut copied = record(&scope, 7);
    copied.body = "x".repeat(tito::MAX_BATCH_BYTES / 4);
    assert!(matches!(
        model.create_many(vec![copied]).execute(&invalid).await,
        Err(TitoError::InvalidInput(ref message)) if message.contains("bytes")
    ));
    assert!(matches!(
        model
            .create_many(vec![first.clone(), first.clone()])
            .execute(&invalid)
            .await,
        Err(TitoError::InvalidInput(_))
    ));
    assert!(matches!(
        model
            .create_many(vec![first.clone(), existing.clone()])
            .execute(&invalid)
            .await,
        Err(TitoError::CreateFailed(_))
    ));
    for conflict in [rival, twin] {
        assert!(matches!(
            model
                .create_many(vec![first.clone(), conflict])
                .execute(&invalid)
                .await,
            Err(TitoError::UniqueViolation { .. })
        ));
    }
    let damaged = record(&scope, 6);
    let reverse = format!("reverse-index:{}", primary(&damaged));
    invalid.put(&reverse, b"orphan").await.unwrap();
    assert!(matches!(
        model
            .create_many(vec![first.clone(), damaged])
            .execute(&invalid)
            .await,
        Err(TitoError::IndexError(_))
    ));
    invalid.delete(reverse).await.unwrap();
    invalid.commit().await.unwrap();
    assert!(matches!(
        model.get_for_share(&first.id, None).await,
        Err(TitoError::NotFound(_))
    ));
    assert_record_reads(&model, &existing).await;
    remove(&engine, &model, &existing.id).await.unwrap();
}

#[tokio::test]
async fn native_batch_creation_competing_with_another_batch_or_set_leaves_only_the_committed_owner()
{
    let (engine, model, scope) = fixture().await;
    for same_id in [true, false] {
        let rank = if same_id { 10 } else { 20 };
        let winner = record(&scope, rank);
        let mut rival = record(&scope, rank + 1);
        if same_id {
            rival.id = winner.id.clone();
        } else {
            rival.code = winner.code.clone();
        }
        let witness = record(&scope, rank + 2);
        let first = engine.begin_transaction().await.unwrap();
        let second = engine.begin_transaction().await.unwrap();
        if same_id {
            model
                .set(winner.clone())
                .timestamps(false)
                .execute(&first)
                .await
                .unwrap();
        } else {
            model
                .create_many(vec![winner.clone()])
                .timestamps(false)
                .execute(&first)
                .await
                .unwrap();
        }
        model
            .create_many(vec![witness.clone(), rival.clone()])
            .timestamps(false)
            .execute(&second)
            .await
            .unwrap();
        first.commit().await.unwrap();
        assert_conflict(second).await;
        let retry = engine.begin_transaction().await.unwrap();
        let error = model
            .create_many(vec![witness.clone(), rival])
            .timestamps(false)
            .execute(&retry)
            .await
            .unwrap_err();
        if same_id {
            assert!(matches!(error, TitoError::CreateFailed(_)));
        } else {
            assert!(matches!(error, TitoError::UniqueViolation { .. }));
        }
        retry.commit().await.unwrap();
        assert!(matches!(
            model.get_for_share(&witness.id, None).await,
            Err(TitoError::NotFound(_))
        ));
        assert_record_reads(&model, &winner).await;
        remove(&engine, &model, &winner.id).await.unwrap();
    }
}
