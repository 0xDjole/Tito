mod native;

use native::{native_endpoint, native_engine, remove, save};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tito::backend::tikv::{TiKVBackend, TiKVTransaction};
use tito::types::{
    TitoFindOneByIndexPayload, TitoIndexConfig, TitoIndexField, TitoIndexFieldType, TitoIndexValue,
    TitoScanPayload, TitoTransaction,
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
    enabled: bool,
    body: String,
    created_at: i64,
    updated_at: i64,
}

fn index(
    name: &str,
    value: TitoIndexValue,
    fields: Vec<(&str, TitoIndexFieldType)>,
    condition: bool,
) -> TitoIndexConfig {
    TitoIndexConfig {
        name: name.into(),
        value,
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
                "native-value-rank-copy",
                TitoIndexValue::RecordCopy,
                vec![
                    ("scope", TitoIndexFieldType::String),
                    ("rank", TitoIndexFieldType::Number),
                ],
                true,
            ),
            index(
                "native-value-rank-reference",
                TitoIndexValue::RecordReference,
                vec![
                    ("scope", TitoIndexFieldType::String),
                    ("rank", TitoIndexFieldType::Number),
                ],
                true,
            ),
            index(
                "native-value-tag-copy",
                TitoIndexValue::RecordCopy,
                vec![
                    ("scope", TitoIndexFieldType::String),
                    ("tags", TitoIndexFieldType::String),
                ],
                self.enabled,
            ),
            index(
                "native-value-tag-reference",
                TitoIndexValue::RecordReference,
                vec![
                    ("scope", TitoIndexFieldType::String),
                    ("tags", TitoIndexFieldType::String),
                ],
                self.enabled,
            ),
        ]
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            index(
                "native-value-code-copy",
                TitoIndexValue::RecordCopy,
                vec![
                    ("scope", TitoIndexFieldType::String),
                    ("code", TitoIndexFieldType::String),
                ],
                true,
            ),
            index(
                "native-value-code-reference",
                TitoIndexValue::RecordReference,
                vec![
                    ("scope", TitoIndexFieldType::String),
                    ("code", TitoIndexFieldType::String),
                ],
                true,
            ),
        ]
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
        enabled: true,
        body: format!("body {rank}"),
        created_at: 1,
        updated_at: 1,
    }
}

fn primary(record: &IndexValueRecord) -> String {
    format!("table:native-index-values:{}", record.id)
}

fn by_code(record: &IndexValueRecord, index: &str) -> TitoFindOneByIndexPayload {
    TitoFindOneByIndexPayload {
        index: index.into(),
        values: vec![record.scope.clone(), record.code.clone()],
    }
}

async fn assert_record_reads(
    model: &TitoModel<TiKVBackend, IndexValueRecord>,
    record: &IndexValueRecord,
) {
    for index in ["native-value-rank-copy", "native-value-rank-reference"] {
        assert_eq!(
            model
                .find_one_by_index(
                    TitoFindOneByIndexPayload {
                        index: index.into(),
                        values: vec![record.scope.clone(), record.rank.to_string()],
                    },
                    None,
                )
                .await
                .unwrap(),
            *record
        );
    }
    for index in ["native-value-code-copy", "native-value-code-reference"] {
        assert_eq!(
            model
                .find_one_by_unique_index(by_code(record, index), None)
                .await
                .unwrap(),
            *record
        );
    }
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
async fn native_index_value_modes_store_selected_bytes_and_preserve_pages() {
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
            assert_eq!(keys.len(), 8);
            let stored_primary = tx.get(primary(record)).await.unwrap().unwrap();
            for (key, expected) in keys {
                let bytes = tx.get(&key).await.unwrap().unwrap();
                assert_eq!(bytes, expected);
                if key.contains("-copy:") {
                    assert_eq!(bytes, stored_primary);
                } else if key.starts_with("unique-index:") {
                    assert_eq!(bytes, record.id.as_bytes());
                } else {
                    assert!(bytes.is_empty());
                }
            }
        }
        tx.rollback().await.unwrap();
        for index in ["native-value-rank-copy", "native-value-rank-reference"] {
            let mut query = model.query_by_index(index);
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
        }
        for index in ["native-value-tag-copy", "native-value-tag-reference"] {
            let mut query = model.query_by_index(index);
            query.value(&scope).value("common").limit(Some(10));
            let page = query.execute(None).await.unwrap();
            assert_eq!(page.items.len(), records.len());
            assert!(page.cursor.is_none());
            for record in &records {
                assert!(page.items.contains(record));
            }
        }
        for index in ["native-value-code-copy", "native-value-code-reference"] {
            let tx = engine.begin_transaction().await.unwrap();
            let start = format!("unique-index:native-index-values:{index}:scope:{scope}:");
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
        }
        for record in &records {
            assert_record_reads(&model, record).await;
            assert!(remove(&engine, &model, &record.id).await.unwrap());
        }
    })
    .await
    .expect("Native mixed index pages must finish within their owned run");
}

#[tokio::test]
async fn native_copied_index_reads_avoid_primaries_and_id_queries_skip_values() {
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
            .find(|(key, _)| key.starts_with("index:native-value-rank-copy:"))
            .unwrap();
        let tx = engine.begin_transaction().await.unwrap();
        let primary_bytes = tx.get(primary(&record)).await.unwrap().unwrap();
        tx.delete(primary(&record)).await.unwrap();
        tx.commit().await.unwrap();

        let mut copies = model.query_by_index("native-value-rank-copy");
        copies.value(&scope);
        assert_eq!(
            copies.execute(None).await.unwrap().items,
            vec![record.clone()]
        );
        assert_eq!(
            model
                .find_one_by_unique_index(by_code(&record, "native-value-code-copy"), None)
                .await
                .unwrap(),
            record
        );
        let mut references = model.query_by_index("native-value-rank-reference");
        references.value(&scope);
        assert!(matches!(
            references.execute(None).await,
            Err(TitoError::IndexError(_))
        ));
        assert!(matches!(
            model
                .find_one_by_unique_index(by_code(&record, "native-value-code-reference"), None)
                .await,
            Err(TitoError::IndexError(_))
        ));

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
async fn native_index_value_modes_refresh_copies_on_every_write_and_remove_old_entries() {
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
        let changed = IndexValueRecord {
            rank: 9,
            code: "changed".into(),
            tags: vec!["changed-tag".into()],
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
                .find_one_by_unique_index(by_code(&changed, "native-value-code-copy"), Some(&tx))
                .await
                .unwrap(),
            changed
        );
        tx.rollback().await.unwrap();
        assert_record_reads(&model, &updated).await;
        save(&engine, &model, changed.clone()).await.unwrap();
        assert_record_reads(&model, &changed).await;
        let after = model
            .get_index_keys(
                primary(&changed),
                &changed,
                &serde_json::to_value(&changed).unwrap(),
            )
            .unwrap();
        assert_eq!(after.len(), 4);
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
async fn native_index_value_modes_preserve_unique_claims_and_batch_swaps() {
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
            model.get(&duplicate.id).execute(None).await,
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
            model.get(&losing.id).execute(None).await,
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
async fn native_copied_unique_hits_guard_the_owner_while_lists_remain_unguarded() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let (engine, model, scope) = fixture().await;
        let first = record(&scope, 1);
        save(&engine, &model, first.clone()).await.unwrap();
        let guarded = engine.begin_transaction().await.unwrap();
        assert_eq!(
            model
                .find_one_by_unique_index(by_code(&first, "native-value-code-copy"), Some(&guarded))
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
        let mut query = model.query_by_index("native-value-rank-copy");
        query.value(&scope);
        assert_eq!(
            query.execute(Some(&list)).await.unwrap().items,
            vec![next.clone()]
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
                .find_one_by_unique_index(
                    by_code(&current, "native-value-code-copy"),
                    Some(&read_only)
                )
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
async fn native_index_value_restore_rebuilds_both_modes_and_preserves_versions() {
    tokio::time::timeout(Duration::from_secs(120), async {
        let (engine, model, scope) = fixture().await;
        let record = record(&scope, 1);
        save(&engine, &model, record.clone()).await.unwrap();
        let version = model.get_versioned(&record.id, None).await.unwrap().version;
        let keys = model
            .get_index_keys(
                primary(&record),
                &record,
                &serde_json::to_value(&record).unwrap(),
            )
            .unwrap();
        let tx = engine.begin_transaction().await.unwrap();
        let manifest_key = format!("reverse-index:{}", primary(&record));
        let manifest = tx.get(&manifest_key).await.unwrap().unwrap();
        for (key, _) in &keys {
            tx.delete(key).await.unwrap();
        }
        tx.commit().await.unwrap();
        for _ in 0..2 {
            let tx = engine.begin_transaction().await.unwrap();
            assert_eq!(
                model
                    .rebuild_indexes_for_restore(&record.id, &tx)
                    .await
                    .unwrap(),
                record
            );
            tx.commit().await.unwrap();
            assert_eq!(
                model.get_versioned(&record.id, None).await.unwrap().version,
                version
            );
            assert_record_reads(&model, &record).await;
            let tx = engine.begin_transaction().await.unwrap();
            assert_eq!(tx.get(&manifest_key).await.unwrap().unwrap(), manifest);
            for (key, bytes) in &keys {
                assert_eq!(tx.get(key).await.unwrap().unwrap(), *bytes);
            }
            tx.rollback().await.unwrap();
        }
        for (key, bytes) in keys
            .iter()
            .filter(|(key, _)| key.starts_with("unique-index:"))
        {
            let foreign = IndexValueRecord {
                id: format!("{scope}:foreign"),
                ..record.clone()
            };
            let bad = if key.contains("-copy:") {
                serde_json::to_vec(&foreign).unwrap()
            } else {
                foreign.id.as_bytes().to_vec()
            };
            let tx = engine.begin_transaction().await.unwrap();
            tx.put(key, bad.clone()).await.unwrap();
            tx.commit().await.unwrap();
            let tx = engine.begin_transaction().await.unwrap();
            assert!(matches!(
                model.rebuild_indexes_for_restore(&record.id, &tx).await,
                Err(TitoError::UniqueViolation { .. })
            ));
            assert!(matches!(
                model.remove(&record.id, &tx).await,
                Err(TitoError::IndexError(_))
            ));
            tx.rollback().await.unwrap();
            let tx = engine.begin_transaction().await.unwrap();
            assert_eq!(tx.get(key).await.unwrap().unwrap(), bad);
            assert_eq!(tx.get(&manifest_key).await.unwrap().unwrap(), manifest);
            tx.put(key, bytes).await.unwrap();
            tx.commit().await.unwrap();
        }
        assert_record_reads(&model, &record).await;
        assert!(remove(&engine, &model, &record.id).await.unwrap());
    })
    .await
    .expect("Native index value restore must finish within its owned run");
}
