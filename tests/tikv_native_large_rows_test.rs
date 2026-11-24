mod native;

use native::{native_endpoint, native_engine, remove, save};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tito::types::{
    TitoFindByIndexPayload, TitoFindOneByIndexPayload, TitoIndexConfig, TitoIndexField,
    TitoIndexFieldType, TitoScanPayload, TitoTransaction,
};
use tito::{
    Queue, QueueConfig, QueueEvent, QueueEventStatus, TitoEngine, TitoModelOptions, TitoModelTrait,
    MAX_QUEUE_EVENT_BYTES,
};
use uuid::Uuid;

const RECORDS: usize = 40;
const EVENTS: usize = 16;
const ROW_BYTES: usize = 1024 * 1024;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeLargeRecord {
    id: String,
    scope: String,
    body: String,
}

impl TitoModelTrait for NativeLargeRecord {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "native-large-by-scope".into(),
            fields: vec![TitoIndexField {
                name: "scope".into(),
                r#type: TitoIndexFieldType::String,
            }],
        }]
    }

    fn table() -> String {
        "native-large-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeIndexedRecord {
    id: String,
    scope: String,
    kind: String,
    code: String,
    tags: Vec<String>,
    body: String,
}

fn native_index(name: &str, fields: &[&str]) -> TitoIndexConfig {
    TitoIndexConfig {
        condition: true,
        name: name.into(),
        fields: fields
            .iter()
            .map(|field| TitoIndexField {
                name: field.to_string(),
                r#type: TitoIndexFieldType::String,
            })
            .collect(),
    }
}

impl TitoModelTrait for NativeIndexedRecord {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            native_index("native-indexed-by-scope", &["scope"]),
            native_index("native-indexed-by-scope-kind", &["scope", "kind"]),
            native_index("native-indexed-by-tag", &["tags"]),
        ]
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![native_index("native-indexed-code", &["scope", "code"])]
    }

    fn holder_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![native_index("native-indexed-holder", &["scope", "tags"])]
    }

    fn table() -> String {
        "native-indexed-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeBoundRecord {
    id: String,
    scope: String,
    tags: Vec<String>,
    body: String,
}

impl TitoModelTrait for NativeBoundRecord {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![native_index("native-bound-by-tag", &["scope", "tags"])]
    }

    fn table() -> String {
        "native-bound-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeLargePayload {
    body: String,
}

fn body(index: usize, bytes: usize) -> String {
    char::from(b'a' + (index % 26) as u8)
        .to_string()
        .repeat(bytes)
}

#[tokio::test]
async fn native_reads_forty_one_mib_records_through_byte_bounded_requests() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(180), async move {
        let engine = native_engine(endpoint).await;
        let model = engine
            .clone()
            .model::<NativeLargeRecord>(TitoModelOptions::default());
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let records = (0..RECORDS)
            .map(|index| NativeLargeRecord {
                id: format!("{scope}-{index:02}"),
                scope: scope.clone(),
                body: body(index, ROW_BYTES),
            })
            .collect::<Vec<_>>();
        for record in &records {
            save(&engine, &model, record.clone()).await.unwrap();
        }

        let (rows, more) = engine
            .transaction(|tx| {
                let model = model.clone();
                let start = format!("table:native-large-records:{scope}-");
                async move {
                    model
                        .scan(
                            TitoScanPayload {
                                start,
                                end: None,
                                limit: Some(RECORDS as u32),
                                cursor: None,
                            },
                            &tx,
                        )
                        .await
                }
            })
            .await
            .unwrap();
        assert!(!more);
        assert_eq!(rows.len(), RECORDS);
        for ((key, value), record) in rows.iter().zip(&records) {
            assert_eq!(key, &format!("table:native-large-records:{}", record.id));
            assert_eq!(value, &serde_json::to_value(record).unwrap());
        }

        let (reversed, more) = engine
            .transaction(|tx| {
                let model = model.clone();
                let start = format!("table:native-large-records:{scope}-");
                async move {
                    model
                        .scan_reverse(
                            TitoScanPayload {
                                start,
                                end: None,
                                limit: Some(RECORDS as u32),
                                cursor: None,
                            },
                            &tx,
                        )
                        .await
                }
            })
            .await
            .unwrap();
        assert!(!more);
        assert_eq!(
            reversed
                .iter()
                .map(|(key, _)| key.clone())
                .collect::<Vec<_>>(),
            records
                .iter()
                .rev()
                .map(|record| format!("table:native-large-records:{}", record.id))
                .collect::<Vec<_>>()
        );

        let indexed = model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "native-large-by-scope".into(),
                    values: vec![scope.clone()],
                    limit: Some(RECORDS as u32),
                    cursor: None,
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(indexed.items, records);
        assert!(indexed.cursor.is_none());

        let indexed_reverse = model
            .find_by_index_reverse(
                TitoFindByIndexPayload {
                    index: "native-large-by-scope".into(),
                    values: vec![scope.clone()],
                    limit: Some(RECORDS as u32),
                    cursor: None,
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(
            indexed_reverse.items,
            records.iter().rev().cloned().collect::<Vec<_>>()
        );

        let mut fetched = model
            .get_many_for_share(records.iter().map(|record| record.id.clone()).collect(), None)
            .await
            .unwrap();
        fetched.sort_by(|left, right| left.id.cmp(&right.id));
        assert_eq!(fetched, records);

        for record in &records {
            assert!(remove(&engine, &model, &record.id).await.unwrap());
        }
        let left = model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "native-large-by-scope".into(),
                    values: vec![scope.clone()],
                    limit: Some(1),
                    cursor: None,
                },
                None,
            )
            .await
            .unwrap();
        assert!(left.items.is_empty());
        println!(
            "native Tito large rows: records={RECORDS}, record_body_bytes={ROW_BYTES}, scans=forward+reverse+index+index_reverse, batch_get={RECORDS}"
        );
    })
    .await
    .expect("The native large-row reads must finish within their owned run");
}

#[tokio::test]
async fn native_index_entries_of_a_large_record_hold_its_copy_and_holder_entries_hold_nothing() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(120), async move {
        let engine = native_engine(endpoint).await;
        let model = engine
            .clone()
            .model::<NativeIndexedRecord>(TitoModelOptions::default());
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let record = NativeIndexedRecord {
            id: format!("{scope}:record"),
            scope: scope.clone(),
            kind: "large".into(),
            code: "only".into(),
            tags: (0..3).map(|position| format!("{scope}-tag-{position}")).collect(),
            body: body(7, ROW_BYTES),
        };
        save(&engine, &model, record.clone()).await.unwrap();
        let primary = format!("table:native-indexed-records:{}", record.id);

        let tx = engine.begin_transaction().await.unwrap();
        let stored_primary = tx.get(primary.clone()).await.unwrap().unwrap();
        let manifest: serde_json::Value = serde_json::from_slice(
            &tx.get(format!("reverse-index:{primary}"))
                .await
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        let keys = manifest["value"]
            .as_array()
            .unwrap()
            .iter()
            .map(|key| key.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        let ordinary = keys
            .iter()
            .filter(|key| key.starts_with("index:"))
            .cloned()
            .collect::<Vec<_>>();
        let holders = keys
            .iter()
            .filter(|key| key.starts_with("holder-index:"))
            .cloned()
            .collect::<Vec<_>>();
        assert_eq!(ordinary.len(), 5);
        assert_eq!(holders.len(), 3);
        assert_eq!(keys.len(), 9);
        for key in &keys {
            let stored = tx.get(key.clone()).await.unwrap().unwrap();
            if key.starts_with("unique-index:") {
                assert_eq!(
                    key,
                    &format!("unique-index:native-indexed-records:native-indexed-code:scope:{scope}:code:only")
                );
                assert_eq!(stored, stored_primary);
            } else if key.starts_with("holder-index:") {
                assert!(key.ends_with(&format!(":{primary}")));
                assert!(stored.is_empty());
            } else {
                assert!(key.ends_with(&format!(":{primary}")));
                assert_eq!(stored, stored_primary);
                assert_eq!(
                    tx.scan(
                        key.clone().into_bytes()..tito::key_after(key.clone()).into_bytes(),
                        2
                    )
                    .await
                    .unwrap(),
                    vec![(key.clone().into_bytes(), stored_primary.clone())]
                );
            }
        }
        let fetched = tx.batch_get(ordinary.clone()).await.unwrap();
        assert_eq!(fetched.len(), ordinary.len());
        assert!(fetched.iter().all(|(_, value)| *value == stored_primary));
        let fetched = tx.batch_get(holders.clone()).await.unwrap();
        assert_eq!(fetched.len(), holders.len());
        assert!(fetched.iter().all(|(_, value)| value.is_empty()));
        tx.rollback().await.unwrap();

        let by_scope = model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "native-indexed-by-scope".into(),
                    values: vec![scope.clone()],
                    limit: Some(10),
                    cursor: None,
                },
                None,
            )
            .await
            .unwrap();
        assert_eq!(by_scope.items, vec![record.clone()]);
        assert!(by_scope.cursor.is_none());
        let mut by_tag = model.query_by_index("native-indexed-by-tag");
        by_tag.value(record.tags[1].clone());
        assert_eq!(
            by_tag.execute(None).await.unwrap().items,
            vec![record.clone()]
        );
        assert_eq!(
            by_tag.execute_ids(None).await.unwrap().items,
            vec![record.id.clone()]
        );
        let mut by_kind = model.query_by_index("native-indexed-by-scope-kind");
        by_kind.value(scope.clone());
        assert_eq!(
            by_kind.execute_reverse(None).await.unwrap().items,
            vec![record.clone()]
        );
        assert_eq!(
            model
                .find_one_by_unique_index_for_share(
                    TitoFindOneByIndexPayload {
                        index: "native-indexed-code".into(),
                        values: vec![scope.clone(), "only".into()],
                    },
                    None,
                )
                .await
                .unwrap(),
            record
        );
        let mut held = model.query_by_index("native-indexed-holder");
        held.value(scope.clone()).value(record.tags[2].clone());
        assert_eq!(
            held.execute_ids(None).await.unwrap().items,
            vec![record.id.clone()]
        );

        assert!(remove(&engine, &model, &record.id).await.unwrap());
        let tx = engine.begin_transaction().await.unwrap();
        for key in &keys {
            assert!(tx.get(key.clone()).await.unwrap().is_none());
        }
        tx.rollback().await.unwrap();
        println!(
            "native Tito index entries: record_body_bytes={ROW_BYTES}, ordinary_copies=5, unique_copies=1, holder_entries=3, save_bytes_about={}",
            7 * stored_primary.len()
        );
    })
    .await
    .expect("The native index entries of a large record must finish within their owned run");
}

#[tokio::test]
async fn native_a_save_under_the_bound_commits_its_copies_and_a_larger_one_is_refused_before_writing(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(180), async move {
        let engine = native_engine(endpoint).await;
        let model = engine
            .clone()
            .model::<NativeBoundRecord>(TitoModelOptions::default());
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let copies = tito::MAX_SAVE_BYTES / ROW_BYTES - 2;
        let record = NativeBoundRecord {
            id: format!("{scope}:under"),
            scope: scope.clone(),
            tags: (0..copies).map(|tag| format!("tag-{tag:03}")).collect(),
            body: body(3, ROW_BYTES),
        };
        let saved = save(&engine, &model, record.clone()).await.unwrap();
        assert_eq!(saved, record);
        let mut last = model.query_by_index("native-bound-by-tag");
        last.value(scope.clone()).value(format!("tag-{:03}", copies - 1));
        assert_eq!(last.execute(None).await.unwrap().items, vec![record.clone()]);

        let over = NativeBoundRecord {
            id: format!("{scope}:over"),
            tags: (0..copies + 2).map(|tag| format!("tag-{tag:03}")).collect(),
            ..record.clone()
        };
        let refused = save(&engine, &model, over.clone()).await.unwrap_err();
        assert_eq!(
            refused,
            tito::TitoError::InvalidInput(format!(
                "A save exceeds the {}-byte limit",
                tito::MAX_SAVE_BYTES
            ))
        );
        let tx = engine.begin_transaction().await.unwrap();
        assert!(tx
            .get(format!("table:native-bound-records:{}", over.id))
            .await
            .unwrap()
            .is_none());
        assert!(tx
            .get(format!("reverse-index:table:native-bound-records:{}", over.id))
            .await
            .unwrap()
            .is_none());
        tx.rollback().await.unwrap();

        assert!(remove(&engine, &model, &record.id).await.unwrap());
        let mut any = model.query_by_index("native-bound-by-tag");
        any.value(scope.clone()).value("tag-000");
        assert!(any.execute_ids(None).await.unwrap().items.is_empty());
        println!(
            "native Tito save bound: bound={}, committed_copies={copies}, refused_copies={}, row_bytes_about={ROW_BYTES}",
            tito::MAX_SAVE_BYTES,
            copies + 2
        );
    })
    .await
    .expect("The native save bound case must finish within its owned run");
}

#[tokio::test]
async fn native_queue_pulls_and_completes_sixteen_one_mib_events() {
    let (endpoint, _run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(180), async move {
        let engine = native_engine(endpoint).await;
        let queue = Queue::new(
            engine.clone(),
            QueueConfig::new(1, Duration::from_secs(60 * 60))
                .with_name(format!("native-large-{}", Uuid::new_v4().simple()))
                .unwrap(),
        );
        let due = chrono::Utc::now().timestamp_millis() - 1_000;
        let payload_bytes = MAX_QUEUE_EVENT_BYTES - 1_024;
        for index in 0..EVENTS {
            queue
                .publish(QueueEvent::new(
                    format!("native-large:{index:02}"),
                    NativeLargePayload {
                        body: body(index, payload_bytes),
                    },
                    due,
                ))
                .await
                .unwrap();
        }

        let pending = queue
            .scan_by_status::<NativeLargePayload>(QueueEventStatus::Pending, None, EVENTS as u32)
            .await
            .unwrap();
        assert_eq!(pending.events.len(), EVENTS);

        let pulled = queue
            .pull::<NativeLargePayload>(0, None, EVENTS as u32)
            .await
            .unwrap();
        assert_eq!(pulled.events.len(), EVENTS);
        let mut keys = pulled
            .events
            .iter()
            .map(|(_, event)| event.key.clone())
            .collect::<Vec<_>>();
        keys.sort();
        assert_eq!(
            keys,
            (0..EVENTS)
                .map(|index| format!("native-large:{index:02}"))
                .collect::<Vec<_>>()
        );
        for (_, event) in &pulled.events {
            assert_eq!(event.payload.body.len(), payload_bytes);
        }

        for (storage_key, _) in &pulled.events {
            queue.ack(storage_key).await.unwrap();
        }
        let completed = queue
            .scan_by_status::<NativeLargePayload>(QueueEventStatus::Completed, None, EVENTS as u32)
            .await
            .unwrap();
        assert_eq!(completed.events.len(), EVENTS);
        assert!(queue
            .scan_by_status::<NativeLargePayload>(QueueEventStatus::Pending, None, 1)
            .await
            .unwrap()
            .events
            .is_empty());

        queue.clear().await.unwrap();
        assert!(queue
            .scan_by_status::<NativeLargePayload>(QueueEventStatus::Completed, None, 1)
            .await
            .unwrap()
            .events
            .is_empty());
        println!(
            "native Tito large queue rows: events={EVENTS}, payload_bytes={payload_bytes}, pulled={EVENTS}, completed={EVENTS}"
        );
    })
    .await
    .expect("The native large queue rows must finish within their owned run");
}
