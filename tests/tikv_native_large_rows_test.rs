mod native;

use native::{create, native_endpoint, native_engine, read, remove};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tito::types::TitoTransaction;
use tito::{
    encode_index_string, Guard, Queue, QueueConfig, QueueEvent, TitoEngine, TitoError, TitoIndex,
    TitoIndexField, TitoIndexValue, TitoModelTrait, MAX_QUEUE_EVENT_BYTES, MAX_SAVE_BYTES,
};
use uuid::Uuid;

const RECORDS: usize = 40;
const EVENTS: usize = 16;
const ROW_BYTES: usize = 1024 * 1024;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeLargeRecord {
    id: String,
    scope: String,
    body: String,
}

impl TitoModelTrait for NativeLargeRecord {
    fn indexes(&self) -> Vec<TitoIndex> {
        vec![TitoIndex {
            name: "native-large-rows-by-scope",
            fields: vec![TitoIndexField::Text(self.scope.clone())],
            value: TitoIndexValue::Full,
            unique: false,
        }]
    }

    fn table() -> String {
        "native-large-rows-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeIndexedRecord {
    id: String,
    scope: String,
    kind: String,
    code: String,
    tags: Vec<String>,
    body: String,
}

impl TitoModelTrait for NativeIndexedRecord {
    fn indexes(&self) -> Vec<TitoIndex> {
        let mut indexes = vec![
            TitoIndex {
                name: "native-large-rows-indexed-by-scope",
                fields: vec![TitoIndexField::Text(self.scope.clone())],
                value: TitoIndexValue::Full,
                unique: false,
            },
            TitoIndex {
                name: "native-large-rows-indexed-by-scope-kind",
                fields: vec![
                    TitoIndexField::Text(self.scope.clone()),
                    TitoIndexField::Text(self.kind.clone()),
                ],
                value: TitoIndexValue::Full,
                unique: false,
            },
            TitoIndex {
                name: "native-large-rows-indexed-code",
                fields: vec![
                    TitoIndexField::Text(self.scope.clone()),
                    TitoIndexField::Text(self.code.clone()),
                ],
                value: TitoIndexValue::Full,
                unique: true,
            },
            TitoIndex {
                name: "native-large-rows-indexed-summary",
                fields: vec![TitoIndexField::Text(self.scope.clone())],
                value: TitoIndexValue::Part(&["id", "kind", "tags"]),
                unique: false,
            },
        ];
        for tag in &self.tags {
            indexes.push(TitoIndex {
                name: "native-large-rows-indexed-by-tag",
                fields: vec![TitoIndexField::Text(tag.clone())],
                value: TitoIndexValue::Full,
                unique: false,
            });
            indexes.push(TitoIndex {
                name: "native-large-rows-indexed-holder",
                fields: vec![
                    TitoIndexField::Text(self.scope.clone()),
                    TitoIndexField::Text(tag.clone()),
                ],
                value: TitoIndexValue::KeysOnly,
                unique: false,
            });
        }
        indexes
    }

    fn table() -> String {
        "native-large-rows-indexed-records".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
struct NativeIndexedSummary {
    id: String,
    kind: String,
    tags: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeBoundRecord {
    id: String,
    scope: String,
    tags: Vec<String>,
    body: String,
}

impl TitoModelTrait for NativeBoundRecord {
    fn indexes(&self) -> Vec<TitoIndex> {
        self.tags
            .iter()
            .map(|tag| TitoIndex {
                name: "native-large-rows-bound-by-tag",
                fields: vec![
                    TitoIndexField::Text(self.scope.clone()),
                    TitoIndexField::Text(tag.clone()),
                ],
                value: TitoIndexValue::Full,
                unique: false,
            })
            .collect()
    }

    fn table() -> String {
        "native-large-rows-bound-records".into()
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
        let model = engine.clone().model::<NativeLargeRecord>();
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let records = (0..RECORDS)
            .map(|index| NativeLargeRecord {
                id: format!("{scope}-{index:02}"),
                scope: scope.clone(),
                body: body(index, ROW_BYTES),
            })
            .collect::<Vec<_>>();
        for record in &records {
            create(&engine, &model, record).await.unwrap();
        }

        let mut scanned = Vec::new();
        let mut cursor = None;
        loop {
            let page = engine
                .transaction(|tx| {
                    let model = model.clone();
                    let cursor = cursor.clone();
                    async move { model.scan_records(RECORDS as u32, cursor, &tx).await }
                })
                .await
                .unwrap();
            scanned.extend(
                page.items
                    .into_iter()
                    .filter(|record: &NativeLargeRecord| record.scope == scope),
            );
            cursor = page.cursor;
            if cursor.is_none() {
                break;
            }
        }
        assert_eq!(scanned, records);

        let mut by_scope = model.query_by_index("native-large-rows-by-scope");
        by_scope
            .value(TitoIndexField::Text(scope.clone()))
            .limit(Some(RECORDS as u32));
        let indexed = by_scope.execute(None).await.unwrap();
        assert_eq!(indexed.items, records);
        assert!(indexed.cursor.is_none());
        by_scope.descending();
        let reversed = by_scope.execute(None).await.unwrap();
        assert_eq!(
            reversed.items,
            records.iter().rev().cloned().collect::<Vec<_>>()
        );
        assert!(reversed.cursor.is_none());

        let note = model.read_many(
            records.iter().rev().map(|record| record.id.clone()).collect(),
            Guard::None,
        );
        let fetched = engine
            .transaction(|tx| async move { tx.batch_get(note).await })
            .await
            .unwrap();
        assert_eq!(fetched, records.iter().rev().cloned().collect::<Vec<_>>());

        for record in &records {
            remove(&engine, &model, &record.id).await.unwrap();
        }
        let mut left = model.query_by_index("native-large-rows-by-scope");
        left.value(TitoIndexField::Text(scope.clone()))
            .limit(Some(1));
        assert!(left.execute_ids(None).await.unwrap().items.is_empty());
        println!(
            "native Tito large rows: records={RECORDS}, record_body_bytes={ROW_BYTES}, reads=scan_records+index+index_descending+read_many"
        );
    })
    .await
    .expect("The native large-row reads must finish within their owned run");
}

#[tokio::test]
async fn native_index_entries_of_a_large_record_hold_full_copies_keys_only_and_parts() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(120), async move {
        let engine = native_engine(endpoint).await;
        let model = engine.clone().model::<NativeIndexedRecord>();
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let record = NativeIndexedRecord {
            id: format!("{scope}:record"),
            scope: scope.clone(),
            kind: "large".into(),
            code: "only".into(),
            tags: (0..3)
                .map(|position| format!("{scope}-tag-{position}"))
                .collect(),
            body: body(7, ROW_BYTES),
        };
        create(&engine, &model, &record).await.unwrap();

        let row = model.key(&record.id);
        let id = encode_index_string(&record.id);
        let scope_part = encode_index_string(&scope);
        let mut full = vec![
            format!("index:native-large-rows-indexed-by-scope:{scope_part}:{id}:"),
            format!("index:native-large-rows-indexed-by-scope-kind:{scope_part}:large:{id}:"),
            format!("index:native-large-rows-indexed-code:{scope_part}:only:"),
        ];
        let mut keys_only = Vec::new();
        for tag in &record.tags {
            let tag = encode_index_string(tag);
            full.push(format!("index:native-large-rows-indexed-by-tag:{tag}:{id}:"));
            keys_only.push(format!(
                "index:native-large-rows-indexed-holder:{scope_part}:{tag}:{id}:"
            ));
        }
        let part = format!("index:native-large-rows-indexed-summary:{scope_part}:{id}:");

        let tx = engine.begin_transaction().await.unwrap();
        let stored = tx
            .get(&row)
            .await
            .unwrap()
            .expect("The record row is stored");
        assert_eq!(
            serde_json::from_slice::<NativeIndexedRecord>(&stored).unwrap(),
            record
        );
        for key in &full {
            assert_eq!(tx.get(key).await.unwrap(), Some(stored.clone()), "{key}");
            assert_eq!(
                tx.scan(
                    key.clone().into_bytes()..tito::key_after(key.clone()).into_bytes(),
                    2
                )
                .await
                .unwrap(),
                vec![(key.clone().into_bytes(), stored.clone())],
                "{key}"
            );
        }
        for key in &keys_only {
            assert_eq!(tx.get(key).await.unwrap(), Some(Vec::new()), "{key}");
        }
        let stored_part = tx
            .get(&part)
            .await
            .unwrap()
            .expect("The part entry is stored");
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&stored_part).unwrap(),
            serde_json::json!([
                record.id,
                {
                    "id": record.id,
                    "kind": record.kind,
                    "tags": record.tags,
                }
            ])
        );
        tx.rollback().await.unwrap();

        let mut by_scope = model.query_by_index("native-large-rows-indexed-by-scope");
        by_scope
            .value(TitoIndexField::Text(scope.clone()))
            .limit(Some(10));
        let page = by_scope.execute(None).await.unwrap();
        assert_eq!(page.items, vec![record.clone()]);
        assert!(page.cursor.is_none());
        let mut by_tag = model.query_by_index("native-large-rows-indexed-by-tag");
        by_tag.value(TitoIndexField::Text(record.tags[1].clone()));
        assert_eq!(
            by_tag.execute(None).await.unwrap().items,
            vec![record.clone()]
        );
        assert_eq!(
            by_tag.execute_ids(None).await.unwrap().items,
            vec![record.id.clone()]
        );
        let mut by_kind = model.query_by_index("native-large-rows-indexed-by-scope-kind");
        by_kind
            .value(TitoIndexField::Text(scope.clone()))
            .descending();
        assert_eq!(
            by_kind.execute(None).await.unwrap().items,
            vec![record.clone()]
        );
        let note = model.read_unique(
            "native-large-rows-indexed-code",
            vec![
                TitoIndexField::Text(scope.clone()),
                TitoIndexField::Text("only".into()),
            ],
            Guard::None,
        );
        assert_eq!(
            engine
                .transaction(|tx| async move { tx.batch_get(note).await })
                .await
                .unwrap(),
            record
        );
        let mut held = model.query_by_index("native-large-rows-indexed-holder");
        held.value(TitoIndexField::Text(scope.clone()))
            .value(TitoIndexField::Text(record.tags[2].clone()));
        assert_eq!(
            held.execute_ids(None).await.unwrap().items,
            vec![record.id.clone()]
        );
        let mut summaries = model.query_by_index("native-large-rows-indexed-summary");
        summaries.value(TitoIndexField::Text(scope.clone()));
        assert_eq!(
            summaries
                .execute_part::<NativeIndexedSummary>(None)
                .await
                .unwrap()
                .items,
            vec![NativeIndexedSummary {
                id: record.id.clone(),
                kind: record.kind.clone(),
                tags: record.tags.clone(),
            }]
        );
        assert_eq!(
            summaries.execute_ids(None).await.unwrap().items,
            vec![record.id.clone()]
        );

        remove(&engine, &model, &record.id).await.unwrap();
        let tx = engine.begin_transaction().await.unwrap();
        assert_eq!(tx.get(&row).await.unwrap(), None);
        for key in full.iter().chain(&keys_only).chain([&part]) {
            assert_eq!(tx.get(key).await.unwrap(), None, "{key}");
        }
        tx.rollback().await.unwrap();
        println!(
            "native Tito index entries: record_body_bytes={ROW_BYTES}, full_copies={}, keys_only_entries={}, part_entries=1, save_bytes_about={}",
            full.len(),
            keys_only.len(),
            (full.len() + 1) * stored.len()
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
        let model = engine.clone().model::<NativeBoundRecord>();
        let scope = format!("{run_id}-{}", Uuid::new_v4());
        let copies = MAX_SAVE_BYTES / ROW_BYTES - 2;
        let record = NativeBoundRecord {
            id: format!("{scope}:under"),
            scope: scope.clone(),
            tags: (0..copies).map(|tag| format!("tag-{tag:03}")).collect(),
            body: body(3, ROW_BYTES),
        };
        create(&engine, &model, &record).await.unwrap();
        assert_eq!(
            read(&engine, &model, &record.id).await.unwrap(),
            Some(record.clone())
        );
        let mut last = model.query_by_index("native-large-rows-bound-by-tag");
        last.value(TitoIndexField::Text(scope.clone()))
            .value(TitoIndexField::Text(format!("tag-{:03}", copies - 1)));
        assert_eq!(last.execute(None).await.unwrap().items, vec![record.clone()]);

        let over = NativeBoundRecord {
            id: format!("{scope}:over"),
            tags: (0..copies + 2).map(|tag| format!("tag-{tag:03}")).collect(),
            ..record.clone()
        };
        let refused = TitoError::InvalidInput(format!(
            "A save exceeds the {MAX_SAVE_BYTES}-byte limit"
        ));
        assert_eq!(create(&engine, &model, &over).await, Err(refused.clone()));
        let tx = engine.begin_transaction().await.unwrap();
        assert_eq!(model.create(&over, &tx).await, Err(refused));
        tx.commit()
            .await
            .expect("A refused create leaves nothing in its transaction to commit");
        assert_eq!(read(&engine, &model, &over.id).await.unwrap(), None);
        let tx = engine.begin_transaction().await.unwrap();
        let over_id = encode_index_string(&over.id);
        let scope_part = encode_index_string(&scope);
        for tag in [0, copies + 1] {
            assert_eq!(
                tx.get(format!(
                    "index:native-large-rows-bound-by-tag:{scope_part}:tag-{tag:03}:{over_id}:"
                ))
                .await
                .unwrap(),
                None
            );
        }
        tx.rollback().await.unwrap();

        remove(&engine, &model, &record.id).await.unwrap();
        let mut any = model.query_by_index("native-large-rows-bound-by-tag");
        any.value(TitoIndexField::Text(scope.clone()));
        assert!(any.execute_ids(None).await.unwrap().items.is_empty());
        println!(
            "native Tito save bound: bound={MAX_SAVE_BYTES}, committed_copies={copies}, refused_copies={}, row_bytes_about={ROW_BYTES}",
            copies + 2
        );
    })
    .await
    .expect("The native save bound case must finish within its owned run");
}

#[tokio::test]
async fn native_queue_pulls_and_acks_sixteen_one_mib_events() {
    let (endpoint, _run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(180), async move {
        let engine = native_engine(endpoint).await;
        let queue = Queue::new(
            engine.clone(),
            QueueConfig::new(1)
                .with_name(format!("native-large-rows-queue-{}", Uuid::new_v4().simple()))
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
            .scan_by_status::<NativeLargePayload>(None, EVENTS as u32)
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
        assert!(queue
            .scan_by_status::<NativeLargePayload>(None, 1)
            .await
            .unwrap()
            .events
            .is_empty());
        let prefix = format!("{}:", queue.config.name());
        let tx = engine.begin_transaction().await.unwrap();
        let left = tx
            .scan_keys(
                prefix.clone().into_bytes()..tito::prefix_end(prefix.clone()).into_bytes(),
                10,
            )
            .await
            .unwrap();
        tx.rollback().await.unwrap();
        assert!(left.is_empty(), "An acked event leaves no row behind");

        queue.clear().await.unwrap();
        println!(
            "native Tito large queue rows: events={EVENTS}, payload_bytes={payload_bytes}, pulled={EVENTS}, acked={EVENTS}"
        );
    })
    .await
    .expect("The native large queue rows must finish within their owned run");
}
