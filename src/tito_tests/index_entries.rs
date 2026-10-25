use super::*;
use crate::encode_index_integer;
use crate::types::TitoIndexValue;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Entry {
    id: String,
    tenant: String,
    rank: i64,
    code: String,
    tags: Vec<String>,
    scores: HashMap<String, i64>,
}

fn entry_index(name: &str, fields: &[(&str, TitoIndexFieldType)]) -> TitoIndexConfig {
    TitoIndexConfig {
        value: TitoIndexValue::RecordReference,
        condition: true,
        name: name.to_string(),
        fields: fields
            .iter()
            .map(|(name, r#type)| TitoIndexField {
                name: name.to_string(),
                r#type: r#type.clone(),
            })
            .collect(),
    }
}

impl TitoModelTrait for Entry {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            entry_index(
                "entry-by-tenant-rank",
                &[
                    ("tenant", TitoIndexFieldType::String),
                    ("rank", TitoIndexFieldType::Number),
                ],
            ),
            entry_index(
                "entry-by-tenant-tag",
                &[
                    ("tenant", TitoIndexFieldType::String),
                    ("tags", TitoIndexFieldType::String),
                ],
            ),
            entry_index(
                "entry-by-tenant-score",
                &[
                    ("tenant", TitoIndexFieldType::String),
                    ("scores", TitoIndexFieldType::Number),
                ],
            ),
            entry_index(
                "entry-by-score-tenant",
                &[
                    ("scores", TitoIndexFieldType::Number),
                    ("tenant", TitoIndexFieldType::String),
                ],
            ),
        ]
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![entry_index(
            "entry-by-code",
            &[
                ("tenant", TitoIndexFieldType::String),
                ("code", TitoIndexFieldType::String),
            ],
        )]
    }

    fn table() -> String {
        "entries".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn entry(id: &str, tenant: &str, rank: i64) -> Entry {
    Entry {
        id: id.to_string(),
        tenant: tenant.to_string(),
        rank,
        code: format!("code-{id}"),
        tags: Vec::new(),
        scores: HashMap::new(),
    }
}

fn entries(engine: &MemoryEngine) -> crate::TitoModel<MemoryEngine, Entry> {
    engine.clone().model::<Entry>(TitoModelOptions::default())
}

async fn save_entry(engine: &MemoryEngine, value: Entry) -> Entry {
    let model = entries(engine);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let value = value.clone();
            async move { model.set(value).timestamps(false).execute(&tx).await }
        })
        .await
        .unwrap()
}

fn rank_key(tenant: &str, rank: i64, id: &str) -> String {
    format!(
        "index:entry-by-tenant-rank:tenant:{}:rank:{}:table:entries:{id}",
        encode_index_string(tenant),
        encode_index_integer(rank)
    )
}

fn by_rank(tenant: &str) -> TitoFindByIndexPayload {
    TitoFindByIndexPayload {
        index: "entry-by-tenant-rank".to_string(),
        values: vec![tenant.to_string()],
        limit: None,
        cursor: None,
    }
}

fn first_by_rank(tenant: &str) -> TitoFindOneByIndexPayload {
    TitoFindOneByIndexPayload {
        index: "entry-by-tenant-rank".to_string(),
        values: vec![tenant.to_string()],
    }
}

fn by_code(tenant: &str, code: &str) -> TitoFindOneByIndexPayload {
    TitoFindOneByIndexPayload {
        index: "entry-by-code".to_string(),
        values: vec![tenant.to_string(), code.to_string()],
    }
}

fn whole(start: &str) -> TitoScanPayload {
    TitoScanPayload {
        start: start.to_string(),
        end: None,
        limit: None,
        cursor: None,
    }
}

fn ids(items: &[Entry]) -> Vec<String> {
    items.iter().map(|item| item.id.clone()).collect()
}

fn assert_index_error_naming<V: std::fmt::Debug>(result: Result<V, TitoError>, key: &str) {
    match result {
        Err(TitoError::IndexError(message)) => assert!(message.contains(key), "{message}"),
        other => panic!("Expected an IndexError naming '{key}', got {other:?}"),
    }
}

#[tokio::test]
async fn a_list_reads_its_records_by_id_in_one_batch_and_keeps_index_order() {
    let engine = engine();
    let model = entries(&engine);
    let third = save_entry(&engine, entry("e3", "t", 2)).await;
    let first = save_entry(&engine, entry("e:1", "t", 1)).await;
    let second = save_entry(&engine, entry("e\\2", "t", 1)).await;
    save_entry(&engine, entry("other", "u", 1)).await;
    let expected = vec![first.clone(), second.clone(), third.clone()];
    let primaries = vec![
        "table:entries:e:1".to_string(),
        "table:entries:e\\2".to_string(),
        "table:entries:e3".to_string(),
    ];

    engine.start_recording_reads().await;
    engine.start_recording_batch_gets().await;
    let mut query = model.query_by_index("entry-by-tenant-rank");
    query.value("t");
    let page = query.execute(None).await.unwrap();
    assert_eq!(page.items, expected);
    assert!(page.cursor.is_none());
    let mut reads = vec![b"scan:index:entry-by-tenant-rank:tenant:t:".to_vec()];
    reads.extend(primaries.iter().map(|key| key.as_bytes().to_vec()));
    assert_eq!(engine.take_recorded_reads().await, reads);
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![primaries.clone()]
    );

    engine.start_recording_batch_gets().await;
    let reversed = query.execute_reverse(None).await.unwrap();
    assert_eq!(
        reversed.items,
        expected.iter().rev().cloned().collect::<Vec<_>>()
    );
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![primaries.iter().rev().cloned().collect::<Vec<_>>()]
    );

    let tx = engine.begin_transaction().await.unwrap();
    let rows = vec![
        (rank_key("t", 1, "e:1"), json!(first)),
        (rank_key("t", 1, "e\\2"), json!(second)),
        (rank_key("t", 2, "e3"), json!(third)),
    ];
    assert_eq!(
        model.find_by_index_raw(by_rank("t"), &tx).await.unwrap(),
        (rows.clone(), false)
    );
    assert_eq!(
        model
            .find_by_index_reverse_raw(by_rank("t"), &tx)
            .await
            .unwrap(),
        (rows.into_iter().rev().collect(), false)
    );
    assert_eq!(
        model
            .find_one_by_index(first_by_rank("t"), Some(&tx))
            .await
            .unwrap(),
        first
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn execute_ids_reads_index_keys_only_and_returns_ids_in_index_order() {
    let engine = engine();
    let model = entries(&engine);
    save_entry(
        &engine,
        Entry {
            tags: vec!["y".to_string(), "x".to_string()],
            ..entry("e3", "t", 2)
        },
    )
    .await;
    save_entry(
        &engine,
        Entry {
            scores: HashMap::from([
                ("a:b".to_string(), 5),
                ("12345678901234567890".to_string(), -1),
            ]),
            ..entry("e:1", "t", 1)
        },
    )
    .await;
    save_entry(
        &engine,
        Entry {
            tags: vec!["x".to_string()],
            scores: HashMap::from([("z".to_string(), 0)]),
            ..entry("e\\2", "t", 1)
        },
    )
    .await;
    save_entry(&engine, entry("other", "u", 1)).await;

    for (index, expected) in [
        ("entry-by-tenant-rank", vec!["e:1", "e\\2", "e3"]),
        ("entry-by-tenant-tag", vec!["e3", "e\\2", "e3"]),
        ("entry-by-tenant-score", vec!["e:1", "e:1", "e\\2"]),
    ] {
        engine.start_recording_reads().await;
        engine.start_recording_batch_gets().await;
        let mut query = model.query_by_index(index);
        query.value("t");
        let page = query.execute_ids(None).await.unwrap();
        assert_eq!(page.items, expected);
        assert!(page.cursor.is_none());
        assert_eq!(
            engine.take_recorded_reads().await,
            vec![format!("scan-keys:index:{index}:tenant:t:").into_bytes()]
        );
        assert!(engine.take_recorded_batch_gets().await.is_empty());
        assert_eq!(
            query
                .execute(None)
                .await
                .unwrap()
                .items
                .into_iter()
                .map(|item| item.id)
                .collect::<Vec<_>>(),
            expected
        );
    }
}

#[tokio::test]
async fn records_page_in_both_directions_and_ids_page_with_the_same_limits_and_cursors() {
    let engine = engine();
    let model = entries(&engine);
    let mut saved = Vec::new();
    for (id, rank) in [("p5", 5), ("p1", 1), ("p3", 3), ("p2", 2), ("p4", 4)] {
        saved.push(save_entry(&engine, entry(id, "t", rank)).await);
    }
    save_entry(&engine, entry("q1", "u", 1)).await;
    saved.sort_by_key(|item| item.rank);

    let mut records = Vec::new();
    let mut listed_ids = Vec::new();
    let mut record_cursor: Option<String> = None;
    let mut id_cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut query = model.query_by_index("entry-by-tenant-rank");
        query
            .value("t")
            .limit(Some(2))
            .cursor(record_cursor.clone());
        let page = query.execute(None).await.unwrap();
        let mut query = model.query_by_index("entry-by-tenant-rank");
        query.value("t").limit(Some(2)).cursor(id_cursor.clone());
        let id_page = query.execute_ids(None).await.unwrap();
        assert!(page.items.len() <= 2);
        assert_eq!(id_page.items, ids(&page.items));
        assert_eq!(id_page.cursor, page.cursor);
        pages += 1;
        records.extend(page.items);
        listed_ids.extend(id_page.items);
        record_cursor = page.cursor;
        id_cursor = id_page.cursor;
        if record_cursor.is_none() {
            break;
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(records, saved);
    assert_eq!(listed_ids, ids(&saved));

    let mut reversed = Vec::new();
    let mut cursor: Option<String> = None;
    let mut pages = 0;
    loop {
        let mut query = model.query_by_index("entry-by-tenant-rank");
        query.value("t").limit(Some(2)).cursor(cursor.clone());
        let page = query.execute_reverse(None).await.unwrap();
        assert!(page.items.len() <= 2);
        pages += 1;
        reversed.extend(page.items);
        cursor = page.cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(pages, 3);
    assert_eq!(reversed, saved.iter().rev().cloned().collect::<Vec<_>>());

    let mut exact = model.query_by_index("entry-by-tenant-rank");
    exact.value("t").limit(Some(5));
    assert!(exact.execute(None).await.unwrap().cursor.is_none());
    assert!(exact.execute_ids(None).await.unwrap().cursor.is_none());

    let mut first = model.query_by_index("entry-by-tenant-rank");
    first.value("t").limit(Some(1));
    let cursor = first.execute(None).await.unwrap().cursor;
    assert!(cursor.is_some());
    let mut rest = model.query_by_index("entry-by-tenant-rank");
    rest.value("t").cursor(cursor);
    assert_eq!(
        rest.execute_ids(None).await.unwrap().items,
        vec!["p2", "p3", "p4", "p5"]
    );

    let mut foreign = model.query_by_index("entry-by-tenant-rank");
    foreign
        .value("u")
        .cursor(Some(cursor_for_key(&rank_key("t", 1, "p1"))));
    assert_eq!(
        foreign.execute_ids(None).await.unwrap_err(),
        TitoError::InvalidInput("Cursor is outside the requested scan range".to_string())
    );
}

#[tokio::test]
async fn an_entry_whose_record_is_missing_is_an_index_error_naming_its_key() {
    let engine = engine();
    let model = entries(&engine);
    save_entry(&engine, entry("e1", "t", 1)).await;
    let ghost = rank_key("t", 0, "ghost");
    engine.put_raw(&ghost, Vec::new()).await;

    let mut query = model.query_by_index("entry-by-tenant-rank");
    query.value("t");
    assert_index_error_naming(query.execute(None).await, &ghost);
    assert_index_error_naming(query.execute_reverse(None).await, &ghost);
    assert_index_error_naming(model.find_by_index(by_rank("t"), None).await, &ghost);
    assert_index_error_naming(
        model.find_one_by_index(first_by_rank("t"), None).await,
        &ghost,
    );
    let tx = engine.begin_transaction().await.unwrap();
    assert_index_error_naming(model.find_by_index_raw(by_rank("t"), &tx).await, &ghost);
    assert_index_error_naming(
        model.scan(whole("index:entry-by-tenant-rank:"), &tx).await,
        &ghost,
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        query.execute_ids(None).await.unwrap().items,
        vec!["ghost", "e1"]
    );

    let foreign = format!(
        "index:entry-by-tenant-rank:tenant:f:rank:{}:table:others:x",
        encode_index_integer(1)
    );
    engine.put_raw(&foreign, Vec::new()).await;
    let mut query = model.query_by_index("entry-by-tenant-rank");
    query.value("f");
    assert_index_error_naming(query.execute(None).await, &foreign);
    assert_index_error_naming(query.execute_ids(None).await, &foreign);

    engine
        .put_raw(
            "unique-index:entries:entry-by-code:tenant:t:code:lost",
            b"ghost".to_vec(),
        )
        .await;
    engine
        .put_raw(
            "unique-index:entries:entry-by-code:tenant:t:code:stale",
            b"e1".to_vec(),
        )
        .await;
    for code in ["lost", "stale"] {
        assert!(matches!(
            model
                .find_one_by_unique_index(by_code("t", code), None)
                .await,
            Err(TitoError::IndexError(_))
        ));
    }
    assert_eq!(
        model
            .find_one_by_unique_index(by_code("t", "code-e1"), None)
            .await
            .unwrap(),
        entry("e1", "t", 1)
    );
}

#[tokio::test]
async fn an_update_of_indexed_fields_moves_their_entries_and_keeps_them_free_of_data() {
    let engine = engine();
    let model = entries(&engine);
    let before = save_entry(
        &engine,
        Entry {
            tags: vec!["x".to_string()],
            ..entry("e:1", "t", 1)
        },
    )
    .await;
    let after = save_entry(
        &engine,
        Entry {
            rank: 2,
            code: "moved".to_string(),
            tags: vec!["y".to_string()],
            ..before.clone()
        },
    )
    .await;

    for key in [
        rank_key("t", 1, "e:1"),
        "index:entry-by-tenant-tag:tenant:t:tags:x:table:entries:e:1".to_string(),
        "unique-index:entries:entry-by-code:tenant:t:code:code-e\\:1".to_string(),
    ] {
        assert!(!engine.contains_key(&key).await, "{key}");
    }
    for key in [
        rank_key("t", 2, "e:1"),
        "index:entry-by-tenant-tag:tenant:t:tags:y:table:entries:e:1".to_string(),
    ] {
        assert_eq!(engine.raw_bytes(&key).await.unwrap(), Vec::<u8>::new());
    }
    assert_eq!(
        engine
            .raw_bytes("unique-index:entries:entry-by-code:tenant:t:code:moved")
            .await
            .unwrap(),
        b"e:1".to_vec()
    );
    let manifest = engine
        .raw_json("reverse-index:table:entries:e:1")
        .await
        .unwrap()["value"]
        .as_array()
        .unwrap()
        .iter()
        .map(|key| key.as_str().unwrap().to_string())
        .collect::<Vec<_>>();
    assert_eq!(
        sorted(manifest),
        sorted(
            model
                .get_index_keys("table:entries:e:1".to_string(), &after, &json!(after))
                .unwrap()
                .into_iter()
                .map(|(key, _)| key)
                .collect()
        )
    );

    let mut old_tag = model.query_by_index("entry-by-tenant-tag");
    old_tag.value("t").value("x");
    assert!(old_tag.execute(None).await.unwrap().items.is_empty());
    assert!(old_tag.execute_ids(None).await.unwrap().items.is_empty());
    let mut new_tag = model.query_by_index("entry-by-tenant-tag");
    new_tag.value("t").value("y");
    assert_eq!(
        new_tag.execute(None).await.unwrap().items,
        vec![after.clone()]
    );
    assert_eq!(new_tag.execute_ids(None).await.unwrap().items, vec!["e:1"]);
    assert_eq!(
        model
            .find_one_by_unique_index(by_code("t", "moved"), None)
            .await
            .unwrap(),
        after
    );
    assert!(matches!(
        model
            .find_one_by_unique_index(by_code("t", "code-e:1"), None)
            .await,
        Err(TitoError::NotFound(_))
    ));
}

#[tokio::test]
async fn remove_and_remove_by_index_delete_every_entry_of_their_records() {
    let engine = engine();
    let model = entries(&engine);
    for (id, rank) in [("e:1", 1), ("e\\2", 1), ("e3", 2)] {
        save_entry(
            &engine,
            Entry {
                tags: vec!["x".to_string(), "y".to_string()],
                scores: HashMap::from([("s".to_string(), rank)]),
                ..entry(id, "t", rank)
            },
        )
        .await;
    }
    assert_eq!(engine.keys_with_prefix("index:").await.len(), 15);
    assert_eq!(engine.keys_with_prefix("unique-index:").await.len(), 3);

    engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove("e:1", &tx).await }
        })
        .await
        .unwrap();
    for key in engine.keys_with_prefix("index:").await {
        assert!(!key.ends_with(":table:entries:e:1"), "{key}");
    }
    for key in engine.keys_with_prefix("unique-index:").await {
        assert_ne!(engine.raw_bytes(&key).await.unwrap(), b"e:1".to_vec());
    }
    assert_eq!(engine.keys_with_prefix("index:").await.len(), 10);
    assert_eq!(engine.keys_with_prefix("unique-index:").await.len(), 2);

    let removed = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .remove_by_index("entry-by-tenant-rank", "t", 10, &tx)
                    .await
            }
        })
        .await
        .unwrap();
    assert_eq!(removed, vec!["e\\2", "e3"]);
    assert!(engine.keys_with_prefix("index:").await.is_empty());
    assert!(engine.keys_with_prefix("unique-index:").await.is_empty());
    assert!(engine.keys_with_prefix("table:entries:").await.is_empty());
    assert!(engine
        .keys_with_prefix("reverse-index:table:entries:")
        .await
        .is_empty());
}

#[tokio::test]
async fn index_lists_and_ids_add_no_guard_and_commit_beside_a_save_of_a_listed_record() {
    let engine = engine();
    let model = entries(&engine);
    for (id, rank) in [("e1", 1), ("e2", 2), ("e3", 3)] {
        save_entry(&engine, entry(id, "t", rank)).await;
    }

    let tx = engine.begin_transaction().await.unwrap();
    let mut query = model.query_by_index("entry-by-tenant-rank");
    query.value("t");
    assert_eq!(
        ids(&query.execute(Some(&tx)).await.unwrap().items),
        vec!["e1", "e2", "e3"]
    );
    assert_eq!(
        ids(&query.execute_reverse(Some(&tx)).await.unwrap().items),
        vec!["e3", "e2", "e1"]
    );
    assert_eq!(
        query.execute_ids(Some(&tx)).await.unwrap().items,
        vec!["e1", "e2", "e3"]
    );
    assert_eq!(
        model
            .find_one_by_index(first_by_rank("t"), Some(&tx))
            .await
            .unwrap()
            .id,
        "e1"
    );
    assert_eq!(
        model
            .find_by_index_raw(by_rank("t"), &tx)
            .await
            .unwrap()
            .0
            .len(),
        3
    );
    assert_eq!(
        model
            .scan(whole("index:entry-by-tenant-rank:"), &tx)
            .await
            .unwrap()
            .0
            .len(),
        3
    );
    write_scratch(&tx).await;
    save_entry(
        &engine,
        Entry {
            code: "renamed".to_string(),
            ..entry("e2", "t", 2)
        },
    )
    .await;
    assert!(commit_writes(&engine, tx).await.is_empty());
}

#[tokio::test]
async fn a_scan_over_an_index_range_returns_the_records_of_ordinary_and_unique_entries() {
    let engine = engine();
    let model = entries(&engine);
    let first = save_entry(
        &engine,
        Entry {
            scores: HashMap::from([
                ("12345678901234567890".to_string(), -1),
                ("a:b".to_string(), 5),
            ]),
            ..entry("e:1", "t", 1)
        },
    )
    .await;
    let second = save_entry(
        &engine,
        Entry {
            scores: HashMap::from([("z".to_string(), 0)]),
            ..entry("e\\2", "u", 1)
        },
    )
    .await;

    let tx = engine.begin_transaction().await.unwrap();
    let rows = vec![
        (
            format!(
                "index:entry-by-score-tenant:scores:12345678901234567890:{}:tenant:t:table:entries:e:1",
                encode_index_integer(-1)
            ),
            json!(first),
        ),
        (
            format!(
                "index:entry-by-score-tenant:scores:a\\:b:{}:tenant:t:table:entries:e:1",
                encode_index_integer(5)
            ),
            json!(first),
        ),
        (
            format!(
                "index:entry-by-score-tenant:scores:z:{}:tenant:u:table:entries:e\\2",
                encode_index_integer(0)
            ),
            json!(second),
        ),
    ];
    assert_eq!(
        model
            .scan(whole("index:entry-by-score-tenant:"), &tx)
            .await
            .unwrap(),
        (rows.clone(), false)
    );
    assert_eq!(
        model
            .scan_reverse(whole("index:entry-by-score-tenant:"), &tx)
            .await
            .unwrap(),
        (rows.into_iter().rev().collect(), false)
    );
    assert_eq!(
        model
            .scan(whole("unique-index:entries:entry-by-code:"), &tx)
            .await
            .unwrap(),
        (
            vec![
                (
                    "unique-index:entries:entry-by-code:tenant:t:code:code-e\\:1".to_string(),
                    json!(first),
                ),
                (
                    "unique-index:entries:entry-by-code:tenant:u:code:code-e\\\\2".to_string(),
                    json!(second),
                ),
            ],
            false
        )
    );
    assert_eq!(
        model.scan(whole("table:entries:"), &tx).await.unwrap(),
        (
            vec![
                ("table:entries:e:1".to_string(), json!(first)),
                ("table:entries:e\\2".to_string(), json!(second)),
            ],
            false
        )
    );
    tx.rollback().await.unwrap();

    save_unique_account(
        &engine,
        unique_account("a1", "store-1", "ada@example.com", true),
    )
    .await;
    let tx = engine.begin_transaction().await.unwrap();
    assert_index_error_naming(
        model.scan(whole("unique-index:"), &tx).await,
        "unique-index:unique-accounts:verified-account-by-tenant-email:tenant_id:store-1:email:ada@example.com",
    );
    tx.rollback().await.unwrap();
}
