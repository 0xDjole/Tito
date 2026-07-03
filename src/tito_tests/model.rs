use super::*;

#[tokio::test]
async fn model_set_get_and_get_many_round_trip() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    save_author(&engine, author("a1", "Ada@Example.com", 36, "org-a")).await;
    save_author(&engine, author("a2", "grace@example.com", 42, "org-a")).await;

    assert_eq!(
        model.get("a1").execute(None).await.unwrap().email,
        "Ada@Example.com"
    );
    let many = model
        .get_many(vec!["a2".to_string(), "a1".to_string()])
        .execute(None)
        .await
        .unwrap();
    assert_eq!(
        many.into_iter().map(|item| item.id).collect::<Vec<_>>(),
        vec!["a2", "a1"]
    );
}

#[tokio::test]
async fn model_set_adds_timestamps_by_default() {
    let engine = engine();

    let saved = save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    assert!(saved.created_at > 0);
    assert!(saved.updated_at >= saved.created_at);
}

#[tokio::test]
async fn model_set_can_skip_timestamps() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    let saved = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "ada@example.com", 36, "org-a"))
                    .timestamps(false)
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap();

    assert_eq!(saved.created_at, 0);
    assert_eq!(saved.updated_at, 0);
}

#[tokio::test]
async fn transaction_commits_successful_writes_and_rolls_back_errors() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("committed", "ok@example.com", 1, "org"))
                    .execute(&tx)
                    .await?;
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();

    let result = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("rolled-back", "no@example.com", 1, "org"))
                    .execute(&tx)
                    .await?;
                Err::<(), TitoError>(TitoError::Internal("boom".to_string()))
            }
        })
        .await;

    assert!(result.is_err());
    assert!(model.get("committed").execute(None).await.is_ok());
    assert!(model.get("rolled-back").execute(None).await.is_err());
}

#[tokio::test]
async fn transaction_reads_own_writes_before_commit() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    let found = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "ada@example.com", 1, "org"))
                    .execute(&tx)
                    .await?;
                model.get("a1").execute(Some(&tx)).await
            }
        })
        .await
        .unwrap();

    assert_eq!(found.id, "a1");
}

#[tokio::test]
async fn get_missing_record_returns_not_found() {
    let model = engine().model::<Author>(TitoModelOptions::default());

    assert!(matches!(
        model.get("missing").execute(None).await.unwrap_err(),
        TitoError::NotFound(_)
    ));
}

#[tokio::test]
async fn find_paginates_with_cursor() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    for id in ["a1", "a2", "a3"] {
        save_author(&engine, author(id, &format!("{id}@example.com"), 1, "org")).await;
    }

    let first = model
        .find(TitoFindPayload {
            start: "".to_string(),
            end: None,
            limit: Some(2),
            cursor: None,
            rels: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(first.items.len(), 2);
    assert!(first.cursor.is_some());

    let second = model
        .find(TitoFindPayload {
            start: "".to_string(),
            end: None,
            limit: Some(2),
            cursor: first.cursor,
            rels: Vec::new(),
        })
        .await
        .unwrap();
    assert_eq!(second.items.len(), 1);
    assert_eq!(second.items[0].id, "a3");
}

#[tokio::test]
async fn scan_reverse_returns_items_in_reverse_key_order() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    for id in ["a1", "a2", "a3"] {
        save_author(&engine, author(id, &format!("{id}@example.com"), 1, "org")).await;
    }

    let tx = engine.begin_transaction().await.unwrap();
    let (items, has_more) = model
        .scan_reverse(
            TitoScanPayload {
                start: "table:authors:".to_string(),
                end: None,
                limit: Some(2),
                cursor: None,
            },
            &tx,
        )
        .await
        .unwrap();

    assert!(has_more);
    assert_eq!(
        items
            .iter()
            .map(|(_, value)| value["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["a3", "a2"]
    );
}

#[tokio::test]
async fn batch_get_returns_only_existing_keys_in_requested_order() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "a1@example.com", 1, "org")).await;
    save_author(&engine, author("a2", "a2@example.com", 1, "org")).await;
    let tx = engine.begin_transaction().await.unwrap();

    let items = model
        .batch_get(
            vec![
                "table:authors:a2".to_string(),
                "table:authors:missing".to_string(),
                "table:authors:a1".to_string(),
            ],
            &tx,
        )
        .await
        .unwrap();

    assert_eq!(
        items
            .into_iter()
            .map(|(_, value)| value["id"].as_str().unwrap().to_string())
            .collect::<Vec<_>>(),
        vec!["a2", "a1"]
    );
}

#[tokio::test]
async fn get_key_reads_raw_json_by_storage_key() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 1, "org")).await;
    let tx = engine.begin_transaction().await.unwrap();

    let value = model.get_key("table:authors:a1", &tx).await.unwrap();

    assert_eq!(value["id"], "a1");
    assert_eq!(
        engine.raw_json("table:authors:a1").await.unwrap()["id"],
        "a1"
    );
}

#[tokio::test]
async fn delete_range_removes_keys_inside_range_only() {
    let engine = engine();
    engine
        .put_json("table:authors:a1", &json!({"id": "a1"}))
        .await;
    engine
        .put_json("table:authors:a2", &json!({"id": "a2"}))
        .await;
    engine
        .put_json("table:posts:p1", &json!({"id": "p1"}))
        .await;

    engine
        .delete_range(b"table:authors:", b"table:authors;")
        .await
        .unwrap();

    assert!(!engine.contains_key("table:authors:a1").await);
    assert!(!engine.contains_key("table:authors:a2").await);
    assert!(engine.contains_key("table:posts:p1").await);
}

#[tokio::test]
async fn model_tx_runs_through_engine_transaction() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    let saved = model
        .tx(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "ada@example.com", 1, "org"))
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap();

    assert_eq!(saved.id, "a1");
    assert!(model.get("a1").execute(None).await.is_ok());
}

#[tokio::test]
async fn invalid_find_cursor_returns_deserialization_error() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "a1@example.com", 1, "org")).await;

    let error = model
        .find(TitoFindPayload {
            start: "".to_string(),
            end: None,
            limit: Some(1),
            cursor: Some("not-base64".to_string()),
            rels: Vec::new(),
        })
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::DeserializationFailed(_)));
}

#[test]
fn model_options_partition_config_cursor_and_id_helpers_are_stable() {
    let model = engine().model::<Author>(TitoModelOptions::with_partitions(12));
    let partition = PartitionConfig::new(7);
    let paginated = TitoPaginated::new(vec!["a".to_string()], Some("cursor".to_string()));
    let id = TitoId::new("a1", "author");
    let cursor = TitoCursor { ids: vec![None] };

    assert_eq!(model.partition_count, 12);
    assert_eq!(partition.partition, 7);
    assert_eq!(paginated.items, vec!["a"]);
    assert_eq!(paginated.cursor.as_deref(), Some("cursor"));
    assert_eq!(id.to_string(), "author:a1");
    assert!(cursor.first_id().is_err());
}

#[test]
fn model_key_helpers_extract_last_key_segment() {
    let model = engine().model::<Author>(TitoModelOptions::default());

    assert_eq!(
        model.get_id_from_table("table:authors:a1".to_string()),
        "a1"
    );
    assert_eq!(
        model.get_last_id("index:author-by-email:email:x:table:authors:a1".to_string()),
        Some("a1".to_string())
    );
}

#[test]
fn safe_encode_snake_cases_and_escapes_key_separators() {
    assert_eq!(safe_encode("Ada:Admin\\User"), "ada\\:admin\\\\user");
}

#[test]
fn lexicographic_helpers_move_string_bounds() {
    assert_eq!(next_string_lexicographically("abc".to_string()), "abd");
    assert_eq!(previous_string_lexicographically("abd".to_string()), "abc");
}
