use super::*;

#[tokio::test]
async fn string_index_queries_are_case_normalized_and_escape_colons() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "Ada:Admin@Example.com", 36, "org-a")).await;

    let mut query = model.query_by_index("author-by-email");
    let found = query
        .value("ada:admin@example.com")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a1");
    assert!(
        engine
            .contains_key("index:author-by-email:email:ada\\:admin@example.com:table:authors:a1")
            .await
    );
}

#[tokio::test]
async fn number_index_queries_sort_and_match_padded_values() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a9", "nine@example.com", 9, "org-a")).await;
    save_author(&engine, author("a12", "twelve@example.com", 12, "org-a")).await;

    let mut query = model.query_by_index("author-by-age");
    let found = query.value("9").execute(None).await.unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a9");
    assert!(
        engine
            .contains_key("index:author-by-age:age:0000000009:table:authors:a9")
            .await
    );
}

#[tokio::test]
async fn compound_index_queries_match_all_values() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;
    save_author(&engine, author("a2", "ada@example.com", 42, "org-b")).await;

    let mut query = model.query_by_index("author-by-org-email");
    let found = query
        .value("org-a")
        .value("ada@example.com")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a1");
}

#[tokio::test]
async fn custom_index_values_are_queryable() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    let mut query = model.query_by_index("author-by-kind-org");
    let found = query
        .value("author")
        .value("org-a")
        .execute(None)
        .await
        .unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a1");
}

#[tokio::test]
async fn null_index_values_are_not_indexed() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    let mut query = model.query_by_index("author-by-optional");
    let found = query.value("__null__").execute(None).await.unwrap();

    assert!(found.items.is_empty());
    assert!(engine
        .keys_with_prefix("index:author-by-optional:")
        .await
        .is_empty());
    assert!(engine
        .keys_with_prefix("index:author-by-org-optional:")
        .await
        .is_empty());
}

#[test]
fn missing_empty_and_wrong_typed_index_values_are_not_indexed() {
    let engine = engine();
    let model = engine.model::<Author>(TitoModelOptions::default());
    let value = author("a1", "ada@example.com", 36, "org-a");

    for optional in [None, Some(json!("")), Some(json!(false)), Some(json!(7))] {
        let mut json = serde_json::to_value(&value).unwrap();
        match optional {
            Some(optional) => json["optional"] = optional,
            None => {
                json.as_object_mut().unwrap().remove("optional");
            }
        }

        let keys = model
            .get_index_keys("table:authors:a1".to_string(), &value, &json)
            .unwrap();

        assert!(keys
            .iter()
            .all(|(key, _)| !key.starts_with("index:author-by-optional:")
                && !key.starts_with("index:author-by-org-optional:")));
    }
}

#[tokio::test]
async fn removing_an_optional_value_removes_its_index_key() {
    let engine = engine();
    let mut value = author("a1", "ada@example.com", 36, "org-a");
    value.optional = Some("present".to_string());
    save_author(&engine, value.clone()).await;

    assert!(
        engine
            .contains_key("index:author-by-optional:optional:present:table:authors:a1")
            .await
    );

    value.optional = None;
    save_author(&engine, value).await;

    assert!(
        !engine
            .contains_key("index:author-by-optional:optional:present:table:authors:a1")
            .await
    );
    assert!(engine
        .keys_with_prefix("index:author-by-optional:")
        .await
        .is_empty());
    assert!(engine
        .keys_with_prefix("index:author-by-org-optional:")
        .await
        .is_empty());
}

#[tokio::test]
async fn disabled_indexes_are_not_written() {
    let engine = engine();
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    assert!(engine
        .keys_with_prefix("index:author-disabled:")
        .await
        .is_empty());
}

#[tokio::test]
async fn updating_record_replaces_old_index_keys() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "old@example.com", 36, "org-a")).await;
    save_author(&engine, author("a1", "new@example.com", 36, "org-a")).await;

    let mut old_query = model.query_by_index("author-by-email");
    let old = old_query
        .value("old@example.com")
        .execute(None)
        .await
        .unwrap();
    let mut new_query = model.query_by_index("author-by-email");
    let new = new_query
        .value("new@example.com")
        .execute(None)
        .await
        .unwrap();

    assert!(old.items.is_empty());
    assert_eq!(new.items.len(), 1);
    assert!(
        !engine
            .contains_key("index:author-by-email:email:old@example.com:table:authors:a1")
            .await
    );
}

#[tokio::test]
async fn malformed_reverse_manifest_blocks_update_and_remove_without_mutation() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "old@example.com", 36, "org-a")).await;
    engine
        .put_raw("reverse-index:table:authors:a1", b"{".to_vec())
        .await;

    let update_error = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "new@example.com", 36, "org-a"))
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(update_error, TitoError::DeserializationFailed(_)));
    assert_eq!(
        engine.raw_json("table:authors:a1").await.unwrap()["email"],
        "old@example.com"
    );
    assert!(
        engine
            .contains_key("index:author-by-email:email:old@example.com:table:authors:a1")
            .await
    );
    assert!(
        !engine
            .contains_key("index:author-by-email:email:new@example.com:table:authors:a1")
            .await
    );

    let remove_error = engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove("a1", &tx).await }
        })
        .await
        .unwrap_err();
    assert!(matches!(remove_error, TitoError::DeserializationFailed(_)));
    assert!(engine.contains_key("table:authors:a1").await);
}

#[tokio::test]
async fn missing_or_orphaned_reverse_manifest_is_an_integrity_error() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .put_json(
            "table:authors:a1",
            &serde_json::to_value(author("a1", "old@example.com", 36, "org-a")).unwrap(),
        )
        .await;

    let update_error = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "new@example.com", 36, "org-a"))
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(update_error, TitoError::IndexError(_)));

    let remove_error = engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove("a1", &tx).await }
        })
        .await
        .unwrap_err();
    assert!(matches!(remove_error, TitoError::IndexError(_)));
    assert!(engine.contains_key("table:authors:a1").await);

    let engine = MemoryEngine::default();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .put_json("reverse-index:table:authors:a1", &json!({"value": []}))
        .await;
    let orphan_error = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "new@example.com", 36, "org-a"))
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(orphan_error, TitoError::IndexError(_)));
    assert!(!engine.contains_key("table:authors:a1").await);
    assert!(engine.contains_key("reverse-index:table:authors:a1").await);
}

#[tokio::test]
async fn reverse_manifest_cannot_delete_keys_outside_its_own_index_set() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "a1@example.com", 36, "org-a")).await;
    save_author(&engine, author("a2", "a2@example.com", 36, "org-a")).await;
    engine
        .put_json(
            "reverse-index:table:authors:a1",
            &json!({"value": ["table:authors:a2"]}),
        )
        .await;

    let error = engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove("a1", &tx).await }
        })
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::IndexError(_)));
    assert!(engine.contains_key("table:authors:a1").await);
    assert!(engine.contains_key("table:authors:a2").await);
}

#[tokio::test]
async fn set_propagates_index_state_read_failures_without_writing() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine.fail_next_get("manifest state read failed").await;

    let error = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(author("a1", "a1@example.com", 36, "org-a"))
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap_err();

    assert_eq!(
        error,
        TitoError::QueryFailed("manifest state read failed".to_string())
    );
    assert!(!engine.contains_key("table:authors:a1").await);
    assert!(engine.keys_with_prefix("index:").await.is_empty());
}

#[tokio::test]
async fn secondary_index_wire_value_remains_the_complete_primary_json() {
    let engine = engine();
    save_author(&engine, author("a1", "a1@example.com", 36, "org-a")).await;

    assert_eq!(
        engine
            .raw_json("index:author-by-email:email:a1@example.com:table:authors:a1")
            .await,
        engine.raw_json("table:authors:a1").await
    );
}

#[tokio::test]
async fn removing_record_deletes_row_reverse_index_and_index_keys() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove("a1", &tx).await.map(|_| ()) }
        })
        .await
        .unwrap();

    assert!(model.get("a1").execute(None).await.is_err());
    assert!(!engine.contains_key("table:authors:a1").await);
    assert!(!engine.contains_key("reverse-index:table:authors:a1").await);
    assert!(engine
        .keys_with_prefix("index:author-by-email:")
        .await
        .is_empty());
}

#[tokio::test]
async fn remove_by_index_deletes_matching_batch() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "a1@example.com", 36, "org-a")).await;
    save_author(&engine, author("a2", "a2@example.com", 42, "org-a")).await;
    save_author(&engine, author("a3", "a3@example.com", 42, "org-b")).await;

    let removed = engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove_by_index("author-by-age", "42", 10, &tx).await }
        })
        .await
        .unwrap();

    assert_eq!(removed.len(), 2);
    assert!(model.get("a2").execute(None).await.is_err());
    assert!(model.get("a3").execute(None).await.is_err());
    assert!(model.get("a1").execute(None).await.is_ok());
}

#[tokio::test]
async fn find_one_by_index_returns_first_match_or_not_found() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    let found = model
        .find_one_by_index(
            TitoFindOneByIndexPayload {
                index: "author-by-email".to_string(),
                values: vec!["ada@example.com".to_string()],
            },
            None,
        )
        .await
        .unwrap();

    assert_eq!(found.id, "a1");
    assert!(model
        .find_one_by_index(
            TitoFindOneByIndexPayload {
                index: "author-by-email".to_string(),
                values: vec!["missing@example.com".to_string()],
            },
            None,
        )
        .await
        .is_err());
}

#[tokio::test]
async fn unknown_index_and_extra_index_values_return_errors() {
    let model = engine().model::<Author>(TitoModelOptions::default());

    assert!(matches!(
        model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "missing".to_string(),
                    values: vec!["x".to_string()],
                    limit: None,
                    cursor: None,
                },
                None,
            )
            .await
            .unwrap_err(),
        TitoError::IndexError(_)
    ));

    assert!(matches!(
        model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "author-by-email".to_string(),
                    values: vec!["x".to_string(), "y".to_string()],
                    limit: None,
                    cursor: None,
                },
                None,
            )
            .await
            .unwrap_err(),
        TitoError::IndexError(_)
    ));
}

#[tokio::test]
async fn index_reverse_query_returns_reverse_index_order() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "a@example.com", 7, "org")).await;
    save_author(&engine, author("a2", "b@example.com", 7, "org")).await;
    save_author(&engine, author("a20", "bb@example.com", 7, "org")).await;
    save_author(&engine, author("a3", "c@example.com", 7, "org")).await;

    let mut query = model.query_by_index("author-by-age");
    let page = query
        .value("7")
        .limit(Some(2))
        .execute_reverse(None)
        .await
        .unwrap();

    assert_eq!(page.items.len(), 2);
    assert_eq!(page.items[0].id, "a3");
    assert_eq!(page.items[1].id, "a20");
    let cursor = page.cursor;
    assert!(cursor.is_some());

    query.cursor(cursor);
    let second = query.execute_reverse(None).await.unwrap();
    assert_eq!(
        second
            .items
            .into_iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec!["a2", "a1"]
    );
    assert!(second.cursor.is_none());
}

#[tokio::test]
async fn index_queries_fail_on_malformed_and_schema_incompatible_rows() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .put_raw(
            "index:author-by-age:age:0000000007:table:authors:malformed",
            b"{".to_vec(),
        )
        .await;

    let mut malformed_query = model.query_by_index("author-by-age");
    assert!(matches!(
        malformed_query.value("7").execute(None).await.unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));

    let engine = MemoryEngine::default();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .put_json(
            "index:author-by-age:age:0000000007:table:authors:incompatible",
            &json!({"id": "incompatible"}),
        )
        .await;

    let mut incompatible_query = model.query_by_index("author-by-age");
    assert!(matches!(
        incompatible_query
            .value("7")
            .execute(None)
            .await
            .unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));
    assert!(matches!(
        model
            .find_one_by_index(
                TitoFindOneByIndexPayload {
                    index: "author-by-age".to_string(),
                    values: vec!["7".to_string()],
                },
                None,
            )
            .await
            .unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));
}

#[tokio::test]
async fn index_query_rejects_cursor_from_a_different_index_value_scope() {
    let model = engine().model::<Author>(TitoModelOptions::default());
    let mut query = model.query_by_index("author-by-age");
    let error = query
        .value("7")
        .cursor(Some(cursor_for_key(
            "index:author-by-age:age:0000000008:table:authors:a1",
        )))
        .execute(None)
        .await
        .unwrap_err();

    assert_eq!(
        error,
        TitoError::InvalidInput("Cursor is outside the requested scan range".to_string())
    );
}

#[tokio::test]
async fn array_indexes_write_one_key_per_value() {
    let engine = engine();
    let model = engine.clone().model::<Post>(TitoModelOptions::default());
    save_tag(&engine, tag("t1", "Tech")).await;
    save_tag(&engine, tag("t2", "Rust")).await;
    save_post(
        &engine,
        post("p1", "a1", vec!["t1".to_string(), "t2".to_string()]),
    )
    .await;

    let mut query = model.query_by_index("post-by-tag");
    let found = query.value("t2").execute(None).await.unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "p1");
    assert!(
        engine
            .contains_key("index:post-by-tag:tag_ids:t1:table:posts:p1")
            .await
    );
    assert!(
        engine
            .contains_key("index:post-by-tag:tag_ids:t2:table:posts:p1")
            .await
    );
}

#[tokio::test]
async fn nested_array_indexes_write_one_key_per_nested_value() {
    let engine = engine();
    save_post(
        &engine,
        Post {
            comments: vec![
                Comment {
                    author_id: "a1".to_string(),
                    body: "one".to_string(),
                },
                Comment {
                    author_id: "a2".to_string(),
                    body: "two".to_string(),
                },
            ],
            ..post("p1", "a1", vec![])
        },
    )
    .await;
    let model = engine.clone().model::<Post>(TitoModelOptions::default());

    let mut query = model.query_by_index("post-by-comment-author");
    let found = query.value("a2").execute(None).await.unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "p1");
}

#[tokio::test]
async fn map_indexes_include_map_entry_keys_in_index_key() {
    let engine = engine();
    save_post(&engine, post("p1", "a1", vec![])).await;

    let keys = engine.keys_with_prefix("index:post-by-metadata:").await;

    assert!(keys
        .iter()
        .any(|key| key.contains("metadata:locale.en:table:posts:p1")));
    assert!(keys
        .iter()
        .any(|key| key.contains("metadata:channel.web:table:posts:p1")));
}
