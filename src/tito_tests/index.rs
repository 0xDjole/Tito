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
async fn missing_or_null_index_values_use_null_sentinel() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;

    let mut query = model.query_by_index("author-by-optional");
    let found = query.value("__null__").execute(None).await.unwrap();

    assert_eq!(found.items.len(), 1);
    assert_eq!(found.items[0].id, "a1");
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
                rels: Vec::new(),
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
                rels: Vec::new(),
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
                    rels: Vec::new(),
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
                    rels: Vec::new(),
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
    assert_eq!(page.items[1].id, "a2");
    assert!(page.cursor.is_some());
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
