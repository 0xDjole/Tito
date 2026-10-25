use super::*;
use crate::backend::tikv::MAX_TRANSACTION_RETRIES;
use crate::fence::{exclusive_fence_key, record_fence_keys, shared_fence_key};
use crate::TitoModel;
use std::sync::atomic::{AtomicUsize, Ordering};

fn tags(engine: &MemoryEngine) -> TitoModel<MemoryEngine, Tag> {
    engine.clone().model::<Tag>(TitoModelOptions::default())
}

fn unique_accounts(engine: &MemoryEngine) -> TitoModel<MemoryEngine, UniqueAccount> {
    engine
        .clone()
        .model::<UniqueAccount>(TitoModelOptions::default())
}

fn account_by_email(email: &str) -> TitoFindOneByIndexPayload {
    TitoFindOneByIndexPayload {
        index: "verified-account-by-tenant-email".to_string(),
        values: vec!["tenant".to_string(), email.to_string()],
    }
}

fn seventeen_fence_keys(table_and_id: &str) -> Vec<String> {
    let mut keys = (0..16)
        .map(|number| format!("fence:{table_and_id}:{number}"))
        .collect::<Vec<_>>();
    keys.push(format!("fence:{table_and_id}:exclusive"));
    keys
}

#[tokio::test]
async fn get_exclusive_requires_an_existing_record_and_changes_no_stored_byte() {
    let engine = engine();
    let model = shops(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();
    let primary = engine.raw_bytes("table:shops:s1").await.unwrap();
    let manifest = engine
        .raw_bytes("reverse-index:table:shops:s1")
        .await
        .unwrap();
    let version = model.get_versioned("s1", None).await.unwrap().version;

    let tx = engine.begin_transaction().await.unwrap();
    assert!(matches!(
        model.get_exclusive("missing", &tx).await,
        Err(TitoError::NotFound(_))
    ));
    assert_eq!(model.get_exclusive("s1", &tx).await.unwrap(), shop("s1"));
    assert_eq!(
        commit_writes(&engine, tx).await,
        vec![RecordedWrite::Delete(exclusive_fence_key("shops", "s1"))]
    );

    assert_eq!(engine.raw_bytes("table:shops:s1").await.unwrap(), primary);
    assert_eq!(
        engine
            .raw_bytes("reverse-index:table:shops:s1")
            .await
            .unwrap(),
        manifest
    );
    assert_eq!(
        model.get_versioned("s1", None).await.unwrap().version,
        version
    );
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_read_by_id_in_a_writing_transaction_deletes_one_shared_key_of_its_pick_at_commit() {
    let engine = engine();
    let model = shops(&engine);
    let accounts = unique_accounts(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();
    save_unique_account(
        &engine,
        unique_account("a1", "tenant", "a1@example.com", true),
    )
    .await;

    let tx = engine.begin_transaction().await.unwrap();
    model.get("s1").execute(Some(&tx)).await.unwrap();
    write_scratch(&tx).await;
    let key = shared_fence_key("shops", "s1", picked_number(&tx));
    assert_eq!(
        commit_writes(&engine, tx).await,
        vec![RecordedWrite::Delete(key)]
    );

    let tx = engine.begin_transaction().await.unwrap();
    model
        .get_many(vec!["s1".to_string()])
        .execute(Some(&tx))
        .await
        .unwrap();
    write_scratch(&tx).await;
    let key = shared_fence_key("shops", "s1", picked_number(&tx));
    assert_eq!(
        commit_writes(&engine, tx).await,
        vec![RecordedWrite::Delete(key)]
    );

    let tx = engine.begin_transaction().await.unwrap();
    model.get_versioned("s1", Some(&tx)).await.unwrap();
    write_scratch(&tx).await;
    let key = shared_fence_key("shops", "s1", picked_number(&tx));
    assert_eq!(
        commit_writes(&engine, tx).await,
        vec![RecordedWrite::Delete(key)]
    );

    let tx = engine.begin_transaction().await.unwrap();
    accounts
        .find_one_by_unique_index(account_by_email("a1@example.com"), Some(&tx))
        .await
        .unwrap();
    write_scratch(&tx).await;
    let key = shared_fence_key("unique-accounts", "a1", picked_number(&tx));
    assert_eq!(
        commit_writes(&engine, tx).await,
        vec![RecordedWrite::Delete(key)]
    );
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn get_exclusive_deletes_the_exclusive_key_at_commit_in_place_of_the_shared_one() {
    let engine = engine();
    let model = shops(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();
    save_shop(&engine, shop("s2")).await.unwrap();

    let tx = engine.begin_transaction().await.unwrap();
    model.get("s1").execute(Some(&tx)).await.unwrap();
    model.get_exclusive("s1", &tx).await.unwrap();
    model.get_exclusive("s2", &tx).await.unwrap();
    model
        .get_many(vec!["s1".to_string(), "s2".to_string()])
        .execute(Some(&tx))
        .await
        .unwrap();
    model.get_versioned("s2", Some(&tx)).await.unwrap();
    write_scratch(&tx).await;
    assert_eq!(
        commit_writes(&engine, tx).await,
        vec![
            RecordedWrite::Delete(exclusive_fence_key("shops", "s1")),
            RecordedWrite::Delete(exclusive_fence_key("shops", "s2")),
        ]
    );
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_transaction_that_writes_nothing_and_a_read_without_a_transaction_add_no_guard() {
    let engine = engine();
    let model = shops(&engine);
    let accounts = unique_accounts(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();
    save_unique_account(
        &engine,
        unique_account("a1", "tenant", "a1@example.com", true),
    )
    .await;

    let tx = engine.begin_transaction().await.unwrap();
    model.get("s1").execute(Some(&tx)).await.unwrap();
    model
        .get_many(vec!["s1".to_string()])
        .execute(Some(&tx))
        .await
        .unwrap();
    model.get_versioned("s1", Some(&tx)).await.unwrap();
    accounts
        .find_one_by_unique_index(account_by_email("a1@example.com"), Some(&tx))
        .await
        .unwrap();
    assert!(commit_writes(&engine, tx).await.is_empty());

    engine.start_recording_writes().await;
    model.get("s1").execute(None).await.unwrap();
    model
        .get_many(vec!["s1".to_string()])
        .execute(None)
        .await
        .unwrap();
    model.get_versioned("s1", None).await.unwrap();
    accounts
        .find_one_by_unique_index(account_by_email("a1@example.com"), None)
        .await
        .unwrap();
    assert!(engine.take_recorded_writes().await.is_empty());
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn get_exclusive_counts_as_a_write_so_its_transaction_deletes_every_key_it_noted() {
    let engine = engine();
    let model = shops(&engine);
    for id in ["s1", "s2", "s3"] {
        save_shop(&engine, shop(id)).await.unwrap();
    }

    let tx = engine.begin_transaction().await.unwrap();
    model.get("s1").execute(Some(&tx)).await.unwrap();
    model
        .get_many(vec!["s2".to_string()])
        .execute(Some(&tx))
        .await
        .unwrap();
    model.get_exclusive("s3", &tx).await.unwrap();
    let number = picked_number(&tx);
    assert_eq!(
        commit_writes(&engine, tx).await,
        vec![
            RecordedWrite::Delete(shared_fence_key("shops", "s1", number)),
            RecordedWrite::Delete(shared_fence_key("shops", "s2", number)),
            RecordedWrite::Delete(exclusive_fence_key("shops", "s3")),
        ]
    );

    let first = engine.begin_transaction().await.unwrap();
    let second = engine.begin_transaction().await.unwrap();
    model.get_exclusive("s1", &first).await.unwrap();
    model.get_exclusive("s1", &second).await.unwrap();
    second.commit().await.unwrap();
    assert!(matches!(first.commit().await, Err(TitoError::Retryable(_))));

    let reader = engine.begin_transaction().await.unwrap();
    model.get_exclusive("s1", &reader).await.unwrap();
    model.get("s2").execute(Some(&reader)).await.unwrap();
    save_shop(
        &engine,
        Shop {
            name: "Renamed".to_string(),
            ..shop("s2")
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        reader.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_record_saved_or_removed_in_its_own_transaction_adds_no_guard() {
    let engine = engine();
    let model = shops(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();

    let tx = engine.begin_transaction().await.unwrap();
    model.get("s1").execute(Some(&tx)).await.unwrap();
    engine.start_recording_writes().await;
    model
        .set(Shop {
            name: "Renamed".to_string(),
            ..shop("s1")
        })
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(
        fence_writes(&engine.take_recorded_writes().await),
        record_fence_keys("shops", "s1")
    );
    assert!(commit_writes(&engine, tx).await.is_empty());

    let tx = engine.begin_transaction().await.unwrap();
    model.get_exclusive("s1", &tx).await.unwrap();
    engine.start_recording_writes().await;
    model
        .set(shop("s1"))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(
        fence_writes(&engine.take_recorded_writes().await),
        record_fence_keys("shops", "s1")
    );
    assert!(commit_writes(&engine, tx).await.is_empty());

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_writes().await;
    model
        .set(shop("s2"))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(
        fence_writes(&engine.take_recorded_writes().await),
        record_fence_keys("shops", "s2")
    );
    model.get("s2").execute(Some(&tx)).await.unwrap();
    model.get_exclusive("s2", &tx).await.unwrap();
    assert!(commit_writes(&engine, tx).await.is_empty());

    let tx = engine.begin_transaction().await.unwrap();
    model.get("s1").execute(Some(&tx)).await.unwrap();
    model.get_exclusive("s2", &tx).await.unwrap();
    engine.start_recording_writes().await;
    assert!(model.remove("s1", &tx).await.unwrap());
    assert!(model.remove("s2", &tx).await.unwrap());
    let mut removed = record_fence_keys("shops", "s1");
    removed.extend(record_fence_keys("shops", "s2"));
    assert_eq!(fence_writes(&engine.take_recorded_writes().await), removed);
    assert!(commit_writes(&engine, tx).await.is_empty());
    assert!(engine.keys_with_prefix("table:shops:").await.is_empty());
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn index_reads_scans_and_misses_add_no_guard() {
    let engine = engine();
    let model = shops(&engine);
    let accounts = unique_accounts(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();
    let by_name = TitoFindByIndexPayload {
        index: "shop-by-name".to_string(),
        values: vec!["Shop s1".to_string()],
        limit: Some(10),
        cursor: None,
    };
    let scan = || TitoScanPayload {
        start: "table:shops:".to_string(),
        end: None,
        limit: Some(10),
        cursor: None,
    };

    let tx = engine.begin_transaction().await.unwrap();
    assert_eq!(
        model
            .find_by_index(by_name.clone(), Some(&tx))
            .await
            .unwrap()
            .items,
        vec![shop("s1")]
    );
    assert_eq!(
        model
            .find_by_index_reverse(by_name, Some(&tx))
            .await
            .unwrap()
            .items,
        vec![shop("s1")]
    );
    let mut query = model.query_by_index("shop-by-name");
    query.value("Shop s1");
    assert_eq!(
        query.execute(Some(&tx)).await.unwrap().items,
        vec![shop("s1")]
    );
    assert_eq!(
        query.execute_reverse(Some(&tx)).await.unwrap().items,
        vec![shop("s1")]
    );
    assert_eq!(
        query.execute_ids(Some(&tx)).await.unwrap().items,
        vec!["s1"]
    );
    assert_eq!(
        model
            .find_one_by_index(
                TitoFindOneByIndexPayload {
                    index: "shop-by-name".to_string(),
                    values: vec!["Shop s1".to_string()],
                },
                Some(&tx),
            )
            .await
            .unwrap(),
        shop("s1")
    );
    assert_eq!(model.scan(scan(), &tx).await.unwrap().0.len(), 1);
    assert_eq!(model.scan_reverse(scan(), &tx).await.unwrap().0.len(), 1);
    assert!(matches!(
        model.get("missing").execute(Some(&tx)).await,
        Err(TitoError::NotFound(_))
    ));
    assert!(model
        .get_many(vec!["missing".to_string()])
        .execute(Some(&tx))
        .await
        .unwrap()
        .is_empty());
    assert!(matches!(
        model.get_versioned("missing", Some(&tx)).await,
        Err(TitoError::NotFound(_))
    ));
    assert!(matches!(
        model.get_exclusive("missing", &tx).await,
        Err(TitoError::NotFound(_))
    ));
    assert!(matches!(
        accounts
            .find_one_by_unique_index(account_by_email("missing@example.com"), Some(&tx))
            .await,
        Err(TitoError::NotFound(_))
    ));
    write_scratch(&tx).await;
    assert!(commit_writes(&engine, tx).await.is_empty());
}

#[tokio::test]
async fn every_read_of_one_transaction_uses_the_shared_key_of_its_pick_once_per_record() {
    let engine = engine();
    let model = shops(&engine);
    let ids = (0..20).map(|n| format!("s{n}")).collect::<Vec<_>>();
    for id in &ids {
        save_shop(&engine, shop(id)).await.unwrap();
    }

    let tx = engine.begin_transaction().await.unwrap();
    for id in &ids {
        model.get(id).execute(Some(&tx)).await.unwrap();
        model.get_versioned(id, Some(&tx)).await.unwrap();
    }
    model
        .get_many(ids.clone())
        .execute(Some(&tx))
        .await
        .unwrap();
    write_scratch(&tx).await;
    let number = picked_number(&tx);
    let writes = commit_writes(&engine, tx).await;
    assert_eq!(writes.len(), ids.len());
    assert_eq!(
        sorted(fence_writes(&writes)),
        sorted(
            ids.iter()
                .map(|id| format!("fence:shops:{id}:{number}"))
                .collect()
        )
    );
}

#[tokio::test]
async fn shared_reads_take_turns_only_when_their_transactions_pick_one_key() {
    let engine = engine();
    let model = shops(&engine);
    let ids = (0..20).map(|n| format!("s{n}")).collect::<Vec<_>>();
    for id in &ids {
        save_shop(&engine, shop(id)).await.unwrap();
    }

    let first = engine.begin_transaction().await.unwrap();
    let number = picked_number(&first);
    let other = transaction_with_shared_key(&engine, |picked| picked != number).await;
    let same = transaction_with_shared_key(&engine, |picked| picked == number).await;
    for tx in [&first, &other, &same] {
        assert_eq!(
            model.get_many(ids.clone()).execute(Some(tx)).await.unwrap(),
            ids.iter().map(|id| shop(id)).collect::<Vec<_>>()
        );
        write_scratch(tx).await;
    }
    first.commit().await.unwrap();
    other.commit().await.unwrap();
    assert!(matches!(same.commit().await, Err(TitoError::Retryable(_))));
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn every_attempt_of_a_retried_transaction_guards_with_its_own_pick_and_starts_clean() {
    let engine = engine();
    let model = shops(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();
    save_shop(&engine, shop("s2")).await.unwrap();
    let numbers = Arc::new(tokio::sync::Mutex::new(Vec::<u16>::new()));

    engine
        .transaction(|tx| {
            let engine = engine.clone();
            let model = model.clone();
            let numbers = numbers.clone();
            async move {
                let attempt = {
                    let mut numbers = numbers.lock().await;
                    numbers.push(picked_number(&tx));
                    numbers.len()
                };
                let read = if attempt == 1 { "s1" } else { "s2" };
                model.get(read).execute(Some(&tx)).await?;
                write_scratch(&tx).await;
                save_shop(
                    &engine,
                    Shop {
                        name: format!("Renamed during attempt {attempt}"),
                        ..shop("s1")
                    },
                )
                .await?;
                if attempt > 1 {
                    engine.start_recording_writes().await;
                }
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();

    let numbers = numbers.lock().await;
    assert_eq!(numbers.len(), 2);
    assert_eq!(
        engine.take_recorded_writes().await,
        vec![RecordedWrite::Delete(shared_fence_key(
            "shops", "s2", numbers[1]
        ))]
    );
    assert_eq!(
        shops(&engine).get("s1").execute(None).await.unwrap().name,
        "Renamed during attempt 2"
    );
}

#[tokio::test]
async fn every_save_replace_remove_and_repair_deletes_the_seventeen_escaped_fence_keys() {
    let engine = engine();
    let model = shops(&engine);
    let tag_model = tags(&engine);
    let id = "a:b\\c";
    let escaped = seventeen_fence_keys("shops:a\\:b\\\\c");
    assert_eq!(escaped[7], "fence:shops:a\\:b\\\\c:7");
    let renamed = Shop {
        name: "Renamed".to_string(),
        ..shop(id)
    };

    assert_eq!(
        fence_writes(&save_writes(&engine, &model, shop(id)).await),
        escaped
    );
    assert_eq!(
        fence_writes(&save_writes(&engine, &model, renamed.clone()).await),
        escaped
    );
    engine.start_recording_writes().await;
    engine
        .transaction(|tx| {
            let model = model.clone();
            let renamed = renamed.clone();
            async move {
                model
                    .replace_many(vec![(renamed, shop(id))])
                    .timestamps(false)
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap();
    assert_eq!(
        sorted(fence_writes(&engine.take_recorded_writes().await)),
        sorted(escaped.clone())
    );
    assert_eq!(
        fence_writes(&repair_writes(&engine, &model, id).await),
        escaped
    );
    assert_eq!(
        fence_writes(&remove_writes(&engine, &model, id).await),
        escaped
    );

    let tag_keys = seventeen_fence_keys("tags:t1");
    assert_eq!(
        fence_writes(&save_writes(&engine, &tag_model, tag("t1", "database")).await),
        tag_keys
    );
    assert_eq!(
        fence_writes(&save_writes(&engine, &tag_model, tag("t1", "storage")).await),
        tag_keys
    );
    assert_eq!(
        fence_writes(&repair_writes(&engine, &tag_model, "t1").await),
        tag_keys
    );
    assert_eq!(
        fence_writes(&remove_writes(&engine, &tag_model, "t1").await),
        tag_keys
    );
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_manifest_may_not_list_a_fence_key() {
    for fence in ["fence:shops:s1:0", "fence:shops:s1:exclusive"] {
        let engine = engine();
        let model = shops(&engine);
        save_shop(&engine, shop("s1")).await.unwrap();
        let reverse_key = "reverse-index:table:shops:s1";
        let mut listed = engine.raw_json(reverse_key).await.unwrap();
        listed["value"].as_array_mut().unwrap().push(json!(fence));
        engine.put_json(reverse_key, &listed).await;

        assert!(matches!(
            save_shop(&engine, shop("s1")).await,
            Err(TitoError::IndexError(_))
        ));
        assert!(matches!(
            engine
                .transaction(|tx| {
                    let model = model.clone();
                    async move { model.remove("s1", &tx).await }
                })
                .await,
            Err(TitoError::IndexError(_))
        ));
        let tx = engine.begin_transaction().await.unwrap();
        assert!(matches!(
            model.rebuild_indexes_for_restore("s1", &tx).await,
            Err(TitoError::IndexError(_))
        ));
        tx.rollback().await.unwrap();
        assert_eq!(engine.raw_json(reverse_key).await.unwrap(), listed);
    }
}

#[tokio::test]
async fn memory_transactions_report_write_write_conflicts_like_tikv() {
    let engine = engine();

    let first = engine.begin_transaction().await.unwrap();
    let second = engine.begin_transaction().await.unwrap();
    first.put("shared", b"first").await.unwrap();
    first.put("first-only", b"1").await.unwrap();
    second.put("shared", b"second").await.unwrap();
    second.put("second-only", b"2").await.unwrap();
    first.commit().await.unwrap();
    assert!(matches!(
        second.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert_eq!(engine.raw_bytes("shared").await.unwrap(), b"first");
    assert!(engine.contains_key("first-only").await);
    assert!(!engine.contains_key("second-only").await);

    let left = engine.begin_transaction().await.unwrap();
    let right = engine.begin_transaction().await.unwrap();
    left.put("left", b"l").await.unwrap();
    right.put("right", b"r").await.unwrap();
    left.commit().await.unwrap();
    right.commit().await.unwrap();
    assert!(engine.contains_key("left").await);
    assert!(engine.contains_key("right").await);

    let first = engine.begin_transaction().await.unwrap();
    let second = engine.begin_transaction().await.unwrap();
    first.delete("absent").await.unwrap();
    second.delete("absent").await.unwrap();
    first.commit().await.unwrap();
    assert!(matches!(
        second.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert!(!engine.contains_key("absent").await);

    let reader = engine.begin_transaction().await.unwrap();
    assert_eq!(reader.get("shared").await.unwrap(), Some(b"first".to_vec()));
    let writer = engine.begin_transaction().await.unwrap();
    writer.put("shared", b"third").await.unwrap();
    writer.commit().await.unwrap();
    assert_eq!(reader.get("shared").await.unwrap(), Some(b"first".to_vec()));
    reader.commit().await.unwrap();

    let later = engine.begin_transaction().await.unwrap();
    later.delete("shared").await.unwrap();
    later.commit().await.unwrap();
    assert!(!engine.contains_key("shared").await);
}

#[tokio::test]
async fn memory_transactions_retry_a_conflicting_commit_within_the_tikv_budget() {
    let engine = engine();
    let attempts = Arc::new(AtomicUsize::new(0));
    let value = engine
        .transaction(|tx| {
            let engine = engine.clone();
            let attempts = attempts.clone();
            async move {
                let attempt = attempts.fetch_add(1, Ordering::SeqCst);
                tx.put("counter", format!("outer-{attempt}")).await?;
                if attempt == 0 {
                    let competing = engine.begin_transaction().await?;
                    competing.put("counter", "competing").await?;
                    competing.commit().await?;
                }
                Ok::<_, TitoError>(attempt)
            }
        })
        .await
        .unwrap();
    assert_eq!(value, 1);
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert_eq!(engine.raw_bytes("counter").await.unwrap(), b"outer-1");

    let attempts = Arc::new(AtomicUsize::new(0));
    let error = engine
        .transaction(|tx| {
            let engine = engine.clone();
            let attempts = attempts.clone();
            async move {
                attempts.fetch_add(1, Ordering::SeqCst);
                tx.put("counter", "outer").await?;
                let competing = engine.begin_transaction().await?;
                competing.put("counter", "competing").await?;
                competing.commit().await?;
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(error, TitoError::Retryable(_)));
    assert_eq!(
        attempts.load(Ordering::SeqCst),
        MAX_TRANSACTION_RETRIES as usize + 1
    );
    assert_eq!(engine.raw_bytes("counter").await.unwrap(), b"competing");
}

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
    let before = Utc::now().timestamp_millis();

    let saved = save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;
    let after = Utc::now().timestamp_millis();

    assert!((before..=after).contains(&saved.created_at));
    assert_eq!(saved.updated_at, saved.created_at);

    let mut changed = saved.clone();
    changed.name = "Updated".to_string();
    let before_update = Utc::now().timestamp_millis();
    let updated = save_author(&engine, changed).await;
    let after_update = Utc::now().timestamp_millis();
    assert_eq!(updated.created_at, saved.created_at);
    assert!((before_update..=after_update).contains(&updated.updated_at));
    let stored = engine.raw_json("table:authors:a1").await.unwrap();
    assert_eq!(stored["created_at"], json!(saved.created_at));
    assert_eq!(stored["updated_at"], json!(updated.updated_at));
}

#[tokio::test]
async fn model_explicit_signed_epoch_milliseconds_round_trip_without_unit_guessing() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());

    for timestamp in [-1, 0, 123, 1_700_000_000_123] {
        let mut item = author(&format!("at-{timestamp}"), "ada@example.com", 36, "org-a");
        item.created_at = timestamp;
        item.updated_at = timestamp;
        let saved = engine
            .transaction(|tx| {
                let model = model.clone();
                let item = item.clone();
                async move { model.set(item).timestamps(false).execute(&tx).await }
            })
            .await
            .unwrap();
        let restored = model.get(&saved.id).execute(None).await.unwrap();
        assert_eq!(saved.created_at, timestamp);
        assert_eq!(restored.created_at, timestamp);
        assert_eq!(restored.updated_at, timestamp);
    }
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
async fn model_set_does_not_create_undeclared_timestamp_fields() {
    let engine = engine();
    let model = engine.clone().model::<Tag>(TitoModelOptions::default());

    engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model.set(tag("t1", "database")).execute(&tx).await?;
                let bytes = tx
                    .get("table:tags:t1")
                    .await?
                    .expect("saved tag must exist");
                let value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(value, json!({"id": "t1", "name": "database"}));
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();
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
async fn get_malformed_record_returns_deserialization_error() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .put_raw("table:authors:malformed", b"{".to_vec())
        .await;

    assert!(matches!(
        model.get("malformed").execute(None).await.unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));
}

#[tokio::test]
async fn get_schema_incompatible_record_returns_deserialization_error() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .put_json(
            "table:authors:incompatible",
            &json!({ "id": "incompatible" }),
        )
        .await;

    assert!(matches!(
        model.get("incompatible").execute(None).await.unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));
}

#[tokio::test]
async fn get_preserves_backend_read_error() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine.fail_next_get("read failed").await;

    assert_eq!(
        model.get("a1").execute(None).await.unwrap_err(),
        TitoError::QueryFailed("read failed".to_string())
    );
}

#[tokio::test]
async fn find_paginates_with_cursor() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    for id in ["a1", "a2", "a20", "a3"] {
        save_author(&engine, author(id, &format!("{id}@example.com"), 1, "org")).await;
    }

    let first = model
        .find(TitoFindPayload {
            start: "".to_string(),
            end: None,
            limit: Some(2),
            cursor: None,
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
        })
        .await
        .unwrap();
    assert_eq!(
        second
            .items
            .into_iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec!["a20", "a3"]
    );
    assert!(second.cursor.is_none());
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
async fn scan_reverse_cursor_does_not_skip_adjacent_prefix_keys() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    for id in ["a1", "a2", "a20", "a3"] {
        save_author(&engine, author(id, &format!("{id}@example.com"), 1, "org")).await;
    }

    let tx = engine.begin_transaction().await.unwrap();
    let (first_items, has_more) = model
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
    let first = model.to_paginated_items(first_items, has_more).unwrap();
    assert_eq!(
        first
            .items
            .into_iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec!["a3", "a20"]
    );

    let (second_items, has_more) = model
        .scan_reverse(
            TitoScanPayload {
                start: "table:authors:".to_string(),
                end: None,
                limit: Some(2),
                cursor: first.cursor,
            },
            &tx,
        )
        .await
        .unwrap();
    let second = model.to_paginated_items(second_items, has_more).unwrap();
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
async fn scan_rejects_cursor_outside_requested_range_in_both_directions() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    let cursor = cursor_for_key("table:posts:p1");
    let tx = engine.begin_transaction().await.unwrap();

    for reverse in [false, true] {
        let payload = TitoScanPayload {
            start: "table:authors:".to_string(),
            end: None,
            limit: Some(2),
            cursor: Some(cursor.clone()),
        };
        let error = if reverse {
            model.scan_reverse(payload, &tx).await.unwrap_err()
        } else {
            model.scan(payload, &tx).await.unwrap_err()
        };
        assert_eq!(
            error,
            TitoError::InvalidInput("Cursor is outside the requested scan range".to_string())
        );
    }
}

#[tokio::test]
async fn scan_validates_bounds_and_nonzero_limit() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    let tx = engine.begin_transaction().await.unwrap();

    let invalid_bounds = model
        .scan(
            TitoScanPayload {
                start: "table:authors:z".to_string(),
                end: Some("table:authors:a".to_string()),
                limit: Some(1),
                cursor: None,
            },
            &tx,
        )
        .await
        .unwrap_err();
    assert_eq!(
        invalid_bounds,
        TitoError::InvalidInput("Scan range start must be less than end".to_string())
    );

    let zero_limit = model
        .scan(
            TitoScanPayload {
                start: "table:authors:".to_string(),
                end: None,
                limit: Some(0),
                cursor: None,
            },
            &tx,
        )
        .await
        .unwrap_err();
    assert_eq!(
        zero_limit,
        TitoError::InvalidInput("Scan limit must be greater than zero".to_string())
    );
}

#[tokio::test]
async fn find_honors_explicit_exclusive_end_bound() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    for id in ["a1", "a2", "a3"] {
        save_author(&engine, author(id, &format!("{id}@example.com"), 1, "org")).await;
    }

    let page = model
        .find(TitoFindPayload {
            start: "a1".to_string(),
            end: Some("a3".to_string()),
            limit: None,
            cursor: None,
        })
        .await
        .unwrap();

    assert_eq!(
        page.items
            .into_iter()
            .map(|item| item.id)
            .collect::<Vec<_>>(),
        vec!["a1", "a2"]
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
async fn get_key_reports_malformed_json_instead_of_not_found() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .put_raw("table:authors:malformed", b"{".to_vec())
        .await;
    let tx = engine.begin_transaction().await.unwrap();

    assert!(matches!(
        model
            .get_key("table:authors:malformed", &tx)
            .await
            .unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));
}

#[tokio::test]
async fn scans_fail_on_malformed_json_and_non_utf8_storage_keys() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .put_raw("table:authors:malformed", b"{".to_vec())
        .await;
    let tx = engine.begin_transaction().await.unwrap();

    assert!(matches!(
        model
            .scan(
                TitoScanPayload {
                    start: "table:authors:".to_string(),
                    end: None,
                    limit: None,
                    cursor: None,
                },
                &tx,
            )
            .await
            .unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));

    let engine = MemoryEngine::default();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    let mut invalid_key = b"table:authors:".to_vec();
    invalid_key.push(0xff);
    engine
        .put_raw_bytes(
            invalid_key,
            serde_json::to_vec(&json!({"id": "x"})).unwrap(),
        )
        .await;
    let tx = engine.begin_transaction().await.unwrap();

    assert!(matches!(
        model
            .scan(
                TitoScanPayload {
                    start: "table:authors:".to_string(),
                    end: Some("table:authors;".to_string()),
                    limit: None,
                    cursor: None,
                },
                &tx,
            )
            .await
            .unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));
}

#[tokio::test]
async fn paginated_find_and_get_many_fail_on_schema_incompatible_rows() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine
        .put_json("table:authors:incompatible", &json!({"id": "incompatible"}))
        .await;

    assert!(matches!(
        model
            .find(TitoFindPayload {
                start: String::new(),
                end: None,
                limit: None,
                cursor: None,
            })
            .await
            .unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));
    assert!(matches!(
        model
            .get_many(vec!["incompatible".to_string()])
            .execute(None)
            .await
            .unwrap_err(),
        TitoError::DeserializationFailed(_)
    ));
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
async fn delete_range_rejects_invalid_bounds_before_touching_data() {
    let engine = engine();
    engine
        .put_json("table:authors:a1", &json!({"id": "a1"}))
        .await;

    for (start, end, message) in [
        (&[][..], &b"z"[..], "Delete range bounds must not be empty"),
        (&b"a"[..], &[][..], "Delete range bounds must not be empty"),
        (
            &b"table:authors;"[..],
            &b"table:authors:"[..],
            "Delete range start must be less than end",
        ),
    ] {
        assert_eq!(
            engine.delete_range(start, end).await.unwrap_err(),
            TitoError::InvalidInput(message.to_string())
        );
    }

    assert!(engine.contains_key("table:authors:a1").await);
}

#[tokio::test]
async fn memory_engine_garbage_collection_preserves_current_values() {
    let engine = engine();
    engine
        .put_json("table:authors:a1", &json!({"id": "a1"}))
        .await;

    engine
        .garbage_collect(std::time::Duration::from_secs(24 * 60 * 60))
        .await
        .unwrap();

    assert!(engine.contains_key("table:authors:a1").await);

    assert_eq!(
        engine
            .garbage_collect(std::time::Duration::ZERO)
            .await
            .unwrap_err(),
        TitoError::InvalidInput(
            "Garbage collection retention must be greater than zero".to_string()
        )
    );
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
fn encode_index_string_escapes_only_key_separators() {
    assert_eq!(
        encode_index_string("Ada:Admin\\User"),
        "Ada\\:Admin\\\\User"
    );
    assert_eq!(
        encode_index_string(" camelCase  Value "),
        " camelCase  Value "
    );
    assert_eq!(encode_index_string("ž:\\:"), "ž\\:\\\\\\:");
    assert_eq!(encode_index_string(""), "");
    assert_ne!(encode_index_string("ABC"), encode_index_string("abc"));
}

#[test]
fn lexicographic_helpers_move_string_bounds() {
    assert_eq!(prefix_end("abc".to_string()), "abd");
    assert_eq!(prefix_end("ab\u{10ffff}".to_string()), "ac");
    assert_eq!(key_after("abc".to_string()).as_bytes(), b"abc\0");
    assert_eq!(prefix_end_bytes(b"abc"), Some(b"abd".to_vec()));
    assert_eq!(prefix_end_bytes(&[b'a', 0xff]), Some(b"b".to_vec()));
    assert_eq!(prefix_end_bytes(&[0xff]), None);
    assert_eq!(key_after_bytes(b"abc"), b"abc\0".to_vec());
}
