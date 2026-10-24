use super::*;
use crate::backend::tikv::MAX_TRANSACTION_RETRIES;
use crate::fence::{
    fence_stripe_key, link_fence_key, shared_fence_stripe, source_fence_stripe, FenceStripes,
    LINK_FENCE_STRIPES,
};
use crate::reference::deleting_marker_key;
use crate::TitoModel;
use std::sync::atomic::{AtomicUsize, Ordering};

fn tags(engine: &MemoryEngine) -> TitoModel<MemoryEngine, Tag> {
    engine.clone().model::<Tag>(TitoModelOptions::default())
}

async fn staged_shared_checks(
    engine: &MemoryEngine,
    id: &str,
    checks: usize,
) -> Vec<MemoryTransaction> {
    let model = shops(engine);
    let mut transactions = Vec::new();
    for _ in 0..checks {
        let tx = engine.begin_transaction().await.unwrap();
        model.assert_current_shared(id, &tx).await.unwrap();
        transactions.push(tx);
    }
    transactions
}

#[tokio::test]
async fn assert_current_requires_an_existing_record_and_preserves_exact_bytes() {
    let engine = engine();
    let model = engine.clone().model::<Tag>(TitoModelOptions::default());
    save_tag(&engine, tag("t1", "database")).await;
    let before = engine.raw_bytes("table:tags:t1").await.unwrap();

    engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.assert_current("t1", &tx).await }
        })
        .await
        .unwrap();

    assert_eq!(engine.raw_bytes("table:tags:t1").await.unwrap(), before);
    let missing = engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.assert_current("missing", &tx).await }
        })
        .await
        .unwrap_err();
    assert!(matches!(missing, TitoError::NotFound(_)));
}

#[tokio::test]
async fn assert_current_observes_a_write_already_staged_in_the_transaction() {
    let engine = engine();
    let model = engine.clone().model::<Tag>(TitoModelOptions::default());

    engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .set(tag("t1", "database"))
                    .timestamps(false)
                    .execute(&tx)
                    .await?;
                model.assert_current("t1", &tx).await
            }
        })
        .await
        .unwrap();

    assert_eq!(
        model.get("t1").execute(None).await.unwrap().name,
        "database"
    );
}

#[tokio::test]
async fn assert_current_shared_on_a_one_stripe_record_rewrites_it_like_assert_current() {
    let engine = engine();
    let model = tags(&engine);
    save_tag(&engine, tag("t1", "database")).await;
    let before = engine.raw_bytes("table:tags:t1").await.unwrap();
    let version = model.get_versioned("t1", None).await.unwrap().version;

    let tx = engine.begin_transaction().await.unwrap();
    assert!(matches!(
        model.assert_current_shared("missing", &tx).await,
        Err(TitoError::NotFound(_))
    ));
    tx.rollback().await.unwrap();

    engine.start_recording_writes().await;
    let tx = engine.begin_transaction().await.unwrap();
    model.assert_current_shared("t1", &tx).await.unwrap();
    tx.commit().await.unwrap();

    assert_eq!(
        engine.take_recorded_writes().await,
        vec![RecordedWrite::Put("table:tags:t1".to_string())]
    );
    assert_eq!(engine.raw_bytes("table:tags:t1").await.unwrap(), before);
    assert_eq!(
        model.get_versioned("t1", None).await.unwrap().version,
        version
    );
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn one_stripe_shared_checks_collide_with_each_other_with_assert_current_and_with_saves() {
    let engine = engine();
    let model = tags(&engine);
    save_tag(&engine, tag("t1", "database")).await;

    let first = engine.begin_transaction().await.unwrap();
    let second = engine.begin_transaction().await.unwrap();
    model.assert_current_shared("t1", &first).await.unwrap();
    model.assert_current_shared("t1", &second).await.unwrap();
    first.commit().await.unwrap();
    assert!(matches!(
        second.commit().await,
        Err(TitoError::Retryable(_))
    ));

    let shared = engine.begin_transaction().await.unwrap();
    let exclusive = engine.begin_transaction().await.unwrap();
    model.assert_current_shared("t1", &shared).await.unwrap();
    model.assert_current("t1", &exclusive).await.unwrap();
    exclusive.commit().await.unwrap();
    assert!(matches!(
        shared.commit().await,
        Err(TitoError::Retryable(_))
    ));

    let checked = engine.begin_transaction().await.unwrap();
    model.assert_current_shared("t1", &checked).await.unwrap();
    save_tag(&engine, tag("t1", "renamed by a committed writer")).await;
    assert!(matches!(
        checked.commit().await,
        Err(TitoError::Retryable(_))
    ));

    let check_first = engine.begin_transaction().await.unwrap();
    let late_writer = engine.begin_transaction().await.unwrap();
    model
        .assert_current_shared("t1", &check_first)
        .await
        .unwrap();
    model
        .set(tag("t1", "renamed by a late writer"))
        .timestamps(false)
        .execute(&late_writer)
        .await
        .unwrap();
    check_first.commit().await.unwrap();
    assert!(matches!(
        late_writer.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert_eq!(
        model.get("t1").execute(None).await.unwrap().name,
        "renamed by a committed writer"
    );
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn assert_current_shared_on_a_sixteen_stripe_record_writes_the_stripe_of_its_transactions_pick(
) {
    let engine = engine();
    let model = shops(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();
    let before = engine.raw_bytes("table:shops:s1").await.unwrap();
    let version = model.get_versioned("s1", None).await.unwrap().version;

    let tx = engine.begin_transaction().await.unwrap();
    assert!(matches!(
        model.assert_current_shared("missing", &tx).await,
        Err(TitoError::NotFound(_))
    ));
    tx.rollback().await.unwrap();

    for _ in 0..3 {
        let tx = engine.begin_transaction().await.unwrap();
        let stripe = shared_fence_stripe(tx.shared_fence_pick(), 16);
        let key = fence_stripe_key("shops", "s1", stripe);
        engine.start_recording_writes().await;
        model.assert_current_shared("s1", &tx).await.unwrap();
        assert_eq!(
            engine.take_recorded_writes().await,
            vec![RecordedWrite::Delete(key)]
        );
        tx.commit().await.unwrap();
    }

    assert_eq!(engine.raw_bytes("table:shops:s1").await.unwrap(), before);
    assert_eq!(
        model.get_versioned("s1", None).await.unwrap().version,
        version
    );
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn sixteen_stripe_shared_checks_on_one_record_take_turns_only_when_they_pick_one_stripe() {
    let engine = engine();
    let model = shops(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();

    assert_shared_checks_take_turns_only_within_one_stripe(&engine, &model, "s1").await;

    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn every_shared_check_of_one_transaction_writes_the_stripe_of_its_pick() {
    let engine = engine();
    let model = shops(&engine);
    let pairs = stands::<2>(&engine);
    let ids = (0..20).map(|n| format!("s{n}")).collect::<Vec<_>>();
    for id in &ids {
        save_shop(&engine, shop(id)).await.unwrap();
    }
    save_stand(&engine, stand::<2>("k1")).await.unwrap();

    let tx = engine.begin_transaction().await.unwrap();
    let pick = tx.shared_fence_pick();
    engine.start_recording_writes().await;
    for id in &ids {
        model.assert_current_shared(id, &tx).await.unwrap();
        model.assert_current_shared(id, &tx).await.unwrap();
    }
    pairs.assert_current_shared("k1", &tx).await.unwrap();

    let shop_stripe = shared_fence_stripe(pick, 16);
    let mut expected = Vec::new();
    for id in &ids {
        let key = fence_stripe_key("shops", id, shop_stripe);
        expected.push(RecordedWrite::Delete(key.clone()));
        expected.push(RecordedWrite::Delete(key));
    }
    let pair_key = fence_stripe_key("stands-2", "k1", shared_fence_stripe(pick, 2));
    expected.push(RecordedWrite::Delete(pair_key));
    assert_eq!(engine.take_recorded_writes().await, expected);
    tx.commit().await.unwrap();
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn transactions_in_different_stripes_check_twenty_records_side_by_side_and_one_stripe_takes_turns(
) {
    let engine = engine();
    let model = shops(&engine);
    let ids = (0..20).map(|n| format!("s{n}")).collect::<Vec<_>>();
    for id in &ids {
        save_shop(&engine, shop(id)).await.unwrap();
    }

    let first = engine.begin_transaction().await.unwrap();
    let stripe = shared_fence_stripe(first.shared_fence_pick(), 16);
    let other = transaction_in_stripe(&engine, 16, |picked| picked != stripe).await;
    let same = transaction_in_stripe(&engine, 16, |picked| picked == stripe).await;
    for id in &ids {
        for tx in [&first, &other, &same] {
            model.assert_current_shared(id, tx).await.unwrap();
        }
    }
    first.commit().await.unwrap();
    other.commit().await.unwrap();
    assert!(matches!(same.commit().await, Err(TitoError::Retryable(_))));
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn every_attempt_of_a_retried_transaction_checks_in_the_stripe_of_its_own_pick() {
    let engine = engine();
    let model = shops(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();
    let attempts = Arc::new(tokio::sync::Mutex::new(Vec::<(u64, u16)>::new()));

    engine
        .transaction(|tx| {
            let engine = engine.clone();
            let model = model.clone();
            let attempts = attempts.clone();
            async move {
                engine.start_recording_writes().await;
                model.assert_current_shared("s1", &tx).await?;
                let writes = engine.take_recorded_writes().await;
                let stripe = picked_stripe(&writes, "shops", "s1", 16);
                let first = {
                    let mut attempts = attempts.lock().await;
                    attempts.push((tx.shared_fence_pick(), stripe));
                    attempts.len() == 1
                };
                if first {
                    save_shop(
                        &engine,
                        Shop {
                            name: "Renamed".to_string(),
                            ..shop("s1")
                        },
                    )
                    .await?;
                }
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();

    let attempts = attempts.lock().await;
    assert_eq!(attempts.len(), 2);
    for (pick, stripe) in attempts.iter() {
        assert_eq!(*stripe, shared_fence_stripe(*pick, 16));
    }
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn every_save_remove_and_erase_of_a_sixteen_stripe_record_collides_with_its_shared_checks() {
    let engine = engine();
    let model = shops(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();

    let checks = staged_shared_checks(&engine, "s1", 4).await;
    save_shop(
        &engine,
        Shop {
            deleting: true,
            ..shop("s1")
        },
    )
    .await
    .unwrap();
    for tx in checks {
        assert!(matches!(tx.commit().await, Err(TitoError::Retryable(_))));
    }

    let renaming = engine.begin_transaction().await.unwrap();
    let (check, _) = staged_shared_check(&engine, &model, "s1").await;
    model
        .set(Shop {
            name: "Renamed".to_string(),
            ..shop("s1")
        })
        .timestamps(false)
        .execute(&renaming)
        .await
        .unwrap();
    check.commit().await.unwrap();
    assert!(matches!(
        renaming.commit().await,
        Err(TitoError::Retryable(_))
    ));

    let checks = staged_shared_checks(&engine, "s1", 4).await;
    let plain = engine.begin_transaction().await.unwrap();
    model
        .set(Shop {
            deleting: true,
            name: "Renamed while deleting".to_string(),
            ..shop("s1")
        })
        .timestamps(false)
        .execute(&plain)
        .await
        .unwrap();
    plain.commit().await.unwrap();
    for tx in checks {
        assert!(matches!(tx.commit().await, Err(TitoError::Retryable(_))));
    }

    let checks = staged_shared_checks(&engine, "s1", 4).await;
    assert!(engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove("s1", &tx).await }
        })
        .await
        .unwrap());
    for tx in checks {
        assert!(matches!(tx.commit().await, Err(TitoError::Retryable(_))));
    }

    save_shop(&engine, shop("s2")).await.unwrap();
    let checks = staged_shared_checks(&engine, "s2", 4).await;
    assert!(engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.erase("s2".to_string(), &tx).await }
        })
        .await
        .unwrap());
    for tx in checks {
        assert!(matches!(tx.commit().await, Err(TitoError::Retryable(_))));
    }
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_sixteen_stripe_model_writes_all_sixteen_stripe_keys_on_every_save_remove_erase_and_repair(
) {
    let engine = engine();
    let model = shops(&engine);
    let every = stripe_keys("shops", "s1", 16);
    let marker = deleting_marker_key("shops", "s1");
    let deleting = Shop {
        deleting: true,
        ..shop("s1")
    };

    let writes = save_writes(&engine, &model, shop("s1")).await;
    assert_eq!(fence_writes(&writes), every);
    let writes = repair_writes(&engine, &model, "s1").await;
    assert_eq!(fence_writes(&writes), every);

    let writes = save_writes(&engine, &model, deleting).await;
    assert!(writes.contains(&RecordedWrite::Put(marker.clone())));
    assert_eq!(fence_writes(&writes), every);
    let writes = repair_writes(&engine, &model, "s1").await;
    assert!(writes.contains(&RecordedWrite::Put(marker.clone())));
    assert_eq!(fence_writes(&writes), every);

    let writes = save_writes(&engine, &model, shop("s1")).await;
    assert!(writes.contains(&RecordedWrite::Delete(marker.clone())));
    assert_eq!(fence_writes(&writes), every);

    let writes = remove_writes(&engine, &model, "s1").await;
    assert!(writes.contains(&RecordedWrite::Delete(marker)));
    assert_eq!(fence_writes(&writes), every);

    save_shop(&engine, shop("s1")).await.unwrap();
    let writes = erase_writes(&engine, &model, "s1").await;
    assert_eq!(fence_writes(&writes), every);

    engine.start_recording_writes().await;
    save_tag(&engine, tag("t1", "database")).await;
    assert!(fence_writes(&engine.take_recorded_writes().await).is_empty());
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_two_stripe_model_writes_its_two_stripe_keys_and_all_sixteen_link_stripes_with_its_deleting_marker(
) {
    let engine = engine();
    let model = stands::<2>(&engine);
    let two = stripe_keys("stands-2", "k1", 2);
    let sixteen = stripe_keys("stands-2", "k1", LINK_FENCE_STRIPES);
    let marker = deleting_marker_key("stands-2", "k1");
    let deleting = Stand::<2> {
        deleting: true,
        ..stand("k1")
    };
    let renamed = Stand::<2> {
        name: "Renamed".to_string(),
        ..deleting.clone()
    };

    let writes = save_writes(&engine, &model, stand::<2>("k1")).await;
    assert_eq!(fence_writes(&writes), two);
    let writes = repair_writes(&engine, &model, "k1").await;
    assert_eq!(fence_writes(&writes), two);

    let writes = save_writes(&engine, &model, deleting.clone()).await;
    assert!(writes.contains(&RecordedWrite::Put(marker.clone())));
    assert_eq!(fence_writes(&writes), sixteen);

    let writes = save_writes(&engine, &model, renamed).await;
    assert!(deleting_writes(&writes).is_empty());
    assert_eq!(fence_writes(&writes), two);

    let writes = repair_writes(&engine, &model, "k1").await;
    assert!(writes.contains(&RecordedWrite::Put(marker.clone())));
    assert_eq!(fence_writes(&writes), sixteen);

    let writes = erase_writes(&engine, &model, "k1").await;
    assert!(writes.contains(&RecordedWrite::Delete(marker.clone())));
    assert_eq!(fence_writes(&writes), sixteen);

    save_stand(&engine, deleting).await.unwrap();
    let writes = save_writes(&engine, &model, stand::<2>("k1")).await;
    assert!(writes.contains(&RecordedWrite::Delete(marker.clone())));
    assert_eq!(fence_writes(&writes), sixteen);

    let writes = remove_writes(&engine, &model, "k1").await;
    assert!(writes.contains(&RecordedWrite::Delete(marker)));
    assert_eq!(fence_writes(&writes), sixteen);

    save_stand(&engine, stand::<2>("k1")).await.unwrap();
    let writes = erase_writes(&engine, &model, "k1").await;
    assert!(deleting_writes(&writes).is_empty());
    assert_eq!(fence_writes(&writes), two);
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn two_stripe_shared_checks_pick_one_of_its_two_stripes_and_collide_with_every_save() {
    let engine = engine();
    let model = stands::<2>(&engine);
    save_stand(&engine, stand::<2>("k1")).await.unwrap();

    assert_shared_checks_take_turns_only_within_one_stripe(&engine, &model, "k1").await;

    let mut checks = Vec::new();
    for _ in 0..4 {
        checks.push(staged_shared_check(&engine, &model, "k1").await);
    }
    let renamed = Stand::<2> {
        name: "Renamed".to_string(),
        ..stand("k1")
    };
    save_stand(&engine, renamed).await.unwrap();
    for (tx, _) in checks {
        assert!(matches!(tx.commit().await, Err(TitoError::Retryable(_))));
    }
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn assert_current_collides_with_saves_and_exclusive_fences_but_not_with_striped_shared_checks(
) {
    let engine = engine();
    let model = shops(&engine);
    save_shop(&engine, shop("s1")).await.unwrap();

    let fenced = engine.begin_transaction().await.unwrap();
    model.assert_current("s1", &fenced).await.unwrap();
    save_shop(
        &engine,
        Shop {
            name: "Renamed by a committed writer".to_string(),
            ..shop("s1")
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        fenced.commit().await,
        Err(TitoError::Retryable(_))
    ));

    let fence_first = engine.begin_transaction().await.unwrap();
    let late_writer = engine.begin_transaction().await.unwrap();
    model.assert_current("s1", &fence_first).await.unwrap();
    model
        .set(Shop {
            name: "Renamed by a late writer".to_string(),
            ..shop("s1")
        })
        .timestamps(false)
        .execute(&late_writer)
        .await
        .unwrap();
    fence_first.commit().await.unwrap();
    assert!(matches!(
        late_writer.commit().await,
        Err(TitoError::Retryable(_))
    ));

    let first = engine.begin_transaction().await.unwrap();
    let second = engine.begin_transaction().await.unwrap();
    model.assert_current("s1", &first).await.unwrap();
    model.assert_current("s1", &second).await.unwrap();
    first.commit().await.unwrap();
    assert!(matches!(
        second.commit().await,
        Err(TitoError::Retryable(_))
    ));

    let version = model.get_versioned("s1", None).await.unwrap().version;
    let versioned = engine.begin_transaction().await.unwrap();
    assert!(model
        .assert_version("s1", version, &versioned)
        .await
        .unwrap());
    save_shop(&engine, shop("s1")).await.unwrap();
    assert!(matches!(
        versioned.commit().await,
        Err(TitoError::Retryable(_))
    ));

    let exclusive = engine.begin_transaction().await.unwrap();
    let shared = engine.begin_transaction().await.unwrap();
    model.assert_current("s1", &exclusive).await.unwrap();
    model.assert_current_shared("s1", &shared).await.unwrap();
    exclusive.commit().await.unwrap();
    shared.commit().await.unwrap();
}

#[tokio::test]
async fn a_model_with_zero_stripes_is_refused_before_any_read_or_write() {
    let engine = engine();
    let model = stands::<0>(&engine);
    engine.start_recording_reads().await;
    engine.start_recording_writes().await;

    let tx = engine.begin_transaction().await.unwrap();
    let refusals = vec![
        model
            .set(stand::<0>("z1"))
            .timestamps(false)
            .execute(&tx)
            .await
            .map(|_| ()),
        model
            .replace_many(vec![(stand::<0>("z1"), stand::<0>("z1"))])
            .timestamps(false)
            .execute(&tx)
            .await
            .map(|_| ()),
        model.assert_current_shared("z1", &tx).await,
        model.remove("z1", &tx).await.map(|_| ()),
        model.erase("z1".to_string(), &tx).await.map(|_| ()),
        model
            .rebuild_indexes_for_restore("z1", &tx)
            .await
            .map(|_| ()),
    ];
    tx.rollback().await.unwrap();

    for refusal in refusals {
        assert!(matches!(
            refusal,
            Err(TitoError::Configuration(message)) if message.contains("'stands-0'")
        ));
    }
    assert!(engine.take_recorded_reads().await.is_empty());
    assert!(engine.take_recorded_writes().await.is_empty());
    assert!(engine.keys_with_prefix("").await.is_empty());
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

#[test]
fn fence_stripe_keys_escape_their_segments_and_link_and_shared_stripes_are_fixed() {
    assert_eq!(
        fence_stripe_key("shops", "a:b\\c", 7),
        "fence:shops:a\\:b\\\\c:7"
    );
    let keys_below = |stripes: u16| {
        (0..stripes)
            .map(|stripe| format!("fence:shops:s1:{stripe}"))
            .collect::<Vec<_>>()
    };
    assert!(FenceStripes::Record.record_keys("shops", "s1").is_empty());
    assert_eq!(
        FenceStripes::Striped(2).record_keys("shops", "s1"),
        keys_below(2)
    );
    assert_eq!(
        FenceStripes::Striped(16).record_keys("shops", "s1"),
        keys_below(16)
    );
    for stripes in [
        FenceStripes::Record,
        FenceStripes::Striped(2),
        FenceStripes::Striped(16),
    ] {
        assert_eq!(
            stripes.marker_keys("shops", "s1"),
            keys_below(LINK_FENCE_STRIPES)
        );
    }
    assert_eq!(
        FenceStripes::Striped(32).marker_keys("shops", "s1"),
        keys_below(32)
    );
    assert_eq!(
        link_fence_key("pictures", "a:b", "galleries", "g1"),
        "fence:pictures:a\\:b:9"
    );
    assert_eq!(
        link_fence_key("shops", "s1", "galleries", "g1"),
        "fence:shops:s1:9"
    );
    assert_eq!(source_fence_stripe("galleries", "g1", 16), 9);
    assert_eq!(
        source_fence_stripe("orders", "4f6c1a52-7e0b-4f43-9d0c-2b4c1a9e8f10", 16),
        13
    );
    assert_eq!(source_fence_stripe("a:b", "c\\d", 16), 13);
    assert_eq!(shared_fence_stripe(0, 16), 0);
    assert_eq!(shared_fence_stripe(37, 16), 5);
    assert_eq!(shared_fence_stripe(u64::MAX, 16), 15);
    assert_eq!(shared_fence_stripe(37, 2), 1);
    assert_eq!(shared_fence_stripe(u64::MAX, 3), 0);
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
