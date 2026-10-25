use super::*;
use crate::fence::record_fence_keys;
use crate::{MAX_REPLACE_MANY_BYTES, MAX_REPLACE_MANY_RECORDS};

async fn replace_accounts(
    engine: &MemoryEngine,
    replacements: Vec<(UniqueAccount, UniqueAccount)>,
) -> Result<Vec<UniqueAccount>, TitoError> {
    let model = engine
        .clone()
        .model::<UniqueAccount>(TitoModelOptions::default());
    engine
        .transaction(|tx| {
            let model = model.clone();
            let replacements = replacements.clone();
            async move {
                model
                    .replace_many(replacements)
                    .timestamps(false)
                    .execute(&tx)
                    .await
            }
        })
        .await
}

async fn unique_owner(engine: &MemoryEngine, email: &str) -> UniqueAccount {
    engine
        .clone()
        .model::<UniqueAccount>(TitoModelOptions::default())
        .find_one_by_unique_index(
            TitoFindOneByIndexPayload {
                index: "verified-account-by-tenant-email".into(),
                values: vec!["tenant".into(), email.into()],
            },
            None,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn batch_replacement_rotates_unique_ownership_and_preserves_current_metadata() {
    let engine = engine();
    let a = save_unique_account(
        &engine,
        unique_account("a", "tenant", "a@example.com", true),
    )
    .await;
    let b = save_unique_account(
        &engine,
        unique_account("b", "tenant", "b@example.com", true),
    )
    .await;
    let c = save_unique_account(
        &engine,
        unique_account("c", "tenant", "c@example.com", true),
    )
    .await;
    let model = engine
        .clone()
        .model::<UniqueAccount>(TitoModelOptions::default());
    let version = model.get_versioned("a", None).await.unwrap().version;
    let next = vec![
        UniqueAccount {
            email: b.email.clone(),
            ..a.clone()
        },
        UniqueAccount {
            email: c.email.clone(),
            ..b.clone()
        },
        UniqueAccount {
            email: a.email.clone(),
            ..c.clone()
        },
    ];
    let saved = replace_accounts(
        &engine,
        vec![
            (a.clone(), next[0].clone()),
            (b.clone(), next[1].clone()),
            (c.clone(), next[2].clone()),
        ],
    )
    .await
    .unwrap();
    assert_eq!(saved, next);
    for id in ["a", "b", "c"] {
        let manifest = engine
            .raw_json(&format!("reverse-index:table:unique-accounts:{id}"))
            .await
            .unwrap()["value"]
            .as_array()
            .unwrap()
            .iter()
            .map(|key| key.as_str().unwrap().to_string())
            .collect::<Vec<_>>();
        assert!(manifest.iter().any(|key| key.starts_with("index:")));
        assert!(manifest.iter().any(|key| key.starts_with("unique-index:")));
        for key in manifest {
            let expected = if key.starts_with("unique-index:") {
                id.as_bytes().to_vec()
            } else {
                Vec::new()
            };
            assert_eq!(engine.raw_bytes(&key).await.unwrap(), expected, "{key}");
        }
    }
    assert_eq!(unique_owner(&engine, &a.email).await.id, "c");
    assert_eq!(unique_owner(&engine, &b.email).await.id, "a");
    assert_eq!(unique_owner(&engine, &c.email).await.id, "b");
    let versions = [
        model.get_versioned("a", None).await.unwrap().version,
        model.get_versioned("b", None).await.unwrap().version,
        model.get_versioned("c", None).await.unwrap().version,
    ];
    assert!(versions
        .iter()
        .all(|value| *value == versions[0] && *value != version));
    engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                let current = model.get_versioned("a", Some(&tx)).await?.version;
                assert_ne!(current, version);
                assert_eq!(current, versions[0]);
                model.rebuild_indexes_for_restore("a", &tx).await
            }
        })
        .await
        .unwrap();
    assert_eq!(unique_owner(&engine, &b.email).await, next[0]);
}

#[tokio::test]
async fn batch_replacement_refuses_outside_owners_before_any_write() {
    let engine = engine();
    let a = save_unique_account(
        &engine,
        unique_account("a", "tenant", "a@example.com", true),
    )
    .await;
    let b = save_unique_account(
        &engine,
        unique_account("b", "tenant", "b@example.com", true),
    )
    .await;
    let outside = save_unique_account(
        &engine,
        unique_account("outside", "tenant", "outside@example.com", true),
    )
    .await;
    engine.start_recording_writes().await;
    let error = replace_accounts(
        &engine,
        vec![
            (
                a.clone(),
                UniqueAccount {
                    email: b.email.clone(),
                    ..a.clone()
                },
            ),
            (
                b.clone(),
                UniqueAccount {
                    email: outside.email.clone(),
                    ..b.clone()
                },
            ),
        ],
    )
    .await
    .unwrap_err();
    assert!(matches!(error, TitoError::UniqueViolation { .. }));
    assert!(engine.take_recorded_writes().await.is_empty());
    assert_eq!(unique_owner(&engine, &a.email).await, a);
    assert_eq!(unique_owner(&engine, &b.email).await, b);
    assert_eq!(unique_owner(&engine, &outside.email).await, outside);
}

#[tokio::test]
async fn batch_replacement_rejects_duplicate_inputs_final_keys_and_stale_sources() {
    let engine = engine();
    let a = save_unique_account(
        &engine,
        unique_account("a", "tenant", "a@example.com", true),
    )
    .await;
    let b = save_unique_account(
        &engine,
        unique_account("b", "tenant", "b@example.com", true),
    )
    .await;
    for replacements in [
        vec![(a.clone(), a.clone()), (a.clone(), a.clone())],
        vec![(a.clone(), b.clone())],
        vec![
            (a.clone(), a.clone()),
            (
                b.clone(),
                UniqueAccount {
                    email: a.email.clone(),
                    ..b.clone()
                },
            ),
        ],
        vec![(
            UniqueAccount {
                display_name: "stale".into(),
                ..a.clone()
            },
            a.clone(),
        )],
    ] {
        engine.start_recording_writes().await;
        assert!(replace_accounts(&engine, replacements).await.is_err());
        assert!(engine.take_recorded_writes().await.is_empty());
    }
    assert_eq!(unique_owner(&engine, &a.email).await, a);
    assert_eq!(unique_owner(&engine, &b.email).await, b);
}

#[tokio::test]
async fn batch_replacement_requires_sources_and_bounds_prepared_work() {
    let engine = engine();
    let a = save_unique_account(
        &engine,
        unique_account("a", "tenant", "a@example.com", true),
    )
    .await;
    for replacements in [
        vec![],
        (0..=MAX_REPLACE_MANY_RECORDS)
            .map(|position| {
                let value = unique_account(
                    &format!("{position}"),
                    "tenant",
                    "unused@example.com",
                    false,
                );
                (value.clone(), value)
            })
            .collect(),
        vec![(
            unique_account("missing", "tenant", "missing@example.com", true),
            unique_account("missing", "tenant", "missing@example.com", true),
        )],
        vec![(
            a.clone(),
            UniqueAccount {
                display_name: "x".repeat(MAX_REPLACE_MANY_BYTES),
                ..a.clone()
            },
        )],
    ] {
        engine.start_recording_writes().await;
        assert!(replace_accounts(&engine, replacements).await.is_err());
        assert!(engine.take_recorded_writes().await.is_empty());
    }
    assert_eq!(unique_owner(&engine, &a.email).await, a);
}

#[tokio::test]
async fn batch_replacement_stages_source_fences_and_rolls_back_with_its_transaction() {
    let engine = engine();
    let a = save_unique_account(
        &engine,
        unique_account("a", "tenant", "a@example.com", true),
    )
    .await;
    let b = save_unique_account(
        &engine,
        unique_account("b", "tenant", "b@example.com", true),
    )
    .await;
    let model = engine
        .clone()
        .model::<UniqueAccount>(TitoModelOptions::default());
    engine.start_recording_writes().await;
    let error = engine
        .transaction(|tx| {
            let model = model.clone();
            let a = a.clone();
            let b = b.clone();
            async move {
                model
                    .replace_many(vec![
                        (
                            a.clone(),
                            UniqueAccount {
                                email: b.email.clone(),
                                ..a
                            },
                        ),
                        (
                            b.clone(),
                            UniqueAccount {
                                email: "a@example.com".into(),
                                ..b
                            },
                        ),
                    ])
                    .timestamps(false)
                    .execute(&tx)
                    .await?;
                Err::<(), _>(TitoError::UpdateFailed("later owner work failed".into()))
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(error, TitoError::UpdateFailed(_)));
    let writes = engine.take_recorded_writes().await;
    assert!(writes.contains(&RecordedWrite::Put("table:unique-accounts:a".into())));
    assert!(writes.contains(&RecordedWrite::Put("table:unique-accounts:b".into())));
    assert_eq!(unique_owner(&engine, &a.email).await, a);
    assert_eq!(unique_owner(&engine, &b.email).await, b);
}

#[tokio::test]
async fn batch_replacement_deletes_the_seventeen_fence_keys_of_each_replaced_record() {
    let engine = engine();
    let a = save_unique_account(
        &engine,
        unique_account("a", "tenant", "a@example.com", true),
    )
    .await;
    let b = save_unique_account(
        &engine,
        unique_account("b", "tenant", "b@example.com", true),
    )
    .await;
    let model = engine
        .clone()
        .model::<UniqueAccount>(TitoModelOptions::default());
    let reader = engine.begin_transaction().await.unwrap();
    model.get("a").execute(Some(&reader)).await.unwrap();
    write_scratch(&reader).await;

    engine.start_recording_writes().await;
    replace_accounts(
        &engine,
        vec![
            (
                a.clone(),
                UniqueAccount {
                    email: b.email.clone(),
                    ..a.clone()
                },
            ),
            (
                b.clone(),
                UniqueAccount {
                    email: a.email.clone(),
                    ..b.clone()
                },
            ),
        ],
    )
    .await
    .unwrap();
    let mut expected = record_fence_keys("unique-accounts", "a");
    expected.extend(record_fence_keys("unique-accounts", "b"));
    assert_eq!(expected.len(), 34);
    assert_eq!(
        sorted(fence_writes(&engine.take_recorded_writes().await)),
        sorted(expected)
    );
    assert!(matches!(
        reader.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert_eq!(unique_owner(&engine, &a.email).await.id, "b");
    assert_eq!(unique_owner(&engine, &b.email).await.id, "a");
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}
