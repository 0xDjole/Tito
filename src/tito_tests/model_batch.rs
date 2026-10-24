use super::*;
use crate::fence::{fence_stripe_key, source_fence_stripe, LINK_FENCE_STRIPES};
use crate::reference::deleting_marker_key;
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
                assert!(!model.assert_version("a", version, &tx).await?);
                assert!(model.assert_version("a", versions[0], &tx).await?);
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

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct BatchNode {
    id: String,
    parent_id: Option<String>,
    shop_id: Option<String>,
    deleting: bool,
    created_at: i64,
    updated_at: i64,
}

impl TitoModelTrait for BatchNode {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }
    fn references(&self) -> Vec<TitoReference> {
        self.parent_id
            .iter()
            .map(|id| TitoReference::new("batch-nodes", id, "parent_id"))
            .chain(
                self.shop_id
                    .iter()
                    .map(|id| TitoReference::new("shops", id, "shop_id")),
            )
            .collect()
    }
    fn is_deleting(&self) -> bool {
        self.deleting
    }
    fn table() -> String {
        "batch-nodes".into()
    }
    fn id(&self) -> String {
        self.id.clone()
    }
}

fn node(id: &str, parent: Option<&str>) -> BatchNode {
    BatchNode {
        id: id.into(),
        parent_id: parent.map(str::to_owned),
        shop_id: None,
        deleting: false,
        created_at: -4,
        updated_at: -4,
    }
}

async fn save_node(engine: &MemoryEngine, value: BatchNode) -> BatchNode {
    let model = engine
        .clone()
        .model::<BatchNode>(TitoModelOptions::default());
    engine
        .transaction(|tx| {
            let model = model.clone();
            let value = value.clone();
            async move { model.set(value).timestamps(false).execute(&tx).await }
        })
        .await
        .unwrap()
}

#[tokio::test]
async fn batch_replacement_checks_final_deleting_targets_and_retains_existing_links() {
    let engine = engine();
    let a = save_node(&engine, node("a", None)).await;
    let b = save_node(&engine, node("b", None)).await;
    let model = engine
        .clone()
        .model::<BatchNode>(TitoModelOptions::default());
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
                            BatchNode {
                                parent_id: Some(b.id.clone()),
                                ..a
                            },
                        ),
                        (
                            b.clone(),
                            BatchNode {
                                deleting: true,
                                ..b
                            },
                        ),
                    ])
                    .timestamps(false)
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(error, TitoError::ReferenceDeleting { .. }));
    assert!(engine.take_recorded_writes().await.is_empty());
    let a = save_node(
        &engine,
        BatchNode {
            parent_id: Some(b.id.clone()),
            ..a
        },
    )
    .await;
    let saved = engine
        .transaction(|tx| {
            let model = model.clone();
            let a = a.clone();
            let b = b.clone();
            async move {
                model
                    .replace_many(vec![
                        (a.clone(), a),
                        (
                            b.clone(),
                            BatchNode {
                                deleting: true,
                                ..b
                            },
                        ),
                    ])
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap();
    assert_eq!(saved[0].created_at, -4);
    assert!(saved.iter().all(|value| value.updated_at > 0));
    engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                assert_eq!(
                    model.referenced_by("b", &tx).await?,
                    vec![TitoIncomingReference::new("batch-nodes", "a", "parent_id")]
                );
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();
    let c = save_node(&engine, node("c", None)).await;
    for parent in ["missing", "b"] {
        let result = engine
            .transaction(|tx| {
                let model = model.clone();
                let c = c.clone();
                async move {
                    model
                        .replace_many(vec![(
                            c.clone(),
                            BatchNode {
                                parent_id: Some(parent.into()),
                                ..c
                            },
                        )])
                        .timestamps(false)
                        .execute(&tx)
                        .await
                }
            })
            .await;
        assert!(matches!(
            result,
            Err(TitoError::ReferenceMissing { .. } | TitoError::ReferenceDeleting { .. })
        ));
    }
    engine
        .transaction(|tx| {
            let model = model.clone();
            let a = saved[0].clone();
            let b = saved[1].clone();
            async move {
                model
                    .replace_many(vec![
                        (
                            a.clone(),
                            BatchNode {
                                parent_id: None,
                                ..a
                            },
                        ),
                        (
                            b.clone(),
                            BatchNode {
                                deleting: false,
                                ..b
                            },
                        ),
                    ])
                    .timestamps(false)
                    .execute(&tx)
                    .await?;
                assert!(model.referenced_by("b", &tx).await?.is_empty());
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();
}

fn sorted(mut keys: Vec<String>) -> Vec<String> {
    keys.sort();
    keys
}

async fn replace_nodes(
    engine: &MemoryEngine,
    replacements: Vec<(BatchNode, BatchNode)>,
) -> Result<Vec<BatchNode>, TitoError> {
    let model = engine
        .clone()
        .model::<BatchNode>(TitoModelOptions::default());
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

#[tokio::test]
async fn batch_replacement_fences_each_new_target_by_one_link_stripe_and_each_replaced_record_by_its_stripes(
) {
    let engine = engine();
    for id in ["a", "b", "c", "d"] {
        save_node(&engine, node(id, None)).await;
    }
    save_shop(&engine, shop("s1")).await.unwrap();
    let model = engine
        .clone()
        .model::<BatchNode>(TitoModelOptions::default());
    let stripe = source_fence_stripe("batch-nodes", "a", LINK_FENCE_STRIPES);

    engine.start_recording_writes().await;
    replace_nodes(&engine, vec![(node("a", None), node("a", Some("c")))])
        .await
        .unwrap();
    let writes = engine.take_recorded_writes().await;
    assert_eq!(
        fence_writes(&writes),
        vec![fence_stripe_key("batch-nodes", "c", stripe)]
    );
    assert!(deleting_writes(&writes).is_empty());

    engine.start_recording_writes().await;
    replace_nodes(
        &engine,
        vec![(
            node("a", Some("c")),
            BatchNode {
                shop_id: Some("s1".to_string()),
                ..node("a", Some("c"))
            },
        )],
    )
    .await
    .unwrap();
    let writes = engine.take_recorded_writes().await;
    assert_eq!(
        fence_writes(&writes),
        vec![fence_stripe_key("shops", "s1", stripe)]
    );
    assert!(deleting_writes(&writes).is_empty());

    let pointing = engine.begin_transaction().await.unwrap();
    model
        .replace_many(vec![(node("b", None), node("b", Some("d")))])
        .timestamps(false)
        .execute(&pointing)
        .await
        .unwrap();
    assert!(engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove("d", &tx).await }
        })
        .await
        .unwrap());
    assert!(matches!(
        pointing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert!(matches!(
        replace_nodes(&engine, vec![(node("b", None), node("b", Some("d")))]).await,
        Err(TitoError::ReferenceMissing { .. })
    ));

    let marker = deleting_marker_key("batch-nodes", "b");
    let links = sorted(stripe_keys("batch-nodes", "b", LINK_FENCE_STRIPES));
    let deleting = BatchNode {
        deleting: true,
        ..node("b", None)
    };
    engine.start_recording_writes().await;
    replace_nodes(&engine, vec![(node("b", None), deleting.clone())])
        .await
        .unwrap();
    let writes = engine.take_recorded_writes().await;
    assert_eq!(
        deleting_writes(&writes),
        vec![RecordedWrite::Put(marker.clone())]
    );
    assert_eq!(sorted(fence_writes(&writes)), links);

    engine.start_recording_writes().await;
    replace_nodes(&engine, vec![(deleting, node("b", None))])
        .await
        .unwrap();
    let writes = engine.take_recorded_writes().await;
    assert_eq!(
        deleting_writes(&writes),
        vec![RecordedWrite::Delete(marker)]
    );
    assert_eq!(sorted(fence_writes(&writes)), links);

    let shops = shops(&engine);
    let check = engine.begin_transaction().await.unwrap();
    shops.assert_current_shared("s1", &check).await.unwrap();
    engine.start_recording_writes().await;
    engine
        .transaction(|tx| {
            let shops = shops.clone();
            async move {
                shops
                    .replace_many(vec![(
                        shop("s1"),
                        Shop {
                            name: "Renamed".to_string(),
                            ..shop("s1")
                        },
                    )])
                    .timestamps(false)
                    .execute(&tx)
                    .await
            }
        })
        .await
        .unwrap();
    assert_eq!(
        sorted(fence_writes(&engine.take_recorded_writes().await)),
        sorted(stripe_keys("shops", "s1", 16))
    );
    assert!(matches!(check.commit().await, Err(TitoError::Retryable(_))));

    let stands = stands::<2>(&engine);
    save_stand(&engine, stand::<2>("k1")).await.unwrap();
    let renamed = Stand::<2> {
        name: "Renamed".to_string(),
        ..stand("k1")
    };
    let marked = Stand::<2> {
        deleting: true,
        ..renamed.clone()
    };
    for (expected, next, stripes) in [
        (stand::<2>("k1"), renamed.clone(), 2),
        (renamed, marked, LINK_FENCE_STRIPES),
    ] {
        engine.start_recording_writes().await;
        engine
            .transaction(|tx| {
                let stands = stands.clone();
                let expected = expected.clone();
                let next = next.clone();
                async move {
                    stands
                        .replace_many(vec![(expected, next)])
                        .timestamps(false)
                        .execute(&tx)
                        .await
                }
            })
            .await
            .unwrap();
        assert_eq!(
            sorted(fence_writes(&engine.take_recorded_writes().await)),
            sorted(stripe_keys("stands-2", "k1", stripes))
        );
    }
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}
