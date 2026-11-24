use super::*;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Member {
    id: String,
    tenant_id: String,
    email: String,
    handle: String,
    display_name: String,
}

impl TitoModelTrait for Member {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "member-by-tenant".to_string(),
            fields: vec![TitoIndexField {
                name: "tenant_id".to_string(),
                r#type: TitoIndexFieldType::String,
            }],
        }]
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            TitoIndexConfig {
                condition: true,
                name: "member-by-email".to_string(),
                fields: vec![
                    TitoIndexField {
                        name: "tenant_id".to_string(),
                        r#type: TitoIndexFieldType::String,
                    },
                    TitoIndexField {
                        name: "email".to_string(),
                        r#type: TitoIndexFieldType::String,
                    },
                ],
            },
            TitoIndexConfig {
                condition: true,
                name: "member-by-handle".to_string(),
                fields: vec![
                    TitoIndexField {
                        name: "tenant_id".to_string(),
                        r#type: TitoIndexFieldType::String,
                    },
                    TitoIndexField {
                        name: "handle".to_string(),
                        r#type: TitoIndexFieldType::String,
                    },
                ],
            },
        ]
    }

    fn table() -> String {
        "members".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Stamp {
    id: String,
    created_at: i64,
    updated_at: i64,
}

impl TitoModelTrait for Stamp {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "stamp-by-created".to_string(),
            fields: vec![TitoIndexField {
                name: "created_at".to_string(),
                r#type: TitoIndexFieldType::Number,
            }],
        }]
    }

    fn table() -> String {
        "stamps".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn member(id: &str, email: &str, handle: &str) -> Member {
    Member {
        id: id.to_string(),
        tenant_id: "t".to_string(),
        email: email.to_string(),
        handle: handle.to_string(),
        display_name: format!("Member {id}"),
    }
}

fn members(engine: &MemoryEngine) -> crate::TitoModel<MemoryEngine, Member> {
    engine.clone().model::<Member>(TitoModelOptions::default())
}

fn email_key(email: &str) -> String {
    format!("unique-index:members:member-by-email:tenant_id:t:email:{email}")
}

fn handle_key(handle: &str) -> String {
    format!("unique-index:members:member-by-handle:tenant_id:t:handle:{handle}")
}

fn stamp(id: &str, created_at: i64) -> Stamp {
    Stamp {
        id: id.to_string(),
        created_at,
        updated_at: 0,
    }
}

fn created_key(created_at: i64) -> String {
    format!(
        "unique-index:stamps:stamp-by-created:created_at:{}",
        crate::encode_index_integer(created_at)
    )
}

fn violation(model: &str, index: &str) -> TitoError {
    TitoError::UniqueViolation {
        model: model.to_string(),
        index: index.to_string(),
    }
}

async fn save_member(engine: &MemoryEngine, value: Member) -> Result<Member, TitoError> {
    let model = members(engine);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let value = value.clone();
            async move { model.set(value).timestamps(false).execute(&tx).await }
        })
        .await
}

async fn member_by(engine: &MemoryEngine, index: &str, value: &str) -> Member {
    members(engine)
        .find_one_by_unique_index_for_share(
            TitoFindOneByIndexPayload {
                index: index.to_string(),
                values: vec!["t".to_string(), value.to_string()],
            },
            None,
        )
        .await
        .unwrap()
}

#[tokio::test]
async fn a_create_with_two_unique_indexes_reads_every_key_it_checks_in_one_round_trip() {
    let engine = engine();
    let model = members(&engine);

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_batch_gets().await;
    let before = engine.read_round_trips();
    model
        .set(member("m1", "ada@example.com", "ada"))
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(engine.read_round_trips() - before, 1);
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![vec![
            "reverse-index:table:members:m1".to_string(),
            "table:members:m1".to_string(),
            email_key("ada@example.com"),
            handle_key("ada"),
        ]]
    );
    tx.commit().await.unwrap();

    assert_eq!(
        member_by(&engine, "member-by-email", "ada@example.com").await,
        member("m1", "ada@example.com", "ada")
    );
    assert_eq!(member_by(&engine, "member-by-handle", "ada").await.id, "m1");
}

#[tokio::test]
async fn an_update_reads_its_old_unique_keys_in_a_second_round_trip_only_when_a_unique_value_changes(
) {
    let engine = engine();
    let model = members(&engine);
    save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();

    let tx = engine.begin_transaction().await.unwrap();
    let before = engine.read_round_trips();
    model
        .set(Member {
            display_name: "Ada".to_string(),
            ..member("m1", "ada@example.com", "ada")
        })
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(engine.read_round_trips() - before, 1);
    tx.commit().await.unwrap();

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_batch_gets().await;
    let before = engine.read_round_trips();
    model
        .set(member("m1", "ada@example.com", "lovelace"))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(engine.read_round_trips() - before, 2);
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![
            vec![
                "reverse-index:table:members:m1".to_string(),
                "table:members:m1".to_string(),
                email_key("ada@example.com"),
                handle_key("lovelace"),
            ],
            vec![handle_key("ada")],
        ]
    );
    tx.commit().await.unwrap();

    assert!(!engine.contains_key(&handle_key("ada")).await);
    assert_eq!(
        member_by(&engine, "member-by-handle", "lovelace").await,
        member("m1", "ada@example.com", "lovelace")
    );
    assert_eq!(
        member_by(&engine, "member-by-email", "ada@example.com")
            .await
            .handle,
        "lovelace"
    );
}

#[tokio::test]
async fn a_remove_reads_the_record_with_its_manifest_and_then_its_unique_keys() {
    let engine = engine();
    let model = members(&engine);
    save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();
    save_shop(&engine, shop("s1")).await.unwrap();

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_batch_gets().await;
    let before = engine.read_round_trips();
    assert!(model.remove("m1", &tx).await.unwrap());
    assert_eq!(engine.read_round_trips() - before, 2);
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![
            vec![
                "reverse-index:table:members:m1".to_string(),
                "table:members:m1".to_string(),
            ],
            vec![email_key("ada@example.com"), handle_key("ada")],
        ]
    );
    let before = engine.read_round_trips();
    assert!(shops(&engine).remove("s1", &tx).await.unwrap());
    assert_eq!(engine.read_round_trips() - before, 1);
    tx.commit().await.unwrap();

    for prefix in ["table:", "reverse-index:", "index:", "unique-index:"] {
        assert!(engine.keys_with_prefix(prefix).await.is_empty(), "{prefix}");
    }
}

#[tokio::test]
async fn a_versioned_read_reads_the_record_and_its_manifest_in_one_round_trip() {
    let engine = engine();
    let model = members(&engine);
    let saved = save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();
    let version = engine
        .raw_json("reverse-index:table:members:m1")
        .await
        .unwrap()["version"]
        .clone();

    let tx = engine.begin_transaction().await.unwrap();
    let before = engine.read_round_trips();
    let versioned = model
        .get_versioned_for_share("m1", Some(&tx))
        .await
        .unwrap();
    assert_eq!(engine.read_round_trips() - before, 1);
    assert_eq!(versioned.value, saved);
    assert_eq!(json!(versioned.version.to_string()), version);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn a_batch_replacement_reads_its_sources_and_then_its_unique_keys_in_two_round_trips() {
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
    let next = vec![
        UniqueAccount {
            email: b.email.clone(),
            ..a.clone()
        },
        UniqueAccount {
            email: a.email.clone(),
            ..b.clone()
        },
    ];

    let tx = engine.begin_transaction().await.unwrap();
    let before = engine.read_round_trips();
    let saved = model
        .replace_many(vec![(a, next[0].clone()), (b, next[1].clone())])
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(engine.read_round_trips() - before, 2);
    assert_eq!(saved, next);
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn a_rebalance_reads_every_partition_assignment_in_one_round_trip() {
    let engine = engine();
    let queue = queue(engine.clone(), 8);
    let config = cluster_config("node-a");
    queue.heartbeat_cluster_worker(&config).await.unwrap();

    let before = engine.read_round_trips();
    let assignments = queue.rebalance_cluster_partitions(&config).await.unwrap();
    assert_eq!(engine.read_round_trips() - before, 2);
    assert_eq!(
        assignments
            .iter()
            .map(|assignment| assignment.partition)
            .collect::<Vec<_>>(),
        (0..8).collect::<Vec<_>>()
    );
    assert!(assignments.iter().all(|assignment| {
        assignment.desired_node_id.as_deref() == Some("node-a") && assignment.generation == 1
    }));

    let before = engine.read_round_trips();
    let again = queue.rebalance_cluster_partitions(&config).await.unwrap();
    assert_eq!(engine.read_round_trips() - before, 2);
    assert_eq!(again, assignments);
}

#[tokio::test]
async fn a_unique_violation_names_the_first_taken_index_of_the_new_value() {
    let engine = engine();
    save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();
    let stored = engine.keys_with_prefix("").await;

    assert_eq!(
        save_member(&engine, member("m2", "grace@example.com", "ada"))
            .await
            .unwrap_err(),
        violation("members", "member-by-handle")
    );
    assert_eq!(
        save_member(&engine, member("m3", "ada@example.com", "ada"))
            .await
            .unwrap_err(),
        violation("members", "member-by-email")
    );
    assert_eq!(engine.keys_with_prefix("").await, stored);
}

#[tokio::test]
async fn a_record_saved_twice_in_one_transaction_is_updated_from_its_first_save() {
    let engine = engine();
    let model = members(&engine);

    let tx = engine.begin_transaction().await.unwrap();
    model
        .set(member("m1", "ada@example.com", "ada"))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    engine.start_recording_batch_gets().await;
    let before = engine.read_round_trips();
    model
        .set(member("m1", "ada@example.com", "lovelace"))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(engine.read_round_trips() - before, 1);
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![
            vec![
                "reverse-index:table:members:m1".to_string(),
                "table:members:m1".to_string(),
                email_key("ada@example.com"),
                handle_key("lovelace"),
            ],
            vec![handle_key("ada")],
        ]
    );
    tx.commit().await.unwrap();

    assert!(!engine.contains_key(&handle_key("ada")).await);
    assert_eq!(
        member_by(&engine, "member-by-handle", "lovelace").await,
        member("m1", "ada@example.com", "lovelace")
    );
    assert_eq!(
        engine
            .raw_json("reverse-index:table:members:m1")
            .await
            .unwrap()["value"],
        json!([
            "index:member-by-tenant:tenant_id:t:table:members:m1",
            email_key("ada@example.com"),
            handle_key("lovelace"),
        ])
    );
}

#[tokio::test]
async fn unique_values_freed_earlier_in_a_transaction_can_be_taken_later_in_it() {
    let engine = engine();
    let model = members(&engine);
    save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();

    let tx = engine.begin_transaction().await.unwrap();
    model
        .set(member("m1", "ada@example.com", "lovelace"))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    model
        .set(member("m2", "grace@example.com", "ada"))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(
        model
            .set(member("m4", "hopper@example.com", "ada"))
            .timestamps(false)
            .execute(&tx)
            .await
            .unwrap_err(),
        violation("members", "member-by-handle")
    );
    assert!(model.remove("m1", &tx).await.unwrap());
    model
        .set(member("m3", "ada@example.com", "lovelace"))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();

    assert_eq!(member_by(&engine, "member-by-handle", "ada").await.id, "m2");
    assert_eq!(
        member_by(&engine, "member-by-email", "ada@example.com")
            .await
            .id,
        "m3"
    );
    assert_eq!(
        member_by(&engine, "member-by-handle", "lovelace").await.id,
        "m3"
    );
    assert!(!engine.contains_key("table:members:m1").await);
    assert!(!engine.contains_key("table:members:m4").await);
}

#[tokio::test]
async fn the_record_state_is_checked_before_the_unique_keys_read_with_it() {
    let engine = engine();
    let original = member("m1", "ada@example.com", "ada");
    engine
        .put_json(
            "table:members:m1",
            &serde_json::to_value(&original).unwrap(),
        )
        .await;
    engine
        .put_raw(&email_key("ada@example.com"), b"someone-else".to_vec())
        .await;
    let missing = TitoError::IndexError(
        "Primary record 'table:members:m1' exists without reverse index 'reverse-index:table:members:m1'"
            .to_string(),
    );

    assert_eq!(
        save_member(&engine, original.clone()).await.unwrap_err(),
        missing
    );
    assert_eq!(
        members(&engine)
            .get_versioned_for_share("m1", None)
            .await
            .unwrap_err(),
        missing
    );
    assert!(!engine.contains_key("reverse-index:table:members:m1").await);
    assert!(!engine.contains_key(&handle_key("ada")).await);

    let engine = MemoryEngine::default();
    engine
        .put_json(
            "reverse-index:table:members:m1",
            &json!({"value": [], "version": "tito:v1:0000000000000001"}),
        )
        .await;
    engine
        .put_raw(&email_key("ada@example.com"), b"someone-else".to_vec())
        .await;
    assert_eq!(
        save_member(&engine, original).await.unwrap_err(),
        TitoError::IndexError(
            "Reverse index 'reverse-index:table:members:m1' exists without primary record 'table:members:m1'"
                .to_string()
        )
    );
    assert!(!engine.contains_key("table:members:m1").await);
}

#[tokio::test]
async fn a_remove_of_a_missing_record_is_not_found_after_one_round_trip_and_writes_nothing() {
    let engine = engine();
    let model = members(&engine);

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_writes().await;
    let before = engine.read_round_trips();
    assert_eq!(
        model.remove("missing", &tx).await.unwrap_err(),
        TitoError::NotFound("Entity not found: table:members:missing".to_string())
    );
    assert_eq!(engine.read_round_trips() - before, 1);
    assert!(engine.take_recorded_writes().await.is_empty());
    assert_eq!(
        model
            .get_versioned_for_share("missing", Some(&tx))
            .await
            .unwrap_err(),
        TitoError::NotFound("Record 'table:members:missing' not found in database".to_string())
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn a_unique_key_built_from_a_timestamp_the_save_sets_is_read_once_the_record_state_is_known()
{
    let engine = engine();
    let model = engine.clone().model::<Stamp>(TitoModelOptions::default());

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_batch_gets().await;
    model
        .set(stamp("s1", 5))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    model
        .set(stamp("s2", 9))
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![
            vec![
                "reverse-index:table:stamps:s1".to_string(),
                "table:stamps:s1".to_string(),
                created_key(5),
            ],
            vec![
                "reverse-index:table:stamps:s2".to_string(),
                "table:stamps:s2".to_string(),
                created_key(9),
            ],
        ]
    );
    tx.commit().await.unwrap();

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_batch_gets().await;
    assert_eq!(
        model.set(stamp("s2", 5)).execute(&tx).await.unwrap_err(),
        violation("stamps", "stamp-by-created")
    );
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![vec![
            "reverse-index:table:stamps:s2".to_string(),
            "table:stamps:s2".to_string(),
            created_key(5),
        ]]
    );
    engine.start_recording_batch_gets().await;
    let created = model.set(stamp("s3", 5)).execute(&tx).await.unwrap();
    assert_ne!(created.created_at, 5);
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![
            vec![
                "reverse-index:table:stamps:s3".to_string(),
                "table:stamps:s3".to_string(),
                created_key(5),
            ],
            vec![created_key(created.created_at)],
        ]
    );
    tx.commit().await.unwrap();

    assert!(engine.contains_key(&created_key(created.created_at)).await);
    assert!(engine.contains_key(&created_key(5)).await);
    assert_eq!(
        engine.raw_json("table:stamps:s2").await.unwrap()["created_at"],
        9
    );
}

fn unique_payload(index: &str, value: &str) -> TitoFindOneByIndexPayload {
    TitoFindOneByIndexPayload {
        index: index.to_string(),
        values: vec!["t".to_string(), value.to_string()],
    }
}

fn keys_the_reads_will_send(model: &crate::TitoModel<MemoryEngine, Member>) -> Vec<String> {
    vec![
        model.key("m1"),
        model.key("m2"),
        model.key("missing"),
        model
            .unique_key(&unique_payload("member-by-email", "ada@example.com"))
            .unwrap(),
        model
            .unique_key(&unique_payload("member-by-handle", "grace"))
            .unwrap(),
        model
            .unique_key(&unique_payload("member-by-email", "none@example.com"))
            .unwrap(),
    ]
}

async fn reads_of_known_keys(
    model: &crate::TitoModel<MemoryEngine, Member>,
    tx: &MemoryTransaction,
) -> Vec<Result<Member, TitoError>> {
    vec![
        model.get_for_share("m1", Some(tx)).await,
        model.get_for_share("missing", Some(tx)).await,
        model
            .find_one_by_unique_index_for_share(
                unique_payload("member-by-email", "ada@example.com"),
                Some(tx),
            )
            .await,
        model
            .find_one_by_unique_index_for_share(
                unique_payload("member-by-handle", "grace"),
                Some(tx),
            )
            .await,
        model
            .find_one_by_unique_index_for_share(
                unique_payload("member-by-email", "none@example.com"),
                Some(tx),
            )
            .await,
    ]
}

fn guarded_records(writes: &[RecordedWrite]) -> Vec<String> {
    sorted(
        fence_writes(writes)
            .into_iter()
            .filter_map(|key| key.rsplit_once(':').map(|(record, _)| record.to_string()))
            .collect(),
    )
}

#[tokio::test]
async fn the_key_of_a_record_is_the_key_every_read_by_id_sends() {
    let engine = engine();
    let model = members(&engine);
    save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();
    save_member(&engine, member("m2", "grace@example.com", "grace"))
        .await
        .unwrap();

    assert_eq!(model.key("m1"), "table:members:m1");
    assert!(engine.contains_key(&model.key("m1")).await);
    assert!(engine.contains_key(&model.key("m2")).await);

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_reads().await;
    model.get_for_share("m1", Some(&tx)).await.unwrap();
    model.get_for_update("m2", &tx).await.unwrap();
    assert_eq!(
        engine.take_recorded_reads().await,
        vec![model.key("m1").into_bytes(), model.key("m2").into_bytes()]
    );

    engine.start_recording_batch_gets().await;
    model
        .get_many_for_share(vec!["m1".to_string(), "m2".to_string()], Some(&tx))
        .await
        .unwrap();
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![vec![model.key("m1"), model.key("m2")]]
    );

    engine.start_recording_batch_gets().await;
    model
        .get_versioned_for_share("m1", Some(&tx))
        .await
        .unwrap();
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![vec![
            "reverse-index:table:members:m1".to_string(),
            model.key("m1"),
        ]]
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn the_unique_key_of_a_payload_is_the_key_a_find_one_sends() {
    let engine = engine();
    let model = members(&engine);
    save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();
    let by_email = unique_payload("member-by-email", "ada@example.com");
    let by_handle = unique_payload("member-by-handle", "ada");
    let by_missing = unique_payload("member-by-email", "none@example.com");

    assert_eq!(
        model.unique_key(&by_email).unwrap(),
        email_key("ada@example.com")
    );
    assert_eq!(model.unique_key(&by_handle).unwrap(), handle_key("ada"));
    assert!(
        engine
            .contains_key(&model.unique_key(&by_email).unwrap())
            .await
    );
    assert!(
        engine
            .contains_key(&model.unique_key(&by_handle).unwrap())
            .await
    );

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_reads().await;
    model
        .find_one_by_unique_index_for_share(by_email.clone(), Some(&tx))
        .await
        .unwrap();
    assert_eq!(
        engine.take_recorded_reads().await,
        vec![model.unique_key(&by_email).unwrap().into_bytes()]
    );

    engine.start_recording_reads().await;
    model
        .find_one_by_unique_index_for_share(by_handle.clone(), Some(&tx))
        .await
        .unwrap();
    assert_eq!(
        engine.take_recorded_reads().await,
        vec![model.unique_key(&by_handle).unwrap().into_bytes()]
    );

    engine.start_recording_reads().await;
    assert_eq!(
        model
            .find_one_by_unique_index_for_share(by_missing.clone(), Some(&tx))
            .await
            .unwrap_err(),
        TitoError::NotFound("No record found for unique index 'member-by-email'".to_string())
    );
    assert_eq!(
        engine.take_recorded_reads().await,
        vec![model.unique_key(&by_missing).unwrap().into_bytes()]
    );
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn the_unique_key_refuses_a_payload_the_way_the_find_one_refuses_it() {
    let engine = engine();
    let model = members(&engine);
    let stamps = engine.clone().model::<Stamp>(TitoModelOptions::default());

    for payload in [
        unique_payload("member-by-tenant", "t"),
        unique_payload("no-such-index", "t"),
        TitoFindOneByIndexPayload {
            index: "member-by-email".to_string(),
            values: vec!["t".to_string()],
        },
        TitoFindOneByIndexPayload {
            index: "member-by-email".to_string(),
            values: Vec::new(),
        },
        TitoFindOneByIndexPayload {
            index: "member-by-email".to_string(),
            values: vec!["t".to_string(), "a".to_string(), "b".to_string()],
        },
    ] {
        let refused = model.unique_key(&payload).unwrap_err();
        assert!(matches!(refused, TitoError::IndexError(_)));
        assert_eq!(
            model
                .find_one_by_unique_index_for_share(payload.clone(), None)
                .await
                .unwrap_err(),
            refused
        );
    }

    let not_canonical = TitoFindOneByIndexPayload {
        index: "stamp-by-created".to_string(),
        values: vec!["05".to_string()],
    };
    let refused = stamps.unique_key(&not_canonical).unwrap_err();
    assert!(matches!(refused, TitoError::InvalidInput(_)));
    assert_eq!(
        stamps
            .find_one_by_unique_index_for_share(not_canonical, None)
            .await
            .unwrap_err(),
        refused
    );
}

#[tokio::test]
async fn a_batch_get_of_the_keys_a_path_knows_leaves_the_reads_that_follow_unchanged() {
    let engine = engine();
    let model = members(&engine);
    save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();
    save_member(&engine, member("m2", "grace@example.com", "grace"))
        .await
        .unwrap();

    let alone = engine.begin_transaction().await.unwrap();
    let before = engine.read_round_trips();
    let alone_results = reads_of_known_keys(&model, &alone).await;
    let alone_trips = engine.read_round_trips() - before;
    alone.rollback().await.unwrap();

    let primed = engine.begin_transaction().await.unwrap();
    let before = engine.read_round_trips();
    let found = primed
        .batch_get(keys_the_reads_will_send(&model))
        .await
        .unwrap();
    assert_eq!(found.len(), 4);
    assert_eq!(engine.read_round_trips() - before, 1);
    let primed_results = reads_of_known_keys(&model, &primed).await;
    let primed_trips = engine.read_round_trips() - before;
    primed.rollback().await.unwrap();

    assert_eq!(
        alone_results,
        vec![
            Ok(member("m1", "ada@example.com", "ada")),
            Err(TitoError::NotFound(
                "Key 'table:members:missing' not found in database".to_string()
            )),
            Ok(member("m1", "ada@example.com", "ada")),
            Ok(member("m2", "grace@example.com", "grace")),
            Err(TitoError::NotFound(
                "No record found for unique index 'member-by-email'".to_string()
            )),
        ]
    );
    assert_eq!(primed_results, alone_results);
    assert_eq!(alone_trips, 5);
    assert_eq!(primed_trips, 1);
}

#[tokio::test]
async fn a_batch_get_takes_no_guard_and_the_reads_after_it_take_the_guards_they_took_before() {
    let engine = engine();
    let model = members(&engine);
    save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();
    save_member(&engine, member("m2", "grace@example.com", "grace"))
        .await
        .unwrap();

    let tx = engine.begin_transaction().await.unwrap();
    tx.batch_get(keys_the_reads_will_send(&model))
        .await
        .unwrap();
    write_scratch(&tx).await;
    assert!(guarded_records(&commit_writes(&engine, tx).await).is_empty());

    let alone = engine.begin_transaction().await.unwrap();
    reads_of_known_keys(&model, &alone).await;
    write_scratch(&alone).await;
    let alone_guards = guarded_records(&commit_writes(&engine, alone).await);

    let primed = engine.begin_transaction().await.unwrap();
    primed
        .batch_get(keys_the_reads_will_send(&model))
        .await
        .unwrap();
    reads_of_known_keys(&model, &primed).await;
    write_scratch(&primed).await;
    let primed_guards = guarded_records(&commit_writes(&engine, primed).await);

    assert_eq!(
        alone_guards,
        vec![
            "fence:members:m1".to_string(),
            "fence:members:m2".to_string()
        ]
    );
    assert_eq!(primed_guards, alone_guards);
}

#[tokio::test]
async fn the_test_engine_counts_a_read_trip_only_for_keys_the_transaction_does_not_hold() {
    let engine = engine();
    for key in ["a", "b", "c", "d", "e", "f"] {
        engine.put_raw(key, key.as_bytes().to_vec()).await;
    }
    let tx = engine.begin_transaction().await.unwrap();
    let before = engine.read_round_trips();
    let trips = || engine.read_round_trips() - before;

    assert_eq!(tx.get("a").await.unwrap(), Some(b"a".to_vec()));
    assert_eq!(tx.get("a").await.unwrap(), Some(b"a".to_vec()));
    assert_eq!(trips(), 1);

    assert_eq!(tx.get("none").await.unwrap(), None);
    assert_eq!(tx.get("none").await.unwrap(), None);
    assert_eq!(trips(), 2);

    assert_eq!(
        tx.batch_get(vec!["a", "b", "nothing"]).await.unwrap().len(),
        2
    );
    assert_eq!(trips(), 3);
    assert_eq!(tx.batch_get(vec!["a", "b"]).await.unwrap().len(), 2);
    assert_eq!(trips(), 3);
    assert_eq!(tx.batch_get(vec!["a", "nothing"]).await.unwrap().len(), 1);
    assert_eq!(tx.get("nothing").await.unwrap(), None);
    assert_eq!(trips(), 3);
    assert_eq!(tx.batch_get(vec!["c", "nowhere"]).await.unwrap().len(), 1);
    assert_eq!(trips(), 4);
    assert_eq!(tx.batch_get(vec!["nowhere"]).await.unwrap().len(), 0);
    assert_eq!(tx.get("nowhere").await.unwrap(), None);
    assert_eq!(trips(), 4);

    tx.put("p", b"p".to_vec()).await.unwrap();
    tx.delete("c").await.unwrap();
    tx.put("nowhere", b"written".to_vec()).await.unwrap();
    assert_eq!(tx.get("p").await.unwrap(), Some(b"p".to_vec()));
    assert_eq!(tx.get("c").await.unwrap(), None);
    assert_eq!(
        tx.batch_get(vec!["nowhere", "c"]).await.unwrap(),
        vec![(b"nowhere".to_vec(), b"written".to_vec())]
    );
    assert_eq!(trips(), 4);

    assert_eq!(tx.scan("d".."f", u32::MAX).await.unwrap().len(), 2);
    assert_eq!(tx.get("e").await.unwrap(), Some(b"e".to_vec()));
    assert_eq!(trips(), 5);

    assert_eq!(tx.scan_keys("f".."g", u32::MAX).await.unwrap().len(), 1);
    assert_eq!(tx.get("f").await.unwrap(), Some(b"f".to_vec()));
    assert_eq!(trips(), 7);
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn the_save_keys_of_a_payload_are_the_keys_its_save_reads_first() {
    let engine = engine();
    let model = members(&engine);
    let payload = member("m1", "ada@example.com", "ada");
    assert_eq!(
        model.save_keys(&payload),
        vec![
            "reverse-index:table:members:m1".to_string(),
            "table:members:m1".to_string(),
            email_key("ada@example.com"),
            handle_key("ada"),
        ]
    );

    let tx = engine.begin_transaction().await.unwrap();
    engine.start_recording_batch_gets().await;
    model
        .set(payload.clone())
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(
        engine.take_recorded_batch_gets().await,
        vec![model.save_keys(&payload)]
    );
    tx.commit().await.unwrap();
}

#[tokio::test]
async fn save_keys_reads_nothing_writes_nothing_and_checks_nothing() {
    let engine = engine();
    let model = members(&engine);
    save_member(&engine, member("m1", "ada@example.com", "ada"))
        .await
        .unwrap();
    let stored = engine.keys_with_prefix("").await;
    engine.start_recording_reads().await;
    engine.start_recording_writes().await;
    let taken = member("m2", "ada@example.com", "ada");
    assert_eq!(
        model.save_keys(&taken),
        vec![
            "reverse-index:table:members:m2".to_string(),
            "table:members:m2".to_string(),
            email_key("ada@example.com"),
            handle_key("ada"),
        ]
    );
    assert!(engine.take_recorded_reads().await.is_empty());
    assert!(engine.take_recorded_writes().await.is_empty());
    assert_eq!(engine.keys_with_prefix("").await, stored);

    let stamps = engine.clone().model::<Stamp>(TitoModelOptions::default());
    assert_eq!(
        stamps.save_keys(&stamp("s1", 7)),
        vec![
            "reverse-index:table:stamps:s1".to_string(),
            "table:stamps:s1".to_string(),
            created_key(7),
        ]
    );
}

#[tokio::test]
async fn after_a_batch_read_of_its_save_keys_a_save_sends_no_read() {
    let engine = engine();
    let model = members(&engine);

    let created = member("m1", "ada@example.com", "ada");
    let tx = engine.begin_transaction().await.unwrap();
    tx.batch_get(model.save_keys(&created)).await.unwrap();
    let before = engine.read_round_trips();
    model
        .set(created.clone())
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(engine.read_round_trips() - before, 0);
    tx.commit().await.unwrap();

    let edited = Member {
        display_name: "Ada".to_string(),
        ..created.clone()
    };
    let tx = engine.begin_transaction().await.unwrap();
    tx.batch_get(model.save_keys(&edited)).await.unwrap();
    let before = engine.read_round_trips();
    model
        .set(edited.clone())
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(engine.read_round_trips() - before, 0);
    tx.commit().await.unwrap();

    let renamed = member("m1", "ada@example.com", "lovelace");
    let tx = engine.begin_transaction().await.unwrap();
    let mut keys = model.save_keys(&renamed);
    keys.extend(model.save_keys(&edited));
    tx.batch_get(keys).await.unwrap();
    let before = engine.read_round_trips();
    model
        .set(renamed.clone())
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert_eq!(engine.read_round_trips() - before, 0);
    tx.commit().await.unwrap();
    assert_eq!(
        member_by(&engine, "member-by-handle", "lovelace").await,
        renamed
    );
    assert!(!engine.contains_key(&handle_key("ada")).await);

    let taken = member("m2", "ada@example.com", "grace");
    let tx = engine.begin_transaction().await.unwrap();
    tx.batch_get(model.save_keys(&taken)).await.unwrap();
    let before = engine.read_round_trips();
    assert_eq!(
        model
            .set(taken)
            .timestamps(false)
            .execute(&tx)
            .await
            .unwrap_err(),
        violation("members", "member-by-email")
    );
    assert_eq!(engine.read_round_trips() - before, 0);
    tx.rollback().await.unwrap();
}
