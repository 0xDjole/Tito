use super::*;

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ComputedRecord {
    id: String,
    tenant: String,
    target_type: String,
    target_id: String,
    rank: i64,
    enabled: bool,
    unique: bool,
}

impl ComputedRecord {
    fn target_key(&self) -> String {
        if self.target_id.is_empty() {
            String::new()
        } else {
            format!("{}:{}", self.target_type, self.target_id)
        }
    }

    fn index(&self, name: &str, ranked: bool, unique: bool) -> TitoIndexConfig {
        let mut fields = vec![
            TitoIndexField {
                name: "tenant".to_string(),
                r#type: TitoIndexFieldType::String,
            },
            TitoIndexField {
                name: "target_key".to_string(),
                r#type: TitoIndexFieldType::CustomString(self.target_key()),
            },
        ];
        if ranked {
            fields.push(TitoIndexField {
                name: "rank".to_string(),
                r#type: TitoIndexFieldType::Number,
            });
        }
        TitoIndexConfig {
            name: name.to_string(),
            condition: self.enabled && (!unique || self.unique),
            fields,
        }
    }
}

impl TitoModelTrait for ComputedRecord {
    fn id(&self) -> String {
        self.id.clone()
    }

    fn table() -> String {
        "computed-records".to_string()
    }

    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            self.index("computed-by-target", false, false),
            self.index("computed-by-target-rank", true, false),
        ]
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![self.index("computed-unique-target", false, true)]
    }
}

fn record(id: &str) -> ComputedRecord {
    ComputedRecord {
        id: id.to_string(),
        tenant: "store-1".to_string(),
        target_type: "DigitalProduct".to_string(),
        target_id: "opaque:ID\\value".to_string(),
        rank: -1,
        enabled: true,
        unique: false,
    }
}

async fn save(engine: &MemoryEngine, value: ComputedRecord) -> Result<ComputedRecord, TitoError> {
    let model = engine
        .clone()
        .model::<ComputedRecord>(TitoModelOptions::default());
    engine
        .transaction(|tx| {
            let model = model.clone();
            let value = value.clone();
            async move { model.set(value).timestamps(false).execute(&tx).await }
        })
        .await
}

#[tokio::test]
async fn custom_string_uses_existing_encoding_without_a_stored_field_and_default_schema_can_query_it(
) {
    let engine = engine();
    let model = engine
        .clone()
        .model::<ComputedRecord>(TitoModelOptions::default());
    let mut source = record("first");
    source.unique = true;
    let stored = save(&engine, source).await.unwrap();
    let json = serde_json::to_value(&stored).unwrap();
    assert!(json.get("target_key").is_none());
    let encoded = safe_encode(&stored.target_key());
    assert_eq!(
        engine.raw_json(&format!(
            "index:computed-by-target:tenant:store-1:target_key:{encoded}:table:computed-records:first"
        )).await.unwrap(),
        json
    );
    assert_eq!(
        engine.raw_json(&format!(
            "index:computed-by-target-rank:tenant:store-1:target_key:{encoded}:rank:{}:table:computed-records:first",
            crate::encode_index_integer(-1)
        )).await.unwrap(),
        json
    );
    let query = TitoFindByIndexPayload {
        index: "computed-by-target".to_string(),
        values: vec![stored.tenant.clone(), stored.target_key()],
        limit: Some(10),
        cursor: None,
    };
    assert_eq!(
        model.find_by_index(query, None).await.unwrap().items,
        vec![stored.clone()]
    );
    assert_eq!(
        model
            .find_one_by_unique_index(
                TitoFindOneByIndexPayload {
                    index: "computed-unique-target".to_string(),
                    values: vec![stored.tenant.clone(), stored.target_key()],
                },
                None
            )
            .await
            .unwrap(),
        stored
    );
}

#[tokio::test]
async fn computed_unique_ownership_is_scoped_and_updates_release_old_reverse_indexes() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<ComputedRecord>(TitoModelOptions::default());
    let mut owner = record("owner");
    owner.unique = true;
    save(&engine, owner.clone()).await.unwrap();
    let mut duplicate = owner.clone();
    duplicate.id = "competitor".to_string();
    assert!(matches!(
        save(&engine, duplicate.clone()).await,
        Err(TitoError::UniqueViolation { .. })
    ));
    assert!(
        !engine
            .contains_key("table:computed-records:competitor")
            .await
    );
    let mut foreign = duplicate.clone();
    foreign.id = "foreign".to_string();
    foreign.tenant = "store-2".to_string();
    save(&engine, foreign.clone()).await.unwrap();
    let old_target = owner.target_key();
    owner.target_id = "changed".to_string();
    save(&engine, owner.clone()).await.unwrap();
    save(&engine, duplicate.clone()).await.unwrap();
    assert_eq!(
        model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "computed-by-target".to_string(),
                    values: vec!["store-1".to_string(), old_target],
                    limit: Some(10),
                    cursor: None,
                },
                None
            )
            .await
            .unwrap()
            .items,
        vec![duplicate]
    );
    owner.enabled = false;
    save(&engine, owner.clone()).await.unwrap();
    for prefix in [
        "index:computed-by-target:",
        "index:computed-by-target-rank:",
        "unique-index:computed-records:",
    ] {
        for key in engine.keys_with_prefix(prefix).await {
            assert_ne!(engine.raw_json(&key).await.unwrap()["id"], "owner");
        }
    }
    assert_eq!(model.get("owner").execute(None).await.unwrap(), owner);
    assert_eq!(model.get("foreign").execute(None).await.unwrap(), foreign);
}

#[tokio::test]
async fn empty_computed_values_remain_sparse_and_definition_discovery_is_not_conditional() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<ComputedRecord>(TitoModelOptions::default());
    let mut source = record("empty");
    source.target_id.clear();
    source.unique = true;
    let source = save(&engine, source).await.unwrap();
    assert_eq!(model.get("empty").execute(None).await.unwrap(), source);
    assert!(engine.keys_with_prefix("index:computed-").await.is_empty());
    assert!(engine
        .keys_with_prefix("unique-index:computed-records:")
        .await
        .is_empty());
    let default = ComputedRecord::default();
    assert_eq!(default.indexes().len(), 2);
    assert_eq!(default.unique_indexes().len(), 1);
    let visible = record("visible");
    save(&engine, visible.clone()).await.unwrap();
    assert_eq!(
        model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "computed-by-target".to_string(),
                    values: vec![visible.tenant.clone(), visible.target_key()],
                    limit: Some(10),
                    cursor: None,
                },
                None
            )
            .await
            .unwrap()
            .items,
        vec![visible]
    );
}

#[tokio::test]
async fn computed_prefix_and_numeric_order_page_without_duplicate_ties() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<ComputedRecord>(TitoModelOptions::default());
    let mut expected = Vec::new();
    for (id, rank) in [
        ("a", i64::MIN),
        ("b", -1),
        ("c", -1),
        ("d", 0),
        ("e", i64::MAX),
    ] {
        let mut value = record(id);
        value.rank = rank;
        expected.push(save(&engine, value).await.unwrap());
    }
    let mut cursor = None;
    let mut received = Vec::new();
    for _ in 0..=expected.len() {
        let page = model
            .find_by_index(
                TitoFindByIndexPayload {
                    index: "computed-by-target-rank".to_string(),
                    values: vec!["store-1".to_string(), expected[0].target_key()],
                    limit: Some(1),
                    cursor: cursor.clone(),
                },
                None,
            )
            .await
            .unwrap();
        received.extend(page.items);
        match page.cursor {
            Some(next) => {
                assert_ne!(cursor.as_ref(), Some(&next));
                cursor = Some(next);
            }
            None => break,
        }
    }
    assert_eq!(received, expected);
}
