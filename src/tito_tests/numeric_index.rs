use super::*;
use crate::encode_index_integer;
use serde_json::Value;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NumericRecord {
    id: String,
    tenant: String,
    value: Value,
    values: Vec<Value>,
    measurements: HashMap<String, Value>,
    indexed: bool,
    unique: bool,
}

fn numeric_index(
    name: &str,
    fields: &[(&str, TitoIndexBlockType)],
    condition: bool,
) -> TitoIndexConfig {
    TitoIndexConfig {
        name: name.to_string(),
        fields: fields
            .iter()
            .map(|(name, r#type)| TitoIndexField {
                name: name.to_string(),
                r#type: r#type.clone(),
            })
            .collect(),
        condition,
    }
}

impl TitoModelTrait for NumericRecord {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![
            numeric_index(
                "numeric-by-value",
                &[("value", TitoIndexBlockType::Number)],
                self.indexed,
            ),
            numeric_index(
                "numeric-by-tenant-value",
                &[
                    ("tenant", TitoIndexBlockType::String),
                    ("value", TitoIndexBlockType::Number),
                ],
                self.indexed,
            ),
            numeric_index(
                "numeric-by-values",
                &[("values", TitoIndexBlockType::Number)],
                self.indexed,
            ),
            numeric_index(
                "numeric-by-measurements",
                &[("measurements", TitoIndexBlockType::Number)],
                self.indexed,
            ),
        ]
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![numeric_index(
            "numeric-unique-value",
            &[
                ("tenant", TitoIndexBlockType::String),
                ("value", TitoIndexBlockType::Number),
            ],
            self.indexed && self.unique,
        )]
    }

    fn table() -> String {
        "numeric-records".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn numeric_record(id: &str, value: Value) -> NumericRecord {
    NumericRecord {
        id: id.to_string(),
        tenant: "tenant".to_string(),
        value,
        indexed: true,
        ..Default::default()
    }
}

async fn save_numeric(
    engine: &MemoryEngine,
    value: NumericRecord,
) -> Result<NumericRecord, TitoError> {
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    engine
        .transaction(|tx| {
            let model = model.clone();
            let value = value.clone();
            async move { model.set(value).timestamps(false).execute(&tx).await }
        })
        .await
}

async fn snapshot(engine: &MemoryEngine) -> Vec<(String, Vec<u8>)> {
    let mut rows = Vec::new();
    for key in engine.keys_with_prefix("").await {
        let value = engine.raw_bytes(&key).await.unwrap();
        rows.push((key, value));
    }
    rows
}

fn integer_boundaries() -> Vec<i64> {
    let mut values = vec![i64::MIN, i64::MIN + 1, -1, 0, 1, i64::MAX - 1, i64::MAX];
    for exponent in 1..=18 {
        let power = 10_i64.pow(exponent);
        values.extend([power - 1, power, power + 1, -power - 1, -power, -power + 1]);
    }
    values.sort_unstable();
    values.dedup();
    values
}

#[test]
fn integer_encoding_has_fixed_width_exact_extrema_and_signed_lexical_order() {
    assert_eq!(encode_index_integer(i64::MIN), "00000000000000000000");
    assert_eq!(encode_index_integer(-1), "09223372036854775807");
    assert_eq!(encode_index_integer(0), "09223372036854775808");
    assert_eq!(encode_index_integer(1), "09223372036854775809");
    assert_eq!(encode_index_integer(i64::MAX), "18446744073709551615");
    let keys: Vec<_> = integer_boundaries()
        .into_iter()
        .map(encode_index_integer)
        .collect();
    assert!(keys
        .iter()
        .all(|key| key.len() == 20 && key.bytes().all(|byte| byte.is_ascii_digit())));
    assert!(keys.windows(2).all(|pair| pair[0] < pair[1]));
}

#[tokio::test]
async fn signed_integer_writes_and_exact_queries_share_one_encoding_at_every_width() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    for value in integer_boundaries() {
        let id = format!("id-{value}");
        let saved = save_numeric(&engine, numeric_record(&id, json!(value)))
            .await
            .unwrap();
        assert_eq!(saved.value, json!(value));
        let encoded = encode_index_integer(value);
        let key = format!("index:numeric-by-value:value:{encoded}:table:numeric-records:{id}");
        assert_eq!(
            engine.raw_json(&key).await.unwrap(),
            serde_json::to_value(&saved).unwrap()
        );
        for reverse in [false, true] {
            let mut query = model.query_by_index("numeric-by-value");
            query.value(value.to_string());
            let page = if reverse {
                query.execute_reverse(None).await
            } else {
                query.execute(None).await
            }
            .unwrap();
            assert_eq!(page.items, vec![saved.clone()]);
            assert!(page.cursor.is_none());
        }
    }
}

#[tokio::test]
async fn signed_compound_indexes_page_both_directions_without_skipping_equal_value_ids() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    let mut expected = Vec::new();
    for value in [
        i64::MIN,
        -10_000_000_000,
        -1,
        0,
        1,
        9_999_999_999,
        10_000_000_000,
        1_700_000_000_123,
        i64::MAX,
    ] {
        for suffix in ["a2", "a20", "a3"] {
            let record = numeric_record(&format!("id-{value}-{suffix}"), json!(value));
            save_numeric(&engine, record.clone()).await.unwrap();
            expected.push(record);
        }
    }
    let mut independent = numeric_record("other-tenant", json!(0));
    independent.tenant = "other".to_string();
    save_numeric(&engine, independent).await.unwrap();

    for reverse in [false, true] {
        let mut cursor: Option<String> = None;
        let mut seen_cursors = std::collections::HashSet::new();
        let mut actual = Vec::new();
        loop {
            let mut query = model.query_by_index("numeric-by-tenant-value");
            query.value("tenant").limit(Some(2)).cursor(cursor);
            let page = if reverse {
                query.execute_reverse(None).await
            } else {
                query.execute(None).await
            }
            .unwrap();
            assert!(!page.items.is_empty());
            assert!(page.items.len() <= 2);
            actual.extend(page.items);
            cursor = page.cursor;
            if let Some(cursor) = &cursor {
                assert!(seen_cursors.insert(cursor.clone()));
            } else {
                break;
            }
        }
        let ordered: Vec<_> = if reverse {
            expected.iter().rev().cloned().collect()
        } else {
            expected.clone()
        };
        assert_eq!(actual, ordered);
    }
}

#[tokio::test]
async fn exported_integer_encoder_drives_half_open_ranges_and_exact_cursors() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    for value in [i64::MIN, -2, -1, 0, 1, 2, i64::MAX] {
        save_numeric(
            &engine,
            numeric_record(&format!("id-{value}"), json!(value)),
        )
        .await
        .unwrap();
    }
    for (from, to, expected) in [
        (i64::MIN, -1, vec![i64::MIN, -2]),
        (-1, 2, vec![-1, 0, 1]),
        (2, i64::MAX, vec![2]),
    ] {
        for reverse in [false, true] {
            let tx = engine.begin_transaction().await.unwrap();
            let mut cursor = None;
            let mut actual = Vec::new();
            loop {
                let payload = TitoScanPayload {
                    start: format!(
                        "index:numeric-by-value:value:{}:",
                        encode_index_integer(from)
                    ),
                    end: Some(format!(
                        "index:numeric-by-value:value:{}:",
                        encode_index_integer(to)
                    )),
                    limit: Some(1),
                    cursor,
                };
                let (rows, has_more) = if reverse {
                    model.scan_reverse(payload, &tx).await
                } else {
                    model.scan(payload, &tx).await
                }
                .unwrap();
                let page = model.to_paginated_items(rows, has_more).unwrap();
                actual.extend(
                    page.items
                        .into_iter()
                        .map(|row| row.value.as_i64().unwrap()),
                );
                cursor = page.cursor;
                if cursor.is_none() {
                    break;
                }
            }
            let ordered = if reverse {
                expected.iter().rev().copied().collect::<Vec<_>>()
            } else {
                expected.clone()
            };
            assert_eq!(actual, ordered);
        }
    }
}

#[tokio::test]
async fn numeric_unique_indexes_match_extrema_and_release_conditional_ownership() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    for value in [i64::MIN, -1, 0, i64::MAX] {
        let id = format!("owner-{value}");
        let mut owner = numeric_record(&id, json!(value));
        owner.unique = true;
        save_numeric(&engine, owner.clone()).await.unwrap();
        let payload = TitoFindOneByIndexPayload {
            index: "numeric-unique-value".to_string(),
            values: vec!["tenant".to_string(), value.to_string()],
        };
        assert_eq!(
            model
                .find_one_by_unique_index(payload.clone(), None)
                .await
                .unwrap(),
            owner
        );
        let before = snapshot(&engine).await;
        let mut competitor = owner.clone();
        competitor.id = format!("competitor-{value}");
        assert!(matches!(
            save_numeric(&engine, competitor.clone()).await.unwrap_err(),
            TitoError::UniqueViolation { .. }
        ));
        assert_eq!(snapshot(&engine).await, before);
        owner.unique = false;
        save_numeric(&engine, owner).await.unwrap();
        save_numeric(&engine, competitor.clone()).await.unwrap();
        assert_eq!(
            model.find_one_by_unique_index(payload, None).await.unwrap(),
            competitor
        );
    }
}

#[tokio::test]
async fn numeric_array_and_map_entries_use_the_same_signed_encoding() {
    let engine = engine();
    let mut record = numeric_record("collections", json!(0));
    record.values = vec![json!(i64::MIN), json!(-1), json!(0), json!(i64::MAX)];
    record.measurements = HashMap::from([
        ("negative".to_string(), json!(i64::MIN)),
        ("positive".to_string(), json!(i64::MAX)),
    ]);
    save_numeric(&engine, record.clone()).await.unwrap();
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    for value in &record.values {
        let found = model
            .query_by_index("numeric-by-values")
            .value(value.to_string())
            .execute(None)
            .await
            .unwrap();
        assert_eq!(found.items, vec![record.clone()]);
    }
    for (name, value) in &record.measurements {
        let key = format!("index:numeric-by-measurements:measurements:{name}:{}:table:numeric-records:collections", encode_index_integer(value.as_i64().unwrap()));
        assert_eq!(
            engine.raw_json(&key).await.unwrap(),
            serde_json::to_value(&record).unwrap()
        );
    }
}

#[tokio::test]
async fn unsupported_numeric_values_reject_insert_and_update_without_any_mutation() {
    let engine = engine();
    save_numeric(&engine, numeric_record("existing", json!(-1)))
        .await
        .unwrap();
    let before = snapshot(&engine).await;
    for invalid in [
        json!(0.0),
        json!(1.0),
        json!(-1.5),
        json!(1e100),
        json!(i64::MAX as u64 + 1),
        json!(u64::MAX),
    ] {
        for id in ["new", "existing"] {
            for location in ["value", "values", "measurements"] {
                let mut record = numeric_record(id, json!(0));
                match location {
                    "value" => record.value = invalid.clone(),
                    "values" => record.values = vec![json!(1), invalid.clone()],
                    _ => {
                        record
                            .measurements
                            .insert("invalid".to_string(), invalid.clone());
                    }
                }
                let error = save_numeric(&engine, record).await.unwrap_err();
                assert!(matches!(error, TitoError::InvalidInput(_)));
                assert_eq!(snapshot(&engine).await, before);
            }
        }
    }
}

#[tokio::test]
async fn supported_unsigned_integers_keep_exact_numeric_identity() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    for value in [0_u64, u32::MAX as u64, i64::MAX as u64] {
        let record = numeric_record(&format!("unsigned-{value}"), json!(value));
        save_numeric(&engine, record.clone()).await.unwrap();
        let found = model
            .query_by_index("numeric-by-value")
            .value(value.to_string())
            .execute(None)
            .await
            .unwrap();
        assert_eq!(found.items, vec![record]);
    }
}

#[tokio::test]
async fn numeric_indexes_remain_sparse_for_non_numbers_and_skip_disabled_conditions() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    for (position, value) in [
        Value::Null,
        json!(false),
        json!("42"),
        json!(""),
        json!([]),
        json!({}),
    ]
    .into_iter()
    .enumerate()
    {
        let record = numeric_record(&format!("sparse-{position}"), value);
        save_numeric(&engine, record).await.unwrap();
    }
    let record = numeric_record("missing", json!(0));
    let mut json = serde_json::to_value(&record).unwrap();
    json.as_object_mut().unwrap().remove("value");
    assert!(model
        .get_index_keys("table:numeric-records:missing".to_string(), &record, &json)
        .unwrap()
        .is_empty());
    let mut disabled = numeric_record("disabled", json!(u64::MAX));
    disabled.indexed = false;
    disabled.unique = true;
    save_numeric(&engine, disabled).await.unwrap();
    assert!(engine.keys_with_prefix("index:").await.is_empty());
    assert!(engine.keys_with_prefix("unique-index:").await.is_empty());
}

#[tokio::test]
async fn numeric_queries_reject_noncanonical_text_in_every_lookup_direction() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    for invalid in [
        "",
        "+1",
        "01",
        "-0",
        "00",
        "-01",
        " 1",
        "1 ",
        "1.0",
        "1e3",
        "NaN",
        "9223372036854775808",
        "-9223372036854775809",
        "18446744073709551615",
        "x:0",
    ] {
        let payload = TitoFindByIndexPayload {
            index: "numeric-by-value".to_string(),
            values: vec![invalid.to_string()],
            limit: None,
            cursor: None,
        };
        assert!(matches!(
            model
                .find_by_index(payload.clone(), None)
                .await
                .unwrap_err(),
            TitoError::InvalidInput(_)
        ));
        assert!(matches!(
            model
                .find_by_index_reverse(payload, None)
                .await
                .unwrap_err(),
            TitoError::InvalidInput(_)
        ));
        assert!(matches!(
            model
                .find_one_by_unique_index(
                    TitoFindOneByIndexPayload {
                        index: "numeric-unique-value".to_string(),
                        values: vec!["tenant".to_string(), invalid.to_string()],
                    },
                    None
                )
                .await
                .unwrap_err(),
            TitoError::InvalidInput(_)
        ));
    }
}

#[tokio::test]
async fn numeric_updates_and_bounded_batch_removal_preserve_other_values_and_opaque_ids() {
    let engine = engine();
    let model = engine
        .clone()
        .model::<NumericRecord>(TitoModelOptions::default());
    let opaque_id = "Provider:AbC_0001";
    save_numeric(&engine, numeric_record(opaque_id, json!(i64::MAX)))
        .await
        .unwrap();
    let old_key = format!(
        "index:numeric-by-value:value:{}:table:numeric-records:{opaque_id}",
        encode_index_integer(i64::MAX)
    );
    assert!(engine.contains_key(&old_key).await);
    save_numeric(&engine, numeric_record(opaque_id, json!(-1)))
        .await
        .unwrap();
    assert!(!engine.contains_key(&old_key).await);
    let updated_key = format!(
        "index:numeric-by-value:value:{}:table:numeric-records:{opaque_id}",
        encode_index_integer(-1)
    );
    assert!(engine.contains_key(&updated_key).await);
    for id in ["a2", "a20", "a3"] {
        save_numeric(&engine, numeric_record(id, json!(-1)))
            .await
            .unwrap();
    }
    save_numeric(&engine, numeric_record("retained", json!(0)))
        .await
        .unwrap();
    let mut removed = Vec::new();
    loop {
        let batch = engine
            .transaction(|tx| {
                let model = model.clone();
                async move {
                    model
                        .remove_by_index("numeric-by-value", "-1", 2, &tx)
                        .await
                }
            })
            .await
            .unwrap();
        assert!(batch.len() <= 2);
        if batch.is_empty() {
            break;
        }
        removed.extend(batch);
    }
    assert_eq!(removed, vec![opaque_id, "a2", "a20", "a3"]);
    assert_eq!(
        model.get("retained").execute(None).await.unwrap().value,
        json!(0)
    );
    assert!(!engine.contains_key(&updated_key).await);
    assert!(
        !engine
            .contains_key(&format!("table:numeric-records:{opaque_id}"))
            .await
    );
    assert!(
        !engine
            .contains_key(&format!("reverse-index:table:numeric-records:{opaque_id}"))
            .await
    );
}
