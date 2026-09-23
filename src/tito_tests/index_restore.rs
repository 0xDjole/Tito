use super::*;
use crate::types::TitoTransaction;

fn account(id: &str, email: &str) -> UniqueAccount {
    UniqueAccount {
        id: id.into(),
        tenant_id: "tenant".into(),
        email: email.into(),
        verified: true,
        display_name: "Retained".into(),
    }
}

async fn save(engine: &MemoryEngine, account: UniqueAccount) {
    let model = engine.clone().model::<UniqueAccount>(Default::default());
    let tx = engine.begin_transaction().await.unwrap();
    model
        .set(account)
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    tx.commit().await.unwrap();
}

async fn erase_indexes(engine: &MemoryEngine, id: &str) -> serde_json::Value {
    let metadata = engine
        .raw_json(&format!("reverse-index:table:unique-accounts:{id}"))
        .await
        .unwrap();
    let tx = engine.begin_transaction().await.unwrap();
    for key in metadata["value"].as_array().unwrap() {
        tx.delete(key.as_str().unwrap()).await.unwrap();
    }
    tx.commit().await.unwrap();
    metadata
}

#[tokio::test]
async fn offline_index_rebuild_preserves_exact_primary_metadata_and_version_on_replay() {
    let engine = engine();
    let model = engine.clone().model::<UniqueAccount>(Default::default());
    let value = account("a1", "ada@example.com");
    save(&engine, value.clone()).await;
    let primary_key = "table:unique-accounts:a1";
    let reverse_key = format!("reverse-index:{primary_key}");
    let primary = serde_json::to_vec_pretty(&value).unwrap();
    engine.put_raw(primary_key, primary.clone()).await;
    let reverse = engine.raw_bytes(&reverse_key).await.unwrap();
    let metadata = erase_indexes(&engine, "a1").await;
    let tx = engine.begin_transaction().await.unwrap();
    assert!(matches!(
        model
            .set(value.clone())
            .timestamps(false)
            .execute(&tx)
            .await,
        Err(TitoError::IndexError(_))
    ));
    tx.rollback().await.unwrap();
    for _ in 0..2 {
        let tx = engine.begin_transaction().await.unwrap();
        assert_eq!(
            model.rebuild_indexes_for_restore("a1", &tx).await.unwrap(),
            value
        );
        tx.commit().await.unwrap();
        assert_eq!(engine.raw_bytes(primary_key).await.unwrap(), primary);
        assert_eq!(engine.raw_bytes(&reverse_key).await.unwrap(), reverse);
        for key in metadata["value"].as_array().unwrap() {
            assert_eq!(
                engine.raw_json(key.as_str().unwrap()).await.unwrap(),
                json!(value)
            );
        }
        assert_eq!(
            model
                .get_versioned("a1", None)
                .await
                .unwrap()
                .version
                .to_string(),
            metadata["version"].as_str().unwrap()
        );
    }
}

#[tokio::test]
async fn offline_index_rebuild_rejects_another_unique_owner_without_changing_it() {
    let engine = engine();
    let model = engine.clone().model::<UniqueAccount>(Default::default());
    let first = account("a1", "ada@example.com");
    let second = account("a2", "grace@example.com");
    save(&engine, first.clone()).await;
    save(&engine, second.clone()).await;
    let mut duplicate = second.clone();
    duplicate.email = first.email.clone();
    let primary_key = "table:unique-accounts:a2";
    let reverse_key = format!("reverse-index:{primary_key}");
    let mut metadata = erase_indexes(&engine, "a2").await;
    let indexes = model
        .get_index_keys(primary_key.into(), &duplicate, &json!(duplicate))
        .unwrap();
    metadata["value"] = json!(indexes.iter().map(|(key, _)| key).collect::<Vec<_>>());
    engine.put_json(primary_key, &json!(duplicate)).await;
    engine.put_json(&reverse_key, &metadata).await;
    let tx = engine.begin_transaction().await.unwrap();
    assert!(matches!(
        model.rebuild_indexes_for_restore("a2", &tx).await,
        Err(TitoError::UniqueViolation { .. })
    ));
    tx.rollback().await.unwrap();
    assert_eq!(model.get("a1").execute(None).await.unwrap(), first);
    assert_eq!(model.get("a2").execute(None).await.unwrap(), duplicate);
    assert_eq!(engine.raw_json(&reverse_key).await.unwrap(), metadata);
    for (key, _) in indexes {
        if key.starts_with("unique-index:") {
            assert_eq!(engine.raw_json(&key).await.unwrap(), json!(first));
        } else {
            assert!(engine.raw_bytes(&key).await.is_none());
        }
    }
}

#[tokio::test]
async fn offline_index_rebuild_requires_compatible_complete_versioned_metadata() {
    let engine = engine();
    let model = engine.clone().model::<UniqueAccount>(Default::default());
    save(&engine, account("a1", "ada@example.com")).await;
    let reverse_key = "reverse-index:table:unique-accounts:a1";
    let valid = erase_indexes(&engine, "a1").await;
    let mut duplicate = valid.clone();
    duplicate["value"]
        .as_array_mut()
        .unwrap()
        .push(valid["value"][0].clone());
    let mut missing = valid.clone();
    missing["value"].as_array_mut().unwrap().pop();
    let mut foreign = valid.clone();
    foreign["value"][0] = json!("index:foreign:table:unique-accounts:a2");
    let mut unversioned = valid.clone();
    unversioned.as_object_mut().unwrap().remove("version");
    let mut zero = valid.clone();
    zero["version"] = json!("0");
    for invalid in [duplicate, missing, foreign, unversioned, zero] {
        engine.put_json(reverse_key, &invalid).await;
        let tx = engine.begin_transaction().await.unwrap();
        assert!(model.rebuild_indexes_for_restore("a1", &tx).await.is_err());
        tx.rollback().await.unwrap();
        assert_eq!(engine.raw_json(reverse_key).await.unwrap(), invalid);
        for key in valid["value"].as_array().unwrap() {
            assert!(engine.raw_bytes(key.as_str().unwrap()).await.is_none());
        }
    }
    let tx = engine.begin_transaction().await.unwrap();
    tx.delete(reverse_key).await.unwrap();
    tx.commit().await.unwrap();
    let tx = engine.begin_transaction().await.unwrap();
    assert!(model.rebuild_indexes_for_restore("a1", &tx).await.is_err());
    tx.rollback().await.unwrap();
    assert!(engine.raw_bytes(reverse_key).await.is_none());
}

#[tokio::test]
async fn offline_index_rebuild_is_transactional_and_rejects_primary_identity_mismatch() {
    let engine = engine();
    let model = engine.clone().model::<UniqueAccount>(Default::default());
    let value = account("a1", "ada@example.com");
    save(&engine, value.clone()).await;
    let metadata = erase_indexes(&engine, "a1").await;
    let tx = engine.begin_transaction().await.unwrap();
    model.rebuild_indexes_for_restore("a1", &tx).await.unwrap();
    tx.rollback().await.unwrap();
    for key in metadata["value"].as_array().unwrap() {
        assert!(engine.raw_bytes(key.as_str().unwrap()).await.is_none());
    }
    let mut foreign = value;
    foreign.id = "a2".into();
    engine
        .put_json("table:unique-accounts:a1", &json!(foreign))
        .await;
    let tx = engine.begin_transaction().await.unwrap();
    assert!(model.rebuild_indexes_for_restore("a1", &tx).await.is_err());
    assert!(matches!(
        model.rebuild_indexes_for_restore("missing", &tx).await,
        Err(TitoError::NotFound(_))
    ));
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn offline_index_rebuild_reads_only_exact_authority_and_unique_claims() {
    let engine = engine();
    let model = engine.clone().model::<UniqueAccount>(Default::default());
    save(&engine, account("a1", "ada@example.com")).await;
    let metadata = erase_indexes(&engine, "a1").await;
    engine.start_recording_reads().await;
    let tx = engine.begin_transaction().await.unwrap();
    for id in ["", &"x".repeat(513)] {
        assert!(model.rebuild_indexes_for_restore(id, &tx).await.is_err());
    }
    assert!(engine.take_recorded_reads().await.is_empty());
    engine.start_recording_reads().await;
    model.rebuild_indexes_for_restore("a1", &tx).await.unwrap();
    tx.commit().await.unwrap();
    let mut expected = vec![
        b"table:unique-accounts:a1".to_vec(),
        b"reverse-index:table:unique-accounts:a1".to_vec(),
    ];
    expected.extend(
        metadata["value"]
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|key| key.as_str().filter(|key| key.starts_with("unique-index:")))
            .map(|key| key.as_bytes().to_vec()),
    );
    assert_eq!(engine.take_recorded_reads().await, expected);
}
