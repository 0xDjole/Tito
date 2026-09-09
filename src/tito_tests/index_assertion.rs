use super::*;
use crate::types::TitoTransaction;

fn query(index: &str, values: &[&str]) -> TitoFindOneByIndexPayload {
    TitoFindOneByIndexPayload {
        index: index.to_string(),
        values: values.iter().map(|value| value.to_string()).collect(),
    }
}

#[tokio::test]
async fn index_assertion_reads_only_reverse_metadata_and_preserves_its_exact_bytes() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;
    let reverse_key = "reverse-index:table:authors:a1";
    let before = engine.raw_bytes(reverse_key).await.unwrap();
    engine.start_recording_reads().await;
    let tx = engine.begin_transaction().await.unwrap();
    for request in [
        query("author-by-org-email", &["org-a", "ada@example.com"]),
        query("author-by-age", &["36"]),
        query("author-by-kind-org", &["author", "org-a"]),
    ] {
        assert!(model.assert_index_match("a1", request, &tx).await.unwrap());
    }
    tx.commit().await.unwrap();
    assert_eq!(
        engine.take_recorded_reads().await,
        vec![reverse_key.as_bytes().to_vec(); 3]
    );
    assert_eq!(engine.raw_bytes(reverse_key).await.unwrap(), before);
}

#[tokio::test]
async fn index_assertion_rejects_stale_foreign_missing_and_staged_removed_values() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    let original = save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;
    let tx = engine.begin_transaction().await.unwrap();
    for (id, request) in [
        ("missing", query("author-by-age", &["36"])),
        ("a1", query("author-by-age", &["35"])),
        (
            "a1",
            query("author-by-org-email", &["foreign", "ada@example.com"]),
        ),
    ] {
        assert!(!model.assert_index_match(id, request, &tx).await.unwrap());
    }
    let mut changed = original;
    changed.age = i64::MAX;
    model
        .set(changed)
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    assert!(!model
        .assert_index_match("a1", query("author-by-age", &["36"]), &tx)
        .await
        .unwrap());
    assert!(model
        .assert_index_match("a1", query("author-by-age", &[&i64::MAX.to_string()]), &tx)
        .await
        .unwrap());
    model.remove("a1", &tx).await.unwrap();
    assert!(!model
        .assert_index_match("a1", query("author-by-age", &[&i64::MAX.to_string()]), &tx)
        .await
        .unwrap());
    tx.rollback().await.unwrap();
}

#[tokio::test]
async fn index_assertion_validates_query_before_storage_access() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    engine.start_recording_reads().await;
    let tx = engine.begin_transaction().await.unwrap();
    for request in [
        query("undeclared", &["value"]),
        query("author-by-age", &[]),
        query("author-by-age", &["36", "37"]),
        query("author-by-age", &["36.5"]),
        query("author-by-age", &["9223372036854775808"]),
        query("author-by-email", &[""]),
        query("author-by-email", &[&"x".repeat(4097)]),
    ] {
        assert!(model.assert_index_match("a1", request, &tx).await.is_err());
    }
    assert!(model
        .assert_index_match("", query("author-by-age", &["36"]), &tx)
        .await
        .is_err());
    tx.rollback().await.unwrap();
    assert!(engine.take_recorded_reads().await.is_empty());
}

#[tokio::test]
async fn index_assertion_fails_closed_on_corrupt_ambiguous_and_oversized_metadata() {
    let engine = engine();
    let model = engine.clone().model::<Author>(TitoModelOptions::default());
    save_author(&engine, author("a1", "ada@example.com", 36, "org-a")).await;
    let reverse_key = "reverse-index:table:authors:a1";
    let valid = engine.raw_json(reverse_key).await.unwrap();
    let mut duplicate = valid.clone();
    let first = duplicate["value"][0].clone();
    duplicate["value"].as_array_mut().unwrap().push(first);
    let mut ambiguous = valid.clone();
    ambiguous["value"]
        .as_array_mut()
        .unwrap()
        .push(json!(format!(
            "index:author-by-age:age:{}:table:authors:a1",
            crate::encode_index_integer(37)
        )));
    let mut foreign = valid.clone();
    foreign["value"].as_array_mut().unwrap().push(json!(
        "index:author-by-email:email:foreign:table:authors:a2"
    ));
    let mut unknown = valid.clone();
    unknown["extra"] = json!(true);
    for invalid in [
        duplicate,
        ambiguous,
        foreign,
        unknown,
        json!({"value": "invalid"}),
        json!({"value": vec!["x"; 10_001]}),
    ] {
        engine.put_json(reverse_key, &invalid).await;
        let tx = engine.begin_transaction().await.unwrap();
        assert!(model
            .assert_index_match("a1", query("author-by-age", &["36"]), &tx)
            .await
            .is_err());
        tx.rollback().await.unwrap();
    }
    engine.put_raw(reverse_key, vec![b' '; 1_048_577]).await;
    let tx = engine.begin_transaction().await.unwrap();
    assert!(model
        .assert_index_match("a1", query("author-by-age", &["36"]), &tx)
        .await
        .is_err());
    tx.rollback().await.unwrap();
    engine.put_json(reverse_key, &json!({"value": []})).await;
    let tx = engine.begin_transaction().await.unwrap();
    assert!(!model
        .assert_index_match("a1", query("author-by-age", &["36"]), &tx)
        .await
        .unwrap());
    tx.rollback().await.unwrap();
}
