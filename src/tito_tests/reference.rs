use super::*;
use crate::fence::{fence_stripe_key, link_fence_key, source_fence_stripe, LINK_FENCE_STRIPES};
use crate::reference::{deleting_marker_key, reference_key};
use crate::TitoModel;
use std::collections::HashSet;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Picture {
    id: String,
    name: String,
    deleting: bool,
}

impl TitoModelTrait for Picture {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "picture-by-name".to_string(),
            fields: vec![TitoIndexField {
                name: "name".to_string(),
                r#type: TitoIndexFieldType::String,
            }],
        }]
    }

    fn references(&self) -> Vec<TitoReference> {
        Vec::new()
    }

    fn is_deleting(&self) -> bool {
        self.deleting
    }

    fn table() -> String {
        "pictures".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Gallery {
    id: String,
    title: String,
    picture_ids: Vec<String>,
    cover_id: Option<String>,
    parent_id: Option<String>,
    deleting: bool,
}

impl TitoModelTrait for Gallery {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "gallery-by-title".to_string(),
            fields: vec![TitoIndexField {
                name: "title".to_string(),
                r#type: TitoIndexFieldType::String,
            }],
        }]
    }

    fn references(&self) -> Vec<TitoReference> {
        let mut references: Vec<TitoReference> = self
            .picture_ids
            .iter()
            .enumerate()
            .map(|(position, id)| {
                TitoReference::new("pictures", id, format!("picture_ids.{position}"))
            })
            .collect();
        if let Some(cover_id) = &self.cover_id {
            references.push(TitoReference::new("pictures", cover_id, "cover_id"));
        }
        if let Some(parent_id) = &self.parent_id {
            references.push(TitoReference::new("galleries", parent_id, "parent_id"));
        }
        references
    }

    fn is_deleting(&self) -> bool {
        self.deleting
    }

    fn table() -> String {
        "galleries".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Pointer {
    id: String,
    targets: Vec<TitoReference>,
}

impl TitoModelTrait for Pointer {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }

    fn references(&self) -> Vec<TitoReference> {
        self.targets.clone()
    }

    fn is_deleting(&self) -> bool {
        false
    }

    fn table() -> String {
        "pointers".to_string()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn picture(id: &str) -> Picture {
    Picture {
        id: id.to_string(),
        name: format!("Picture {id}"),
        deleting: false,
    }
}

fn deleting_picture(id: &str) -> Picture {
    Picture {
        deleting: true,
        ..picture(id)
    }
}

fn gallery(id: &str, picture_ids: &[&str]) -> Gallery {
    Gallery {
        id: id.to_string(),
        title: format!("Gallery {id}"),
        picture_ids: picture_ids.iter().map(|id| id.to_string()).collect(),
        cover_id: None,
        parent_id: None,
        deleting: false,
    }
}

fn pointer(id: &str, targets: Vec<TitoReference>) -> Pointer {
    Pointer {
        id: id.to_string(),
        targets,
    }
}

fn pictures(engine: &MemoryEngine) -> TitoModel<MemoryEngine, Picture> {
    engine.clone().model::<Picture>(TitoModelOptions::default())
}

fn galleries(engine: &MemoryEngine) -> TitoModel<MemoryEngine, Gallery> {
    engine.clone().model::<Gallery>(TitoModelOptions::default())
}

fn pointers(engine: &MemoryEngine) -> TitoModel<MemoryEngine, Pointer> {
    engine.clone().model::<Pointer>(TitoModelOptions::default())
}

async fn save_picture(engine: &MemoryEngine, value: Picture) -> Result<Picture, TitoError> {
    let model = pictures(engine);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let value = value.clone();
            async move { model.set(value).timestamps(false).execute(&tx).await }
        })
        .await
}

async fn save_gallery(engine: &MemoryEngine, value: Gallery) -> Result<Gallery, TitoError> {
    let model = galleries(engine);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let value = value.clone();
            async move { model.set(value).timestamps(false).execute(&tx).await }
        })
        .await
}

async fn save_pointer(engine: &MemoryEngine, value: Pointer) -> Result<Pointer, TitoError> {
    let model = pointers(engine);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let value = value.clone();
            async move { model.set(value).timestamps(false).execute(&tx).await }
        })
        .await
}

async fn remove_picture(engine: &MemoryEngine, id: &str) -> Result<bool, TitoError> {
    let model = pictures(engine);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let id = id.to_string();
            async move { model.remove(&id, &tx).await }
        })
        .await
}

async fn remove_gallery(engine: &MemoryEngine, id: &str) -> Result<bool, TitoError> {
    let model = galleries(engine);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let id = id.to_string();
            async move { model.remove(&id, &tx).await }
        })
        .await
}

async fn erase_picture(engine: &MemoryEngine, id: &str) -> Result<bool, TitoError> {
    let model = pictures(engine);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let id = id.to_string();
            async move { model.erase(id, &tx).await }
        })
        .await
}

async fn picture_referenced_by(
    engine: &MemoryEngine,
    id: &str,
) -> Result<Vec<TitoIncomingReference>, TitoError> {
    let model = pictures(engine);
    let tx = engine.begin_transaction().await.unwrap();
    let result = model.referenced_by(id, &tx).await;
    tx.rollback().await.unwrap();
    result
}

async fn gallery_referenced_by(
    engine: &MemoryEngine,
    id: &str,
) -> Result<Vec<TitoIncomingReference>, TitoError> {
    let model = galleries(engine);
    let tx = engine.begin_transaction().await.unwrap();
    let result = model.referenced_by(id, &tx).await;
    tx.rollback().await.unwrap();
    result
}

async fn manifest_keys(engine: &MemoryEngine, primary_key: &str) -> Vec<String> {
    engine
        .raw_json(&format!("reverse-index:{primary_key}"))
        .await
        .unwrap()["value"]
        .as_array()
        .unwrap()
        .iter()
        .map(|key| key.as_str().unwrap().to_string())
        .collect()
}

fn picture_key(picture_id: &str, gallery_id: &str, path: &str) -> String {
    reference_key("pictures", picture_id, "galleries", gallery_id, path)
}

fn from_gallery(gallery_id: &str, path: &str) -> TitoIncomingReference {
    TitoIncomingReference::new("galleries", gallery_id, path)
}

async fn picture_referenced_by_except(
    engine: &MemoryEngine,
    id: &str,
    source_tables: &[&str],
    limit: usize,
) -> Result<Vec<TitoIncomingReference>, TitoError> {
    let model = pictures(engine);
    let tx = engine.begin_transaction().await.unwrap();
    let result = model
        .referenced_by_except(id, source_tables, limit, &tx)
        .await;
    tx.rollback().await.unwrap();
    result
}

async fn staged_gallery(engine: &MemoryEngine, value: Gallery) -> MemoryTransaction {
    let tx = engine.begin_transaction().await.unwrap();
    galleries(engine)
        .set(value)
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    tx
}

async fn staged_pointer(engine: &MemoryEngine, value: Pointer) -> MemoryTransaction {
    let tx = engine.begin_transaction().await.unwrap();
    pointers(engine)
        .set(value)
        .timestamps(false)
        .execute(&tx)
        .await
        .unwrap();
    tx
}

fn link(table: &str, id: &str) -> Vec<TitoReference> {
    vec![TitoReference::new(table, id, "items.0")]
}

fn from_pointer(pointer_id: &str) -> TitoIncomingReference {
    TitoIncomingReference::new("pointers", pointer_id, "items.0")
}

fn link_stripe(pointer_id: &str) -> u16 {
    source_fence_stripe("pointers", pointer_id, LINK_FENCE_STRIPES)
}

fn pointer_ids_in_distinct_link_stripes() -> Vec<String> {
    let mut picked = HashSet::new();
    let ids: Vec<String> = (0..1_000)
        .map(|position| format!("x{position}"))
        .filter(|id| picked.insert(link_stripe(id)))
        .take(usize::from(LINK_FENCE_STRIPES))
        .collect();
    assert_eq!(ids.len(), usize::from(LINK_FENCE_STRIPES));
    ids
}

fn pointer_id_whose_link_stripe(prefix: &str, wanted: impl Fn(u16) -> bool) -> String {
    (0..1_000)
        .map(|position| format!("{prefix}{position}"))
        .find(|id| wanted(link_stripe(id)))
        .unwrap()
}

async fn remove_shop(engine: &MemoryEngine, id: &str) -> Result<bool, TitoError> {
    let model = shops(engine);
    engine
        .transaction(|tx| {
            let model = model.clone();
            let id = id.to_string();
            async move { model.remove(&id, &tx).await }
        })
        .await
}

async fn shop_referenced_by(engine: &MemoryEngine, id: &str) -> Vec<TitoIncomingReference> {
    let model = shops(engine);
    let tx = engine.begin_transaction().await.unwrap();
    let result = model.referenced_by(id, &tx).await.unwrap();
    tx.rollback().await.unwrap();
    result
}

async fn put_incoming(
    engine: &MemoryEngine,
    picture_id: &str,
    source_table: &str,
    source_id: &str,
    path: &str,
) {
    engine
        .put_json(
            &reference_key("pictures", picture_id, source_table, source_id, path),
            &json!({"table": source_table, "id": source_id, "path": path}),
        )
        .await;
}

#[test]
fn reference_values_build_from_any_string_type() {
    assert_eq!(
        TitoReference::new("media", String::from("m1"), "gallery.0"),
        TitoReference {
            table: "media".to_string(),
            id: "m1".to_string(),
            path: "gallery.0".to_string(),
        }
    );
    assert_eq!(
        TitoIncomingReference::new(String::from("product"), "p1", String::from("gallery")),
        TitoIncomingReference {
            table: "product".to_string(),
            id: "p1".to_string(),
            path: "gallery".to_string(),
        }
    );
}

#[test]
fn reference_errors_name_the_record_the_path_and_who_points() {
    assert_eq!(
        TitoError::Referenced {
            table: "pictures".to_string(),
            id: "p1".to_string(),
            by: vec![from_gallery("g1", "cover_id"), from_gallery("g2", "picture_ids.0")],
        }
        .to_string(),
        "Record 'pictures:p1' is still referenced by galleries g1 (cover_id), galleries g2 (picture_ids.0)"
    );
    assert_eq!(
        TitoError::ReferenceMissing {
            table: "pictures".to_string(),
            id: "p9".to_string(),
            path: "cover_id".to_string(),
        }
        .to_string(),
        "Reference at 'cover_id' points at missing record 'pictures:p9'"
    );
    assert_eq!(
        TitoError::ReferenceDeleting {
            table: "pictures".to_string(),
            id: "p9".to_string(),
            path: "cover_id".to_string(),
        }
        .to_string(),
        "Reference at 'cover_id' points at record 'pictures:p9', which is being deleted"
    );
}

#[tokio::test]
async fn save_writes_one_key_per_reference_and_lists_it_in_the_manifest() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_picture(&engine, picture("p2")).await.unwrap();
    let mut value = gallery("g1", &["p1", "p2", "p1"]);
    value.cover_id = Some("p2".to_string());
    save_gallery(&engine, value).await.unwrap();

    let expected = vec![
        picture_key("p1", "g1", "picture_ids.0"),
        picture_key("p1", "g1", "picture_ids.2"),
        picture_key("p2", "g1", "cover_id"),
        picture_key("p2", "g1", "picture_ids.1"),
    ];
    assert_eq!(expected[0], "ref:pictures:p1:galleries:g1:picture_ids.0");
    assert_eq!(engine.keys_with_prefix("ref:").await, expected);
    assert_eq!(
        engine.raw_json(&expected[2]).await.unwrap(),
        json!({"table": "galleries", "id": "g1", "path": "cover_id"})
    );
    let manifest = manifest_keys(&engine, "table:galleries:g1").await;
    for key in &expected {
        assert!(manifest.contains(key));
    }
    assert_eq!(manifest.len(), expected.len() + 1);
    assert!(engine.keys_with_prefix("deleting:").await.is_empty());
}

#[tokio::test]
async fn identical_references_are_written_once() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    let target = TitoReference::new("pictures", "p1", "items.0");
    save_pointer(&engine, pointer("x1", vec![target.clone(), target]))
        .await
        .unwrap();

    assert_eq!(
        engine.keys_with_prefix("ref:").await,
        vec![reference_key("pictures", "p1", "pointers", "x1", "items.0")]
    );
    assert_eq!(manifest_keys(&engine, "table:pointers:x1").await.len(), 1);
}

#[tokio::test]
async fn a_missing_target_fails_the_save_without_writing() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();

    let error = save_gallery(&engine, gallery("g1", &["p1", "missing"]))
        .await
        .unwrap_err();

    assert_eq!(
        error,
        TitoError::ReferenceMissing {
            table: "pictures".to_string(),
            id: "missing".to_string(),
            path: "picture_ids.1".to_string(),
        }
    );
    assert!(!engine.contains_key("table:galleries:g1").await);
    assert!(
        !engine
            .contains_key("reverse-index:table:galleries:g1")
            .await
    );
    assert!(engine.keys_with_prefix("ref:").await.is_empty());
    assert!(engine
        .keys_with_prefix("index:gallery-by-title:")
        .await
        .is_empty());
}

#[tokio::test]
async fn a_target_saved_earlier_in_the_same_transaction_counts() {
    let engine = engine();
    let pictures = pictures(&engine);
    let galleries = galleries(&engine);

    engine
        .transaction(|tx| {
            let pictures = pictures.clone();
            let galleries = galleries.clone();
            async move {
                pictures.set(picture("p1")).execute(&tx).await?;
                galleries.set(gallery("g1", &["p1"])).execute(&tx).await?;
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();

    let error = engine
        .transaction(|tx| {
            let pictures = pictures.clone();
            let galleries = galleries.clone();
            async move {
                pictures.set(deleting_picture("p2")).execute(&tx).await?;
                galleries.set(gallery("g2", &["p2"])).execute(&tx).await
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(error, TitoError::ReferenceDeleting { .. }));

    let error = engine
        .transaction(|tx| {
            let pictures = pictures.clone();
            let galleries = galleries.clone();
            async move {
                galleries.set(gallery("g1", &["p1"])).execute(&tx).await?;
                pictures.remove("p1", &tx).await
            }
        })
        .await
        .unwrap_err();
    assert!(matches!(error, TitoError::Referenced { .. }));
    assert_eq!(
        picture_referenced_by(&engine, "p1").await.unwrap(),
        vec![from_gallery("g1", "picture_ids.0")]
    );
}

#[tokio::test]
async fn a_new_reference_to_a_deleting_target_is_refused_and_kept_ones_may_stay() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_picture(&engine, picture("p2")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
    save_picture(&engine, deleting_picture("p1")).await.unwrap();

    let error = save_gallery(&engine, gallery("g2", &["p2", "p1"]))
        .await
        .unwrap_err();
    assert_eq!(
        error,
        TitoError::ReferenceDeleting {
            table: "pictures".to_string(),
            id: "p1".to_string(),
            path: "picture_ids.1".to_string(),
        }
    );
    assert!(!engine.contains_key("table:galleries:g2").await);

    let mut moved = gallery("g1", &["p2", "p1"]);
    moved.cover_id = Some("p1".to_string());
    save_gallery(&engine, moved).await.unwrap();
    assert_eq!(
        engine.keys_with_prefix("ref:").await,
        vec![
            picture_key("p1", "g1", "cover_id"),
            picture_key("p1", "g1", "picture_ids.1"),
            picture_key("p2", "g1", "picture_ids.0"),
        ]
    );

    save_gallery(&engine, gallery("g1", &["p2"])).await.unwrap();
    assert_eq!(
        engine.keys_with_prefix("ref:").await,
        vec![picture_key("p2", "g1", "picture_ids.0")]
    );
    assert!(picture_referenced_by(&engine, "p1")
        .await
        .unwrap()
        .is_empty());
    assert!(remove_picture(&engine, "p1").await.unwrap());
}

#[tokio::test]
async fn an_update_drops_the_references_that_went_away() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_picture(&engine, picture("p2")).await.unwrap();
    let mut value = gallery("g1", &["p1", "p2"]);
    value.cover_id = Some("p1".to_string());
    save_gallery(&engine, value).await.unwrap();

    save_gallery(&engine, gallery("g1", &["p2"])).await.unwrap();

    assert_eq!(
        engine.keys_with_prefix("ref:").await,
        vec![picture_key("p2", "g1", "picture_ids.0")]
    );
    let manifest = manifest_keys(&engine, "table:galleries:g1").await;
    assert!(manifest.contains(&picture_key("p2", "g1", "picture_ids.0")));
    assert!(!manifest.iter().any(|key| key.contains(":p1:")));
    assert_eq!(
        engine
            .raw_json(&picture_key("p2", "g1", "picture_ids.0"))
            .await
            .unwrap(),
        json!({"table": "galleries", "id": "g1", "path": "picture_ids.0"})
    );
}

#[tokio::test]
async fn remove_is_refused_while_another_record_points_at_it() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    let mut value = gallery("g1", &["p1"]);
    value.cover_id = Some("p1".to_string());
    save_gallery(&engine, value).await.unwrap();

    let error = remove_picture(&engine, "p1").await.unwrap_err();

    assert_eq!(
        error,
        TitoError::Referenced {
            table: "pictures".to_string(),
            id: "p1".to_string(),
            by: vec![
                from_gallery("g1", "cover_id"),
                from_gallery("g1", "picture_ids.0"),
            ],
        }
    );
    assert!(engine.contains_key("table:pictures:p1").await);
    assert!(engine.contains_key("reverse-index:table:pictures:p1").await);

    assert!(remove_gallery(&engine, "g1").await.unwrap());
    assert!(remove_picture(&engine, "p1").await.unwrap());
    assert!(!engine.contains_key("table:pictures:p1").await);
    assert!(!engine.contains_key("reverse-index:table:pictures:p1").await);
    assert!(engine
        .keys_with_prefix("index:picture-by-name:")
        .await
        .is_empty());
}

#[tokio::test]
async fn remove_by_index_is_refused_for_a_referenced_record() {
    let engine = engine();
    let model = pictures(&engine);
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();

    let error = engine
        .transaction(|tx| {
            let model = model.clone();
            async move {
                model
                    .remove_by_index("picture-by-name", "Picture p1", 10, &tx)
                    .await
            }
        })
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::Referenced { .. }));
    assert!(engine.contains_key("table:pictures:p1").await);
}

#[tokio::test]
async fn remove_drops_the_source_reference_keys_and_its_deleting_marker() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    let mut value = gallery("g1", &["p1"]);
    value.deleting = true;
    save_gallery(&engine, value).await.unwrap();
    let marker = deleting_marker_key("galleries", "g1");
    assert_eq!(marker, "deleting:galleries:g1");
    assert!(manifest_keys(&engine, "table:galleries:g1")
        .await
        .contains(&marker));

    assert!(remove_gallery(&engine, "g1").await.unwrap());

    assert!(engine.keys_with_prefix("ref:").await.is_empty());
    assert!(!engine.contains_key(&marker).await);
    assert!(!engine.contains_key("table:galleries:g1").await);
    assert!(
        !engine
            .contains_key("reverse-index:table:galleries:g1")
            .await
    );
    assert!(engine
        .keys_with_prefix("index:gallery-by-title:")
        .await
        .is_empty());
    assert!(remove_picture(&engine, "p1").await.unwrap());
}

#[tokio::test]
async fn the_deleting_marker_follows_is_deleting() {
    let engine = engine();
    let marker = deleting_marker_key("pictures", "p1");
    save_picture(&engine, picture("p1")).await.unwrap();
    assert!(!engine.contains_key(&marker).await);
    assert!(!manifest_keys(&engine, "table:pictures:p1")
        .await
        .contains(&marker));

    save_picture(&engine, deleting_picture("p1")).await.unwrap();
    assert_eq!(engine.raw_json(&marker).await.unwrap(), json!(true));
    assert!(manifest_keys(&engine, "table:pictures:p1")
        .await
        .contains(&marker));

    let mut renamed = deleting_picture("p1");
    renamed.name = "Renamed".to_string();
    save_picture(&engine, renamed).await.unwrap();
    assert_eq!(engine.raw_json(&marker).await.unwrap(), json!(true));

    save_picture(&engine, picture("p1")).await.unwrap();
    assert!(!engine.contains_key(&marker).await);
    assert!(!manifest_keys(&engine, "table:pictures:p1")
        .await
        .contains(&marker));
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
}

#[tokio::test]
async fn referenced_by_lists_every_source_and_path_in_key_order() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_picture(&engine, picture("p2")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1", "p2"]))
        .await
        .unwrap();
    let mut second = gallery("g2", &["p1"]);
    second.cover_id = Some("p1".to_string());
    save_gallery(&engine, second).await.unwrap();

    assert_eq!(
        picture_referenced_by(&engine, "p1").await.unwrap(),
        vec![
            from_gallery("g1", "picture_ids.0"),
            from_gallery("g2", "cover_id"),
            from_gallery("g2", "picture_ids.0"),
        ]
    );
    assert_eq!(
        picture_referenced_by(&engine, "p2").await.unwrap(),
        vec![from_gallery("g1", "picture_ids.1")]
    );
    assert!(picture_referenced_by(&engine, "unused")
        .await
        .unwrap()
        .is_empty());
    for invalid in ["".to_string(), "x".repeat(513)] {
        assert!(matches!(
            picture_referenced_by(&engine, &invalid).await,
            Err(TitoError::InvalidInput(_))
        ));
    }
}

#[tokio::test]
async fn referenced_by_reads_every_page_and_a_refusal_names_twenty() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"; 600]))
        .await
        .unwrap();

    let incoming = picture_referenced_by(&engine, "p1").await.unwrap();

    let mut paths: Vec<String> = (0..600)
        .map(|position| format!("picture_ids.{position}"))
        .collect();
    paths.sort();
    assert_eq!(
        incoming
            .iter()
            .map(|reference| reference.path.clone())
            .collect::<Vec<_>>(),
        paths
    );
    assert!(incoming
        .iter()
        .all(|reference| reference.table == "galleries" && reference.id == "g1"));
    match remove_picture(&engine, "p1").await.unwrap_err() {
        TitoError::Referenced { by, .. } => assert_eq!(by, incoming[..20].to_vec()),
        other => panic!("expected a refusal, got {other:?}"),
    }
}

#[tokio::test]
async fn a_reference_key_that_disagrees_with_its_value_is_an_integrity_error() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
    let key = picture_key("p1", "g1", "picture_ids.0");

    engine
        .put_json(
            &key,
            &json!({"table": "galleries", "id": "g2", "path": "picture_ids.0"}),
        )
        .await;
    assert!(matches!(
        picture_referenced_by(&engine, "p1").await,
        Err(TitoError::IndexError(_))
    ));
    assert!(matches!(
        remove_picture(&engine, "p1").await,
        Err(TitoError::IndexError(_))
    ));

    engine.put_raw(&key, b"{".to_vec()).await;
    assert!(matches!(
        picture_referenced_by(&engine, "p1").await,
        Err(TitoError::DeserializationFailed(_))
    ));
    assert!(engine.contains_key("table:pictures:p1").await);
}

#[tokio::test]
async fn erase_removes_a_referenced_record_without_the_check() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
    save_picture(&engine, deleting_picture("p1")).await.unwrap();

    assert!(erase_picture(&engine, "p1").await.unwrap());

    assert!(!engine.contains_key("table:pictures:p1").await);
    assert!(!engine.contains_key("reverse-index:table:pictures:p1").await);
    assert!(
        !engine
            .contains_key(&deleting_marker_key("pictures", "p1"))
            .await
    );
    assert!(engine
        .keys_with_prefix("index:picture-by-name:")
        .await
        .is_empty());
    assert_eq!(
        picture_referenced_by(&engine, "p1").await.unwrap(),
        vec![from_gallery("g1", "picture_ids.0")]
    );
    assert!(matches!(
        erase_picture(&engine, "p1").await,
        Err(TitoError::NotFound(_))
    ));

    let mut renamed = gallery("g1", &["p1"]);
    renamed.title = "Renamed".to_string();
    assert_eq!(
        save_gallery(&engine, renamed).await.unwrap_err(),
        TitoError::ReferenceMissing {
            table: "pictures".to_string(),
            id: "p1".to_string(),
            path: "picture_ids.0".to_string(),
        }
    );
    save_gallery(&engine, gallery("g1", &[])).await.unwrap();
    assert!(engine.keys_with_prefix("ref:").await.is_empty());
}

#[tokio::test]
async fn erase_removes_a_source_with_its_reference_keys() {
    let engine = engine();
    let model = galleries(&engine);
    save_picture(&engine, picture("p1")).await.unwrap();
    let mut value = gallery("g1", &["p1"]);
    value.deleting = true;
    save_gallery(&engine, value).await.unwrap();

    engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.erase("g1".to_string(), &tx).await }
        })
        .await
        .unwrap();

    assert!(!engine.contains_key("table:galleries:g1").await);
    assert!(engine.keys_with_prefix("ref:").await.is_empty());
    assert!(engine.keys_with_prefix("deleting:").await.is_empty());
    assert!(remove_picture(&engine, "p1").await.unwrap());
}

#[tokio::test]
async fn index_repair_rebuilds_reference_keys_and_the_deleting_marker() {
    let engine = engine();
    let model = galleries(&engine);
    save_picture(&engine, picture("p1")).await.unwrap();
    save_picture(&engine, picture("p2")).await.unwrap();
    let mut value = gallery("g1", &["p1", "p2"]);
    value.deleting = true;
    let value = save_gallery(&engine, value).await.unwrap();
    let reverse_key = "reverse-index:table:galleries:g1";
    let reverse = engine.raw_bytes(reverse_key).await.unwrap();
    let manifest = manifest_keys(&engine, "table:galleries:g1").await;
    let mut stored = Vec::new();
    for key in &manifest {
        stored.push((key.clone(), engine.raw_bytes(key).await.unwrap()));
    }
    let tx = engine.begin_transaction().await.unwrap();
    for key in &manifest {
        tx.delete(key.as_str()).await.unwrap();
    }
    tx.commit().await.unwrap();
    assert!(engine.keys_with_prefix("ref:").await.is_empty());
    assert!(engine.keys_with_prefix("deleting:").await.is_empty());
    assert!(erase_picture(&engine, "p2").await.unwrap());

    for _ in 0..2 {
        let tx = engine.begin_transaction().await.unwrap();
        assert_eq!(
            model.rebuild_indexes_for_restore("g1", &tx).await.unwrap(),
            value
        );
        tx.commit().await.unwrap();
        for (key, bytes) in &stored {
            assert_eq!(engine.raw_bytes(key).await.as_ref(), Some(bytes));
        }
        assert_eq!(engine.raw_bytes(reverse_key).await.unwrap(), reverse);
    }
    assert_eq!(
        engine
            .raw_json(&deleting_marker_key("galleries", "g1"))
            .await
            .unwrap(),
        json!(true)
    );
    assert_eq!(
        picture_referenced_by(&engine, "p1").await.unwrap(),
        vec![from_gallery("g1", "picture_ids.0")]
    );
}

#[tokio::test]
async fn index_repair_rejects_a_manifest_that_misses_or_adds_reference_keys() {
    let engine = engine();
    let model = galleries(&engine);
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
    let reverse_key = "reverse-index:table:galleries:g1";
    let valid = engine.raw_json(reverse_key).await.unwrap();
    let key = picture_key("p1", "g1", "picture_ids.0");

    let mut missing = valid.clone();
    missing["value"]
        .as_array_mut()
        .unwrap()
        .retain(|listed| listed.as_str() != Some(key.as_str()));
    let mut extra = valid.clone();
    extra["value"]
        .as_array_mut()
        .unwrap()
        .push(json!(picture_key("p1", "g1", "picture_ids.9")));
    let mut marker = valid.clone();
    marker["value"]
        .as_array_mut()
        .unwrap()
        .push(json!(deleting_marker_key("galleries", "g1")));
    for invalid in [missing, extra, marker] {
        engine.put_json(reverse_key, &invalid).await;
        let tx = engine.begin_transaction().await.unwrap();
        assert!(matches!(
            model.rebuild_indexes_for_restore("g1", &tx).await,
            Err(TitoError::IndexError(_))
        ));
        tx.rollback().await.unwrap();
        assert_eq!(engine.raw_json(reverse_key).await.unwrap(), invalid);
    }
}

#[tokio::test]
async fn a_manifest_may_not_list_reference_keys_of_another_record() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &[])).await.unwrap();
    save_gallery(&engine, gallery("g2", &["p1"])).await.unwrap();
    let reverse_key = "reverse-index:table:galleries:g1";
    let mut foreign = engine.raw_json(reverse_key).await.unwrap();
    foreign["value"]
        .as_array_mut()
        .unwrap()
        .push(json!(picture_key("p1", "g2", "picture_ids.0")));
    engine.put_json(reverse_key, &foreign).await;

    assert!(matches!(
        save_gallery(&engine, gallery("g1", &[])).await,
        Err(TitoError::IndexError(_))
    ));
    assert!(matches!(
        remove_gallery(&engine, "g1").await,
        Err(TitoError::IndexError(_))
    ));
    assert_eq!(
        picture_referenced_by(&engine, "p1").await.unwrap(),
        vec![from_gallery("g2", "picture_ids.0")]
    );
}

#[tokio::test]
async fn a_record_pointing_at_itself_needs_no_guard() {
    let engine = engine();
    let mut value = gallery("g1", &[]);
    value.parent_id = Some("g1".to_string());
    save_gallery(&engine, value).await.unwrap();

    assert!(engine.keys_with_prefix("ref:").await.is_empty());
    assert!(gallery_referenced_by(&engine, "g1")
        .await
        .unwrap()
        .is_empty());

    let mut child = gallery("g2", &[]);
    child.parent_id = Some("g1".to_string());
    save_gallery(&engine, child).await.unwrap();
    assert_eq!(
        remove_gallery(&engine, "g1").await.unwrap_err(),
        TitoError::Referenced {
            table: "galleries".to_string(),
            id: "g1".to_string(),
            by: vec![from_gallery("g2", "parent_id")],
        }
    );
    assert!(remove_gallery(&engine, "g2").await.unwrap());
    assert!(remove_gallery(&engine, "g1").await.unwrap());
}

#[tokio::test]
async fn invalid_references_fail_the_save_before_any_write() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    let long_table = "t".repeat(513);
    let long_id = "i".repeat(513);
    let long_path = "p".repeat(2_049);

    for target in [
        TitoReference::new("", "p1", "items.0"),
        TitoReference::new("pictures", "", "items.0"),
        TitoReference::new("pictures", "p1", ""),
        TitoReference::new(long_table.as_str(), "p1", "items.0"),
        TitoReference::new("pictures", long_id.as_str(), "items.0"),
        TitoReference::new("pictures", "p1", long_path.as_str()),
    ] {
        assert!(matches!(
            save_pointer(&engine, pointer("x1", vec![target])).await,
            Err(TitoError::InvalidInput(_))
        ));
    }
    assert!(!engine.contains_key("table:pointers:x1").await);
    assert!(engine.keys_with_prefix("ref:").await.is_empty());
}

#[tokio::test]
async fn a_manifest_over_the_key_or_byte_limit_fails_the_save() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();

    let too_many = (0..10_001)
        .map(|position| TitoReference::new("pictures", "p1", format!("items.{position}")))
        .collect();
    assert!(matches!(
        save_pointer(&engine, pointer("x1", too_many)).await,
        Err(TitoError::InvalidInput(_))
    ));

    let too_large = (0..600)
        .map(|position| {
            TitoReference::new(
                "pictures",
                "p1",
                format!("{position}.{}", "p".repeat(2_000)),
            )
        })
        .collect();
    assert!(matches!(
        save_pointer(&engine, pointer("x1", too_large)).await,
        Err(TitoError::InvalidInput(_))
    ));

    assert!(!engine.contains_key("table:pointers:x1").await);
    assert!(engine.keys_with_prefix("ref:").await.is_empty());
}

#[tokio::test]
async fn key_segments_escape_separators_so_ids_never_collide() {
    let engine = engine();
    for id in ["a", "a:b", "back\\slash"] {
        save_picture(&engine, picture(id)).await.unwrap();
    }
    save_pointer(
        &engine,
        pointer("x1", vec![TitoReference::new("pictures", "a:b", "items.0")]),
    )
    .await
    .unwrap();
    save_pointer(
        &engine,
        pointer("x2", vec![TitoReference::new("pictures", "a", "items:0")]),
    )
    .await
    .unwrap();
    save_pointer(
        &engine,
        pointer(
            "x3",
            vec![TitoReference::new("pictures", "back\\slash", "items.0")],
        ),
    )
    .await
    .unwrap();

    assert_eq!(
        engine.keys_with_prefix("ref:").await,
        vec![
            "ref:pictures:a:pointers:x2:items\\:0".to_string(),
            "ref:pictures:a\\:b:pointers:x1:items.0".to_string(),
            "ref:pictures:back\\\\slash:pointers:x3:items.0".to_string(),
        ]
    );
    assert_eq!(
        picture_referenced_by(&engine, "a").await.unwrap(),
        vec![TitoIncomingReference::new("pointers", "x2", "items:0")]
    );
    assert_eq!(
        picture_referenced_by(&engine, "a:b").await.unwrap(),
        vec![TitoIncomingReference::new("pointers", "x1", "items.0")]
    );
    assert_eq!(
        picture_referenced_by(&engine, "back\\slash").await.unwrap(),
        vec![TitoIncomingReference::new("pointers", "x3", "items.0")]
    );

    save_pointer(&engine, pointer("x3", Vec::new()))
        .await
        .unwrap();
    assert!(remove_picture(&engine, "back\\slash").await.unwrap());
    assert!(matches!(
        remove_picture(&engine, "a").await,
        Err(TitoError::Referenced { .. })
    ));
    assert!(matches!(
        remove_picture(&engine, "a:b").await,
        Err(TitoError::Referenced { .. })
    ));
}

#[tokio::test]
async fn adding_a_reference_to_a_one_stripe_target_writes_one_link_stripe_and_keeping_one_writes_none(
) {
    let engine = engine();
    for id in ["p1", "p2"] {
        save_picture(&engine, picture(id)).await.unwrap();
    }
    let stripe = source_fence_stripe("galleries", "g1", LINK_FENCE_STRIPES);

    engine.start_recording_writes().await;
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
    let writes = engine.take_recorded_writes().await;
    assert_eq!(
        fence_writes(&writes),
        vec![fence_stripe_key("pictures", "p1", stripe)]
    );
    assert!(deleting_writes(&writes).is_empty());

    engine.start_recording_writes().await;
    let mut renamed = gallery("g1", &["p1", "p2"]);
    renamed.title = "Renamed".to_string();
    save_gallery(&engine, renamed).await.unwrap();
    let writes = engine.take_recorded_writes().await;
    assert_eq!(
        fence_writes(&writes),
        vec![fence_stripe_key("pictures", "p2", stripe)]
    );
    assert!(deleting_writes(&writes).is_empty());
    assert!(!writes.contains(&RecordedWrite::Put(picture_key(
        "p1",
        "g1",
        "picture_ids.0"
    ))));
    assert!(!writes.contains(&RecordedWrite::Delete(picture_key(
        "p1",
        "g1",
        "picture_ids.0"
    ))));
    assert!(writes.contains(&RecordedWrite::Put(picture_key(
        "p2",
        "g1",
        "picture_ids.1"
    ))));
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn every_write_of_a_one_stripe_records_deleting_marker_writes_its_sixteen_link_stripes() {
    let engine = engine();
    let model = pictures(&engine);
    let links = stripe_keys("pictures", "p1", LINK_FENCE_STRIPES);
    let marker = deleting_marker_key("pictures", "p1");
    let mut renamed = deleting_picture("p1");
    renamed.name = "Renamed".to_string();

    let writes = save_writes(&engine, &model, picture("p1")).await;
    assert!(fence_writes(&writes).is_empty());
    let writes = repair_writes(&engine, &model, "p1").await;
    assert!(deleting_writes(&writes).is_empty());
    assert!(fence_writes(&writes).is_empty());

    let writes = save_writes(&engine, &model, deleting_picture("p1")).await;
    assert_eq!(
        deleting_writes(&writes),
        vec![RecordedWrite::Put(marker.clone())]
    );
    assert_eq!(fence_writes(&writes), links);

    let writes = save_writes(&engine, &model, renamed).await;
    assert!(deleting_writes(&writes).is_empty());
    assert!(fence_writes(&writes).is_empty());

    let writes = repair_writes(&engine, &model, "p1").await;
    assert_eq!(
        deleting_writes(&writes),
        vec![RecordedWrite::Put(marker.clone())]
    );
    assert_eq!(fence_writes(&writes), links);

    let writes = save_writes(&engine, &model, picture("p1")).await;
    assert_eq!(
        deleting_writes(&writes),
        vec![RecordedWrite::Delete(marker.clone())]
    );
    assert_eq!(fence_writes(&writes), links);

    let writes = remove_writes(&engine, &model, "p1").await;
    assert_eq!(
        deleting_writes(&writes),
        vec![RecordedWrite::Delete(marker.clone())]
    );
    assert_eq!(fence_writes(&writes), links);

    save_picture(&engine, picture("p1")).await.unwrap();
    let writes = erase_writes(&engine, &model, "p1").await;
    assert!(deleting_writes(&writes).is_empty());
    assert!(fence_writes(&writes).is_empty());

    save_picture(&engine, deleting_picture("p1")).await.unwrap();
    let writes = erase_writes(&engine, &model, "p1").await;
    assert_eq!(
        deleting_writes(&writes),
        vec![RecordedWrite::Delete(marker)]
    );
    assert_eq!(fence_writes(&writes), links);
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn links_to_a_one_stripe_target_from_sources_in_different_link_stripes_commit_together() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    let sources = pointer_ids_in_distinct_link_stripes();
    let mut staged = Vec::new();
    for id in &sources {
        staged.push(staged_pointer(&engine, pointer(id, link("pictures", "p1"))).await);
    }

    for tx in staged {
        tx.commit().await.unwrap();
    }

    let mut pointing = picture_referenced_by(&engine, "p1").await.unwrap();
    pointing.sort();
    let mut expected: Vec<TitoIncomingReference> =
        sources.iter().map(|id| from_pointer(id)).collect();
    expected.sort();
    assert_eq!(pointing, expected);
    assert!(matches!(
        remove_picture(&engine, "p1").await,
        Err(TitoError::Referenced { .. })
    ));
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn links_to_a_one_stripe_target_from_sources_in_one_link_stripe_collide_and_the_retry_keeps_both(
) {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    let neighbour = pointer_id_whose_link_stripe("y", |stripe| stripe == link_stripe("x1"));
    let first = staged_pointer(&engine, pointer("x1", link("pictures", "p1"))).await;
    let second = staged_pointer(&engine, pointer(&neighbour, link("pictures", "p1"))).await;

    first.commit().await.unwrap();
    assert!(matches!(
        second.commit().await,
        Err(TitoError::Retryable(_))
    ));

    save_pointer(&engine, pointer(&neighbour, link("pictures", "p1")))
        .await
        .unwrap();
    assert_eq!(
        picture_referenced_by(&engine, "p1").await.unwrap(),
        vec![from_pointer("x1"), from_pointer(&neighbour)]
    );
}

#[tokio::test]
async fn a_link_to_a_one_stripe_target_collides_with_a_remove_of_it_in_either_order() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();

    let pointing = staged_gallery(&engine, gallery("g1", &["p1"])).await;
    assert!(remove_picture(&engine, "p1").await.unwrap());
    assert!(matches!(
        pointing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert_eq!(
        save_gallery(&engine, gallery("g1", &["p1"]))
            .await
            .unwrap_err(),
        TitoError::ReferenceMissing {
            table: "pictures".to_string(),
            id: "p1".to_string(),
            path: "picture_ids.0".to_string(),
        }
    );
    assert!(!engine.contains_key("table:galleries:g1").await);
    assert!(engine.keys_with_prefix("ref:").await.is_empty());

    save_picture(&engine, picture("p1")).await.unwrap();
    let removing = engine.begin_transaction().await.unwrap();
    assert!(pictures(&engine).remove("p1", &removing).await.unwrap());
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
    assert!(matches!(
        removing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert_eq!(
        remove_picture(&engine, "p1").await.unwrap_err(),
        TitoError::Referenced {
            table: "pictures".to_string(),
            id: "p1".to_string(),
            by: vec![from_gallery("g1", "picture_ids.0")],
        }
    );
    assert!(engine.contains_key("table:pictures:p1").await);
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_link_to_a_one_stripe_target_collides_with_a_deleting_mark_of_it_in_either_order() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_picture(&engine, picture("p2")).await.unwrap();

    let pointing = staged_gallery(&engine, gallery("g1", &["p1"])).await;
    save_picture(&engine, deleting_picture("p1")).await.unwrap();
    assert!(matches!(
        pointing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert_eq!(
        save_gallery(&engine, gallery("g1", &["p1"]))
            .await
            .unwrap_err(),
        TitoError::ReferenceDeleting {
            table: "pictures".to_string(),
            id: "p1".to_string(),
            path: "picture_ids.0".to_string(),
        }
    );
    assert!(picture_referenced_by(&engine, "p1")
        .await
        .unwrap()
        .is_empty());

    let marking = engine.begin_transaction().await.unwrap();
    pictures(&engine)
        .set(deleting_picture("p2"))
        .timestamps(false)
        .execute(&marking)
        .await
        .unwrap();
    save_gallery(&engine, gallery("g2", &["p2"])).await.unwrap();
    assert!(matches!(
        marking.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert!(
        !engine
            .contains_key(&deleting_marker_key("pictures", "p2"))
            .await
    );
    assert_eq!(
        picture_referenced_by(&engine, "p2").await.unwrap(),
        vec![from_gallery("g2", "picture_ids.0")]
    );
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_link_to_a_one_stripe_target_commits_beside_a_plain_save_of_it() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    let mut renamed = picture("p1");
    renamed.name = "Renamed".to_string();

    let pointing = staged_gallery(&engine, gallery("g1", &["p1"])).await;
    save_picture(&engine, renamed).await.unwrap();
    pointing.commit().await.unwrap();

    let mut renamed_again = picture("p1");
    renamed_again.name = "Renamed again".to_string();
    let renaming = engine.begin_transaction().await.unwrap();
    pictures(&engine)
        .set(renamed_again.clone())
        .timestamps(false)
        .execute(&renaming)
        .await
        .unwrap();
    save_gallery(&engine, gallery("g2", &["p1"])).await.unwrap();
    renaming.commit().await.unwrap();

    assert_eq!(
        pictures(&engine).get("p1").execute(None).await.unwrap(),
        renamed_again
    );
    assert_eq!(
        picture_referenced_by(&engine, "p1").await.unwrap(),
        vec![
            from_gallery("g1", "picture_ids.0"),
            from_gallery("g2", "picture_ids.0"),
        ]
    );
}

#[tokio::test]
async fn a_save_that_keeps_its_link_to_a_one_stripe_target_commits_beside_a_deleting_mark_of_it() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
    let mut renamed = gallery("g1", &["p1"]);
    renamed.title = "Renamed".to_string();

    let renaming = staged_gallery(&engine, renamed.clone()).await;
    save_picture(&engine, deleting_picture("p1")).await.unwrap();
    renaming.commit().await.unwrap();

    assert_eq!(
        galleries(&engine).get("g1").execute(None).await.unwrap(),
        renamed
    );
    assert_eq!(
        engine
            .raw_json(&deleting_marker_key("pictures", "p1"))
            .await
            .unwrap(),
        json!(true)
    );
}

#[tokio::test]
async fn adding_a_reference_to_a_sixteen_stripe_target_writes_one_stripe_key_and_keeping_one_writes_none(
) {
    let engine = engine();
    for id in ["s1", "s2"] {
        save_shop(&engine, shop(id)).await.unwrap();
    }
    let stripe = link_stripe("x1");

    engine.start_recording_writes().await;
    save_pointer(&engine, pointer("x1", link("shops", "s1")))
        .await
        .unwrap();
    let writes = engine.take_recorded_writes().await;
    assert_eq!(
        fence_writes(&writes),
        vec![fence_stripe_key("shops", "s1", stripe)]
    );
    assert!(!writes.contains(&RecordedWrite::Delete(deleting_marker_key("shops", "s1"))));

    engine.start_recording_writes().await;
    save_pointer(
        &engine,
        pointer(
            "x1",
            vec![
                TitoReference::new("shops", "s1", "items.0"),
                TitoReference::new("shops", "s2", "items.1"),
            ],
        ),
    )
    .await
    .unwrap();
    let writes = engine.take_recorded_writes().await;
    assert_eq!(
        fence_writes(&writes),
        vec![fence_stripe_key("shops", "s2", stripe)]
    );
    assert!(!writes.contains(&RecordedWrite::Delete(deleting_marker_key("shops", "s2"))));
    let kept = reference_key("shops", "s1", "pointers", "x1", "items.0");
    assert!(!writes.contains(&RecordedWrite::Put(kept.clone())));
    assert!(!writes.contains(&RecordedWrite::Delete(kept)));
    assert!(writes.contains(&RecordedWrite::Put(reference_key(
        "shops", "s2", "pointers", "x1", "items.1"
    ))));
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn links_to_a_sixteen_stripe_target_from_sources_in_different_stripes_commit_together() {
    let engine = engine();
    save_shop(&engine, shop("s1")).await.unwrap();
    let sources = pointer_ids_in_distinct_link_stripes();
    let mut staged = Vec::new();
    for id in &sources {
        staged.push(staged_pointer(&engine, pointer(id, link("shops", "s1"))).await);
    }

    for tx in staged {
        tx.commit().await.unwrap();
    }

    let mut pointing = shop_referenced_by(&engine, "s1").await;
    pointing.sort();
    let mut expected: Vec<TitoIncomingReference> =
        sources.iter().map(|id| from_pointer(id)).collect();
    expected.sort();
    assert_eq!(pointing, expected);
    assert!(matches!(
        remove_shop(&engine, "s1").await,
        Err(TitoError::Referenced { .. })
    ));
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn links_to_a_sixteen_stripe_target_from_sources_in_one_stripe_collide_and_the_retry_keeps_both(
) {
    let engine = engine();
    save_shop(&engine, shop("s1")).await.unwrap();
    let neighbour = pointer_id_whose_link_stripe("y", |stripe| stripe == link_stripe("x1"));
    assert_eq!(link_stripe("x1"), link_stripe(&neighbour));
    let first = staged_pointer(&engine, pointer("x1", link("shops", "s1"))).await;
    let second = staged_pointer(&engine, pointer(&neighbour, link("shops", "s1"))).await;

    first.commit().await.unwrap();
    assert!(matches!(
        second.commit().await,
        Err(TitoError::Retryable(_))
    ));

    save_pointer(&engine, pointer(&neighbour, link("shops", "s1")))
        .await
        .unwrap();
    assert_eq!(
        shop_referenced_by(&engine, "s1").await,
        vec![from_pointer("x1"), from_pointer(&neighbour)]
    );
}

#[tokio::test]
async fn a_link_to_a_sixteen_stripe_target_collides_with_a_remove_of_it_in_either_order() {
    let engine = engine();
    save_shop(&engine, shop("s1")).await.unwrap();

    let pointing = staged_pointer(&engine, pointer("x1", link("shops", "s1"))).await;
    assert!(remove_shop(&engine, "s1").await.unwrap());
    assert!(matches!(
        pointing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert_eq!(
        save_pointer(&engine, pointer("x1", link("shops", "s1")))
            .await
            .unwrap_err(),
        TitoError::ReferenceMissing {
            table: "shops".to_string(),
            id: "s1".to_string(),
            path: "items.0".to_string(),
        }
    );
    assert!(!engine.contains_key("table:pointers:x1").await);
    assert!(engine.keys_with_prefix("ref:").await.is_empty());

    save_shop(&engine, shop("s1")).await.unwrap();
    let removing = engine.begin_transaction().await.unwrap();
    assert!(shops(&engine).remove("s1", &removing).await.unwrap());
    save_pointer(&engine, pointer("x1", link("shops", "s1")))
        .await
        .unwrap();
    assert!(matches!(
        removing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert_eq!(
        remove_shop(&engine, "s1").await.unwrap_err(),
        TitoError::Referenced {
            table: "shops".to_string(),
            id: "s1".to_string(),
            by: vec![from_pointer("x1")],
        }
    );
    assert!(engine.contains_key("table:shops:s1").await);
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_link_to_a_sixteen_stripe_target_collides_with_a_deleting_mark_of_it_in_either_order() {
    let engine = engine();
    save_shop(&engine, shop("s1")).await.unwrap();
    save_shop(&engine, shop("s2")).await.unwrap();

    let pointing = staged_pointer(&engine, pointer("x1", link("shops", "s1"))).await;
    save_shop(
        &engine,
        Shop {
            deleting: true,
            ..shop("s1")
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        pointing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert_eq!(
        save_pointer(&engine, pointer("x1", link("shops", "s1")))
            .await
            .unwrap_err(),
        TitoError::ReferenceDeleting {
            table: "shops".to_string(),
            id: "s1".to_string(),
            path: "items.0".to_string(),
        }
    );
    assert!(shop_referenced_by(&engine, "s1").await.is_empty());

    let marking = engine.begin_transaction().await.unwrap();
    shops(&engine)
        .set(Shop {
            deleting: true,
            ..shop("s2")
        })
        .timestamps(false)
        .execute(&marking)
        .await
        .unwrap();
    save_pointer(&engine, pointer("x2", link("shops", "s2")))
        .await
        .unwrap();
    assert!(matches!(
        marking.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert!(
        !engine
            .contains_key(&deleting_marker_key("shops", "s2"))
            .await
    );
    assert_eq!(
        shop_referenced_by(&engine, "s2").await,
        vec![from_pointer("x2")]
    );
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_link_to_a_sixteen_stripe_target_collides_with_a_plain_save_of_it_in_either_order() {
    let engine = engine();
    save_shop(&engine, shop("s1")).await.unwrap();

    let pointing = staged_pointer(&engine, pointer("x1", link("shops", "s1"))).await;
    save_shop(
        &engine,
        Shop {
            name: "Renamed".to_string(),
            ..shop("s1")
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        pointing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    save_pointer(&engine, pointer("x1", link("shops", "s1")))
        .await
        .unwrap();

    let renaming = engine.begin_transaction().await.unwrap();
    shops(&engine)
        .set(Shop {
            name: "Renamed again".to_string(),
            ..shop("s1")
        })
        .timestamps(false)
        .execute(&renaming)
        .await
        .unwrap();
    save_pointer(&engine, pointer("x2", link("shops", "s1")))
        .await
        .unwrap();
    assert!(matches!(
        renaming.commit().await,
        Err(TitoError::Retryable(_))
    ));

    assert_eq!(
        shops(&engine).get("s1").execute(None).await.unwrap().name,
        "Renamed"
    );
    assert_eq!(
        shop_referenced_by(&engine, "s1").await,
        vec![from_pointer("x1"), from_pointer("x2")]
    );
}

#[tokio::test]
async fn a_save_that_keeps_its_link_to_a_sixteen_stripe_target_commits_beside_a_deleting_mark_of_it(
) {
    let engine = engine();
    save_shop(&engine, shop("s1")).await.unwrap();
    save_pointer(&engine, pointer("x1", link("shops", "s1")))
        .await
        .unwrap();

    let keeping = staged_pointer(&engine, pointer("x1", link("shops", "s1"))).await;
    save_shop(
        &engine,
        Shop {
            deleting: true,
            ..shop("s1")
        },
    )
    .await
    .unwrap();
    keeping.commit().await.unwrap();

    assert_eq!(
        engine
            .raw_json(&deleting_marker_key("shops", "s1"))
            .await
            .unwrap(),
        json!(true)
    );
    assert_eq!(
        shop_referenced_by(&engine, "s1").await,
        vec![from_pointer("x1")]
    );
}

#[tokio::test]
async fn links_to_a_two_stripe_target_write_one_of_its_sixteen_link_stripes() {
    let engine = engine();
    let model = stands::<2>(&engine);
    for id in ["k1", "k2", "k3", "k4"] {
        save_stand(&engine, stand::<2>(id)).await.unwrap();
    }
    let sources = pointer_ids_in_distinct_link_stripes();

    engine.start_recording_writes().await;
    let mut staged = Vec::new();
    for id in &sources {
        staged.push(staged_pointer(&engine, pointer(id, link("stands-2", "k1"))).await);
    }
    assert_eq!(
        fence_writes(&engine.take_recorded_writes().await),
        sources
            .iter()
            .map(|id| fence_stripe_key("stands-2", "k1", link_stripe(id)))
            .collect::<Vec<_>>()
    );
    for tx in staged {
        tx.commit().await.unwrap();
    }

    let low = pointer_id_whose_link_stripe("w", |stripe| stripe < 2);
    let high = pointer_id_whose_link_stripe("w", |stripe| stripe >= 2);
    let low_link = staged_pointer(&engine, pointer(&low, link("stands-2", "k2"))).await;
    let high_link = staged_pointer(&engine, pointer(&high, link("stands-2", "k2"))).await;
    save_stand(
        &engine,
        Stand::<2> {
            name: "Renamed".to_string(),
            ..stand("k2")
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        low_link.commit().await,
        Err(TitoError::Retryable(_))
    ));
    high_link.commit().await.unwrap();

    let removed = pointer_id_whose_link_stripe("v", |stripe| stripe >= 2);
    let pointing = staged_pointer(&engine, pointer(&removed, link("stands-2", "k3"))).await;
    assert!(engine
        .transaction(|tx| {
            let model = model.clone();
            async move { model.remove("k3", &tx).await }
        })
        .await
        .unwrap());
    assert!(matches!(
        pointing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert!(matches!(
        save_pointer(&engine, pointer(&removed, link("stands-2", "k3"))).await,
        Err(TitoError::ReferenceMissing { .. })
    ));

    let marked = pointer_id_whose_link_stripe("u", |stripe| stripe >= 2);
    let pointing = staged_pointer(&engine, pointer(&marked, link("stands-2", "k4"))).await;
    save_stand(
        &engine,
        Stand::<2> {
            deleting: true,
            ..stand("k4")
        },
    )
    .await
    .unwrap();
    assert!(matches!(
        pointing.commit().await,
        Err(TitoError::Retryable(_))
    ));
    assert!(matches!(
        save_pointer(&engine, pointer(&marked, link("stands-2", "k4"))).await,
        Err(TitoError::ReferenceDeleting { .. })
    ));
    assert!(engine.keys_with_prefix("fence:").await.is_empty());
}

#[tokio::test]
async fn a_link_to_a_table_no_model_was_built_for_writes_one_link_stripe() {
    let engine = engine();
    engine
        .put_json("table:unbuilt-targets:u1", &json!({"id": "u1"}))
        .await;
    engine.start_recording_writes().await;

    save_pointer(&engine, pointer("x1", link("unbuilt-targets", "u1")))
        .await
        .unwrap();

    assert_eq!(
        fence_writes(&engine.take_recorded_writes().await),
        vec![link_fence_key("unbuilt-targets", "u1", "pointers", "x1")]
    );
    assert_eq!(
        engine.keys_with_prefix("ref:").await,
        vec![reference_key(
            "unbuilt-targets",
            "u1",
            "pointers",
            "x1",
            "items.0"
        )]
    );
}

#[tokio::test]
async fn a_manifest_may_not_list_a_fence_stripe_key() {
    let engine = engine();
    let model = galleries(&engine);
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
    let reverse_key = "reverse-index:table:galleries:g1";
    let mut listed = engine.raw_json(reverse_key).await.unwrap();
    listed["value"]
        .as_array_mut()
        .unwrap()
        .push(json!(fence_stripe_key("galleries", "g1", 0)));
    engine.put_json(reverse_key, &listed).await;

    assert!(matches!(
        save_gallery(&engine, gallery("g1", &["p1"])).await,
        Err(TitoError::IndexError(_))
    ));
    assert!(matches!(
        remove_gallery(&engine, "g1").await,
        Err(TitoError::IndexError(_))
    ));
    let tx = engine.begin_transaction().await.unwrap();
    assert!(matches!(
        model.rebuild_indexes_for_restore("g1", &tx).await,
        Err(TitoError::IndexError(_))
    ));
    tx.rollback().await.unwrap();
    assert_eq!(engine.raw_json(reverse_key).await.unwrap(), listed);
}

#[tokio::test]
async fn referenced_by_except_leaves_out_the_listed_source_tables() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"])).await.unwrap();
    let mut covered = gallery("g2", &[]);
    covered.cover_id = Some("p1".to_string());
    save_gallery(&engine, covered).await.unwrap();
    save_pointer(
        &engine,
        pointer("x1", vec![TitoReference::new("pictures", "p1", "items.0")]),
    )
    .await
    .unwrap();
    put_incoming(&engine, "p1", "albums", "a1", "photos.0").await;
    put_incoming(&engine, "p1", "frames", "f1", "front").await;
    let album = TitoIncomingReference::new("albums", "a1", "photos.0");
    let frame = TitoIncomingReference::new("frames", "f1", "front");
    let listed = from_gallery("g1", "picture_ids.0");
    let cover = from_gallery("g2", "cover_id");
    let pointed = TitoIncomingReference::new("pointers", "x1", "items.0");
    let everyone = vec![
        album.clone(),
        frame.clone(),
        listed.clone(),
        cover.clone(),
        pointed.clone(),
    ];

    assert_eq!(
        picture_referenced_by(&engine, "p1").await.unwrap(),
        everyone
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &[], 10)
            .await
            .unwrap(),
        everyone
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &["galleries"], 10)
            .await
            .unwrap(),
        vec![album.clone(), frame.clone(), pointed.clone()]
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &["pointers", "albums"], 10)
            .await
            .unwrap(),
        vec![frame.clone(), listed.clone(), cover.clone()]
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &["galleries", "albums", "galleries"], 10)
            .await
            .unwrap(),
        vec![frame.clone(), pointed.clone()]
    );
    assert!(picture_referenced_by_except(
        &engine,
        "p1",
        &["pointers", "galleries", "frames", "albums"],
        10
    )
    .await
    .unwrap()
    .is_empty());
    assert_eq!(
        picture_referenced_by_except(
            &engine,
            "p1",
            &["gallerie", "pointer", "album", "missing"],
            10
        )
        .await
        .unwrap(),
        everyone
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &["frames"], 2)
            .await
            .unwrap(),
        vec![album.clone(), listed.clone()]
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &["albums"], 1)
            .await
            .unwrap(),
        vec![frame]
    );
    assert!(
        picture_referenced_by_except(&engine, "unused", &["galleries"], 10)
            .await
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn referenced_by_except_seeks_past_the_skipped_tables_without_reading_them() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"; 600]))
        .await
        .unwrap();
    save_pointer(
        &engine,
        pointer("x1", vec![TitoReference::new("pictures", "p1", "items.0")]),
    )
    .await
    .unwrap();
    put_incoming(&engine, "p1", "albums", "a1", "photos.0").await;
    engine
        .put_json(
            &reference_key("pictures", "p1", "galleries", "g0", "broken"),
            &json!({"table": "galleries", "id": "g9", "path": "broken"}),
        )
        .await;
    assert!(matches!(
        picture_referenced_by(&engine, "p1").await,
        Err(TitoError::IndexError(_))
    ));

    engine.start_recording_reads().await;
    let outside = picture_referenced_by_except(&engine, "p1", &["galleries"], 50)
        .await
        .unwrap();
    let reads = engine.take_recorded_reads().await;

    assert_eq!(
        outside,
        vec![
            TitoIncomingReference::new("albums", "a1", "photos.0"),
            TitoIncomingReference::new("pointers", "x1", "items.0"),
        ]
    );
    assert_eq!(
        reads,
        vec![
            b"scan:ref:pictures:p1:".to_vec(),
            b"scan:ref:pictures:p1:galleries;".to_vec(),
        ]
    );

    engine.start_recording_reads().await;
    let first = picture_referenced_by_except(&engine, "p1", &["galleries"], 1)
        .await
        .unwrap();
    let reads = engine.take_recorded_reads().await;

    assert_eq!(
        first,
        vec![TitoIncomingReference::new("albums", "a1", "photos.0")]
    );
    assert_eq!(reads, vec![b"scan:ref:pictures:p1:".to_vec()]);
}

#[tokio::test]
async fn referenced_by_except_pages_up_to_its_limit() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    save_gallery(&engine, gallery("g1", &["p1"; 600]))
        .await
        .unwrap();
    save_pointer(
        &engine,
        pointer("x1", vec![TitoReference::new("pictures", "p1", "items.0")]),
    )
    .await
    .unwrap();
    let mut paths: Vec<String> = (0..600)
        .map(|position| format!("picture_ids.{position}"))
        .collect();
    paths.sort();

    let page = picture_referenced_by_except(&engine, "p1", &["pointers"], 300)
        .await
        .unwrap();

    assert_eq!(
        page,
        paths[..300]
            .iter()
            .map(|path| from_gallery("g1", path))
            .collect::<Vec<_>>()
    );
    let all = picture_referenced_by_except(&engine, "p1", &["albums"], 1_000)
        .await
        .unwrap();
    assert_eq!(all.len(), 601);
    assert_eq!(
        all.last(),
        Some(&TitoIncomingReference::new("pointers", "x1", "items.0"))
    );
}

#[tokio::test]
async fn referenced_by_except_keeps_source_tables_with_separators_apart() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    put_incoming(&engine, "p1", "point", "x1", "items.0").await;
    put_incoming(&engine, "p1", "point:ers", "x2", "items.0").await;
    put_incoming(&engine, "p1", "pointers", "x3", "items.0").await;
    put_incoming(&engine, "p1", "point\\", "x4", "items.0").await;
    let point = TitoIncomingReference::new("point", "x1", "items.0");
    let colon = TitoIncomingReference::new("point:ers", "x2", "items.0");
    let pointers = TitoIncomingReference::new("pointers", "x3", "items.0");
    let backslash = TitoIncomingReference::new("point\\", "x4", "items.0");

    assert_eq!(
        picture_referenced_by(&engine, "p1").await.unwrap(),
        vec![
            point.clone(),
            colon.clone(),
            backslash.clone(),
            pointers.clone()
        ]
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &["point"], 10)
            .await
            .unwrap(),
        vec![colon.clone(), backslash.clone(), pointers.clone()]
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &["point:ers"], 10)
            .await
            .unwrap(),
        vec![point.clone(), backslash.clone(), pointers.clone()]
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &["point\\"], 10)
            .await
            .unwrap(),
        vec![point.clone(), colon.clone(), pointers]
    );
    assert_eq!(
        picture_referenced_by_except(&engine, "p1", &["pointers"], 10)
            .await
            .unwrap(),
        vec![point, colon, backslash]
    );
}

#[tokio::test]
async fn referenced_by_except_answers_who_else_points_while_remove_still_refuses() {
    let engine = engine();
    let model = galleries(&engine);
    save_gallery(&engine, gallery("g0", &[])).await.unwrap();
    for id in ["g1", "g2", "g3"] {
        let mut child = gallery(id, &[]);
        child.parent_id = Some("g0".to_string());
        save_gallery(&engine, child).await.unwrap();
    }

    let tx = engine.begin_transaction().await.unwrap();
    assert!(model
        .referenced_by_except("g0", &["galleries"], 20, &tx)
        .await
        .unwrap()
        .is_empty());
    tx.rollback().await.unwrap();

    save_pointer(
        &engine,
        pointer("x1", vec![TitoReference::new("galleries", "g0", "items.0")]),
    )
    .await
    .unwrap();
    let tx = engine.begin_transaction().await.unwrap();
    assert_eq!(
        model
            .referenced_by_except("g0", &["galleries"], 20, &tx)
            .await
            .unwrap(),
        vec![TitoIncomingReference::new("pointers", "x1", "items.0")]
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        remove_gallery(&engine, "g0").await.unwrap_err(),
        TitoError::Referenced {
            table: "galleries".to_string(),
            id: "g0".to_string(),
            by: vec![
                from_gallery("g1", "parent_id"),
                from_gallery("g2", "parent_id"),
                from_gallery("g3", "parent_id"),
                TitoIncomingReference::new("pointers", "x1", "items.0"),
            ],
        }
    );
}

#[tokio::test]
async fn referenced_by_except_validates_its_input() {
    let engine = engine();
    save_picture(&engine, picture("p1")).await.unwrap();
    let long_id = "x".repeat(513);
    let long_table = "t".repeat(513);

    for (id, source_tables, limit) in [
        ("", vec![], 10),
        (long_id.as_str(), vec![], 10),
        ("p1", vec![], 0),
        ("p1", vec![""], 10),
        ("p1", vec!["galleries", long_table.as_str()], 10),
    ] {
        assert!(matches!(
            picture_referenced_by_except(&engine, id, &source_tables, limit).await,
            Err(TitoError::InvalidInput(_))
        ));
    }
}
