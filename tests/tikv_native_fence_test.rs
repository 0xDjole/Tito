mod native;

use native::{native_endpoint, native_engine, remove, save};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tito::types::{
    TitoFindOneByIndexPayload, TitoIndexConfig, TitoIndexField, TitoIndexFieldType,
    TitoTransaction,
};
use tito::{TitoEngine, TitoError, TitoModelOptions, TitoModelTrait, TitoReference};
use uuid::Uuid;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeTarget {
    id: String,
    name: String,
    deleting: bool,
}

impl TitoModelTrait for NativeTarget {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }

    fn references(&self) -> Vec<TitoReference> {
        Vec::new()
    }

    fn is_deleting(&self) -> bool {
        self.deleting
    }

    fn table() -> String {
        "native-fence-targets".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativePointer {
    id: String,
    target_id: String,
}

impl TitoModelTrait for NativePointer {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }

    fn references(&self) -> Vec<TitoReference> {
        vec![TitoReference::new(
            "native-fence-targets",
            self.target_id.clone(),
            "target_id",
        )]
    }

    fn is_deleting(&self) -> bool {
        false
    }

    fn table() -> String {
        "native-fence-pointers".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeClaim {
    id: String,
    scope: String,
    value: String,
}

impl TitoModelTrait for NativeClaim {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }

    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            condition: true,
            name: "native-claim-value".into(),
            fields: vec![
                TitoIndexField {
                    name: "scope".into(),
                    r#type: TitoIndexFieldType::String,
                },
                TitoIndexField {
                    name: "value".into(),
                    r#type: TitoIndexFieldType::String,
                },
            ],
        }]
    }

    fn references(&self) -> Vec<TitoReference> {
        Vec::new()
    }

    fn is_deleting(&self) -> bool {
        false
    }

    fn table() -> String {
        "native-fence-claims".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

fn record_id(run_id: &str) -> String {
    format!("{run_id}-{}", Uuid::new_v4())
}

fn target(run_id: &str) -> NativeTarget {
    NativeTarget {
        id: record_id(run_id),
        name: "target".into(),
        deleting: false,
    }
}

fn pointer(run_id: &str, target: &NativeTarget) -> NativePointer {
    NativePointer {
        id: record_id(run_id),
        target_id: target.id.clone(),
    }
}

#[tokio::test]
async fn native_a_new_pointer_conflicts_with_a_concurrent_removal_of_its_target() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let targets = engine
            .clone()
            .model::<NativeTarget>(TitoModelOptions::default());
        let pointers = engine
            .clone()
            .model::<NativePointer>(TitoModelOptions::default());
        let target = save(&engine, &targets, target(&run_id)).await.unwrap();
        let pointer = pointer(&run_id, &target);

        let pointing = engine.begin_transaction().await.unwrap();
        pointers
            .set(pointer.clone())
            .timestamps(false)
            .execute(&pointing)
            .await
            .unwrap();
        assert!(remove(&engine, &targets, &target.id).await.unwrap());
        assert!(matches!(
            pointing.commit().await,
            Err(TitoError::Retryable(_))
        ));

        assert!(matches!(
            save(&engine, &pointers, pointer.clone()).await,
            Err(TitoError::ReferenceMissing { .. })
        ));
        assert!(matches!(
            pointers.get(&pointer.id).execute(None).await,
            Err(TitoError::NotFound(_))
        ));
        assert!(matches!(
            targets.get(&target.id).execute(None).await,
            Err(TitoError::NotFound(_))
        ));
    })
    .await
    .expect("The native pointer and removal race must finish within its owned run");
}

#[tokio::test]
async fn native_a_new_pointer_conflicts_with_a_concurrent_deleting_mark_on_its_target() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let targets = engine
            .clone()
            .model::<NativeTarget>(TitoModelOptions::default());
        let pointers = engine
            .clone()
            .model::<NativePointer>(TitoModelOptions::default());
        let target = save(&engine, &targets, target(&run_id)).await.unwrap();
        let pointer = pointer(&run_id, &target);

        let pointing = engine.begin_transaction().await.unwrap();
        pointers
            .set(pointer.clone())
            .timestamps(false)
            .execute(&pointing)
            .await
            .unwrap();
        let deleting = save(
            &engine,
            &targets,
            NativeTarget {
                deleting: true,
                ..target.clone()
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            pointing.commit().await,
            Err(TitoError::Retryable(_))
        ));

        assert!(matches!(
            save(&engine, &pointers, pointer.clone()).await,
            Err(TitoError::ReferenceDeleting { .. })
        ));
        assert!(matches!(
            pointers.get(&pointer.id).execute(None).await,
            Err(TitoError::NotFound(_))
        ));
        assert!(remove(&engine, &targets, &deleting.id).await.unwrap());
    })
    .await
    .expect("The native pointer and deleting-mark race must finish within its owned run");
}

#[tokio::test]
async fn native_two_new_pointers_to_one_target_conflict_and_the_retry_keeps_both() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let targets = engine
            .clone()
            .model::<NativeTarget>(TitoModelOptions::default());
        let pointers = engine
            .clone()
            .model::<NativePointer>(TitoModelOptions::default());
        let target = save(&engine, &targets, target(&run_id)).await.unwrap();
        let first = pointer(&run_id, &target);
        let second = pointer(&run_id, &target);

        let first_writer = engine.begin_transaction().await.unwrap();
        let second_writer = engine.begin_transaction().await.unwrap();
        pointers
            .set(first.clone())
            .timestamps(false)
            .execute(&first_writer)
            .await
            .unwrap();
        pointers
            .set(second.clone())
            .timestamps(false)
            .execute(&second_writer)
            .await
            .unwrap();
        first_writer.commit().await.unwrap();
        assert!(matches!(
            second_writer.commit().await,
            Err(TitoError::Retryable(_))
        ));

        save(&engine, &pointers, second.clone()).await.unwrap();
        let mut sources = engine
            .transaction(|tx| {
                let targets = targets.clone();
                let id = target.id.clone();
                async move { targets.referenced_by(&id, &tx).await }
            })
            .await
            .unwrap()
            .into_iter()
            .map(|reference| reference.id)
            .collect::<Vec<_>>();
        sources.sort();
        let mut expected = vec![first.id.clone(), second.id.clone()];
        expected.sort();
        assert_eq!(sources, expected);
        assert!(matches!(
            remove(&engine, &targets, &target.id).await,
            Err(TitoError::Referenced { .. })
        ));

        assert!(remove(&engine, &pointers, &first.id).await.unwrap());
        assert!(remove(&engine, &pointers, &second.id).await.unwrap());
        assert!(remove(&engine, &targets, &target.id).await.unwrap());
    })
    .await
    .expect("The native concurrent pointers must finish within their owned run");
}

#[tokio::test]
async fn native_assert_current_conflicts_with_a_concurrent_update_in_either_order() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let targets = engine
            .clone()
            .model::<NativeTarget>(TitoModelOptions::default());
        let target = save(&engine, &targets, target(&run_id)).await.unwrap();

        let fenced = engine.begin_transaction().await.unwrap();
        targets.assert_current(&target.id, &fenced).await.unwrap();
        let renamed = save(
            &engine,
            &targets,
            NativeTarget {
                name: "renamed by a committed writer".into(),
                ..target.clone()
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
        targets
            .assert_current(&target.id, &fence_first)
            .await
            .unwrap();
        targets
            .set(NativeTarget {
                name: "renamed by a late writer".into(),
                ..target.clone()
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

        assert_eq!(
            targets.get(&target.id).execute(None).await.unwrap(),
            renamed
        );
        assert!(remove(&engine, &targets, &target.id).await.unwrap());
    })
    .await
    .expect("The native assert_current races must finish within their owned run");
}

#[tokio::test]
async fn native_assert_version_conflicts_with_a_concurrent_update_and_then_reports_the_change() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let targets = engine
            .clone()
            .model::<NativeTarget>(TitoModelOptions::default());
        let target = save(&engine, &targets, target(&run_id)).await.unwrap();
        let version = targets
            .get_versioned(&target.id, None)
            .await
            .unwrap()
            .version;

        let fenced = engine.begin_transaction().await.unwrap();
        assert!(targets
            .assert_version(&target.id, version, &fenced)
            .await
            .unwrap());
        save(
            &engine,
            &targets,
            NativeTarget {
                name: "renamed".into(),
                ..target.clone()
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            fenced.commit().await,
            Err(TitoError::Retryable(_))
        ));

        let current = engine.begin_transaction().await.unwrap();
        assert!(!targets
            .assert_version(&target.id, version, &current)
            .await
            .unwrap());
        current.commit().await.unwrap();
        assert_ne!(
            targets
                .get_versioned(&target.id, None)
                .await
                .unwrap()
                .version,
            version
        );
        assert!(remove(&engine, &targets, &target.id).await.unwrap());
    })
    .await
    .expect("The native assert_version race must finish within its owned run");
}

#[tokio::test]
async fn native_concurrent_claims_of_one_unique_value_conflict_and_the_retry_is_refused() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let claims = engine
            .clone()
            .model::<NativeClaim>(TitoModelOptions::default());
        let scope = record_id(&run_id);
        let first = NativeClaim {
            id: record_id(&run_id),
            scope: scope.clone(),
            value: "asset-7".into(),
        };
        let second = NativeClaim {
            id: record_id(&run_id),
            scope: scope.clone(),
            value: "asset-7".into(),
        };

        let first_writer = engine.begin_transaction().await.unwrap();
        let second_writer = engine.begin_transaction().await.unwrap();
        claims
            .set(first.clone())
            .timestamps(false)
            .execute(&first_writer)
            .await
            .unwrap();
        claims
            .set(second.clone())
            .timestamps(false)
            .execute(&second_writer)
            .await
            .unwrap();
        first_writer.commit().await.unwrap();
        assert!(matches!(
            second_writer.commit().await,
            Err(TitoError::Retryable(_))
        ));

        assert!(matches!(
            save(&engine, &claims, second.clone()).await,
            Err(TitoError::UniqueViolation { .. })
        ));
        assert_eq!(
            claims
                .find_one_by_unique_index(
                    TitoFindOneByIndexPayload {
                        index: "native-claim-value".into(),
                        values: vec![scope.clone(), "asset-7".into()],
                    },
                    None,
                )
                .await
                .unwrap(),
            first
        );
        assert!(matches!(
            claims.get(&second.id).execute(None).await,
            Err(TitoError::NotFound(_))
        ));
        assert!(remove(&engine, &claims, &first.id).await.unwrap());
    })
    .await
    .expect("The native unique claim race must finish within its owned run");
}

#[tokio::test]
async fn native_unique_values_differing_only_by_case_or_spacing_coexist_and_are_found_exactly() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let claims = engine
            .clone()
            .model::<NativeClaim>(TitoModelOptions::default());
        let scope = record_id(&run_id);
        let claimed = ["Asset-7", "asset-7", "ASSET-7", " asset-7 ", "asset:7", "asset\\7"]
            .into_iter()
            .map(|value| NativeClaim {
                id: record_id(&run_id),
                scope: scope.clone(),
                value: value.into(),
            })
            .collect::<Vec<_>>();
        for claim in &claimed {
            save(&engine, &claims, claim.clone()).await.unwrap();
        }

        for claim in &claimed {
            assert_eq!(
                claims
                    .find_one_by_unique_index(
                        TitoFindOneByIndexPayload {
                            index: "native-claim-value".into(),
                            values: vec![scope.clone(), claim.value.clone()],
                        },
                        None,
                    )
                    .await
                    .unwrap(),
                *claim
            );
        }
        assert!(matches!(
            claims
                .find_one_by_unique_index(
                    TitoFindOneByIndexPayload {
                        index: "native-claim-value".into(),
                        values: vec![scope.clone(), "aSSET-7".into()],
                    },
                    None,
                )
                .await,
            Err(TitoError::NotFound(_))
        ));
        for claim in &claimed {
            assert!(remove(&engine, &claims, &claim.id).await.unwrap());
        }
    })
    .await
    .expect("The native exact unique values must finish within their owned run");
}
