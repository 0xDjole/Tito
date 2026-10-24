mod native;

use native::{native_endpoint, native_engine, remove, save};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tito::backend::tikv::{TiKVBackend, TiKVTransaction};
use tito::types::{
    TitoFindOneByIndexPayload, TitoIndexConfig, TitoIndexField, TitoIndexFieldType,
    TitoModelConstraints, TitoTransaction,
};
use tito::{
    encode_index_string, TitoEngine, TitoError, TitoModel, TitoModelOptions, TitoModelTrait,
    TitoReference,
};
use uuid::Uuid;

const STRIPES: u16 = 16;
const LINK_STRIPES: u64 = 16;

fn fnv1a_64(bytes: &[u8]) -> u64 {
    bytes.iter().fold(0xcbf2_9ce4_8422_2325, |hash, byte| {
        (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3)
    })
}

fn link_stripe(table: &str, id: &str) -> u64 {
    let source = format!("{}:{}", encode_index_string(table), encode_index_string(id));
    fnv1a_64(source.as_bytes()) % LINK_STRIPES
}

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
struct NativeStriped {
    id: String,
    name: String,
    deleting: bool,
}

impl TitoModelTrait for NativeStriped {
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
        "native-fence-striped".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }

    fn fence_stripes() -> u16 {
        STRIPES
    }
}

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativePointer {
    id: String,
    target_table: String,
    target_id: String,
}

impl TitoModelTrait for NativePointer {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }

    fn references(&self) -> Vec<TitoReference> {
        vec![TitoReference::new(
            self.target_table.clone(),
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

fn striped(run_id: &str) -> NativeStriped {
    NativeStriped {
        id: record_id(run_id),
        name: "striped".into(),
        deleting: false,
    }
}

fn pointer(run_id: &str, target_table: &str, target_id: &str) -> NativePointer {
    NativePointer {
        id: record_id(run_id),
        target_table: target_table.into(),
        target_id: target_id.into(),
    }
}

fn pointer_in_link_stripe(
    run_id: &str,
    target_table: &str,
    target_id: &str,
    wanted: impl Fn(u64) -> bool,
) -> NativePointer {
    for _ in 0..1_000 {
        let candidate = pointer(run_id, target_table, target_id);
        if wanted(link_stripe(&NativePointer::table(), &candidate.id)) {
            return candidate;
        }
    }
    panic!("No pointer id within 1000 draws landed in the wanted link stripe");
}

fn shared_stripe(tx: &TiKVTransaction) -> u64 {
    tx.shared_fence_pick() % u64::from(STRIPES)
}

async fn transaction_in_shared_stripe(
    engine: &TiKVBackend,
    wanted: impl Fn(u64) -> bool,
) -> TiKVTransaction {
    for _ in 0..1_000 {
        let tx = engine.begin_transaction().await.unwrap();
        if wanted(shared_stripe(&tx)) {
            return tx;
        }
        tx.rollback().await.unwrap();
    }
    panic!("No transaction within 1000 begins picked the wanted shared stripe");
}

async fn sources_of<T: TitoModelConstraints>(
    engine: &TiKVBackend,
    targets: &TitoModel<TiKVBackend, T>,
    id: &str,
) -> Vec<String> {
    let mut sources = engine
        .transaction(|tx| {
            let targets = targets.clone();
            let id = id.to_string();
            async move { targets.referenced_by(&id, &tx).await }
        })
        .await
        .unwrap()
        .into_iter()
        .map(|reference| reference.id)
        .collect::<Vec<_>>();
    sources.sort();
    sources
}

#[tokio::test]
async fn native_a_new_pointer_to_a_one_stripe_target_conflicts_with_its_concurrent_removal() {
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
        let pointer = pointer(&run_id, &NativeTarget::table(), &target.id);

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
async fn native_a_new_pointer_to_a_one_stripe_target_conflicts_with_its_concurrent_deleting_mark() {
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
        let pointer = pointer(&run_id, &NativeTarget::table(), &target.id);

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
async fn native_new_pointers_to_a_one_stripe_target_from_sources_in_different_stripes_commit_together(
) {
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
        let first = pointer(&run_id, &NativeTarget::table(), &target.id);
        let first_stripe = link_stripe(&NativePointer::table(), &first.id);
        let second =
            pointer_in_link_stripe(&run_id, &NativeTarget::table(), &target.id, |stripe| {
                stripe != first_stripe
            });

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
        second_writer.commit().await.unwrap();

        let mut expected = vec![first.id.clone(), second.id.clone()];
        expected.sort();
        assert_eq!(sources_of(&engine, &targets, &target.id).await, expected);
        assert!(matches!(
            remove(&engine, &targets, &target.id).await,
            Err(TitoError::Referenced { .. })
        ));

        assert!(remove(&engine, &pointers, &first.id).await.unwrap());
        assert!(remove(&engine, &pointers, &second.id).await.unwrap());
        assert!(remove(&engine, &targets, &target.id).await.unwrap());
    })
    .await
    .expect("The native one-stripe pointers in different stripes must finish within their run");
}

#[tokio::test]
async fn native_new_pointers_to_a_one_stripe_target_from_sources_in_one_stripe_conflict_and_the_retry_keeps_both(
) {
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
        let first = pointer(&run_id, &NativeTarget::table(), &target.id);
        let first_stripe = link_stripe(&NativePointer::table(), &first.id);
        let second =
            pointer_in_link_stripe(&run_id, &NativeTarget::table(), &target.id, |stripe| {
                stripe == first_stripe
            });

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
        let mut expected = vec![first.id.clone(), second.id.clone()];
        expected.sort();
        assert_eq!(sources_of(&engine, &targets, &target.id).await, expected);
        assert!(matches!(
            remove(&engine, &targets, &target.id).await,
            Err(TitoError::Referenced { .. })
        ));

        assert!(remove(&engine, &pointers, &first.id).await.unwrap());
        assert!(remove(&engine, &pointers, &second.id).await.unwrap());
        assert!(remove(&engine, &targets, &target.id).await.unwrap());
    })
    .await
    .expect("The native one-stripe pointers in one stripe must finish within their owned run");
}

#[tokio::test]
async fn native_one_stripe_shared_checks_conflict_with_each_other_and_with_an_update() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let targets = engine
            .clone()
            .model::<NativeTarget>(TitoModelOptions::default());
        let target = save(&engine, &targets, target(&run_id)).await.unwrap();

        let first = engine.begin_transaction().await.unwrap();
        let second = engine.begin_transaction().await.unwrap();
        targets
            .assert_current_shared(&target.id, &first)
            .await
            .unwrap();
        targets
            .assert_current_shared(&target.id, &second)
            .await
            .unwrap();
        first.commit().await.unwrap();
        assert!(matches!(
            second.commit().await,
            Err(TitoError::Retryable(_))
        ));

        let checked = engine.begin_transaction().await.unwrap();
        targets
            .assert_current_shared(&target.id, &checked)
            .await
            .unwrap();
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
            checked.commit().await,
            Err(TitoError::Retryable(_))
        ));

        assert_eq!(
            targets.get(&target.id).execute(None).await.unwrap(),
            renamed
        );
        assert!(remove(&engine, &targets, &target.id).await.unwrap());
    })
    .await
    .expect("The native one-stripe shared checks must finish within their owned run");
}

#[tokio::test]
async fn native_a_new_pointer_to_a_sixteen_stripe_target_conflicts_with_its_concurrent_removal_and_deleting_mark(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let stripeds = engine
            .clone()
            .model::<NativeStriped>(TitoModelOptions::default());
        let pointers = engine
            .clone()
            .model::<NativePointer>(TitoModelOptions::default());
        let removed = save(&engine, &stripeds, striped(&run_id)).await.unwrap();
        let pointer_to_removed = pointer(&run_id, &NativeStriped::table(), &removed.id);

        let pointing = engine.begin_transaction().await.unwrap();
        pointers
            .set(pointer_to_removed.clone())
            .timestamps(false)
            .execute(&pointing)
            .await
            .unwrap();
        assert!(remove(&engine, &stripeds, &removed.id).await.unwrap());
        assert!(matches!(
            pointing.commit().await,
            Err(TitoError::Retryable(_))
        ));
        assert!(matches!(
            save(&engine, &pointers, pointer_to_removed.clone()).await,
            Err(TitoError::ReferenceMissing { .. })
        ));

        let marked = save(&engine, &stripeds, striped(&run_id)).await.unwrap();
        let pointer_to_marked = pointer(&run_id, &NativeStriped::table(), &marked.id);
        let pointing = engine.begin_transaction().await.unwrap();
        pointers
            .set(pointer_to_marked.clone())
            .timestamps(false)
            .execute(&pointing)
            .await
            .unwrap();
        let deleting = save(
            &engine,
            &stripeds,
            NativeStriped {
                deleting: true,
                ..marked.clone()
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            pointing.commit().await,
            Err(TitoError::Retryable(_))
        ));
        assert!(matches!(
            save(&engine, &pointers, pointer_to_marked.clone()).await,
            Err(TitoError::ReferenceDeleting { .. })
        ));

        for pointer in [&pointer_to_removed, &pointer_to_marked] {
            assert!(matches!(
                pointers.get(&pointer.id).execute(None).await,
                Err(TitoError::NotFound(_))
            ));
        }
        assert!(remove(&engine, &stripeds, &deleting.id).await.unwrap());
    })
    .await
    .expect("The native pointers to a sixteen-stripe target must finish within their owned run");
}

#[tokio::test]
async fn native_new_pointers_to_a_sixteen_stripe_target_from_sources_in_different_stripes_commit_together(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let stripeds = engine
            .clone()
            .model::<NativeStriped>(TitoModelOptions::default());
        let pointers = engine
            .clone()
            .model::<NativePointer>(TitoModelOptions::default());
        let target = save(&engine, &stripeds, striped(&run_id)).await.unwrap();
        let first = pointer(&run_id, &NativeStriped::table(), &target.id);
        let first_stripe = link_stripe(&NativePointer::table(), &first.id);
        let second =
            pointer_in_link_stripe(&run_id, &NativeStriped::table(), &target.id, |stripe| {
                stripe != first_stripe
            });

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
        second_writer.commit().await.unwrap();

        let mut expected = vec![first.id.clone(), second.id.clone()];
        expected.sort();
        assert_eq!(sources_of(&engine, &stripeds, &target.id).await, expected);
        assert!(matches!(
            remove(&engine, &stripeds, &target.id).await,
            Err(TitoError::Referenced { .. })
        ));

        assert!(remove(&engine, &pointers, &first.id).await.unwrap());
        assert!(remove(&engine, &pointers, &second.id).await.unwrap());
        assert!(remove(&engine, &stripeds, &target.id).await.unwrap());
    })
    .await
    .expect("The native pointers in different stripes must finish within their owned run");
}

#[tokio::test]
async fn native_new_pointers_to_a_sixteen_stripe_target_from_sources_in_one_stripe_conflict_and_the_retry_keeps_both(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let stripeds = engine
            .clone()
            .model::<NativeStriped>(TitoModelOptions::default());
        let pointers = engine
            .clone()
            .model::<NativePointer>(TitoModelOptions::default());
        let target = save(&engine, &stripeds, striped(&run_id)).await.unwrap();
        let first = pointer(&run_id, &NativeStriped::table(), &target.id);
        let first_stripe = link_stripe(&NativePointer::table(), &first.id);
        let second =
            pointer_in_link_stripe(&run_id, &NativeStriped::table(), &target.id, |stripe| {
                stripe == first_stripe
            });

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
        let mut expected = vec![first.id.clone(), second.id.clone()];
        expected.sort();
        assert_eq!(sources_of(&engine, &stripeds, &target.id).await, expected);
        assert!(matches!(
            remove(&engine, &stripeds, &target.id).await,
            Err(TitoError::Referenced { .. })
        ));

        assert!(remove(&engine, &pointers, &first.id).await.unwrap());
        assert!(remove(&engine, &pointers, &second.id).await.unwrap());
        assert!(remove(&engine, &stripeds, &target.id).await.unwrap());
    })
    .await
    .expect("The native pointers in one stripe must finish within their owned run");
}

#[tokio::test]
async fn native_shared_checks_on_twenty_sixteen_stripe_records_from_transactions_in_different_stripes_commit_together(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let stripeds = engine
            .clone()
            .model::<NativeStriped>(TitoModelOptions::default());
        let mut records = Vec::new();
        for _ in 0..20 {
            records.push(save(&engine, &stripeds, striped(&run_id)).await.unwrap());
        }

        let first = engine.begin_transaction().await.unwrap();
        let first_stripe = shared_stripe(&first);
        let second = transaction_in_shared_stripe(&engine, |stripe| stripe != first_stripe).await;
        for record in &records {
            for checking in [&first, &second] {
                stripeds
                    .assert_current_shared(&record.id, checking)
                    .await
                    .unwrap();
            }
        }
        first.commit().await.unwrap();
        second.commit().await.unwrap();

        for record in &records {
            assert_eq!(
                stripeds.get(&record.id).execute(None).await.unwrap(),
                *record
            );
            assert!(remove(&engine, &stripeds, &record.id).await.unwrap());
        }
    })
    .await
    .expect("The native shared checks in different stripes must finish within their owned run");
}

#[tokio::test]
async fn native_shared_checks_on_a_sixteen_stripe_record_from_transactions_in_one_stripe_conflict_and_the_retry_passes(
) {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let stripeds = engine
            .clone()
            .model::<NativeStriped>(TitoModelOptions::default());
        let record = save(&engine, &stripeds, striped(&run_id)).await.unwrap();

        let first = engine.begin_transaction().await.unwrap();
        let first_stripe = shared_stripe(&first);
        let second = transaction_in_shared_stripe(&engine, |stripe| stripe == first_stripe).await;
        stripeds
            .assert_current_shared(&record.id, &first)
            .await
            .unwrap();
        stripeds
            .assert_current_shared(&record.id, &second)
            .await
            .unwrap();
        first.commit().await.unwrap();
        assert!(matches!(
            second.commit().await,
            Err(TitoError::Retryable(_))
        ));

        engine
            .transaction(|tx| {
                let stripeds = stripeds.clone();
                let id = record.id.clone();
                async move { stripeds.assert_current_shared(&id, &tx).await }
            })
            .await
            .unwrap();
        assert_eq!(
            stripeds.get(&record.id).execute(None).await.unwrap(),
            record
        );
        assert!(remove(&engine, &stripeds, &record.id).await.unwrap());
    })
    .await
    .expect("The native shared checks in one stripe must finish within their owned run");
}

#[tokio::test]
async fn native_a_sixteen_stripe_shared_check_conflicts_with_an_update_in_either_order() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let stripeds = engine
            .clone()
            .model::<NativeStriped>(TitoModelOptions::default());
        let record = save(&engine, &stripeds, striped(&run_id)).await.unwrap();

        let checked = engine.begin_transaction().await.unwrap();
        stripeds
            .assert_current_shared(&record.id, &checked)
            .await
            .unwrap();
        let deleting = save(
            &engine,
            &stripeds,
            NativeStriped {
                deleting: true,
                ..record.clone()
            },
        )
        .await
        .unwrap();
        assert!(matches!(
            checked.commit().await,
            Err(TitoError::Retryable(_))
        ));

        let check_first = engine.begin_transaction().await.unwrap();
        let late_writer = engine.begin_transaction().await.unwrap();
        stripeds
            .assert_current_shared(&record.id, &check_first)
            .await
            .unwrap();
        stripeds
            .set(NativeStriped {
                name: "renamed by a late writer".into(),
                ..deleting.clone()
            })
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
            stripeds.get(&record.id).execute(None).await.unwrap(),
            deleting
        );
        assert!(remove(&engine, &stripeds, &record.id).await.unwrap());
    })
    .await
    .expect("The native sixteen-stripe shared check must finish within its owned run");
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
        let claimed = [
            "Asset-7",
            "asset-7",
            "ASSET-7",
            " asset-7 ",
            "asset:7",
            "asset\\7",
        ]
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
