use serde::{Deserialize, Serialize};
use std::net::SocketAddr;
use std::time::Duration;
use tito::backend::tikv::TiKV;
use tito::types::TitoIndexValue;
use tito::types::{
    TitoFindOneByIndexPayload, TitoIndexConfig, TitoIndexField, TitoIndexFieldType, TitoTransaction,
};
use tito::{TitoEngine, TitoError, TitoModelOptions, TitoModelTrait};
use uuid::Uuid;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeOwner {
    id: String,
    scope: String,
    slot: String,
    name: String,
}

impl TitoModelTrait for NativeOwner {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }
    fn unique_indexes(&self) -> Vec<TitoIndexConfig> {
        vec![TitoIndexConfig {
            value: TitoIndexValue::RecordReference,
            condition: true,
            name: "native-owner-slot".into(),
            fields: vec![
                TitoIndexField {
                    name: "scope".into(),
                    r#type: TitoIndexFieldType::String,
                },
                TitoIndexField {
                    name: "slot".into(),
                    r#type: TitoIndexFieldType::String,
                },
            ],
        }]
    }
    fn table() -> String {
        "native-batch-owners".into()
    }
    fn id(&self) -> String {
        self.id.clone()
    }
}

#[tokio::test]
async fn native_batch_swap_fences_every_source_and_commits_only_final_owners() {
    let endpoint = std::env::var("TITO_NATIVE_PD_URI").expect(
        "TITO_NATIVE_PD_URI must name the disposable PD endpoint owned by cargo arky test serve",
    );
    let address: SocketAddr = endpoint
        .parse()
        .expect("The owned PD endpoint must be an explicit loopback host:port");
    assert!(address.ip().is_loopback() && address.port() != 0);
    let run_id = std::env::var("TITO_NATIVE_RUN_ID")
        .expect("TITO_NATIVE_RUN_ID must name the disposable test serve run");
    assert!(
        run_id.starts_with("run-")
            && run_id.len() > 4
            && run_id.len() <= 64
            && run_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    );
    tokio::time::timeout(Duration::from_secs(30), async move {
        let engine = TiKV::connect(vec![endpoint]).await.unwrap();
        let model = engine
            .clone()
            .model::<NativeOwner>(TitoModelOptions::default());
        let scope = format!("{run_id}:{}", Uuid::new_v4());
        let a = NativeOwner {
            id: Uuid::new_v4().to_string(),
            scope: scope.clone(),
            slot: "a".into(),
            name: "first".into(),
        };
        let b = NativeOwner {
            id: Uuid::new_v4().to_string(),
            scope: scope.clone(),
            slot: "b".into(),
            name: "second".into(),
        };
        engine
            .transaction(|tx| {
                let model = model.clone();
                let a = a.clone();
                let b = b.clone();
                async move {
                    model.set(a).timestamps(false).execute(&tx).await?;
                    model.set(b).timestamps(false).execute(&tx).await?;
                    Ok::<_, TitoError>(())
                }
            })
            .await
            .unwrap();

        let stale = engine.begin_transaction().await.unwrap();
        model
            .replace_many(vec![
                (
                    a.clone(),
                    NativeOwner {
                        slot: b.slot.clone(),
                        ..a.clone()
                    },
                ),
                (
                    b.clone(),
                    NativeOwner {
                        slot: a.slot.clone(),
                        ..b.clone()
                    },
                ),
            ])
            .timestamps(false)
            .execute(&stale)
            .await
            .unwrap();
        let winner = NativeOwner {
            name: "newer source".into(),
            ..b.clone()
        };
        engine
            .transaction(|tx| {
                let model = model.clone();
                let winner = winner.clone();
                async move { model.set(winner).timestamps(false).execute(&tx).await }
            })
            .await
            .unwrap();
        assert!(matches!(stale.commit().await, Err(TitoError::Retryable(_))));
        assert_eq!(model.get(&a.id).execute(None).await.unwrap(), a);
        assert_eq!(model.get(&b.id).execute(None).await.unwrap(), winner);

        let refused = engine
            .transaction(|tx| {
                let model = model.clone();
                let a = a.clone();
                let b = b.clone();
                async move {
                    model
                        .replace_many(vec![(a.clone(), a), (b.clone(), b)])
                        .timestamps(false)
                        .execute(&tx)
                        .await
                }
            })
            .await
            .unwrap_err();
        assert!(matches!(refused, TitoError::UpdateFailed(_)));
        let next_a = NativeOwner {
            slot: b.slot.clone(),
            ..a.clone()
        };
        let next_b = NativeOwner {
            slot: a.slot.clone(),
            ..winner.clone()
        };
        let saved = engine
            .transaction(|tx| {
                let model = model.clone();
                let a = a.clone();
                let b = winner.clone();
                let next_a = next_a.clone();
                let next_b = next_b.clone();
                async move {
                    model
                        .replace_many(vec![(a, next_a), (b, next_b)])
                        .timestamps(false)
                        .execute(&tx)
                        .await
                }
            })
            .await
            .unwrap();
        assert_eq!(saved, vec![next_a.clone(), next_b.clone()]);
        for expected in [&next_a, &next_b] {
            assert_eq!(
                model
                    .find_one_by_unique_index(
                        TitoFindOneByIndexPayload {
                            index: "native-owner-slot".into(),
                            values: vec![scope.clone(), expected.slot.clone()],
                        },
                        None
                    )
                    .await
                    .unwrap(),
                *expected
            );
        }
        let first = model.get_versioned(&a.id, None).await.unwrap();
        let second = model.get_versioned(&b.id, None).await.unwrap();
        assert_eq!(first.version, second.version);
    })
    .await
    .expect("The native batch conflict and final swap must finish within their owned run");
}
