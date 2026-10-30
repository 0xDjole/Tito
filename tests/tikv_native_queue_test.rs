mod native;

use futures::future::BoxFuture;
use native::{native_endpoint, native_engine, remove, save};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tito::backend::tikv::{TiKVBackend, TiKVTransaction};
use tito::types::{TitoIndexConfig, TitoTransaction};
use tito::{
    run_cluster_worker, ClusterWorkerConfig, Queue, QueueConfig, QueueEvent, QueueEventStatus,
    QueueHandlerOutcome, QueueHandlerResult, QueueOwner, TitoEngine, TitoError, TitoModel,
    TitoModelOptions, TitoModelTrait,
};
use tokio::sync::{broadcast, Mutex, Notify};
use uuid::Uuid;

#[derive(Default, Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeTally {
    id: String,
    value: i64,
}

impl TitoModelTrait for NativeTally {
    fn indexes(&self) -> Vec<TitoIndexConfig> {
        Vec::new()
    }

    fn table() -> String {
        "native-queue-tallies".into()
    }

    fn id(&self) -> String {
        self.id.clone()
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct NativeChange {
    value: i64,
}

type NativeHandlerFuture = BoxFuture<'static, QueueHandlerResult<NativeChange>>;

fn record_id(run_id: &str) -> String {
    format!("{run_id}-{}", Uuid::new_v4())
}

fn native_queue(engine: &TiKVBackend) -> Queue<TiKVBackend> {
    Queue::new(
        engine.clone(),
        QueueConfig::new(1, Duration::from_secs(60 * 60))
            .with_name(format!("native-ack-{}", Uuid::new_v4().simple()))
            .unwrap(),
    )
}

async fn new_tally(
    engine: &TiKVBackend,
    tallies: &TitoModel<TiKVBackend, NativeTally>,
    run_id: &str,
) -> String {
    let id = record_id(run_id);
    save(
        engine,
        tallies,
        NativeTally {
            id: id.clone(),
            value: 0,
        },
    )
    .await
    .unwrap();
    id
}

async fn tally_value(tallies: &TitoModel<TiKVBackend, NativeTally>, id: &str) -> i64 {
    tallies.get(id).execute(None).await.unwrap().value
}

async fn publish_change(
    queue: &Queue<TiKVBackend>,
    owner: &QueueOwner,
) -> (String, QueueEvent<NativeChange>) {
    let event = QueueEvent::new(
        format!("native-ack:{}", owner.id),
        NativeChange { value: 7 },
        chrono::Utc::now().timestamp_millis() - 1_000,
    )
    .with_owner(owner.clone());
    let event_id = event.id.clone();
    queue.publish(event).await.unwrap();
    queue
        .scan_by_status::<NativeChange>(QueueEventStatus::Pending, None, 10)
        .await
        .unwrap()
        .events
        .into_iter()
        .find(|(_, event)| event.id == event_id)
        .unwrap()
}

async fn apply_change(
    queue: &Queue<TiKVBackend>,
    tallies: &TitoModel<TiKVBackend, NativeTally>,
    event: &QueueEvent<NativeChange>,
    tally_id: &str,
    tx: &TiKVTransaction,
) -> Result<bool, TitoError> {
    if !queue.ack_in_tx(event, tx).await? {
        return Ok(false);
    }
    let mut tally = tallies.get(tally_id).execute(Some(tx)).await?;
    tally.value += event.payload.value;
    tallies.set(tally).timestamps(false).execute(tx).await?;
    Ok(true)
}

async fn assert_completed_once(
    queue: &Queue<TiKVBackend>,
    owner: &QueueOwner,
    published: &QueueEvent<NativeChange>,
) {
    let completed = queue
        .scan_by_status::<NativeChange>(QueueEventStatus::Completed, None, 10)
        .await
        .unwrap()
        .events;
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].1.id, published.id);
    assert_eq!(completed[0].1.key, published.key);
    assert_eq!(completed[0].1.payload, published.payload);
    assert_eq!(completed[0].1.status, QueueEventStatus::Completed);
    assert!(queue
        .scan_by_status::<NativeChange>(QueueEventStatus::Pending, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
    assert!(queue
        .scan_by_owner::<NativeChange>(owner, QueueEventStatus::Pending, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
    assert_eq!(
        queue
            .scan_by_owner::<NativeChange>(owner, QueueEventStatus::Completed, None, 10)
            .await
            .unwrap()
            .events,
        completed
    );
}

#[tokio::test]
async fn native_a_queue_handler_completes_its_own_pending_row_inside_its_transaction() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let queue = Arc::new(native_queue(&engine));
        let tallies = engine
            .clone()
            .model::<NativeTally>(TitoModelOptions::default());
        let tally_id = new_tally(&engine, &tallies, &run_id).await;
        let owner = QueueOwner::new("store", record_id(&run_id)).unwrap();
        let (storage_key, published) = publish_change(&queue, &owner).await;
        let observed = Arc::new(Mutex::new(Vec::new()));
        let handled = Arc::new(Notify::new());
        let handler_engine = engine.clone();
        let handler_queue = queue.clone();
        let handler_tallies = tallies.clone();
        let handler_observed = observed.clone();
        let handler_handled = handled.clone();
        let handler_tally_id = tally_id.clone();
        let handler = move |event: QueueEvent<NativeChange>| -> NativeHandlerFuture {
            let engine = handler_engine.clone();
            let queue = handler_queue.clone();
            let tallies = handler_tallies.clone();
            let observed = handler_observed.clone();
            let handled = handler_handled.clone();
            let tally_id = handler_tally_id.clone();
            Box::pin(async move {
                let applied = engine
                    .transaction(|tx| {
                        let queue = queue.clone();
                        let tallies = tallies.clone();
                        let event = event.clone();
                        let tally_id = tally_id.clone();
                        async move { apply_change(&queue, &tallies, &event, &tally_id, &tx).await }
                    })
                    .await?;
                let pending = queue
                    .scan_by_status::<NativeChange>(QueueEventStatus::Pending, None, 10)
                    .await?
                    .events
                    .len();
                let completed = queue
                    .scan_by_status::<NativeChange>(QueueEventStatus::Completed, None, 10)
                    .await?
                    .events
                    .len();
                observed.lock().await.push((
                    event.pending_key.clone(),
                    applied,
                    pending,
                    completed,
                ));
                handled.notify_one();
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        };
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
        let mut config = ClusterWorkerConfig::new(record_id(&run_id));
        config.heartbeat_interval = Duration::from_millis(20);
        config.poll_interval = Duration::from_millis(20);
        let handle = run_cluster_worker(queue.clone(), config, handler, shutdown_rx).await;

        handled.notified().await;
        let _ = shutdown_tx.send(());
        handle.await.unwrap();

        assert_eq!(
            *observed.lock().await,
            vec![(Some(storage_key.clone()), true, 0, 1)]
        );
        queue.ack(&storage_key).await.unwrap();
        assert_completed_once(&queue, &owner, &published).await;
        assert_eq!(tally_value(&tallies, &tally_id).await, 7);

        assert!(remove(&engine, &tallies, &tally_id).await.unwrap());
        queue.clear().await.unwrap();
    })
    .await
    .expect("The native in-transaction acknowledgement must finish within its owned run");
}

#[tokio::test]
async fn native_a_duplicate_delivery_conflicts_on_the_pending_row_and_its_retry_skips() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let queue = native_queue(&engine);
        let tallies = engine
            .clone()
            .model::<NativeTally>(TitoModelOptions::default());
        let first_tally = new_tally(&engine, &tallies, &run_id).await;
        let second_tally = new_tally(&engine, &tallies, &run_id).await;
        let owner = QueueOwner::new("store", record_id(&run_id)).unwrap();
        let (storage_key, published) = publish_change(&queue, &owner).await;
        let (pulled_key, mut delivered) = queue
            .pull::<NativeChange>(0, None, 10)
            .await
            .unwrap()
            .events
            .pop()
            .unwrap();
        assert_eq!(pulled_key, storage_key);
        assert!(delivered.pending_key.is_none());
        delivered.pending_key = Some(pulled_key);

        let first = engine.begin_transaction().await.unwrap();
        let second = engine.begin_transaction().await.unwrap();
        assert!(
            apply_change(&queue, &tallies, &delivered, &first_tally, &first)
                .await
                .unwrap()
        );
        assert!(
            apply_change(&queue, &tallies, &delivered, &second_tally, &second)
                .await
                .unwrap()
        );
        first.commit().await.unwrap();
        assert!(matches!(
            second.clone().commit().await,
            Err(TitoError::Retryable(_))
        ));
        second.rollback().await.unwrap();

        let retried = engine
            .transaction(|tx| {
                let queue = queue.clone();
                let tallies = tallies.clone();
                let delivered = delivered.clone();
                let second_tally = second_tally.clone();
                async move { apply_change(&queue, &tallies, &delivered, &second_tally, &tx).await }
            })
            .await
            .unwrap();
        assert!(!retried);
        queue.ack(&storage_key).await.unwrap();

        assert_completed_once(&queue, &owner, &published).await;
        assert_eq!(tally_value(&tallies, &first_tally).await, 7);
        assert_eq!(tally_value(&tallies, &second_tally).await, 0);

        assert!(remove(&engine, &tallies, &first_tally).await.unwrap());
        assert!(remove(&engine, &tallies, &second_tally).await.unwrap());
        queue.clear().await.unwrap();
    })
    .await
    .expect("The native duplicate delivery must finish within its owned run");
}

#[tokio::test]
async fn native_a_rolled_back_handler_transaction_leaves_its_row_pending_for_redelivery() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let queue = native_queue(&engine);
        let tallies = engine
            .clone()
            .model::<NativeTally>(TitoModelOptions::default());
        let tally_id = new_tally(&engine, &tallies, &run_id).await;
        let owner = QueueOwner::new("store", record_id(&run_id)).unwrap();
        let (storage_key, published) = publish_change(&queue, &owner).await;
        let mut delivered = published.clone();
        delivered.pending_key = Some(storage_key.clone());

        let failed = engine
            .transaction(|tx| {
                let queue = queue.clone();
                let tallies = tallies.clone();
                let delivered = delivered.clone();
                let tally_id = tally_id.clone();
                async move {
                    if apply_change(&queue, &tallies, &delivered, &tally_id, &tx).await? {
                        return Err(TitoError::Internal(
                            "the handler failed after applying its change".into(),
                        ));
                    }
                    Ok(())
                }
            })
            .await;
        assert_eq!(
            failed,
            Err(TitoError::Internal(
                "the handler failed after applying its change".into()
            ))
        );

        assert_eq!(
            queue
                .scan_by_status::<NativeChange>(QueueEventStatus::Pending, None, 10)
                .await
                .unwrap()
                .events,
            vec![(storage_key.clone(), published.clone())]
        );
        assert_eq!(
            queue
                .scan_by_owner::<NativeChange>(&owner, QueueEventStatus::Pending, None, 10)
                .await
                .unwrap()
                .events,
            vec![(storage_key.clone(), published.clone())]
        );
        assert!(queue
            .scan_by_status::<NativeChange>(QueueEventStatus::Completed, None, 10)
            .await
            .unwrap()
            .events
            .is_empty());
        assert_eq!(tally_value(&tallies, &tally_id).await, 0);

        let redelivered = engine
            .transaction(|tx| {
                let queue = queue.clone();
                let tallies = tallies.clone();
                let delivered = delivered.clone();
                let tally_id = tally_id.clone();
                async move { apply_change(&queue, &tallies, &delivered, &tally_id, &tx).await }
            })
            .await
            .unwrap();
        assert!(redelivered);
        queue.ack(&storage_key).await.unwrap();

        assert_completed_once(&queue, &owner, &published).await;
        assert_eq!(tally_value(&tallies, &tally_id).await, 7);

        assert!(remove(&engine, &tallies, &tally_id).await.unwrap());
        queue.clear().await.unwrap();
    })
    .await
    .expect("The native rolled-back handler must finish within its owned run");
}

#[tokio::test]
async fn native_ack_in_tx_refuses_an_undelivered_event_and_a_row_of_another_event() {
    let (endpoint, run_id) = native_endpoint();
    tokio::time::timeout(Duration::from_secs(60), async move {
        let engine = native_engine(endpoint).await;
        let queue = native_queue(&engine);
        let tallies = engine
            .clone()
            .model::<NativeTally>(TitoModelOptions::default());
        let tally_id = new_tally(&engine, &tallies, &run_id).await;
        let owner = QueueOwner::new("store", record_id(&run_id)).unwrap();
        let (first_key, first) = publish_change(&queue, &owner).await;
        let (second_key, _) = publish_change(&queue, &owner).await;
        let mut misrouted = first.clone();
        misrouted.pending_key = Some(second_key.clone());
        let mut completed_row = first.clone();
        completed_row.pending_key = Some(first_key.replacen(":pending:", ":completed:", 1));

        for refused in [first.clone(), misrouted, completed_row] {
            let result = engine
                .transaction(|tx| {
                    let queue = queue.clone();
                    let tallies = tallies.clone();
                    let refused = refused.clone();
                    let tally_id = tally_id.clone();
                    async move { apply_change(&queue, &tallies, &refused, &tally_id, &tx).await }
                })
                .await;
            assert!(matches!(result, Err(TitoError::InvalidInput(_))));
        }

        let mut pending = queue
            .scan_by_status::<NativeChange>(QueueEventStatus::Pending, None, 10)
            .await
            .unwrap()
            .events
            .into_iter()
            .map(|(key, _)| key)
            .collect::<Vec<_>>();
        pending.sort();
        let mut expected = vec![first_key, second_key];
        expected.sort();
        assert_eq!(pending, expected);
        assert!(queue
            .scan_by_status::<NativeChange>(QueueEventStatus::Completed, None, 10)
            .await
            .unwrap()
            .events
            .is_empty());
        assert_eq!(tally_value(&tallies, &tally_id).await, 0);

        assert!(remove(&engine, &tallies, &tally_id).await.unwrap());
        queue.clear().await.unwrap();
    })
    .await
    .expect(
        "The native in-transaction acknowledgement refusals must finish within their owned run",
    );
}
