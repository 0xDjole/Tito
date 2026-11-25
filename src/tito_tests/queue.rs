use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use crate::backend::tikv::TiKVBackend;
use crate::queue::test_engine::{
    get_raw, keys_with_prefix, native_engine, put_raw, unique_queue_name,
};
use crate::queue::{
    run_cluster_worker, ClusterWorkerConfig, Queue, QueueConfig, QueueEvent, QueueHandlerOutcome,
    QueueHandlerResult, QueueOwner,
};
use crate::types::{TitoEngine, TitoTransaction, PARTITION_DIGITS};
use crate::{encode_index_integer, queue_partition_for_key, TitoError};

type PayloadRun = BoxFuture<'static, QueueHandlerResult<Payload>>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Payload {
    name: String,
}

fn payload(name: &str) -> Payload {
    Payload {
        name: name.to_string(),
    }
}

fn queue_event(id: &str, key: &str, timestamp: i64) -> QueueEvent<Payload> {
    QueueEvent {
        id: id.to_string(),
        key: key.to_string(),
        owner: None,
        payload: payload(id),
        timestamp,
        pending_key: None,
    }
}

fn named_queue(engine: &TiKVBackend, partition_count: u32) -> Queue<TiKVBackend> {
    Queue::new(
        engine.clone(),
        QueueConfig::new(partition_count)
            .with_name(unique_queue_name("queue"))
            .unwrap(),
    )
}

fn queue_prefix(queue: &Queue<TiKVBackend>) -> String {
    format!("{}:", queue.config.name())
}

fn pending_key(
    queue: &Queue<TiKVBackend>,
    partition: u32,
    timestamp: i64,
    enqueue_version: u64,
    id: &str,
) -> String {
    format!(
        "{}:pending:{partition:0width$}:{}:{enqueue_version:020}:{id}",
        queue.config.name(),
        encode_index_integer(timestamp),
        width = PARTITION_DIGITS,
    )
}

async fn only_pending(queue: &Queue<TiKVBackend>) -> (String, QueueEvent<Payload>) {
    let mut pending = queue
        .scan_by_status::<Payload>(None, 10)
        .await
        .unwrap()
        .events;
    assert_eq!(pending.len(), 1);
    pending.remove(0)
}

fn worker_config(node_id: &str) -> ClusterWorkerConfig {
    ClusterWorkerConfig {
        node_id: node_id.to_string(),
        heartbeat_interval: Duration::from_millis(500),
        poll_interval: Duration::from_millis(50),
        handler_timeout: Duration::from_secs(60),
        lease_ttl: Duration::from_secs(10),
        stale_node_ttl: Duration::from_secs(60),
    }
}

async fn stop_worker(shutdown: broadcast::Sender<()>, worker: JoinHandle<()>) {
    let _ = shutdown.send(());
    timeout(Duration::from_secs(30), worker)
        .await
        .expect("the worker stops")
        .expect("the worker task ends without a panic");
}

async fn wait_until_gone(engine: &TiKVBackend, storage_key: &str) {
    timeout(Duration::from_secs(30), async {
        while get_raw(engine, storage_key).await.is_some() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the worker acknowledges the row");
}

#[tokio::test]
async fn queue_rows_store_exactly_their_id_key_owner_payload_and_signed_timestamp() {
    let engine = native_engine().await;
    let queue = named_queue(&engine, 1);
    let before = Utc::now().timestamp_millis();
    let fresh = QueueEvent::new("entry:entry-1", payload("fresh"), 123)
        .with_owner(QueueOwner::new("store", "store-1").unwrap());
    let after = Utc::now().timestamp_millis();
    let mut native = queue_event("native", "entry:native", 456);
    native.id = format!("{:020}-event", 1_700_000_000_123_456_i64);
    let mut published = vec![fresh.clone(), native.clone()];
    for (index, timestamp) in [
        i64::MIN,
        -9_007_199_254_740_991,
        -1,
        0,
        1_700_000_000_123,
        i64::MAX,
    ]
    .into_iter()
    .enumerate()
    {
        published.push(queue_event(
            &format!("opaque-{index}"),
            "entry:clock",
            timestamp,
        ));
    }
    for published_event in &published {
        queue.publish(published_event.clone()).await.unwrap();
    }

    let stored = queue
        .scan_by_status::<Payload>(None, 100)
        .await
        .unwrap()
        .events;
    assert_eq!(stored.len(), published.len());
    for published_event in &published {
        let (storage_key, read) = stored
            .iter()
            .find(|(_, read)| read.id == published_event.id)
            .unwrap();
        assert_eq!(read, published_event);
        let raw: Value =
            serde_json::from_slice(&get_raw(&engine, storage_key).await.unwrap()).unwrap();
        let mut fields = raw.as_object().unwrap().keys().cloned().collect::<Vec<_>>();
        fields.sort();
        let mut expected = vec!["id", "key", "payload", "timestamp"];
        if published_event.owner.is_some() {
            expected.push("owner");
            assert_eq!(raw["owner"], json!({"type": "store", "id": "store-1"}));
        }
        expected.sort();
        assert_eq!(fields, expected);
        assert_eq!(raw["timestamp"], json!(published_event.timestamp));
        if published_event.id.starts_with("opaque-") {
            assert_eq!(read.created_at_millis(), published_event.timestamp);
        }
    }

    let read_fresh = &stored
        .iter()
        .find(|(_, read)| read.id == fresh.id)
        .unwrap()
        .1;
    assert_eq!(read_fresh.key_type(), "entry");
    assert_eq!(read_fresh.key_value(), "entry-1");
    assert_eq!(read_fresh.event(), &payload("fresh"));
    assert!((before..=after).contains(&read_fresh.created_at_millis()));
    let read_native = &stored
        .iter()
        .find(|(_, read)| read.id == native.id)
        .unwrap()
        .1;
    assert_eq!(read_native.created_at_millis(), 1_700_000_000_123);
    assert_eq!(
        read_native
            .rescheduled(1_700_000_000_987)
            .created_at_millis(),
        1_700_000_000_123
    );
}

#[tokio::test]
async fn queue_signed_millisecond_due_times_are_reachable_ordered_paged_and_never_rounded() {
    let engine = native_engine().await;
    let queue = named_queue(&engine, 1);
    let now = Utc::now().timestamp_millis();
    let past = now.div_euclid(1_000) * 1_000 - 60_000;
    let future = now.div_euclid(1_000) * 1_000 + 60_123;
    let timestamps = [
        i64::MIN,
        i64::MIN + 1,
        -9_007_199_254_740_991,
        -1_000,
        -999,
        -123,
        -100,
        -10,
        -1,
        0,
        1,
        9,
        10,
        99,
        100,
        123,
        past + 122,
        past + 123,
        future,
        i64::MAX - 1,
        i64::MAX,
    ];
    engine
        .transaction(|tx| {
            let queue = &queue;
            async move {
                for timestamp in timestamps.into_iter().rev() {
                    queue
                        .publish_in_tx(
                            queue_event(&format!("at-{timestamp}"), "entry:clock", timestamp),
                            &tx,
                        )
                        .await?;
                }
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();

    let mut cursor = None;
    let mut due = Vec::new();
    loop {
        let page = queue.pull::<Payload>(0, cursor, 3).await.unwrap();
        assert!(page.events.len() <= 3);
        due.extend(page.events.into_iter().map(|(_, event)| event.timestamp));
        assert!(due.len() <= timestamps.len());
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(due, timestamps[..timestamps.len() - 3]);

    let mut cursor = None;
    let mut stored = Vec::new();
    loop {
        let page = queue.scan_by_status::<Payload>(cursor, 4).await.unwrap();
        assert!(page.events.len() <= 4);
        stored.extend(page.events);
        assert!(stored.len() <= timestamps.len());
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert_eq!(stored.len(), timestamps.len());
    for ((key, event), timestamp) in stored.iter().zip(timestamps) {
        assert_eq!(event.timestamp, timestamp);
        assert_eq!(
            key.split(':').nth(3).unwrap(),
            encode_index_integer(timestamp)
        );
    }
}

#[tokio::test]
async fn queue_rows_of_an_older_shape_fail_closed_without_acknowledgement_or_replacement() {
    let engine = native_engine().await;
    let canonical = serde_json::to_value(queue_event("legacy", "entry:legacy", 123)).unwrap();
    let mut variants = Vec::new();
    for (field, value) in [
        ("status", json!({"type": "pending"})),
        ("status", json!("pending")),
        ("state", json!("pending")),
        ("processedAt", json!(123)),
        ("owner", json!({"kind": "store", "id": "store-1"})),
        (
            "owner",
            json!({"type": "store", "kind": "store", "id": "store-1"}),
        ),
    ] {
        let mut variant = canonical.clone();
        variant[field] = value;
        variants.push(variant);
    }

    for variant in variants {
        let queue = named_queue(&engine, 1);
        let key = pending_key(&queue, 0, 123, 0, "legacy");
        let bytes = serde_json::to_vec(&variant).unwrap();
        put_raw(&engine, &key, bytes.clone()).await;
        let mut advanced = queue_event("legacy", "entry:legacy", 456);
        advanced.payload = payload("advanced");

        assert!(serde_json::from_value::<QueueEvent<Payload>>(variant.clone()).is_err());
        assert!(matches!(
            queue.scan_by_status::<Payload>(None, 10).await,
            Err(TitoError::DeserializationFailed(_))
        ));
        assert!(queue
            .pull::<Payload>(0, None, 10)
            .await
            .unwrap()
            .events
            .is_empty());
        assert!(matches!(
            queue.ack(&key).await,
            Err(TitoError::DeserializationFailed(_))
        ));
        assert!(matches!(
            queue
                .reschedule(&key, queue_event("legacy", "entry:legacy", 456))
                .await,
            Err(TitoError::DeserializationFailed(_))
        ));
        assert!(matches!(
            queue.advance(&key, advanced).await,
            Err(TitoError::DeserializationFailed(_))
        ));
        assert_eq!(get_raw(&engine, &key).await, Some(bytes));
        assert_eq!(
            keys_with_prefix(&engine, &queue_prefix(&queue)).await,
            vec![key]
        );
    }
}

#[tokio::test]
async fn queue_pending_key_orders_by_event_timestamp_and_enqueue_generation() {
    let engine = native_engine().await;
    let queue = named_queue(&engine, 1);
    for (id, timestamp) in [("event-1", 123), ("event-2", 123), ("event-0", 122)] {
        queue
            .publish(queue_event(id, &format!("entry:{id}"), timestamp))
            .await
            .unwrap();
    }

    let pending = queue
        .scan_by_status::<Payload>(None, 10)
        .await
        .unwrap()
        .events;
    assert_eq!(
        pending
            .iter()
            .map(|(_, event)| event.id.as_str())
            .collect::<Vec<_>>(),
        ["event-0", "event-1", "event-2"]
    );
    let mut versions = Vec::new();
    for (storage_key, event) in &pending {
        let fields = storage_key.splitn(6, ':').collect::<Vec<_>>();
        assert_eq!(fields.len(), 6);
        assert_eq!(
            &fields[..4],
            [
                queue.config.name(),
                "pending",
                "0000",
                encode_index_integer(event.timestamp).as_str(),
            ]
        );
        assert_eq!(fields[4].len(), 20);
        assert!(fields[4].bytes().all(|byte| byte.is_ascii_digit()));
        assert_eq!(fields[5], event.id);
        versions.push(fields[4].parse::<u64>().unwrap());
    }
    assert!(versions[1] > 0);
    assert!(versions[1] < versions[2]);
    assert!(versions[2] < versions[0]);
}

#[tokio::test]
async fn queue_ack_of_a_missing_row_writes_nothing_and_of_a_malformed_row_keeps_its_bytes() {
    let engine = native_engine().await;
    let queue = named_queue(&engine, 1);

    queue
        .ack(&pending_key(&queue, 0, 1, 0, "missing"))
        .await
        .unwrap();
    assert!(keys_with_prefix(&engine, &queue_prefix(&queue))
        .await
        .is_empty());

    let malformed = pending_key(&queue, 0, 1, 0, "malformed");
    put_raw(&engine, &malformed, b"not-json".to_vec()).await;
    assert!(matches!(
        queue.ack(&malformed).await,
        Err(TitoError::DeserializationFailed(_))
    ));
    assert_eq!(
        get_raw(&engine, &malformed).await,
        Some(b"not-json".to_vec())
    );
}

#[tokio::test]
async fn queue_pull_leaves_malformed_rows_in_place_and_passes_a_full_page_of_them() {
    let engine = native_engine().await;
    let queue = named_queue(&engine, 1);
    let malformed = (0..50)
        .map(|index| pending_key(&queue, 0, i64::MIN, 0, &format!("bad-{index:02}")))
        .collect::<Vec<_>>();
    engine
        .transaction(|tx| {
            let malformed = &malformed;
            async move {
                for key in malformed {
                    tx.put(key.as_bytes(), b"not-json".to_vec()).await?;
                }
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();
    queue
        .publish(queue_event(
            "zz-valid",
            "entry:zz-valid",
            Utc::now().timestamp_millis() - 10_000,
        ))
        .await
        .unwrap();

    let first = queue.pull::<Payload>(0, None, 50).await.unwrap();
    assert!(first.events.is_empty());
    assert!(first.next_cursor.is_some());
    let second = queue
        .pull::<Payload>(0, first.next_cursor, 50)
        .await
        .unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].1.id, "zz-valid");
    assert!(second.next_cursor.is_none());

    assert_eq!(
        keys_with_prefix(&engine, &pending_key(&queue, 0, i64::MIN, 0, "bad-")).await,
        malformed
    );
    assert_eq!(
        get_raw(&engine, &malformed[0]).await,
        Some(b"not-json".to_vec())
    );
}

#[tokio::test]
async fn queue_reschedule_and_advance_refuse_identity_and_payload_changes_without_writing() {
    let engine = native_engine().await;
    let queue = named_queue(&engine, 1);
    let owner = QueueOwner::new("store", "original-owner").unwrap();
    let now = Utc::now().timestamp_millis();
    queue
        .publish(queue_event("refused", "entry:refused", now - 1).with_owner(owner.clone()))
        .await
        .unwrap();
    let (storage_key, current) = only_pending(&queue).await;
    let mut keys = vec![
        storage_key.clone(),
        queue
            .owner_index_key(&owner, storage_key.as_bytes())
            .unwrap(),
    ];
    keys.sort();

    let same = current.rescheduled(now + 60_000);
    let mut changed = same.clone();
    changed.payload = payload("changed");
    let mut refusals = vec![
        (false, changed.clone(), "must preserve the event payload"),
        (true, same.clone(), "must change the event payload"),
    ];
    for (advance, base) in [(false, &same), (true, &changed)] {
        let mut other_id = base.clone();
        other_id.id = "different-id".to_string();
        let mut other_key = base.clone();
        other_key.key = "entry:different".to_string();
        let mut other_owner = base.clone();
        other_owner.owner = Some(QueueOwner::new("store", "different-owner").unwrap());
        refusals.push((advance, other_id, "logical event id"));
        refusals.push((advance, other_key, "partition key"));
        refusals.push((advance, other_owner, "event owner"));
    }

    for (advance, next, reason) in refusals {
        let refused = if advance {
            queue.advance(&storage_key, next).await
        } else {
            queue.reschedule(&storage_key, next).await
        };
        match refused {
            Err(TitoError::InvalidInput(message)) => {
                assert!(message.contains(reason), "{message}")
            }
            other => panic!("a change of the {reason} was not refused: {other:?}"),
        }
        assert_eq!(
            queue
                .scan_by_status::<Payload>(None, 10)
                .await
                .unwrap()
                .events,
            vec![(storage_key.clone(), current.clone())]
        );
        assert_eq!(keys_with_prefix(&engine, &queue_prefix(&queue)).await, keys);
    }
}

#[tokio::test]
async fn queue_signed_reschedule_and_advance_replace_the_row_and_its_owner_index_in_one_step() {
    let engine = native_engine().await;
    let owner = QueueOwner::new("store", "signed-owner").unwrap();
    for advance in [false, true] {
        for (current_at, next_at) in [(i64::MIN, -1), (0, i64::MIN), (1, i64::MAX), (i64::MAX, 0)] {
            let queue = named_queue(&engine, 1);
            let original =
                queue_event("signed-event", "entry:signed", current_at).with_owner(owner.clone());
            queue.publish(original.clone()).await.unwrap();
            let (storage_key, _) = only_pending(&queue).await;
            let mut next = original.rescheduled(next_at);
            if advance {
                next.payload = payload("advanced");
                queue.advance(&storage_key, next.clone()).await.unwrap();
            } else {
                queue.reschedule(&storage_key, next.clone()).await.unwrap();
            }

            let pending = queue
                .scan_by_status::<Payload>(None, 10)
                .await
                .unwrap()
                .events;
            assert_eq!(pending.len(), 1);
            let (next_key, stored) = &pending[0];
            assert_eq!(stored, &next);
            assert_eq!(
                next_key.split(':').nth(3).unwrap(),
                encode_index_integer(next_at)
            );
            let mut keys = vec![
                next_key.clone(),
                queue.owner_index_key(&owner, next_key.as_bytes()).unwrap(),
            ];
            keys.sort();
            assert_eq!(keys_with_prefix(&engine, &queue_prefix(&queue)).await, keys);

            let due = queue.pull::<Payload>(0, None, 10).await.unwrap().events;
            if next_at == i64::MAX {
                assert!(due.is_empty());
            } else {
                assert_eq!(due, pending);
            }
            queue.ack(next_key).await.unwrap();
            assert!(keys_with_prefix(&engine, &queue_prefix(&queue))
                .await
                .is_empty());
        }
    }
}

#[tokio::test]
async fn queue_negative_due_bucket_preserves_the_frozen_enqueue_horizon() {
    let engine = native_engine().await;
    let queue = named_queue(&engine, 1);
    queue
        .publish(queue_event("early", "entry:early", -10))
        .await
        .unwrap();
    queue
        .publish(queue_event("later", "entry:later", -9))
        .await
        .unwrap();
    let first = queue.pull::<Payload>(0, None, 1).await.unwrap();
    assert_eq!(first.events.len(), 1);
    assert_eq!(first.events[0].1.id, "early");
    let successor = first.events[0].1.rescheduled(-10);
    queue
        .reschedule(&first.events[0].0, successor.clone())
        .await
        .unwrap();

    let mut cursor = first.next_cursor;
    assert!(cursor.is_some());
    let mut remaining = Vec::new();
    for _ in 0..4 {
        let page = queue.pull::<Payload>(0, cursor, 1).await.unwrap();
        remaining.extend(page.events.into_iter().map(|(_, event)| event.id));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none());
    assert_eq!(remaining, ["later"]);
    let wrapped = queue.pull::<Payload>(0, None, 1).await.unwrap();
    assert_eq!(wrapped.events[0].1, successor);
}

#[tokio::test]
async fn queue_worker_handler_error_leaves_the_exact_row_and_its_owner_index() {
    let engine = native_engine().await;
    let queue = Arc::new(named_queue(&engine, 1));
    let owner = QueueOwner::new("store", "worker-error").unwrap();
    queue
        .publish(
            queue_event(
                "worker-error",
                "entry:worker-error",
                Utc::now().timestamp_millis() - 10_000,
            )
            .with_owner(owner.clone()),
        )
        .await
        .unwrap();
    let (storage_key, stored) = only_pending(&queue).await;
    let bytes = get_raw(&engine, &storage_key).await.unwrap();
    let delivered = Arc::new(Mutex::new(Vec::new()));
    let called = Arc::new(Notify::new());
    let handler = {
        let delivered = delivered.clone();
        let called = called.clone();
        move |event: QueueEvent<Payload>| -> PayloadRun {
            delivered.lock().unwrap().push(event);
            called.notify_one();
            Box::pin(async { Err(TitoError::Internal("the test handler fails".to_string())) })
        }
    };
    let (shutdown, shutdown_rx) = broadcast::channel(1);
    let worker = run_cluster_worker(
        queue.clone(),
        worker_config("error-node"),
        handler,
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(30), called.notified())
        .await
        .expect("the worker delivers the row");
    stop_worker(shutdown, worker).await;

    let mut expected = stored;
    expected.pending_key = Some(storage_key.clone());
    assert_eq!(delivered.lock().unwrap()[0], expected);
    assert_eq!(get_raw(&engine, &storage_key).await, Some(bytes));
    let mut keys = vec![
        storage_key.clone(),
        queue
            .owner_index_key(&owner, storage_key.as_bytes())
            .unwrap(),
    ];
    keys.sort();
    assert_eq!(keys_with_prefix(&engine, &queue_prefix(&queue)).await, keys);
}

#[tokio::test]
async fn queue_worker_does_not_let_a_failing_first_page_starve_later_rows() {
    let engine = native_engine().await;
    let queue = Arc::new(named_queue(&engine, 1));
    let due = Utc::now().timestamp_millis() - 10_000;
    engine
        .transaction(|tx| {
            let queue = &queue;
            async move {
                for index in 0..50 {
                    let id = format!("blocked-{index:02}");
                    queue
                        .publish_in_tx(queue_event(&id, &format!("entry:{id}"), due), &tx)
                        .await?;
                }
                Ok::<_, TitoError>(())
            }
        })
        .await
        .unwrap();
    queue
        .publish(queue_event("zz-target", "entry:zz-target", due))
        .await
        .unwrap();
    let target_key = queue
        .scan_by_status::<Payload>(None, 100)
        .await
        .unwrap()
        .events
        .into_iter()
        .find(|(_, event)| event.id == "zz-target")
        .unwrap()
        .0;
    let handler = |event: QueueEvent<Payload>| -> PayloadRun {
        Box::pin(async move {
            if event.id == "zz-target" {
                Ok(QueueHandlerOutcome::Acknowledge)
            } else {
                Err(TitoError::Internal(
                    "the test handler fails every row before the target".to_string(),
                ))
            }
        })
    };
    let (shutdown, shutdown_rx) = broadcast::channel(1);
    let worker = run_cluster_worker(
        queue.clone(),
        worker_config("starvation-node"),
        handler,
        shutdown_rx,
    )
    .await;

    wait_until_gone(&engine, &target_key).await;
    stop_worker(shutdown, worker).await;
    assert_eq!(
        keys_with_prefix(&engine, &queue.pending_prefix())
            .await
            .len(),
        50
    );
}

#[tokio::test]
async fn queue_worker_contains_a_synchronous_handler_panic_and_redelivers() {
    let engine = native_engine().await;
    let queue = Arc::new(named_queue(&engine, 1));
    queue
        .publish(queue_event(
            "worker-sync-panic",
            "entry:worker-sync-panic",
            Utc::now().timestamp_millis() - 10_000,
        ))
        .await
        .unwrap();
    let (storage_key, _) = only_pending(&queue).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let handler = {
        let attempts = attempts.clone();
        move |_event: QueueEvent<Payload>| -> PayloadRun {
            if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("the test handler panics before it returns its future");
            }
            Box::pin(async { Ok(QueueHandlerOutcome::Acknowledge) })
        }
    };
    let (shutdown, shutdown_rx) = broadcast::channel(1);
    let worker = run_cluster_worker(
        queue.clone(),
        worker_config("sync-panic-node"),
        handler,
        shutdown_rx,
    )
    .await;

    wait_until_gone(&engine, &storage_key).await;
    stop_worker(shutdown, worker).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn queue_worker_times_out_a_handler_and_redelivers_its_row() {
    let engine = native_engine().await;
    let queue = Arc::new(named_queue(&engine, 1));
    queue
        .publish(queue_event(
            "worker-timeout",
            "entry:worker-timeout",
            Utc::now().timestamp_millis() - 10_000,
        ))
        .await
        .unwrap();
    let (storage_key, _) = only_pending(&queue).await;
    let attempts = Arc::new(AtomicUsize::new(0));
    let handler = {
        let attempts = attempts.clone();
        move |_event: QueueEvent<Payload>| -> PayloadRun {
            let attempt = attempts.fetch_add(1, Ordering::SeqCst);
            Box::pin(async move {
                if attempt == 0 {
                    std::future::pending::<()>().await;
                }
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        }
    };
    let mut config = worker_config("timeout-node");
    config.handler_timeout = Duration::from_millis(20);
    let (shutdown, shutdown_rx) = broadcast::channel(1);
    let worker = run_cluster_worker(queue.clone(), config, handler, shutdown_rx).await;

    wait_until_gone(&engine, &storage_key).await;
    stop_worker(shutdown, worker).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn published_and_rescheduled_rows_sit_in_the_partition_of_their_key_hash() {
    let engine = native_engine().await;
    let queue = named_queue(&engine, 10);
    let partition_prefix = |partition: u32| {
        format!(
            "{}:pending:{partition:0width$}:",
            queue.config.name(),
            width = PARTITION_DIGITS,
        )
    };
    let keys = [
        "store:4f6c1a52-7e0b-4f43-9d0c-2b4c1a9e8f10",
        "entry:due",
        "order:ž",
        "a",
        "foobar",
    ];
    for key in keys {
        queue
            .publish(QueueEvent::new(key, payload(key), 0))
            .await
            .unwrap();
    }

    let pending = queue
        .scan_by_status::<Payload>(None, 100)
        .await
        .unwrap()
        .events;
    assert_eq!(pending.len(), keys.len());
    for (storage_key, event) in &pending {
        assert!(storage_key.starts_with(&partition_prefix(queue_partition_for_key(&event.key, 10))));
    }
    for (key, partition) in [
        ("store:4f6c1a52-7e0b-4f43-9d0c-2b4c1a9e8f10", 3),
        ("entry:due", 9),
        ("order:ž", 8),
    ] {
        assert!(pending.iter().any(|(storage_key, event)| {
            event.key == key && storage_key.starts_with(&partition_prefix(partition))
        }));
    }

    let (storage_key, moved) = pending
        .iter()
        .find(|(_, event)| event.key == "order:ž")
        .cloned()
        .unwrap();
    queue
        .reschedule(&storage_key, moved.rescheduled(1))
        .await
        .unwrap();
    let pending = queue
        .scan_by_status::<Payload>(None, 100)
        .await
        .unwrap()
        .events;
    assert!(pending.iter().any(|(storage_key, event)| {
        event.key == "order:ž"
            && event.timestamp == 1
            && storage_key.starts_with(&partition_prefix(8))
    }));
}
