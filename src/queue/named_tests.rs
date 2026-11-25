use std::collections::BTreeSet;

use super::test_engine::{get_raw, keys_with_prefix, native_engine, put_raw, unique_queue_name};
use super::*;
use crate::backend::tikv::TiKVBackend;

fn named(engine: &TiKVBackend, name: &str) -> Queue<TiKVBackend> {
    Queue::new(engine.clone(), QueueConfig::new(2).with_name(name).unwrap())
}

#[tokio::test]
async fn queue_names_are_explicit_and_unambiguous() {
    assert_eq!(QueueConfig::new(2).name(), "queue");
    for name in ["", "a:b", " a", "a/", "é", &"a".repeat(129)] {
        assert!(QueueConfig::new(2).with_name(name).is_err());
    }
    for name in ["queue", "test-123_A", &"a".repeat(128)] {
        assert_eq!(QueueConfig::new(2).with_name(name).unwrap().name(), name);
    }

    let engine = native_engine().await;
    let base = unique_queue_name("named");
    let short = named(&engine, &base);
    let long = named(&engine, &format!("{base}-x"));
    long.publish(QueueEvent::new("work:same", Value::Null, 0))
        .await
        .unwrap();
    assert!(short
        .scan_by_status::<Value>(None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
    short.clear().await.unwrap();
    assert_eq!(
        long.scan_by_status::<Value>(None, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn named_queue_transitions_and_clear_are_isolated() {
    let engine = native_engine().await;
    let a = named(&engine, &unique_queue_name("a"));
    let b = named(&engine, &unique_queue_name("b"));
    let owner = QueueOwner::new("store", "same-store").unwrap();
    let original =
        QueueEvent::new("work:same", serde_json::json!({"page": 1}), 0).with_owner(owner.clone());
    a.publish(original.clone()).await.unwrap();
    b.publish(original.clone()).await.unwrap();
    let partition = a.partition_for_key(&original.key);
    let a_key = a.pull::<Value>(partition, None, 10).await.unwrap().events[0]
        .0
        .clone();
    let b_key = b.pull::<Value>(partition, None, 10).await.unwrap().events[0]
        .0
        .clone();
    assert_ne!(a_key, b_key);
    assert!(a.ack(&b_key).await.is_err());
    assert!(a
        .reschedule(&b_key, original.rescheduled(10))
        .await
        .is_err());
    let mut advanced = original.rescheduled(20);
    advanced.payload = serde_json::json!({"page": 2});
    assert!(a.advance(&b_key, advanced.clone()).await.is_err());
    a.reschedule(&a_key, original.rescheduled(10))
        .await
        .unwrap();
    let pending = a.scan_by_status::<Value>(None, 10).await.unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.timestamp, 10);
    a.advance(&pending.events[0].0, advanced.clone())
        .await
        .unwrap();
    let pending = a.scan_by_owner::<Value>(&owner, None, 10).await.unwrap();
    assert_eq!(pending.events[0].1, advanced);
    a.ack(&pending.events[0].0).await.unwrap();
    assert!(keys_with_prefix(&engine, &format!("{}:", a.config.name()))
        .await
        .is_empty());
    assert_eq!(
        b.scan_by_owner::<Value>(&owner, None, 10)
            .await
            .unwrap()
            .events[0]
            .0,
        b_key
    );
    a.publish(original.clone()).await.unwrap();
    a.clear().await.unwrap();
    assert!(keys_with_prefix(&engine, &format!("{}:", a.config.name()))
        .await
        .is_empty());
    assert_eq!(
        b.scan_by_owner::<Value>(&owner, None, 10)
            .await
            .unwrap()
            .events[0]
            .1,
        original
    );
}

#[tokio::test]
async fn named_queue_rejects_cross_queue_and_partition_cursors() {
    let engine = native_engine().await;
    let a = named(&engine, &unique_queue_name("a"));
    let b = named(&engine, &unique_queue_name("b"));
    let owner = QueueOwner::new("store", "same-store").unwrap();
    for i in 0..2 {
        a.publish(QueueEvent::new("work:same", Value::from(i), 0).with_owner(owner.clone()))
            .await
            .unwrap();
    }
    let partition = a.partition_for_key("work:same");
    let pull = a.pull::<Value>(partition, None, 1).await.unwrap();
    let cursor = pull.next_cursor.expect("first of two events has a cursor");
    assert!(b
        .pull::<Value>(partition, Some(cursor.clone()), 10)
        .await
        .is_err());
    assert!(a
        .pull::<Value>(1 - partition, Some(cursor.clone()), 10)
        .await
        .is_err());
    assert_eq!(
        a.pull::<Value>(partition, Some(cursor), 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
    let scan = a.scan_by_status::<Value>(None, 1).await.unwrap();
    assert!(b
        .scan_by_status::<Value>(scan.next_cursor.clone(), 10)
        .await
        .is_err());
    assert!(engine
        .transaction(|tx| {
            let cursor = scan.next_cursor.clone();
            let b = b.clone();
            async move {
                b.delete_matching_in_tx::<Value, _>(cursor, 10, &tx, |_| Ok(true))
                    .await
            }
        })
        .await
        .is_err());
    let owned = a.scan_by_owner::<Value>(&owner, None, 1).await.unwrap();
    assert!(b
        .scan_by_owner::<Value>(&owner, owned.next_cursor, 10)
        .await
        .is_err());
    assert_eq!(
        a.scan_by_status::<Value>(None, 10)
            .await
            .unwrap()
            .events
            .len(),
        2
    );
}

#[tokio::test]
async fn named_owner_reads_cannot_follow_an_index_into_another_queue() {
    let engine = native_engine().await;
    let a = named(&engine, &unique_queue_name("a"));
    let b = named(&engine, &unique_queue_name("b"));
    let owner = QueueOwner::new("store", "same-store").unwrap();
    b.publish(QueueEvent::new("work:same", Value::Null, 0).with_owner(owner.clone()))
        .await
        .unwrap();
    let b_key = b.scan_by_status::<Value>(None, 10).await.unwrap().events[0]
        .0
        .clone();
    let corrupt_index = a.owner_index_key(&owner, b_key.as_bytes()).unwrap();
    put_raw(&engine, &corrupt_index, Vec::new()).await;
    assert!(a.scan_by_owner::<Value>(&owner, None, 10).await.is_err());
    assert!(get_raw(&engine, &b_key).await.is_some());
    assert!(get_raw(&engine, &corrupt_index).await.is_some());
}

#[tokio::test]
async fn named_queue_reads_see_only_their_own_rows() {
    let engine = native_engine().await;
    let a = named(&engine, &unique_queue_name("a"));
    let b = named(&engine, &unique_queue_name("b"));
    for i in 0..3 {
        a.publish(QueueEvent::new("work:same", Value::from(i), 0))
            .await
            .unwrap();
    }
    let before = a.scan_by_status::<Value>(None, 100).await.unwrap();
    for i in 0..50 {
        b.publish(QueueEvent::new("work:same", Value::from(i), 0))
            .await
            .unwrap();
    }
    let after = a.scan_by_status::<Value>(None, 100).await.unwrap();
    assert_eq!(before.events.len(), 3);
    assert_eq!(after.events, before.events);
}

#[tokio::test]
async fn named_queue_cluster_steps_are_independent() {
    let engine = native_engine().await;
    let a = named(&engine, &unique_queue_name("a"));
    let b = named(&engine, &unique_queue_name("b"));
    let first = ClusterWorkerConfig::new("first-node");
    let second = ClusterWorkerConfig::new("second-node");
    let none = BTreeSet::new();
    a.heartbeat_cluster_worker(&first).await.unwrap();
    b.heartbeat_cluster_worker(&second).await.unwrap();
    assert_eq!(
        a.step_cluster_worker(&first, &none).await.unwrap().held,
        vec![0, 1]
    );
    assert_eq!(
        b.step_cluster_worker(&second, &none).await.unwrap().held,
        vec![0, 1]
    );
    assert!(a
        .step_cluster_worker(&second, &none)
        .await
        .unwrap()
        .held
        .is_empty());
    assert!(b
        .step_cluster_worker(&first, &none)
        .await
        .unwrap()
        .held
        .is_empty());
}
