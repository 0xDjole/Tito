use super::*;
use crate::test_support::MemoryEngine;

fn named(engine: &MemoryEngine, name: &str) -> Queue<MemoryEngine> {
    Queue::new(
        engine.clone(),
        QueueConfig::new(2, Duration::from_secs(60))
            .with_name(name)
            .unwrap(),
    )
}

#[test]
fn queue_names_are_explicit_and_unambiguous() {
    assert_eq!(QueueConfig::new(2, Duration::ZERO).name(), "queue");
    for name in ["", "a:b", " a", "a/", "é", &"a".repeat(129)] {
        assert!(QueueConfig::new(2, Duration::ZERO).with_name(name).is_err());
    }
    for name in ["queue", "test-123_A", &"a".repeat(128)] {
        assert_eq!(
            QueueConfig::new(2, Duration::ZERO)
                .with_name(name)
                .unwrap()
                .name(),
            name
        );
    }
}

#[tokio::test]
async fn named_queue_transitions_retention_and_clear_are_isolated() {
    let engine = MemoryEngine::default();
    let a = named(&engine, "a");
    let b = named(&engine, "b");
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
    let pending = a
        .scan_by_status::<Value>(QueueEventStatus::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.timestamp, 10);
    a.advance(&pending.events[0].0, advanced.clone())
        .await
        .unwrap();
    let pending = a
        .scan_by_owner::<Value>(&owner, QueueEventStatus::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events[0].1, advanced);
    a.ack(&pending.events[0].0).await.unwrap();
    b.ack(&b_key).await.unwrap();
    assert_eq!(
        a.scan_by_owner::<Value>(&owner, QueueEventStatus::Completed, None, 10)
            .await
            .unwrap()
            .events
            .len(),
        3
    );
    assert_eq!(
        a.delete_by_status_before(QueueEventStatus::Completed, i64::MAX, 10)
            .await
            .unwrap(),
        3
    );
    assert!(a
        .scan_by_owner::<Value>(&owner, QueueEventStatus::Completed, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
    assert_eq!(
        b.scan_by_owner::<Value>(&owner, QueueEventStatus::Completed, None, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
    a.publish(original.clone()).await.unwrap();
    b.publish(original.clone()).await.unwrap();
    a.clear().await.unwrap();
    assert!(engine.keys_with_prefix("a:").await.is_empty());
    assert_eq!(
        b.scan_by_owner::<Value>(&owner, QueueEventStatus::Pending, None, 10)
            .await
            .unwrap()
            .events[0]
            .1,
        original
    );
    assert_eq!(
        b.scan_by_owner::<Value>(&owner, QueueEventStatus::Completed, None, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
}

#[tokio::test]
async fn named_queue_rejects_cross_queue_status_and_partition_cursors() {
    let engine = MemoryEngine::default();
    let a = named(&engine, "a");
    let b = named(&engine, "b");
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
    let scan = a
        .scan_by_status::<Value>(QueueEventStatus::Pending, None, 1)
        .await
        .unwrap();
    assert!(b
        .scan_by_status::<Value>(QueueEventStatus::Pending, scan.next_cursor.clone(), 10)
        .await
        .is_err());
    assert!(a
        .scan_by_status::<Value>(QueueEventStatus::Completed, scan.next_cursor.clone(), 10)
        .await
        .is_err());
    assert!(engine
        .transaction(|tx| {
            let cursor = scan.next_cursor.clone();
            let b = b.clone();
            async move {
                b.delete_matching_in_tx::<Value, _>(
                    QueueEventStatus::Pending,
                    cursor,
                    10,
                    &tx,
                    |_| Ok(true),
                )
                .await
            }
        })
        .await
        .is_err());
    let owned = a
        .scan_by_owner::<Value>(&owner, QueueEventStatus::Pending, None, 1)
        .await
        .unwrap();
    assert!(b
        .scan_by_owner::<Value>(&owner, QueueEventStatus::Pending, owned.next_cursor, 10)
        .await
        .is_err());
    assert_eq!(
        a.scan_by_status::<Value>(QueueEventStatus::Pending, None, 10)
            .await
            .unwrap()
            .events
            .len(),
        2
    );
}

#[tokio::test]
async fn named_owner_deletion_cannot_follow_an_index_into_another_queue() {
    let engine = MemoryEngine::default();
    let a = named(&engine, "a");
    let b = named(&engine, "b");
    let owner = QueueOwner::new("store", "same-store").unwrap();
    b.publish(QueueEvent::new("work:same", Value::Null, 0).with_owner(owner.clone()))
        .await
        .unwrap();
    let b_key = b
        .scan_by_status::<Value>(QueueEventStatus::Pending, None, 10)
        .await
        .unwrap()
        .events[0]
        .0
        .clone();
    let corrupt_index = a
        .owner_index_key(&owner, QueueEventStatus::Pending, b_key.as_bytes())
        .unwrap();
    engine.put_raw(&corrupt_index, Vec::new()).await;
    assert!(a
        .scan_by_owner::<Value>(&owner, QueueEventStatus::Pending, None, 10)
        .await
        .is_err());
    assert!(engine
        .transaction(|tx| {
            let a = a.clone();
            let owner = owner.clone();
            async move {
                a.delete_by_owner_matching_in_tx::<Value, _>(
                    &owner,
                    QueueEventStatus::Pending,
                    10,
                    &tx,
                    |_| Ok(true),
                )
                .await
            }
        })
        .await
        .is_err());
    assert!(engine.contains_key(&b_key).await);
    assert!(engine.contains_key(&corrupt_index).await);
}

#[tokio::test]
async fn named_queue_reads_do_not_grow_with_other_queues() {
    let engine = MemoryEngine::default();
    let a = named(&engine, "a");
    let b = named(&engine, "b");
    for i in 0..3 {
        a.publish(QueueEvent::new("work:same", Value::from(i), 0))
            .await
            .unwrap();
    }
    engine.start_recording_reads().await;
    let before = a
        .scan_by_status::<Value>(QueueEventStatus::Pending, None, 100)
        .await
        .unwrap();
    let reads_before = engine.take_recorded_reads().await;
    for i in 0..200 {
        b.publish(QueueEvent::new("work:same", Value::from(i), 0))
            .await
            .unwrap();
    }
    engine.start_recording_reads().await;
    let after = a
        .scan_by_status::<Value>(QueueEventStatus::Pending, None, 100)
        .await
        .unwrap();
    assert_eq!(after.events, before.events);
    assert_eq!(engine.take_recorded_reads().await, reads_before);
    assert_eq!(reads_before, vec![b"scan:a:pending:".to_vec()]);
}

#[tokio::test]
async fn named_queue_cluster_coordination_is_independent() {
    let engine = MemoryEngine::default();
    let a = named(&engine, "a");
    let b = named(&engine, "b");
    let first = ClusterWorkerConfig::new("first-node");
    let second = ClusterWorkerConfig::new("second-node");
    a.heartbeat_cluster_worker(&first).await.unwrap();
    b.heartbeat_cluster_worker(&second).await.unwrap();
    assert!(a.try_acquire_cluster_coordinator(&first).await.unwrap());
    assert!(b.try_acquire_cluster_coordinator(&second).await.unwrap());
    assert!(!a.try_acquire_cluster_coordinator(&second).await.unwrap());
    assert!(!b.try_acquire_cluster_coordinator(&first).await.unwrap());
    assert_eq!(
        a.active_cluster_workers(&first).await.unwrap()[0].node_id,
        "first-node"
    );
    assert_eq!(
        b.active_cluster_workers(&second).await.unwrap()[0].node_id,
        "second-node"
    );
    a.rebalance_cluster_partitions(&first).await.unwrap();
    b.rebalance_cluster_partitions(&second).await.unwrap();
    a.sync_cluster_partition_leases(&first).await.unwrap();
    b.sync_cluster_partition_leases(&second).await.unwrap();
    assert_eq!(
        a.owned_cluster_partitions(&first).await.unwrap(),
        vec![0, 1]
    );
    assert_eq!(
        b.owned_cluster_partitions(&second).await.unwrap(),
        vec![0, 1]
    );
    assert!(a
        .owned_cluster_partitions(&second)
        .await
        .unwrap()
        .is_empty());
    assert!(b.owned_cluster_partitions(&first).await.unwrap().is_empty());
}
