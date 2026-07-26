use super::*;

#[tokio::test]
async fn cluster_coordinator_lease_blocks_other_nodes_until_expired() {
    let engine = engine();
    let queue = queue(engine.clone(), 2);
    let node_a = cluster_config("node-a");
    let node_b = cluster_config("node-b");

    assert!(queue
        .try_acquire_cluster_coordinator(&node_a)
        .await
        .unwrap());
    assert!(queue
        .try_acquire_cluster_coordinator(&node_a)
        .await
        .unwrap());
    assert!(!queue
        .try_acquire_cluster_coordinator(&node_b)
        .await
        .unwrap());

    let expired = ClusterCoordinatorLease {
        owner_node_id: "node-a".to_string(),
        lease_until: Utc::now().timestamp() - 1,
        updated_at: Utc::now().timestamp() - 2,
    };
    engine
        .put_json("tito:queue:cluster:coordinator", &json!(expired))
        .await;

    assert!(queue
        .try_acquire_cluster_coordinator(&node_b)
        .await
        .unwrap());
}

#[tokio::test]
async fn cluster_rebalance_assigns_and_syncs_partitions_to_active_nodes() {
    let engine = engine();
    let queue = queue(engine, 4);
    let node_a = cluster_config("node-a");
    let node_b = cluster_config("node-b");

    queue.heartbeat_cluster_worker(&node_b).await.unwrap();
    queue.heartbeat_cluster_worker(&node_a).await.unwrap();

    let active = queue.active_cluster_workers(&node_a).await.unwrap();
    assert_eq!(
        active
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["node-a", "node-b"]
    );

    let assignments = queue.rebalance_cluster_partitions(&node_a).await.unwrap();
    assert_eq!(assignments.len(), 4);
    assert_eq!(assignments[0].desired_node_id.as_deref(), Some("node-a"));
    assert_eq!(assignments[1].desired_node_id.as_deref(), Some("node-a"));
    assert_eq!(assignments[2].desired_node_id.as_deref(), Some("node-b"));
    assert_eq!(assignments[3].desired_node_id.as_deref(), Some("node-b"));

    let owned_a = queue.sync_cluster_partition_leases(&node_a).await.unwrap();
    let owned_b = queue.sync_cluster_partition_leases(&node_b).await.unwrap();
    assert_eq!(
        owned_a
            .iter()
            .map(|assignment| assignment.partition)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        owned_b
            .iter()
            .map(|assignment| assignment.partition)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        queue.owned_cluster_partitions(&node_a).await.unwrap(),
        vec![0, 1]
    );
}

#[tokio::test]
async fn cluster_generation_fence_closes_the_precheck_reschedule_race() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let node_a = cluster_config("node-a");
    let node_b = cluster_config("node-b");
    let now = Utc::now().timestamp();
    queue
        .publish(queue_event(
            "generation-race",
            "entry:generation-race",
            now - 10,
        ))
        .await
        .unwrap();
    let assignment_key = "tito:queue:cluster:partitions:0000";
    let generation_a = 7;
    engine
        .put_json(
            assignment_key,
            &json!(ClusterPartitionAssignment {
                partition: 0,
                desired_node_id: Some(node_a.node_id.clone()),
                owner_node_id: Some(node_a.node_id.clone()),
                lease_until: now + 60,
                generation: generation_a,
                updated_at: now,
            }),
        )
        .await;
    assert!(queue
        .cluster_partition_lease_is_current(&node_a, 0, generation_a)
        .await
        .unwrap());

    let (storage_key, stale_delivery) = queue
        .pull::<QueuePayload>(0, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    engine
        .put_json(
            assignment_key,
            &json!(ClusterPartitionAssignment {
                partition: 0,
                desired_node_id: Some(node_b.node_id.clone()),
                owner_node_id: Some(node_b.node_id.clone()),
                lease_until: now + 60,
                generation: generation_a + 1,
                updated_at: now,
            }),
        )
        .await;

    let applied = queue
        .apply_cluster_handler_outcome(
            &node_a,
            0,
            generation_a,
            stale_delivery,
            &storage_key,
            QueueHandlerOutcome::RescheduleAt(now + 3_600),
        )
        .await
        .unwrap();

    assert!(!applied);
    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].0, storage_key);
    assert_eq!(pending.events[0].1.timestamp, now - 10);
    assert_eq!(pending.events[0].1.id, "generation-race");
}

#[tokio::test]
async fn cluster_reassignment_conflicts_with_an_in_flight_outcome_transaction() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let node_a = cluster_config("node-a");
    let node_b = cluster_config("node-b");
    let now = Utc::now().timestamp();
    queue
        .publish(queue_event(
            "assignment-write-conflict",
            "entry:assignment-write-conflict",
            now - 10,
        ))
        .await
        .unwrap();

    let assignment_key = "tito:queue:cluster:partitions:0000";
    let generation_a = 11;
    engine
        .put_json(
            assignment_key,
            &json!(ClusterPartitionAssignment {
                partition: 0,
                desired_node_id: Some(node_a.node_id.clone()),
                owner_node_id: Some(node_a.node_id.clone()),
                lease_until: now + 60,
                generation: generation_a,
                updated_at: now,
            }),
        )
        .await;
    let (storage_key, stale_delivery) = queue
        .pull::<QueuePayload>(0, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();

    // Hold the old owner's queue transition open after it has observed and
    // write-fenced the assignment, then commit a reassignment concurrently.
    let stale_tx = engine.begin_transaction().await.unwrap();
    assert!(Queue::<MemoryEngine>::fence_cluster_partition_lease_in_tx(
        &stale_tx,
        &node_a.node_id,
        0,
        generation_a,
        now,
    )
    .await
    .unwrap());
    assert!(queue
        .apply_handler_outcome_in_tx(
            &stale_tx,
            stale_delivery.clone(),
            &storage_key,
            QueueHandlerOutcome::RescheduleAt(now + 3_600),
        )
        .await
        .unwrap());

    engine
        .put_json(
            assignment_key,
            &json!(ClusterPartitionAssignment {
                partition: 0,
                desired_node_id: Some(node_b.node_id.clone()),
                owner_node_id: Some(node_b.node_id.clone()),
                lease_until: now + 60,
                generation: generation_a + 1,
                updated_at: now,
            }),
        )
        .await;

    assert!(matches!(
        stale_tx.commit().await,
        Err(TitoError::Retryable(message))
            if message.contains("tito:queue:cluster:partitions:0000")
    ));
    assert!(!queue
        .apply_cluster_handler_outcome(
            &node_a,
            0,
            generation_a,
            stale_delivery,
            &storage_key,
            QueueHandlerOutcome::RescheduleAt(now + 3_600),
        )
        .await
        .unwrap());

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].0, storage_key);
    assert_eq!(pending.events[0].1.timestamp, now - 10);
}

#[tokio::test]
async fn stale_cluster_outcomes_cannot_resurrect_after_new_owner_ack() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let node_a = cluster_config("node-a");
    let node_b = cluster_config("node-b");
    let now = Utc::now().timestamp();
    queue
        .publish(queue_event(
            "ack-reschedule-race",
            "entry:ack-reschedule-race",
            now - 10,
        ))
        .await
        .unwrap();
    let assignment_key = "tito:queue:cluster:partitions:0000";
    let generation_a = 3;
    engine
        .put_json(
            assignment_key,
            &json!(ClusterPartitionAssignment {
                partition: 0,
                desired_node_id: Some(node_a.node_id.clone()),
                owner_node_id: Some(node_a.node_id.clone()),
                lease_until: now + 60,
                generation: generation_a,
                updated_at: now,
            }),
        )
        .await;
    let (storage_key, stale_delivery) = queue
        .pull::<QueuePayload>(0, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let stale_error_delivery = stale_delivery.clone();

    let generation_b = generation_a + 1;
    engine
        .put_json(
            assignment_key,
            &json!(ClusterPartitionAssignment {
                partition: 0,
                desired_node_id: Some(node_b.node_id.clone()),
                owner_node_id: Some(node_b.node_id.clone()),
                lease_until: now + 60,
                generation: generation_b,
                updated_at: now,
            }),
        )
        .await;
    assert!(queue
        .apply_cluster_handler_outcome(
            &node_b,
            0,
            generation_b,
            stale_delivery.clone(),
            &storage_key,
            QueueHandlerOutcome::Done,
        )
        .await
        .unwrap());
    assert!(!queue
        .apply_cluster_handler_outcome(
            &node_a,
            0,
            generation_a,
            stale_delivery,
            &storage_key,
            QueueHandlerOutcome::RescheduleAt(now + 3_600),
        )
        .await
        .unwrap());
    assert!(!queue
        .retry_after_cluster_handler_error(
            &node_a,
            0,
            generation_a,
            stale_error_delivery,
            &storage_key,
            TitoError::Internal("stale worker failed".to_string()),
        )
        .await
        .unwrap());

    assert!(queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
    let completed = queue
        .find_by_state_after::<QueuePayload>(QueueEventState::Completed, 0, 10)
        .await
        .unwrap();
    assert_eq!(completed.len(), 1);
    assert_eq!(completed[0].1.id, "ack-reschedule-race");
    assert_eq!(completed[0].1.retry_count, 0);
    assert!(completed[0].1.errors.is_empty());
}
