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
async fn cluster_worker_contains_handler_panic_and_redelivers_pending_invocation() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    queue
        .publish(queue_event(
            "cluster-panic",
            "entry:cluster-panic",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();

    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let processed = Arc::new(Notify::new());
    let handler_attempts = attempts.clone();
    let handler_processed = processed.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let handle = crate::run_cluster_worker(
        queue.clone(),
        cluster_config("node-a"),
        move |event: QueueEvent<QueuePayload>| {
            let handler_attempts = handler_attempts.clone();
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
                assert_eq!(event.id, "cluster-panic");
                if handler_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst) == 0 {
                    panic!("simulated cluster handler panic");
                }
                handler_processed.notify_one();
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(3), processed.notified())
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let completed = queue
                .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
                .await
                .unwrap();
            if completed
                .events
                .iter()
                .any(|(_, event)| event.id == "cluster-panic")
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(attempts.load(std::sync::atomic::Ordering::SeqCst), 2);

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn cluster_worker_shutdown_drains_started_handler_and_persists_outcome_before_join() {
    let engine = engine();
    let queue = Arc::new(queue(engine.clone(), 1));
    queue
        .publish(queue_event(
            "cluster-drain",
            "entry:cluster-drain",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    queue
        .publish(queue_event(
            "cluster-already-pulled",
            "entry:cluster-already-pulled",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handler_started = started.clone();
    let handler_release = release.clone();
    let handler_attempts = attempts.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let mut handle = crate::run_cluster_worker(
        queue.clone(),
        cluster_config("drain-node"),
        move |event: QueueEvent<QueuePayload>| {
            let handler_started = handler_started.clone();
            let handler_release = handler_release.clone();
            let handler_attempts = handler_attempts.clone();
            Box::pin(async move {
                handler_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                assert_eq!(event.id, "cluster-drain");
                handler_started.notify_one();
                handler_release.notified().await;
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(3), started.notified())
        .await
        .unwrap();
    let pulls_before_shutdown = engine.pending_queue_scan_count();
    queue
        .publish(queue_event(
            "cluster-not-pulled",
            "entry:cluster-not-pulled",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let _ = shutdown_tx.send(());
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        !handle.is_finished(),
        "cluster worker joined before its started handler drained"
    );

    release.notify_one();
    timeout(Duration::from_secs(2), &mut handle)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        engine.pending_queue_scan_count(),
        pulls_before_shutdown,
        "shutdown allowed another pending-partition pull"
    );
    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "shutdown allowed another handler to start"
    );
    let completed = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap();
    assert_eq!(completed.events.len(), 1);
    assert_eq!(completed.events[0].1.id, "cluster-drain");
    assert_eq!(
        completed.events[0].1.completion_reason(),
        Some(QueueCompletionReason::Acknowledged)
    );
    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 2);
    assert_eq!(
        pending
            .events
            .into_iter()
            .map(|(_, event)| event.id)
            .collect::<std::collections::BTreeSet<_>>(),
        std::collections::BTreeSet::from([
            "cluster-already-pulled".to_string(),
            "cluster-not-pulled".to_string(),
        ])
    );
}

#[tokio::test]
async fn cluster_worker_shutdown_bounds_a_blocked_handler_by_its_timeout() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    queue
        .publish(queue_event(
            "cluster-timeout-drain",
            "entry:cluster-timeout-drain",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();

    let mut config = cluster_config("timeout-drain-node");
    config.handler_timeout = Duration::from_millis(25);
    let started = Arc::new(Notify::new());
    let handler_started = started.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handle = crate::run_cluster_worker(
        queue.clone(),
        config,
        move |_event: QueueEvent<QueuePayload>| {
            let handler_started = handler_started.clone();
            Box::pin(async move {
                handler_started.notify_one();
                std::future::pending::<()>().await;
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .unwrap()
        .unwrap();

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, "cluster-timeout-drain");
    assert!(queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn cluster_worker_observes_shutdown_sent_immediately_after_start() {
    let engine = engine();
    let queue = Arc::new(queue(engine.clone(), 1));
    queue
        .publish(queue_event(
            "cluster-immediate-shutdown",
            "entry:cluster-immediate-shutdown",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();

    let attempts = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let handler_attempts = attempts.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handle = crate::run_cluster_worker(
        queue.clone(),
        cluster_config("immediate-shutdown-node"),
        move |_event: QueueEvent<QueuePayload>| {
            let handler_attempts = handler_attempts.clone();
            Box::pin(async move {
                handler_attempts.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        },
        shutdown_rx,
    )
    .await;

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(1), handle)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        attempts.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "worker started a handler after shutdown"
    );
    assert_eq!(engine.pending_queue_scan_count(), 0);
    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, "cluster-immediate-shutdown");
}

#[tokio::test]
async fn cluster_coordinator_enforces_fixed_completed_history_retention() {
    const THREE_DAYS_SECONDS: i64 = 3 * 24 * 60 * 60;

    let engine = engine();
    let queue = Arc::new(queue(engine.clone(), 1));
    let now = Utc::now().timestamp();
    let expired_count = crate::queue::COMPLETED_EVENT_MAINTENANCE_BATCH_SIZE as usize
        * crate::queue::COMPLETED_EVENT_MAINTENANCE_MAX_BATCHES
        + 1;
    let mut last_expired_key = String::new();
    for index in 0..expired_count {
        last_expired_key = put_completed_queue_event(
            &engine,
            &format!("cluster-expired-{index:05}"),
            now - THREE_DAYS_SECONDS - 1,
        )
        .await;
    }
    let retained_key =
        put_completed_queue_event(&engine, "cluster-retained", now - THREE_DAYS_SECONDS + 60).await;
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let handle = crate::run_cluster_worker(
        queue,
        cluster_config("retention-node"),
        |_event: QueueEvent<QueuePayload>| {
            Box::pin(async move { Ok(QueueHandlerOutcome::Acknowledge) })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(5), async {
        while engine.contains_key(&last_expired_key).await {
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        engine.keys_with_prefix("queue:completed:").await,
        vec![retained_key.clone()]
    );
    assert!(engine.contains_key(&retained_key).await);

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}
