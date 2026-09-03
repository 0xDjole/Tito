use super::*;
use crate::QueueOwner;
use std::sync::atomic::{AtomicUsize, Ordering};

#[test]
fn queue_event_helpers_read_the_event_timestamp() {
    let event = QueueEvent::new("entry:entry-1", queue_payload("payload"), 123);

    assert_eq!(event.key_type(), "entry");
    assert_eq!(event.key_value(), "entry-1");
    assert_eq!(event.event().name, "payload");
    assert_eq!(event.timestamp, 123);
}

#[test]
fn queue_state_has_no_failed_or_dead_letter_variant() {
    assert_eq!(
        serde_json::to_value(QueueEventState::Pending).unwrap(),
        json!("pending")
    );
    assert_eq!(
        serde_json::to_value(QueueEventState::Completed).unwrap(),
        json!("completed")
    );
    assert!(serde_json::from_value::<QueueEventState>(json!("failed")).is_err());
    assert!(serde_json::from_value::<QueueEventState>(json!("dead_letter")).is_err());
    assert!(serde_json::from_value::<QueueEventState>(json!("processing")).is_err());
}

#[test]
fn queue_event_serialization_contains_no_retry_policy_metadata() {
    let event = QueueEvent::new(
        "entry:clean",
        queue_payload("clean"),
        Utc::now().timestamp(),
    );
    let value = serde_json::to_value(&event).unwrap();

    assert!(value.get("retryCount").is_none());
    assert!(value.get("maxRetries").is_none());
    assert!(value.get("errors").is_none());
    assert!(
        value.get("completionReason").is_none(),
        "pending rows do not carry terminal completion metadata"
    );
}

#[tokio::test]
async fn queue_pending_key_orders_by_event_timestamp_and_enqueue_generation() {
    let engine = engine();
    let queue = queue(engine, 1);
    queue
        .publish(queue_event("event-1", "entry:1", 123))
        .await
        .unwrap();

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    let fields: Vec<_> = pending.events[0].0.splitn(6, ':').collect();

    assert_eq!(fields.len(), 6);
    assert_eq!(
        &fields[..4],
        ["queue", "pending", "0000", "00000000000000000123"]
    );
    assert_eq!(fields[4].len(), 20);
    assert!(fields[4].bytes().all(|byte| byte.is_ascii_digit()));
    assert!(fields[4].parse::<u64>().unwrap() > 0);
    assert_eq!(fields[5], "event-1");
}

#[tokio::test]
async fn queue_ack_missing_key_is_noop() {
    let engine = engine();
    let queue = queue(engine, 1);

    queue
        .ack("queue:pending:0000:00000000000000000001:00000000000000000000:missing")
        .await
        .unwrap();
}

#[tokio::test]
async fn queue_ack_preserves_malformed_pending_bytes() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let key = "queue:pending:0000:00000000000000000001:00000000000000000000:bad";
    engine.put_raw(key, b"not-json".to_vec()).await;

    let error = queue.ack(key).await.unwrap_err();

    assert!(matches!(error, TitoError::DeserializationFailed(_)));
    assert!(engine.contains_key(key).await);
}

#[tokio::test]
async fn queue_reschedule_atomically_completes_current_and_inserts_supplied_event() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp();
    queue
        .publish(QueueEvent::new(
            "entry:1",
            queue_payload("current"),
            now - 1,
        ))
        .await
        .unwrap();
    let (storage_key, current) = queue
        .pull::<QueuePayload>(0, None, 10)
        .await
        .unwrap()
        .events
        .into_iter()
        .next()
        .unwrap();
    let next = current.rescheduled(now + 60);
    let next_id = next.id.clone();
    let created_at_millis = next.created_at_millis();
    assert_eq!(next_id, current.id);
    assert_eq!(created_at_millis, current.created_at_millis());

    queue.reschedule(&storage_key, next).await.unwrap();

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, next_id);
    assert_eq!(pending.events[0].1.timestamp, now + 60);
    let completed = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap();
    assert_eq!(completed.events.len(), 1);
    assert_eq!(completed.events[0].1.id, next_id);
}

#[tokio::test]
async fn queue_reschedule_rejects_a_different_logical_event() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp();
    queue
        .publish(queue_event("current", "entry:1", now - 1))
        .await
        .unwrap();
    let (storage_key, _) = queue
        .pull::<QueuePayload>(0, None, 10)
        .await
        .unwrap()
        .events
        .into_iter()
        .next()
        .unwrap();

    let error = queue
        .reschedule(&storage_key, queue_event("different", "entry:1", now + 60))
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::InvalidInput(_)));
    assert_eq!(
        queue
            .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
    assert!(queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn queue_reschedule_still_rejects_a_changed_payload() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp();
    queue
        .publish(queue_event("reschedule-payload", "entry:1", now - 1))
        .await
        .unwrap();
    let (storage_key, current) = queue
        .pull::<QueuePayload>(0, None, 10)
        .await
        .unwrap()
        .events
        .into_iter()
        .next()
        .unwrap();
    let mut next = current.rescheduled(now + 60);
    next.payload = queue_payload("changed");

    let error = queue.reschedule(&storage_key, next).await.unwrap_err();

    assert!(matches!(error, TitoError::InvalidInput(message)
        if message == "A rescheduled row must preserve the event payload"));
    assert_eq!(
        queue
            .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
            .await
            .unwrap()
            .events,
        vec![(storage_key, current)]
    );
    assert!(queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn queue_advance_atomically_preserves_current_history_and_owner() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let now = Utc::now().timestamp();
    let owner = QueueOwner::new("store", "advance-owner").unwrap();
    queue
        .publish(queue_event("advance", "entry:1", now - 1).with_owner(owner.clone()))
        .await
        .unwrap();
    let (storage_key, current) = queue
        .pull::<QueuePayload>(0, None, 10)
        .await
        .unwrap()
        .events
        .into_iter()
        .next()
        .unwrap();
    let mut next = current.rescheduled(now + 60);
    next.payload = queue_payload("advanced");

    queue.advance(&storage_key, next.clone()).await.unwrap();

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1, next);
    let completed = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap();
    assert_eq!(completed.events.len(), 1);
    assert_eq!(completed.events[0].1.id, current.id);
    assert_eq!(completed.events[0].1.key, current.key);
    assert_eq!(completed.events[0].1.owner, Some(owner));
    assert_eq!(completed.events[0].1.payload, current.payload);
    assert_eq!(completed.events[0].1.timestamp, current.timestamp);
    assert_eq!(completed.events[0].1.state, QueueEventState::Completed);
    assert!(completed.events[0].1.processed_at.is_some());

    let owner_indexes = engine.keys_with_prefix("queue:owner:").await;
    assert_eq!(owner_indexes.len(), 2);
    assert!(owner_indexes.iter().any(|key| key.contains(":pending:")));
    assert!(owner_indexes.iter().any(|key| key.contains(":completed:")));
}

#[tokio::test]
async fn queue_advance_rejects_unchanged_payload_and_identity_changes() {
    for mutation in ["payload", "id", "key", "owner"] {
        let engine = engine();
        let queue = queue(engine.clone(), 1);
        let now = Utc::now().timestamp();
        let owner = QueueOwner::new("store", "original-owner").unwrap();
        queue
            .publish(queue_event("advance-reject", "entry:1", now - 1).with_owner(owner.clone()))
            .await
            .unwrap();
        let (storage_key, current) = queue
            .pull::<QueuePayload>(0, None, 10)
            .await
            .unwrap()
            .events
            .into_iter()
            .next()
            .unwrap();
        let mut next = current.rescheduled(now + 60);
        if mutation != "payload" {
            next.payload = queue_payload("changed");
        }
        match mutation {
            "payload" => {}
            "id" => next.id = "different-id".to_string(),
            "key" => next.key = "entry:different".to_string(),
            "owner" => {
                next.owner = Some(QueueOwner::new("store", "different-owner").unwrap());
            }
            _ => unreachable!(),
        }

        let error = queue.advance(&storage_key, next).await.unwrap_err();

        assert!(matches!(error, TitoError::InvalidInput(_)), "{mutation}");
        assert_eq!(
            queue
                .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
                .await
                .unwrap()
                .events,
            vec![(storage_key, current)]
        );
        assert!(queue
            .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
            .await
            .unwrap()
            .events
            .is_empty());
        let owner_indexes = engine.keys_with_prefix("queue:owner:").await;
        assert_eq!(owner_indexes.len(), 1);
        assert!(owner_indexes[0].contains(":pending:"));
    }
}

#[tokio::test]
async fn queue_rejects_negative_timestamps_without_creating_unreachable_rows() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let error = queue
        .publish(queue_event("negative-event", "entry:negative", -1))
        .await
        .unwrap_err();
    assert!(matches!(error, TitoError::InvalidInput(_)));
    assert!(engine.keys_with_prefix("queue:").await.is_empty());
}

#[tokio::test]
async fn queue_ack_indeterminate_commit_converges_to_one_of_the_two_atomic_states() {
    for after_apply in [false, true] {
        let engine = engine();
        let queue = queue(engine.clone(), 1);
        let timestamp = Utc::now().timestamp() - 10;
        let owner = QueueOwner::new("store", "atomic-owner").unwrap();
        queue
            .publish(queue_event("event-1", "entry:1", timestamp).with_owner(owner.clone()))
            .await
            .unwrap();
        let (storage_key, current) = queue
            .pull::<QueuePayload>(0, None, 10)
            .await
            .unwrap()
            .events
            .into_iter()
            .next()
            .unwrap();
        engine.make_next_commit_outcome_unknown(after_apply).await;

        let error = queue.ack(&storage_key).await.unwrap_err();

        assert!(matches!(error, TitoError::CommitOutcomeUnknown(_)));
        let pending = queue
            .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
            .await
            .unwrap();
        let completed = queue
            .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
            .await
            .unwrap();
        let owner_indexes = engine.keys_with_prefix("queue:owner:").await;
        assert_eq!(owner_indexes.len(), 1);
        if after_apply {
            assert!(pending.events.is_empty());
            assert_eq!(completed.events.len(), 1);
            assert_eq!(completed.events[0].1.id, current.id);
            assert_eq!(completed.events[0].1.state, QueueEventState::Completed);
            assert!(owner_indexes[0].contains(":completed:"));
        } else {
            assert_eq!(pending.events.len(), 1);
            assert_eq!(pending.events[0].0, storage_key);
            assert_eq!(pending.events[0].1.id, current.id);
            assert!(completed.events.is_empty());
            assert!(owner_indexes[0].contains(":pending:"));
        }
    }
}

#[tokio::test]
async fn queue_pull_preserves_malformed_pending_bytes() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let key = "queue:pending:0000:00000000000000000000:00000000000000000000:bad";
    engine.put_raw(key, b"not-json".to_vec()).await;

    let pulled = queue.pull::<QueuePayload>(0, None, 10).await.unwrap();

    assert!(pulled.events.is_empty());
    assert!(engine.contains_key(key).await);
}

#[tokio::test]
async fn queue_pull_cursor_advances_past_a_full_malformed_page() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    for index in 0..50 {
        let key =
            format!("queue:pending:0000:00000000000000000000:00000000000000000000:bad-{index:02}");
        engine.put_raw(&key, b"not-json".to_vec()).await;
    }
    queue
        .publish(queue_event(
            "zz-valid",
            "entry:zz-valid",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();

    let first = queue.pull::<QueuePayload>(0, None, 50).await.unwrap();
    assert!(first.events.is_empty());
    assert!(first.next_cursor.is_some());

    let second = queue
        .pull::<QueuePayload>(0, first.next_cursor, 50)
        .await
        .unwrap();
    assert_eq!(second.events.len(), 1);
    assert_eq!(second.events[0].1.id, "zz-valid");
    assert!(second.next_cursor.is_none());
    assert_eq!(
        engine
            .keys_with_prefix("queue:pending:0000:00000000000000000000:00000000000000000000:bad-",)
            .await
            .len(),
        50
    );
}

#[tokio::test]
async fn queue_clear_removes_pending_and_completed_rows() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let now = Utc::now().timestamp();
    let pending = queue_event("pending", "entry:pending", now);
    let mut completed = queue_event("completed", "entry:completed", now);
    completed.state = QueueEventState::Completed;
    completed.processed_at = Some(now);

    engine
        .put_raw(
            "queue:pending:0000:00000000000000000001:00000000000000000000:pending",
            serde_json::to_vec(&pending).unwrap(),
        )
        .await;
    engine
        .put_raw(
            "queue:completed:00000000000000000001:completed",
            serde_json::to_vec(&completed).unwrap(),
        )
        .await;
    queue.clear().await.unwrap();

    assert!(engine.keys_with_prefix("queue:").await.is_empty());
}

#[tokio::test]
async fn queue_delete_by_state_before_rejects_pending_state() {
    let engine = engine();
    let queue = queue(engine, 1);

    let error = queue
        .delete_by_state_before(QueueEventState::Pending, Utc::now().timestamp(), 10)
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::InvalidInput(_)));
}

#[tokio::test]
async fn queue_completed_retention_uses_the_terminal_time_index_not_value_decoding() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let cutoff = Utc::now().timestamp() - 1;
    let malformed_key = format!(
        "queue:completed:{:020}:00000000000000000000:malformed",
        cutoff - 1
    );
    engine.put_raw(&malformed_key, b"not-json".to_vec()).await;

    let deleted = queue
        .delete_by_state_before(QueueEventState::Completed, cutoff, 10)
        .await
        .unwrap();

    assert_eq!(deleted, 1);
    assert!(!engine.contains_key(&malformed_key).await);
}

#[tokio::test]
async fn standalone_worker_enforces_configured_completed_history_retention() {
    const RETENTION_SECONDS: i64 = 60 * 60;
    let engine = engine();
    let queue = Arc::new(Queue::new(
        engine.clone(),
        QueueConfig::new(1, Duration::from_secs(RETENTION_SECONDS as u64)),
    ));
    let now = Utc::now().timestamp();
    let expired_count = crate::queue::COMPLETED_EVENT_MAINTENANCE_BATCH_SIZE as usize
        * crate::queue::COMPLETED_EVENT_MAINTENANCE_MAX_BATCHES
        + 1;
    let mut last_expired_key = String::new();
    for index in 0..expired_count {
        last_expired_key = put_completed_queue_event(
            &engine,
            &format!("expired-{index:05}"),
            now - RETENTION_SECONDS - 1,
        )
        .await;
    }
    let retained_key =
        put_completed_queue_event(&engine, "retained", now - RETENTION_SECONDS + 60).await;
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let handle = run_worker(
        queue,
        WorkerConfig::new(0..1),
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

#[tokio::test]
async fn queue_scan_cursor_continues_after_previous_page() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp();
    for id in ["event-1", "event-2", "event-3"] {
        queue.publish(queue_event(id, id, now - 10)).await.unwrap();
    }

    let first = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 2)
        .await
        .unwrap();
    assert_eq!(first.events.len(), 2);
    assert!(first.next_cursor.is_some());

    let second = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, first.next_cursor, 2)
        .await
        .unwrap();
    assert_eq!(second.events.len(), 1);
}

#[tokio::test]
async fn queue_worker_acknowledges_successful_jobs() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    queue
        .publish(queue_event(
            "worker-success",
            "entry:worker-success",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let processed = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handler_processed = processed.clone();

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig::new(0..1),
        move |event| {
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
                assert_eq!(event.id, "worker-success");
                handler_processed.notify_one();
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(2), processed.notified())
        .await
        .unwrap();
    wait_for_completed(&queue, "worker-success").await;

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
    assert!(queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn queue_worker_applies_an_advance_outcome() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    let now = Utc::now().timestamp();
    let owner = QueueOwner::new("store", "worker-advance").unwrap();
    queue
        .publish(
            queue_event("worker-advance", "entry:worker-advance", now - 10)
                .with_owner(owner.clone()),
        )
        .await
        .unwrap();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig::new(0..1),
        move |event| {
            Box::pin(async move {
                let mut next = event.rescheduled(now + 3600);
                next.payload = queue_payload("worker-advanced");
                Ok(QueueHandlerOutcome::Advance(next))
            })
        },
        shutdown_rx,
    )
    .await;

    wait_for_completed(&queue, "worker-advance").await;
    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, "worker-advance");
    assert_eq!(pending.events[0].1.key, "entry:worker-advance");
    assert_eq!(pending.events[0].1.owner, Some(owner));
    assert_eq!(
        pending.events[0].1.payload,
        queue_payload("worker-advanced")
    );
    assert_eq!(pending.events[0].1.timestamp, now + 3600);

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queue_worker_handler_error_leaves_exact_pending_invocation_unchanged() {
    let engine = engine();
    let queue = Arc::new(queue(engine.clone(), 1));
    queue
        .publish(queue_event(
            "worker-error",
            "entry:worker-error",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let initial = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    let (storage_key, event) = initial.events.into_iter().next().unwrap();
    let pending_bytes = engine.raw_bytes(&storage_key).await.unwrap();

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler_started = started.clone();
    let handler_release = release.clone();
    let handler_event = event.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig::new(0..1),
        move |handled_event| {
            let handler_started = handler_started.clone();
            let handler_release = handler_release.clone();
            let handler_event = handler_event.clone();
            Box::pin(async move {
                assert_eq!(handled_event, handler_event);
                handler_started.notify_one();
                handler_release.notified().await;
                Err(TitoError::Internal("simulated handler error".to_string()))
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(2), started.notified())
        .await
        .unwrap();
    let _ = shutdown_tx.send(());
    release.notify_one();
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events, vec![(storage_key.clone(), event)]);
    assert_eq!(engine.raw_bytes(&storage_key).await, Some(pending_bytes));
    assert_eq!(
        engine.keys_with_prefix("queue:pending:").await,
        vec![storage_key]
    );
    assert!(engine.keys_with_prefix("queue:completed:").await.is_empty());
}

#[tokio::test]
async fn standalone_worker_shutdown_drains_started_handler_before_join() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    let now = Utc::now().timestamp() - 10;
    queue
        .publish(queue_event(
            "standalone-drain",
            "entry:standalone-drain",
            now,
        ))
        .await
        .unwrap();
    queue
        .publish(queue_event(
            "standalone-second",
            "entry:standalone-second",
            now,
        ))
        .await
        .unwrap();

    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let attempts = Arc::new(AtomicUsize::new(0));
    let handler_started = started.clone();
    let handler_release = release.clone();
    let handler_attempts = attempts.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let mut handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig::new(0..1),
        move |event| {
            let handler_started = handler_started.clone();
            let handler_release = handler_release.clone();
            let handler_attempts = handler_attempts.clone();
            Box::pin(async move {
                handler_attempts.fetch_add(1, Ordering::SeqCst);
                assert_eq!(event.id, "standalone-drain");
                handler_started.notify_one();
                handler_release.notified().await;
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
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        !handle.is_finished(),
        "worker joined before its started handler drained"
    );

    release.notify_one();
    timeout(Duration::from_secs(2), &mut handle)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
    let completed = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap();
    assert_eq!(completed.events.len(), 1);
    assert_eq!(completed.events[0].1.id, "standalone-drain");
    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, "standalone-second");
}

#[tokio::test]
async fn queue_worker_does_not_let_unacknowledged_first_page_starve_later_rows() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    let timestamp = Utc::now().timestamp() - 10;
    for index in 0..50 {
        let id = format!("blocked-{index:02}");
        queue
            .publish(queue_event(&id, &format!("entry:{id}"), timestamp))
            .await
            .unwrap();
    }
    queue
        .publish(queue_event("zz-target", "entry:zz-target", timestamp))
        .await
        .unwrap();

    let target_processed = Arc::new(Notify::new());
    let handler_target_processed = target_processed.clone();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig::new(0..1),
        move |event| {
            let handler_target_processed = handler_target_processed.clone();
            Box::pin(async move {
                if event.id == "zz-target" {
                    handler_target_processed.notify_one();
                    Ok(QueueHandlerOutcome::Acknowledge)
                } else {
                    panic!("simulated interrupted handler")
                }
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(3), target_processed.notified())
        .await
        .unwrap();
    wait_for_completed(&queue, "zz-target").await;

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queue_worker_contains_panic_and_redelivers_the_pending_invocation() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    queue
        .publish(queue_event(
            "worker-panic",
            "entry:worker-panic",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let processed = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handler_attempts = attempts.clone();
    let handler_processed = processed.clone();

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig::new(0..1),
        move |event| {
            let handler_attempts = handler_attempts.clone();
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
                assert_eq!(event.id, "worker-panic");
                if handler_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("simulated handler panic");
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
    wait_for_completed(&queue, "worker-panic").await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queue_worker_contains_synchronous_handler_panic_and_redelivers() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    queue
        .publish(queue_event(
            "worker-sync-panic",
            "entry:worker-sync-panic",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let processed = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handler_attempts = attempts.clone();
    let handler_processed = processed.clone();

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig::new(0..1),
        move |event| {
            assert_eq!(event.id, "worker-sync-panic");
            if handler_attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("simulated synchronous handler panic");
            }
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
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
    wait_for_completed(&queue, "worker-sync-panic").await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queue_worker_times_out_handler_and_redelivers_pending_invocation() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    queue
        .publish(queue_event(
            "worker-timeout",
            "entry:worker-timeout",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let processed = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handler_attempts = attempts.clone();
    let handler_processed = processed.clone();
    let mut config = WorkerConfig::new(0..1);
    config.handler_timeout = Duration::from_millis(20);

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        config,
        move |event| {
            let attempt = handler_attempts.fetch_add(1, Ordering::SeqCst);
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
                assert_eq!(event.id, "worker-timeout");
                if attempt == 0 {
                    std::future::pending::<()>().await;
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
    wait_for_completed(&queue, "worker-timeout").await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

async fn wait_for_completed(queue: &Arc<Queue<MemoryEngine>>, event_id: &str) {
    timeout(Duration::from_secs(2), async {
        loop {
            let completed = queue
                .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
                .await
                .unwrap();
            if completed
                .events
                .iter()
                .any(|(_, event)| event.id == event_id)
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
