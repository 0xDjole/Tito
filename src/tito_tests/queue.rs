use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::time::Instant;

#[test]
fn queue_event_helpers_parse_key_and_update_schedule() {
    let event = QueueEvent::new("entry:entry-1", queue_payload("payload")).scheduled_for(123);

    assert_eq!(event.key_type(), "entry");
    assert_eq!(event.key_value(), "entry-1");
    assert_eq!(event.event().name, "payload");
    assert_eq!(event.timestamp, 123);
    assert_eq!(event.original_scheduled_at(), 123);
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
    let event = QueueEvent::new("entry:clean", queue_payload("clean"));
    let value = serde_json::to_value(event).unwrap();

    assert!(value.get("retryCount").is_none());
    assert!(value.get("maxRetries").is_none());
    assert!(value.get("errors").is_none());
}

#[tokio::test]
async fn queue_ack_missing_key_is_noop() {
    let engine = engine();
    let queue = queue(engine, 1);

    queue.ack("queue:pending:0000:1:missing").await.unwrap();
}

#[tokio::test]
async fn queue_ack_preserves_malformed_pending_bytes() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let key = "queue:pending:0000:1:bad";
    engine.put_raw(key, b"not-json".to_vec()).await;

    let error = queue.ack(key).await.unwrap_err();

    assert!(matches!(error, TitoError::DeserializationFailed(_)));
    assert!(engine.contains_key(key).await);

    let error = queue
        .schedule_at::<QueuePayload>(key, Utc::now().timestamp() + 60)
        .await
        .unwrap_err();
    assert!(matches!(error, TitoError::DeserializationFailed(_)));
    assert!(engine.contains_key(key).await);
}

#[tokio::test]
async fn queue_pull_preserves_malformed_pending_bytes() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let key = "queue:pending:0000:0:bad";
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
        let key = format!("queue:pending:0000:0:bad-{index:02}");
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
            .keys_with_prefix("queue:pending:0000:0:bad-")
            .await
            .len(),
        50
    );
}

#[test]
fn queue_event_without_original_schedule_falls_back_to_timestamp() {
    let legacy = json!({
        "id": "legacy-event",
        "key": "entry:legacy",
        "payload": { "name": "legacy" },
        "timestamp": 123,
        "state": "pending",
        "processedAt": null
    });
    let event: QueueEvent<QueuePayload> = serde_json::from_value(legacy).unwrap();

    assert_eq!(event.original_scheduled_at, None);
    assert_eq!(event.original_scheduled_at(), 123);
}

#[tokio::test]
async fn queue_schedule_at_atomically_hands_pending_to_successor() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let now = Utc::now().timestamp();
    let original_timestamp = now - 10;
    queue
        .publish(queue_event("event-1", "entry:1", original_timestamp))
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
    let future_timestamp = now + 3_600;

    let successor = queue
        .schedule_at::<QueuePayload>(&storage_key, future_timestamp)
        .await
        .unwrap()
        .unwrap();

    assert!(!engine.contains_key(&storage_key).await);
    assert_eq!(successor.id, current.id);
    assert_eq!(successor.key, current.key);
    assert_eq!(successor.payload, current.payload);
    assert_eq!(successor.timestamp, future_timestamp);
    assert_eq!(
        successor.original_scheduled_at(),
        current.original_scheduled_at()
    );

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, successor.id);
    assert!(
        pending.events[0]
            .0
            .contains(&format!(":{future_timestamp}:")),
        "the exact successor schedule belongs in its pending storage key"
    );

    let completed = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap();
    assert!(completed.events.is_empty());
}

#[tokio::test]
async fn queue_schedule_at_rejects_the_same_invocation_storage_key() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let timestamp = Utc::now().timestamp() - 10;
    queue
        .publish(queue_event("event-1", "entry:1", timestamp))
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
        .schedule_at::<QueuePayload>(&storage_key, timestamp)
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::InvalidInput(_)));
    assert!(engine.contains_key(&storage_key).await);
    assert!(queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn duplicate_schedule_at_calls_create_only_one_successor() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    queue
        .publish(queue_event(
            "event-1",
            "entry:1",
            Utc::now().timestamp() - 10,
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
    let timestamp = Utc::now().timestamp() + 3_600;

    let first_queue = queue.clone();
    let first_key = storage_key.clone();
    let second_queue = queue.clone();
    let second_key = storage_key.clone();
    let (first, second) = tokio::join!(
        async move {
            first_queue
                .schedule_at::<QueuePayload>(&first_key, timestamp)
                .await
                .unwrap()
        },
        async move {
            second_queue
                .schedule_at::<QueuePayload>(&second_key, timestamp)
                .await
                .unwrap()
        }
    );

    assert_eq!(
        usize::from(first.is_some()) + usize::from(second.is_some()),
        1,
        "only the transaction that still finds the exact pending key may create a successor"
    );
    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, current.id);
    assert_ne!(
        pending.events[0].0, storage_key,
        "the successor invocation is identified by its new due storage key"
    );
    let completed = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap();
    assert!(completed.events.is_empty());
}

#[tokio::test]
async fn repeated_schedule_at_keeps_one_pending_row_until_terminal_acknowledgment() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp();
    queue
        .publish(queue_event("event-1", "entry:1", now - 20))
        .await
        .unwrap();

    let (first_key, _) = queue
        .pull::<QueuePayload>(0, None, 10)
        .await
        .unwrap()
        .events
        .into_iter()
        .next()
        .unwrap();
    queue
        .schedule_at::<QueuePayload>(&first_key, now - 10)
        .await
        .unwrap()
        .unwrap();

    let (second_key, _) = queue
        .pull::<QueuePayload>(0, None, 10)
        .await
        .unwrap()
        .events
        .into_iter()
        .next()
        .unwrap();
    queue
        .schedule_at::<QueuePayload>(&second_key, now + 3_600)
        .await
        .unwrap()
        .unwrap();

    let completed = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap();
    assert!(completed.events.is_empty());

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, "event-1");
    assert_eq!(pending.events[0].1.timestamp, now + 3_600);

    queue.ack(&pending.events[0].0).await.unwrap();
    assert!(queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
    let completed = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap();
    assert_eq!(completed.events.len(), 1);
    assert_eq!(completed.events[0].1.id, "event-1");
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
            "queue:pending:0000:1:pending",
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
async fn queue_worker_schedule_at_creates_a_new_future_invocation() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    let now = Utc::now().timestamp();
    let future_timestamp = now + 3_600;
    queue
        .publish(queue_event(
            "worker-schedule",
            "entry:worker-schedule",
            now - 10,
        ))
        .await
        .unwrap();
    let processed = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handler_processed = processed.clone();

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig::new(0..1),
        move |_event| {
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
                handler_processed.notify_one();
                Ok(QueueHandlerOutcome::ScheduleAt(future_timestamp))
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(2), processed.notified())
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let pending = queue
                .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
                .await
                .unwrap();
            if pending.events.len() == 1
                && pending.events[0].1.id == "worker-schedule"
                && pending.events[0].1.timestamp == future_timestamp
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert!(queue
        .scan_by_state::<QueuePayload>(QueueEventState::Completed, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queue_worker_redelivers_same_pending_invocation_after_error_at_poll_cadence() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    queue
        .publish(queue_event(
            "worker-redelivery",
            "entry:worker-redelivery",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let (attempt_tx, mut attempt_rx) = mpsc::unbounded_channel();
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handler_attempts = attempts.clone();

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig::new(0..1),
        move |event| {
            let attempt_tx = attempt_tx.clone();
            let handler_attempts = handler_attempts.clone();
            Box::pin(async move {
                let attempt = handler_attempts.fetch_add(1, Ordering::SeqCst);
                attempt_tx.send((event.id, Instant::now())).unwrap();
                if attempt == 0 {
                    Err(TitoError::Internal("handler failed".to_string()))
                } else {
                    Ok(QueueHandlerOutcome::Acknowledge)
                }
            })
        },
        shutdown_rx,
    )
    .await;

    let first = timeout(Duration::from_secs(2), attempt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let second = timeout(Duration::from_secs(3), attempt_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.0, "worker-redelivery");
    assert_eq!(second.0, first.0, "redelivery keeps the invocation ID");
    assert!(
        second.1.duration_since(first.1) >= Duration::from_millis(900),
        "an unchanged due row must wait for the normal worker poll cadence"
    );
    wait_for_completed(&queue, "worker-redelivery").await;

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
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
                    Err(TitoError::Internal("leave pending".to_string()))
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
                .find_by_state_after::<QueuePayload>(QueueEventState::Completed, 0, 10)
                .await
                .unwrap();
            if completed.iter().any(|(_, event)| event.id == event_id) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
}
