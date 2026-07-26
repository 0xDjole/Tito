use super::*;

#[test]
fn queue_event_helpers_parse_key_and_update_builder_fields() {
    let event = QueueEvent::new("entry:entry-1", queue_payload("payload"))
        .scheduled_for(123)
        .with_max_retries(5);

    assert_eq!(event.key_type(), "entry");
    assert_eq!(event.key_value(), "entry-1");
    assert_eq!(event.event().name, "payload");
    assert_eq!(event.timestamp, 123);
    assert_eq!(event.original_scheduled_at(), 123);
    assert_eq!(event.max_retries, 5);
}

#[tokio::test]
async fn queue_ack_missing_key_is_noop() {
    let engine = engine();
    let queue = queue(engine, 1);

    queue.ack("queue:pending:0000:1:missing").await.unwrap();
}

#[tokio::test]
async fn queue_ack_deletes_orphan_event_bytes() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let key = "queue:pending:0000:1:bad";
    engine.put_raw(key, b"not-json".to_vec()).await;

    queue.ack(key).await.unwrap();

    assert!(!engine.contains_key(key).await);
}

#[tokio::test]
async fn queue_reschedule_updates_due_time_but_preserves_original_schedule() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let now = Utc::now().timestamp();
    let original_timestamp = now - 10;
    let mut legacy_event = queue_event("event-1", "entry:1", original_timestamp);
    legacy_event.original_scheduled_at = None;
    queue.publish(legacy_event).await.unwrap();

    let pulled = queue.pull::<QueuePayload>(0, 10).await.unwrap();
    assert_eq!(pulled.len(), 1);
    let (old_storage_key, event) = pulled.into_iter().next().unwrap();
    let future_timestamp = now + 3_600;

    queue
        .reschedule(event, &old_storage_key, future_timestamp)
        .await
        .unwrap();

    assert!(!engine.contains_key(&old_storage_key).await);
    assert!(queue.pull::<QueuePayload>(0, 10).await.unwrap().is_empty());
    let page = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(page.events.len(), 1);
    assert_eq!(page.events[0].1.timestamp, future_timestamp);
    assert_eq!(page.events[0].1.original_scheduled_at(), original_timestamp);
    assert!(
        page.events[0].0.contains(&format!(":{future_timestamp}:")),
        "retry not-before time belongs in the pending storage key"
    );
    let scheduled_after_now = queue
        .find_by_state_after::<QueuePayload>(QueueEventState::Pending, now, 10)
        .await
        .unwrap();
    assert_eq!(scheduled_after_now.len(), 1);
    assert_eq!(scheduled_after_now[0].1.id, "event-1");
}

#[tokio::test]
async fn queue_stale_reschedule_cannot_resurrect_an_acknowledged_event() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp();
    queue
        .publish(queue_event("event-acked", "entry:acked", now - 10))
        .await
        .unwrap();

    let (storage_key, stale_event) = queue
        .pull::<QueuePayload>(0, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    queue.ack(&storage_key).await.unwrap();
    queue
        .reschedule(stale_event, &storage_key, now + 3_600)
        .await
        .unwrap();

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
    assert_eq!(completed[0].1.id, "event-acked");
}

#[tokio::test]
async fn queue_delivery_generation_fences_same_storage_key_reschedule_race() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let due_at = Utc::now().timestamp() - 10;
    queue
        .publish(queue_event("event-same-key", "entry:same-key", due_at))
        .await
        .unwrap();

    let (storage_key, first_delivery) = queue
        .pull::<QueuePayload>(0, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let stale_delivery = first_delivery.clone();
    queue
        .apply_handler_outcome(
            first_delivery,
            &storage_key,
            QueueHandlerOutcome::RescheduleAt(due_at),
        )
        .await
        .unwrap();
    queue
        .apply_handler_outcome(
            stale_delivery,
            &storage_key,
            QueueHandlerOutcome::RescheduleAt(due_at + 3_600),
        )
        .await
        .unwrap();

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].0, storage_key);
    assert_eq!(pending.events[0].1.timestamp, due_at);
    assert_eq!(pending.events[0].1.id, "event-same-key");
    assert_eq!(pending.events[0].1.retry_count, 0);
    assert!(pending.events[0].1.errors.is_empty());
    assert_eq!(
        engine
            .raw_json(&storage_key)
            .await
            .unwrap()
            .get("deliveryGeneration")
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );
}

#[test]
fn queue_event_without_original_schedule_falls_back_to_timestamp() {
    let legacy = serde_json::json!({
        "id": "legacy-event",
        "key": "entry:legacy",
        "payload": { "name": "legacy" },
        "timestamp": 123,
        "state": "pending",
        "processedAt": null,
        "retryCount": 0,
        "maxRetries": 3,
        "errors": []
    });
    let event: QueueEvent<QueuePayload> = serde_json::from_value(legacy).unwrap();

    assert_eq!(event.original_scheduled_at, None);
    assert_eq!(event.original_scheduled_at(), 123);
}

#[test]
fn queue_retry_backoff_is_exponential_and_capped() {
    assert_eq!(retry_backoff_seconds(1), 2);
    assert_eq!(retry_backoff_seconds(7), 128);
    assert_eq!(retry_backoff_seconds(8), 256);
    assert_eq!(retry_backoff_seconds(9), 300);
    assert_eq!(retry_backoff_seconds(u32::MAX), 300);
}

#[tokio::test]
async fn queue_move_to_dlq_is_failed_alias() {
    let engine = engine();
    let queue = queue(engine, 1);
    queue
        .publish(queue_event(
            "event-1",
            "entry:1",
            Utc::now().timestamp() - 10,
        ))
        .await
        .unwrap();
    let (storage_key, event) = queue
        .pull::<QueuePayload>(0, 10)
        .await
        .unwrap()
        .into_iter()
        .next()
        .unwrap();

    queue.move_to_dlq(event, &storage_key).await.unwrap();

    let failed = queue
        .find_by_state_after::<QueuePayload>(QueueEventState::Failed, 0, 10)
        .await
        .unwrap();
    assert_eq!(failed.len(), 1);
    assert_eq!(failed[0].1.state, QueueEventState::Failed);
}

#[tokio::test]
async fn queue_clear_removes_pending_completed_failed_and_dlq_rows() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let now = Utc::now().timestamp();
    let pending = queue_event("pending", "entry:pending", now);
    let mut completed = queue_event("completed", "entry:completed", now);
    completed.state = QueueEventState::Completed;
    completed.processed_at = Some(now);
    let mut failed = queue_event("failed", "entry:failed", now);
    failed.state = QueueEventState::Failed;
    failed.processed_at = Some(now);
    let mut dlq = queue_event("dlq", "entry:dlq", now);
    dlq.state = QueueEventState::Failed;
    dlq.processed_at = Some(now);

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
    engine
        .put_raw(
            "queue:failed:0000:1:failed",
            serde_json::to_vec(&failed).unwrap(),
        )
        .await;
    engine
        .put_raw("queue:dlq:0000:1:dlq", serde_json::to_vec(&dlq).unwrap())
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
async fn queue_worker_acks_successful_jobs() {
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
        WorkerConfig {
            partition_range: 0..1,
        },
        move |event| {
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
                assert_eq!(event.id, "worker-success");
                handler_processed.notify_one();
                Ok(QueueHandlerOutcome::Done)
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
            let completed = queue
                .find_by_state_after::<QueuePayload>(QueueEventState::Completed, 0, 10)
                .await
                .unwrap();
            if completed
                .iter()
                .any(|(_, event)| event.id == "worker-success")
            {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

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
async fn queue_worker_reschedules_to_exact_time_without_retry_or_error() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    let original_schedule = Utc::now().timestamp() - 10;
    let rescheduled_at = Utc::now().timestamp() + 60;
    queue
        .publish(queue_event(
            "worker-rescheduled",
            "entry:worker-rescheduled",
            original_schedule,
        ))
        .await
        .unwrap();
    let handled = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handled_by_handler = handled.clone();

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig {
            partition_range: 0..1,
        },
        move |_event| {
            let handled_by_handler = handled_by_handler.clone();
            Box::pin(async move {
                handled_by_handler.notify_one();
                Ok(QueueHandlerOutcome::RescheduleAt(rescheduled_at))
            })
        },
        shutdown_rx,
    )
    .await;

    timeout(Duration::from_secs(2), handled.notified())
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let pending = queue
                .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
                .await
                .unwrap();
            if pending.events.iter().any(|(_, event)| {
                event.id == "worker-rescheduled" && event.timestamp == rescheduled_at
            }) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap()
        .events
        .into_iter()
        .find(|(_, event)| event.id == "worker-rescheduled")
        .unwrap()
        .1;
    assert_eq!(pending.original_scheduled_at(), original_schedule);
    assert_eq!(pending.retry_count, 0);
    assert!(pending.errors.is_empty());
}

#[tokio::test]
async fn queue_worker_moves_exhausted_retries_to_failed() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    let mut event = queue_event(
        "worker-failed",
        "entry:worker-failed",
        Utc::now().timestamp() - 10,
    );
    event.max_retries = 0;
    queue.publish(event).await.unwrap();
    let processed = Arc::new(Notify::new());
    let (shutdown_tx, shutdown_rx) = broadcast::channel(1);
    let handler_processed = processed.clone();

    let handle = run_worker::<_, QueuePayload, _>(
        queue.clone(),
        WorkerConfig {
            partition_range: 0..1,
        },
        move |_event| {
            let handler_processed = handler_processed.clone();
            Box::pin(async move {
                handler_processed.notify_one();
                Err(TitoError::Internal("handler failed".to_string()))
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
            let failed = queue
                .find_by_state_after::<QueuePayload>(QueueEventState::Failed, 0, 10)
                .await
                .unwrap();
            if failed.iter().any(|(_, event)| {
                event.id == "worker-failed"
                    && event.retry_count == 1
                    && event.errors == vec!["Unexpected error: handler failed".to_string()]
            }) {
                break;
            }
            sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    let _ = shutdown_tx.send(());
    timeout(Duration::from_secs(2), handle)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn queue_handler_reschedule_preserves_event_without_spending_retry_budget() {
    let engine = engine();
    let queue = queue(engine, 1);
    let original_schedule = Utc::now().timestamp() - 10;
    let mut event = queue_event(
        "worker-deferred",
        "entry:worker-deferred",
        original_schedule,
    );
    event.max_retries = 0;
    queue.publish(event).await.unwrap();
    let (storage_key, event) = queue
        .pull::<QueuePayload>(0, 1)
        .await
        .unwrap()
        .pop()
        .unwrap();
    let rescheduled_at = Utc::now().timestamp() + 17;
    queue
        .apply_handler_outcome(
            event,
            &storage_key,
            QueueHandlerOutcome::RescheduleAt(rescheduled_at),
        )
        .await
        .unwrap();

    let pending = queue
        .scan_by_state::<QueuePayload>(QueueEventState::Pending, None, 10)
        .await
        .unwrap()
        .events
        .pop()
        .unwrap()
        .1;
    assert_eq!(pending.state, QueueEventState::Pending);
    assert_eq!(pending.timestamp, rescheduled_at);
    assert_eq!(pending.retry_count, 0);
    assert!(pending.errors.is_empty());
    assert_eq!(pending.original_scheduled_at(), original_schedule);
    assert!(queue
        .find_by_state_after::<QueuePayload>(QueueEventState::Failed, 0, 10)
        .await
        .unwrap()
        .is_empty());
}
