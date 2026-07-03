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
async fn queue_reschedule_moves_pulled_event_to_new_pending_timestamp() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let now = Utc::now().timestamp();
    queue
        .publish(queue_event("event-1", "entry:1", now - 10))
        .await
        .unwrap();

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
                Ok(())
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
