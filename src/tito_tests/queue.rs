use super::*;
use crate::{encode_index_integer, QueueOwner};
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
fn queue_created_at_milliseconds_preserves_native_id_precision_and_exact_fallback() {
    let mut event = QueueEvent::new("entry:clock", queue_payload("clock"), 123);
    event.id = format!("{:020}-event", 1_700_000_000_123_456_i64);
    assert_eq!(event.created_at_millis(), 1_700_000_000_123);
    assert_eq!(
        event.rescheduled(1_700_000_000_987).created_at_millis(),
        1_700_000_000_123
    );

    for timestamp in [
        i64::MIN,
        -9_007_199_254_740_991,
        -1,
        0,
        123,
        1_700_000_000_123,
        i64::MAX,
    ] {
        let event = queue_event("opaque-event-id", "entry:clock", timestamp);
        assert_eq!(event.created_at_millis(), timestamp);
        let encoded = serde_json::to_value(&event).unwrap();
        assert_eq!(encoded["timestamp"], json!(timestamp));
        let decoded: QueueEvent<QueuePayload> = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded.timestamp, timestamp);
    }
}

#[tokio::test]
async fn queue_signed_millisecond_due_times_are_reachable_ordered_paged_and_never_rounded() {
    let engine = engine();
    let queue = queue(engine, 1);
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
    for timestamp in timestamps.into_iter().rev() {
        queue
            .publish(queue_event(
                &format!("at-{timestamp}"),
                "entry:clock",
                timestamp,
            ))
            .await
            .unwrap();
    }

    let mut cursor = None;
    let mut due = Vec::new();
    loop {
        let page = queue.pull::<QueuePayload>(0, cursor, 3).await.unwrap();
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
        let page = queue
            .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, cursor, 4)
            .await
            .unwrap();
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

#[test]
fn queue_status_has_no_failed_or_dead_letter_variant() {
    assert_eq!(
        serde_json::to_value(QueueEventStatus::Pending).unwrap(),
        json!({"type": "pending"})
    );
    assert_eq!(
        serde_json::to_value(QueueEventStatus::Completed).unwrap(),
        json!({"type": "completed"})
    );
    for unsupported in ["failed", "dead_letter", "processing"] {
        assert!(serde_json::from_value::<QueueEventStatus>(json!({"type": unsupported})).is_err());
    }
    for malformed in [
        json!("pending"),
        json!("completed"),
        json!({}),
        json!({"kind": "pending"}),
        json!({"type": "pending", "data": {}}),
    ] {
        assert!(serde_json::from_value::<QueueEventStatus>(malformed).is_err());
    }
}

#[test]
fn queue_owner_uses_type_and_rejects_kind_aliases() {
    let owner = QueueOwner::new("store", "store-1").unwrap();
    assert_eq!(owner.r#type, "store");
    assert_eq!(
        serde_json::to_value(&owner).unwrap(),
        json!({"type": "store", "id": "store-1"})
    );
    for malformed in [
        json!({"kind": "store", "id": "store-1"}),
        json!({"type": "store", "kind": "store", "id": "store-1"}),
    ] {
        assert!(serde_json::from_value::<QueueOwner>(malformed).is_err());
    }
}

#[tokio::test]
async fn queue_legacy_lifecycle_rows_fail_closed_without_acknowledgement_or_replacement() {
    let canonical = serde_json::to_value(queue_event("legacy", "entry:legacy", 123)).unwrap();
    let mut old_field = canonical.clone();
    old_field.as_object_mut().unwrap().remove("status");
    old_field["state"] = json!("pending");
    let mut old_string = canonical.clone();
    old_string["status"] = json!("pending");
    let mut duplicate_field = canonical.clone();
    duplicate_field["state"] = json!("pending");
    let mut missing = canonical;
    missing.as_object_mut().unwrap().remove("status");

    for malformed in [old_field, old_string, duplicate_field, missing] {
        let engine = engine();
        let queue = queue(engine.clone(), 1);
        let key = "queue:pending:0000:09223372036854775931:00000000000000000000:legacy";
        let bytes = serde_json::to_vec(&malformed).unwrap();
        engine.put_raw(key, bytes.clone()).await;
        assert!(serde_json::from_value::<QueueEvent<QueuePayload>>(malformed).is_err());
        assert!(queue
            .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
            .await
            .is_err());
        assert!(queue
            .pull::<QueuePayload>(0, None, 10)
            .await
            .unwrap()
            .events
            .is_empty());
        assert!(matches!(
            queue.ack(key).await,
            Err(TitoError::DeserializationFailed(_))
        ));
        assert!(matches!(
            queue
                .reschedule(key, queue_event("legacy", "entry:legacy", 456))
                .await,
            Err(TitoError::DeserializationFailed(_))
        ));
        assert_eq!(engine.raw_bytes(key).await.unwrap(), bytes);
        assert_eq!(
            engine.keys_with_prefix("queue:").await,
            vec![key.to_string()]
        );
    }
}

#[test]
fn queue_event_serialization_contains_no_retry_policy_metadata() {
    let event = QueueEvent::new(
        "entry:clean",
        queue_payload("clean"),
        Utc::now().timestamp_millis(),
    );
    let value = serde_json::to_value(&event).unwrap();

    assert_eq!(value["status"], json!({"type": "pending"}));
    assert!(value.get("state").is_none());
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
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
        .await
        .unwrap();
    let fields: Vec<_> = pending.events[0].0.splitn(6, ':').collect();

    assert_eq!(fields.len(), 6);
    assert_eq!(
        &fields[..4],
        ["queue", "pending", "0000", &encode_index_integer(123)]
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
        .ack("queue:pending:0000:09223372036854775809:00000000000000000000:missing")
        .await
        .unwrap();
}

#[tokio::test]
async fn queue_ack_preserves_malformed_pending_bytes() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let key = "queue:pending:0000:09223372036854775809:00000000000000000000:bad";
    engine.put_raw(key, b"not-json".to_vec()).await;

    let error = queue.ack(key).await.unwrap_err();

    assert!(matches!(error, TitoError::DeserializationFailed(_)));
    assert!(engine.contains_key(key).await);
}

#[tokio::test]
async fn queue_reschedule_atomically_completes_current_and_inserts_supplied_event() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp_millis();
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
    let next_due_at = now.div_euclid(1_000) * 1_000 + 60_123;
    let next = current.rescheduled(next_due_at);
    let next_id = next.id.clone();
    let created_at_millis = next.created_at_millis();
    assert_eq!(next_id, current.id);
    assert_eq!(created_at_millis, current.created_at_millis());

    let before_completion = Utc::now().timestamp_millis();
    queue.reschedule(&storage_key, next).await.unwrap();
    let after_completion = Utc::now().timestamp_millis();

    let pending = queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, next_id);
    assert_eq!(pending.events[0].1.timestamp, next_due_at);
    let completed = queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 10)
        .await
        .unwrap();
    assert_eq!(completed.events.len(), 1);
    assert_eq!(completed.events[0].1.id, next_id);
    assert_eq!(completed.events[0].1.timestamp, current.timestamp);
    let completed_wire = serde_json::to_value(&completed.events[0].1).unwrap();
    assert_eq!(completed_wire["status"], json!({"type": "completed"}));
    assert!(completed_wire.get("state").is_none());
    assert!((before_completion..=after_completion)
        .contains(&completed.events[0].1.processed_at.unwrap()));
}

#[tokio::test]
async fn queue_completed_retention_preserves_the_exact_millisecond_boundary() {
    let engine = engine();
    let queue = Queue::new(
        engine.clone(),
        QueueConfig::new(1, Duration::from_millis(1_501)),
    );
    let now = 1_700_000_000_123;
    let cutoff = now - 1_501;
    let old = put_completed_queue_event(&engine, "old", cutoff - 1).await;
    let boundary = put_completed_queue_event(&engine, "boundary", cutoff).await;
    let retained = put_completed_queue_event(&engine, "retained", cutoff + 1).await;
    queue
        .publish(queue_event("pending", "entry:pending", 123))
        .await
        .unwrap();

    assert!(!queue.maintain_completed_event_retention(now).await.unwrap());
    assert!(!engine.contains_key(&old).await);
    assert!(!engine.contains_key(&boundary).await);
    assert!(engine.contains_key(&retained).await);
    assert_eq!(engine.keys_with_prefix("queue:pending:").await.len(), 1);
}

#[tokio::test]
async fn queue_completed_retention_rounds_fractional_milliseconds_up() {
    for (duration, expected_ms) in [
        (Duration::ZERO, 0),
        (Duration::from_nanos(1), 1),
        (Duration::from_nanos(999_999), 1),
        (Duration::from_nanos(1_000_000), 1),
        (Duration::from_nanos(1_000_001), 2),
        (Duration::from_nanos(1_501_000_001), 1_502),
    ] {
        let engine = engine();
        let queue = Queue::new(engine.clone(), QueueConfig::new(1, duration));
        let now = 1_700_000_000_123;
        let boundary = put_completed_queue_event(&engine, "boundary", now - expected_ms).await;
        let retained = put_completed_queue_event(&engine, "retained", now - expected_ms + 1).await;

        assert!(!queue.maintain_completed_event_retention(now).await.unwrap());
        assert!(!engine.contains_key(&boundary).await);
        assert!(engine.contains_key(&retained).await);
    }
}

#[tokio::test]
async fn queue_oversized_retention_does_not_wrap_or_delete_history() {
    let engine = engine();
    let queue = Queue::new(engine.clone(), QueueConfig::new(1, Duration::MAX));
    let retained = put_completed_queue_event(&engine, "retained", 123).await;

    assert!(!queue
        .maintain_completed_event_retention(1_700_000_000_123)
        .await
        .unwrap());
    assert!(engine.contains_key(&retained).await);
}

#[tokio::test]
async fn queue_reschedule_rejects_a_different_logical_event() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp_millis();
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
        .reschedule(
            &storage_key,
            queue_event("different", "entry:1", now + 60_000),
        )
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::InvalidInput(_)));
    assert_eq!(
        queue
            .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
            .await
            .unwrap()
            .events
            .len(),
        1
    );
    assert!(queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn queue_reschedule_still_rejects_a_changed_payload() {
    let engine = engine();
    let queue = queue(engine, 1);
    let now = Utc::now().timestamp_millis();
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
    let mut next = current.rescheduled(now + 60_000);
    next.payload = queue_payload("changed");

    let error = queue.reschedule(&storage_key, next).await.unwrap_err();

    assert!(matches!(error, TitoError::InvalidInput(message)
        if message == "A rescheduled row must preserve the event payload"));
    assert_eq!(
        queue
            .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
            .await
            .unwrap()
            .events,
        vec![(storage_key, current)]
    );
    assert!(queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn queue_advance_atomically_preserves_current_history_and_owner() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let now = Utc::now().timestamp_millis();
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
    let mut next = current.rescheduled(now + 60_000);
    next.payload = queue_payload("advanced");

    queue.advance(&storage_key, next.clone()).await.unwrap();

    let pending = queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1, next);
    let completed = queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 10)
        .await
        .unwrap();
    assert_eq!(completed.events.len(), 1);
    assert_eq!(completed.events[0].1.id, current.id);
    assert_eq!(completed.events[0].1.key, current.key);
    assert_eq!(completed.events[0].1.owner, Some(owner));
    assert_eq!(completed.events[0].1.payload, current.payload);
    assert_eq!(completed.events[0].1.timestamp, current.timestamp);
    assert_eq!(completed.events[0].1.status, QueueEventStatus::Completed);
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
        let now = Utc::now().timestamp_millis();
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
        let mut next = current.rescheduled(now + 60_000);
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
                .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
                .await
                .unwrap()
                .events,
            vec![(storage_key, current)]
        );
        assert!(queue
            .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 10)
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
async fn queue_signed_reschedule_and_advance_preserve_identity_owner_and_atomic_history() {
    for advance in [false, true] {
        for (current_at, next_at) in [(i64::MIN, -1), (0, i64::MIN), (1, i64::MAX), (i64::MAX, 0)] {
            let engine = engine();
            let queue = queue(engine.clone(), 1);
            let owner = QueueOwner::new("store", "signed-owner").unwrap();
            let original =
                queue_event("signed-event", "entry:signed", current_at).with_owner(owner.clone());
            queue.publish(original.clone()).await.unwrap();
            let initial = queue
                .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
                .await
                .unwrap();
            let storage_key = initial.events[0].0.clone();
            let mut next = original.rescheduled(next_at);
            if advance {
                next.payload = queue_payload("advanced");
                queue.advance(&storage_key, next.clone()).await.unwrap();
            } else {
                queue.reschedule(&storage_key, next.clone()).await.unwrap();
            }
            assert!(!engine.contains_key(&storage_key).await);
            let pending = queue
                .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
                .await
                .unwrap();
            assert_eq!(pending.events.len(), 1);
            assert_eq!(pending.events[0].1, next);
            let completed = queue
                .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 10)
                .await
                .unwrap();
            assert_eq!(completed.events.len(), 1);
            let (completed_key, completed_event) = &completed.events[0];
            assert_eq!(completed_event.id, original.id);
            assert_eq!(completed_event.key, original.key);
            assert_eq!(completed_event.owner, original.owner);
            assert_eq!(completed_event.payload, original.payload);
            assert_eq!(completed_event.timestamp, current_at);
            assert_eq!(completed_event.status, QueueEventStatus::Completed);
            assert_eq!(
                completed_key.split(':').nth(2).unwrap(),
                encode_index_integer(completed_event.processed_at.unwrap())
            );
            assert_eq!(
                completed_key.split(':').nth(3).unwrap(),
                encode_index_integer(current_at)
            );
            let indexes = engine.keys_with_prefix("queue:owner:").await;
            assert_eq!(indexes.len(), 2);
            for key in [completed_key, &pending.events[0].0] {
                let encoded_key = general_purpose::URL_SAFE_NO_PAD.encode(key);
                assert!(indexes.iter().any(|index| index.ends_with(&encoded_key)));
            }
            let due = queue.pull::<QueuePayload>(0, None, 10).await.unwrap();
            if next_at == i64::MAX {
                assert!(due.events.is_empty());
            } else {
                assert_eq!(due.events, pending.events);
            }
            queue.ack(&pending.events[0].0).await.unwrap();
            assert_eq!(
                queue
                    .delete_by_status_before(QueueEventStatus::Completed, i64::MAX, 10)
                    .await
                    .unwrap(),
                2
            );
            assert!(engine.keys_with_prefix("queue:").await.is_empty());
        }
    }
}

#[tokio::test]
async fn queue_negative_due_bucket_preserves_the_frozen_enqueue_horizon() {
    let queue = queue(engine(), 1);
    queue
        .publish(queue_event("early", "entry:early", -10))
        .await
        .unwrap();
    queue
        .publish(queue_event("later", "entry:later", -9))
        .await
        .unwrap();
    let first = queue.pull::<QueuePayload>(0, None, 1).await.unwrap();
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
        let page = queue.pull::<QueuePayload>(0, cursor, 1).await.unwrap();
        remaining.extend(page.events.into_iter().map(|(_, event)| event.id));
        cursor = page.next_cursor;
        if cursor.is_none() {
            break;
        }
    }
    assert!(cursor.is_none());
    assert_eq!(remaining, ["later"]);
    let wrapped = queue.pull::<QueuePayload>(0, None, 1).await.unwrap();
    assert_eq!(wrapped.events[0].1, successor);
}

#[tokio::test]
async fn queue_completed_retention_includes_exact_signed_cutoffs_and_extrema_in_bounded_pages() {
    let timestamps = [
        i64::MIN,
        i64::MIN + 1,
        -1_000,
        -1,
        0,
        1,
        1_000,
        i64::MAX - 1,
        i64::MAX,
    ];
    for cutoff in timestamps {
        let engine = engine();
        let queue = queue(engine.clone(), 1);
        for (index, at) in timestamps.into_iter().enumerate().rev() {
            put_completed_queue_event(&engine, &format!("at-{index}"), at).await;
            if at == cutoff {
                put_completed_queue_event(&engine, "same-cutoff", at).await;
            }
        }
        queue
            .publish(queue_event("pending", "entry:pending", i64::MIN))
            .await
            .unwrap();
        let mut deleted = 0;
        loop {
            let count = queue
                .delete_by_status_before(QueueEventStatus::Completed, cutoff, 2)
                .await
                .unwrap();
            assert!(count <= 2);
            deleted += count;
            assert!(deleted <= timestamps.len() + 1);
            if count < 2 {
                break;
            }
        }
        assert_eq!(
            deleted,
            timestamps.iter().filter(|at| **at <= cutoff).count() + 1
        );
        let retained = queue
            .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 20)
            .await
            .unwrap();
        assert_eq!(
            retained
                .events
                .iter()
                .map(|(_, event)| event.processed_at.unwrap())
                .collect::<Vec<_>>(),
            timestamps
                .into_iter()
                .filter(|at| *at > cutoff)
                .collect::<Vec<_>>()
        );
        assert_eq!(engine.keys_with_prefix("queue:pending:").await.len(), 1);
    }
}

#[tokio::test]
async fn queue_retention_duration_can_cross_zero_without_skipping_negative_history() {
    let engine = engine();
    let queue = Queue::new(
        engine.clone(),
        QueueConfig::new(1, Duration::from_millis(1_000)),
    );
    let old = put_completed_queue_event(&engine, "old", -501).await;
    let boundary = put_completed_queue_event(&engine, "boundary", -500).await;
    let retained = put_completed_queue_event(&engine, "retained", -499).await;
    assert!(!queue.maintain_completed_event_retention(500).await.unwrap());
    assert!(!engine.contains_key(&old).await);
    assert!(!engine.contains_key(&boundary).await);
    assert!(engine.contains_key(&retained).await);
}

#[tokio::test]
async fn queue_ack_indeterminate_commit_converges_to_one_of_the_two_atomic_statuses() {
    for after_apply in [false, true] {
        let engine = engine();
        let queue = queue(engine.clone(), 1);
        let timestamp = Utc::now().timestamp_millis() - 10_000;
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
            .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
            .await
            .unwrap();
        let completed = queue
            .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 10)
            .await
            .unwrap();
        let owner_indexes = engine.keys_with_prefix("queue:owner:").await;
        assert_eq!(owner_indexes.len(), 1);
        if after_apply {
            assert!(pending.events.is_empty());
            assert_eq!(completed.events.len(), 1);
            assert_eq!(completed.events[0].1.id, current.id);
            assert_eq!(completed.events[0].1.status, QueueEventStatus::Completed);
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
            Utc::now().timestamp_millis() - 10_000,
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
    let now = Utc::now().timestamp_millis();
    let pending = queue_event("pending", "entry:pending", now);
    let mut completed = queue_event("completed", "entry:completed", now);
    completed.status = QueueEventStatus::Completed;
    completed.processed_at = Some(now);

    engine
        .put_raw(
            &format!(
                "queue:pending:0000:{}:00000000000000000000:pending",
                encode_index_integer(now)
            ),
            serde_json::to_vec(&pending).unwrap(),
        )
        .await;
    engine
        .put_raw(
            &format!(
                "queue:completed:{}:{}:completed",
                encode_index_integer(now),
                encode_index_integer(now)
            ),
            serde_json::to_vec(&completed).unwrap(),
        )
        .await;
    queue.clear().await.unwrap();

    assert!(engine.keys_with_prefix("queue:").await.is_empty());
}

#[tokio::test]
async fn queue_delete_by_status_before_rejects_pending_status() {
    let engine = engine();
    let queue = queue(engine, 1);

    let error = queue
        .delete_by_status_before(QueueEventStatus::Pending, Utc::now().timestamp_millis(), 10)
        .await
        .unwrap_err();

    assert!(matches!(error, TitoError::InvalidInput(_)));
}

#[tokio::test]
async fn queue_completed_retention_uses_the_terminal_time_index_not_value_decoding() {
    let engine = engine();
    let queue = queue(engine.clone(), 1);
    let cutoff = Utc::now().timestamp_millis() - 1;
    let malformed_key = format!(
        "queue:completed:{}:{}:malformed",
        encode_index_integer(cutoff - 1),
        encode_index_integer(0),
    );
    engine.put_raw(&malformed_key, b"not-json".to_vec()).await;

    let deleted = queue
        .delete_by_status_before(QueueEventStatus::Completed, cutoff, 10)
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
    let now = Utc::now().timestamp_millis();
    let expired_count = crate::queue::COMPLETED_EVENT_MAINTENANCE_BATCH_SIZE as usize
        * crate::queue::COMPLETED_EVENT_MAINTENANCE_MAX_BATCHES
        + 1;
    let mut last_expired_key = String::new();
    for index in 0..expired_count {
        last_expired_key = put_completed_queue_event(
            &engine,
            &format!("expired-{index:05}"),
            now - RETENTION_SECONDS * 1_000 - 1,
        )
        .await;
    }
    let retained_key = put_completed_queue_event(
        &engine,
        "retained",
        now - RETENTION_SECONDS * 1_000 + 60_000,
    )
    .await;
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
    let now = Utc::now().timestamp_millis();
    for id in ["event-1", "event-2", "event-3"] {
        queue
            .publish(queue_event(id, id, now - 10_000))
            .await
            .unwrap();
    }

    let first = queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 2)
        .await
        .unwrap();
    assert_eq!(first.events.len(), 2);
    assert!(first.next_cursor.is_some());

    let second = queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, first.next_cursor, 2)
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
            Utc::now().timestamp_millis() - 10_000,
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
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn queue_worker_applies_an_advance_outcome() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    let now = Utc::now().timestamp_millis();
    let owner = QueueOwner::new("store", "worker-advance").unwrap();
    queue
        .publish(
            queue_event("worker-advance", "entry:worker-advance", now - 10_000)
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
                let mut next = event.rescheduled(now + 3_600_000);
                next.payload = queue_payload("worker-advanced");
                Ok(QueueHandlerOutcome::Advance(next))
            })
        },
        shutdown_rx,
    )
    .await;

    wait_for_completed(&queue, "worker-advance").await;
    let pending = queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
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
    assert_eq!(pending.events[0].1.timestamp, now + 3_600_000);

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
            Utc::now().timestamp_millis() - 10_000,
        ))
        .await
        .unwrap();
    let initial = queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
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
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
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
    let now = Utc::now().timestamp_millis() - 10_000;
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
        .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 10)
        .await
        .unwrap();
    assert_eq!(completed.events.len(), 1);
    assert_eq!(completed.events[0].1.id, "standalone-drain");
    let pending = queue
        .scan_by_status::<QueuePayload>(QueueEventStatus::Pending, None, 10)
        .await
        .unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.id, "standalone-second");
}

#[tokio::test]
async fn queue_worker_does_not_let_unacknowledged_first_page_starve_later_rows() {
    let engine = engine();
    let queue = Arc::new(queue(engine, 1));
    let timestamp = Utc::now().timestamp_millis() - 10_000;
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
            Utc::now().timestamp_millis() - 10_000,
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
            Utc::now().timestamp_millis() - 10_000,
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
            Utc::now().timestamp_millis() - 10_000,
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
                .scan_by_status::<QueuePayload>(QueueEventStatus::Completed, None, 10)
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
