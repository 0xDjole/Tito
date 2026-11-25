use std::collections::BTreeSet;
use std::ops::Range;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, Notify};
use tokio::task::JoinHandle;
use tokio::time::{sleep, timeout};

use crate::backend::tikv::TiKVBackend;
use crate::queue::test_engine::{
    get_raw, keys_with_prefix, native_engine, put_raw, unique_queue_name,
};
use crate::queue::{
    run_cluster_worker, ClusterPartitionAssignment, ClusterPartitions, ClusterWorkerConfig,
    ClusterWorkerNode, Queue, QueueConfig, QueueEvent, QueueHandlerOutcome, QueueHandlerResult,
};
use crate::TitoError;

type WorkRun = BoxFuture<'static, QueueHandlerResult<Work>>;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
struct Work {
    name: String,
    step: u32,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Moment {
    Started,
    Finished,
}

#[derive(Clone, Debug)]
struct Entry {
    node: &'static str,
    name: String,
    step: u32,
    moment: Moment,
    at: i64,
}

#[derive(Clone, Default)]
struct Log(Arc<Mutex<Vec<Entry>>>);

impl Log {
    fn push(&self, node: &'static str, event: &QueueEvent<Work>, moment: Moment) {
        self.0.lock().unwrap().push(Entry {
            node,
            name: event.payload.name.clone(),
            step: event.payload.step,
            moment,
            at: now(),
        });
    }

    fn entries(&self, node: &str, moment: Moment) -> Vec<Entry> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .filter(|entry| entry.node == node && entry.moment == moment)
            .cloned()
            .collect()
    }
}

fn now() -> i64 {
    Utc::now().timestamp_millis()
}

fn millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap()
}

async fn cluster_queue(partition_count: u32) -> Arc<Queue<TiKVBackend>> {
    Arc::new(Queue::new(
        native_engine().await,
        QueueConfig::new(partition_count)
            .with_name(unique_queue_name("cluster"))
            .unwrap(),
    ))
}

fn node_config(node_id: &str) -> ClusterWorkerConfig {
    ClusterWorkerConfig {
        node_id: node_id.to_string(),
        heartbeat_interval: Duration::from_millis(500),
        poll_interval: Duration::from_millis(50),
        handler_timeout: Duration::from_secs(60),
        lease_ttl: Duration::from_secs(3),
        stale_node_ttl: Duration::from_secs(60),
    }
}

fn work(key: &str, name: &str, timestamp: i64) -> QueueEvent<Work> {
    QueueEvent::new(
        key,
        Work {
            name: name.to_string(),
            step: 0,
        },
        timestamp,
    )
}

fn partitions(share: Range<u32>, held: impl IntoIterator<Item = u32>) -> ClusterPartitions {
    ClusterPartitions {
        share: share.collect(),
        held: held.into_iter().collect(),
    }
}

fn logging_handler(
    node: &'static str,
    log: Log,
) -> impl Fn(QueueEvent<Work>) -> WorkRun + Clone + Send + Sync + 'static {
    move |event: QueueEvent<Work>| -> WorkRun {
        let log = log.clone();
        Box::pin(async move {
            log.push(node, &event, Moment::Started);
            Ok(QueueHandlerOutcome::Acknowledge)
        })
    }
}

fn rescheduling_handler(
    node: &'static str,
    log: Log,
) -> impl Fn(QueueEvent<Work>) -> WorkRun + Clone + Send + Sync + 'static {
    move |event: QueueEvent<Work>| -> WorkRun {
        let log = log.clone();
        Box::pin(async move {
            log.push(node, &event, Moment::Started);
            sleep(Duration::from_millis(20)).await;
            Ok(QueueHandlerOutcome::Reschedule(event.rescheduled(now())))
        })
    }
}

async fn put_node(queue: &Queue<TiKVBackend>, node_id: &str, heartbeat_at: i64) {
    let node = ClusterWorkerNode {
        node_id: node_id.to_string(),
        heartbeat_at,
    };
    put_raw(
        &queue.engine,
        &queue.node_key(node_id),
        serde_json::to_vec(&node).unwrap(),
    )
    .await;
}

async fn put_holder(queue: &Queue<TiKVBackend>, partition: u32, node_id: &str) {
    let assignment = ClusterPartitionAssignment {
        partition,
        owner_node_id: Some(node_id.to_string()),
    };
    put_raw(
        &queue.engine,
        &queue.partition_key(partition),
        serde_json::to_vec(&assignment).unwrap(),
    )
    .await;
}

async fn holder_of(queue: &Queue<TiKVBackend>, partition: u32) -> Option<String> {
    let bytes = get_raw(&queue.engine, &queue.partition_key(partition)).await?;
    serde_json::from_slice::<ClusterPartitionAssignment>(&bytes)
        .unwrap()
        .owner_node_id
}

async fn heartbeat_of(queue: &Queue<TiKVBackend>, node_id: &str) -> Option<i64> {
    let bytes = get_raw(&queue.engine, &queue.node_key(node_id)).await?;
    Some(
        serde_json::from_slice::<ClusterWorkerNode>(&bytes)
            .unwrap()
            .heartbeat_at,
    )
}

async fn wait_for_holder(queue: &Queue<TiKVBackend>, partition: u32, node_id: &str) -> i64 {
    timeout(Duration::from_secs(30), async {
        loop {
            if holder_of(queue, partition).await.as_deref() == Some(node_id) {
                return now();
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the partition reaches its expected holder")
}

async fn wait_for_heartbeat(queue: &Queue<TiKVBackend>, node_id: &str) {
    timeout(Duration::from_secs(30), async {
        while heartbeat_of(queue, node_id).await.is_none() {
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the worker writes its heartbeat");
}

async fn wait_for_entries(log: &Log, node: &str, moment: Moment, count: usize) -> Vec<Entry> {
    timeout(Duration::from_secs(30), async {
        loop {
            let entries = log.entries(node, moment);
            if entries.len() >= count {
                return entries;
            }
            sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the worker runs the expected events")
}

async fn sleep_until(at: i64) {
    let wait = at - now();
    if wait > 0 {
        sleep(Duration::from_millis(wait as u64)).await;
    }
}

async fn stop(shutdown: broadcast::Sender<()>, worker: JoinHandle<()>) {
    let _ = shutdown.send(());
    timeout(Duration::from_secs(30), worker)
        .await
        .expect("the worker stops")
        .expect("the worker task ends without a panic");
}

async fn lock_heartbeat(queue: &Queue<TiKVBackend>, node_id: &str) -> tikv_client::Transaction {
    for _ in 0..20 {
        let mut lock = queue
            .engine
            .client
            .begin_pessimistic()
            .await
            .expect("begin the test's lock");
        if lock
            .lock_keys(vec![queue.node_key(node_id).into_bytes()])
            .await
            .is_ok()
        {
            return lock;
        }
        let _ = lock.rollback().await;
        sleep(Duration::from_millis(50)).await;
    }
    panic!("the test could not lock the heartbeat row of {node_id}");
}

#[tokio::test]
async fn cluster_heartbeats_are_epoch_milliseconds_and_oversized_ages_saturate() {
    let queue = cluster_queue(2).await;
    let config = node_config("node-a");
    let none = BTreeSet::new();
    let before = now();
    let heartbeat_at = queue.heartbeat_cluster_worker(&config).await.unwrap();
    assert!((before..=now()).contains(&heartbeat_at));
    assert_eq!(heartbeat_of(&queue, "node-a").await, Some(heartbeat_at));
    put_node(&queue, "node-dormant", 0).await;

    let mut patient = node_config("node-a");
    patient.lease_ttl = Duration::MAX;
    patient.stale_node_ttl = Duration::MAX;
    assert_eq!(
        queue.step_cluster_worker(&patient, &none).await.unwrap(),
        partitions(0..1, [0])
    );
    assert_eq!(heartbeat_of(&queue, "node-dormant").await, Some(0));

    assert_eq!(
        queue.step_cluster_worker(&config, &none).await.unwrap(),
        partitions(0..2, [0, 1])
    );
    assert_eq!(heartbeat_of(&queue, "node-dormant").await, None);
    assert_eq!(holder_of(&queue, 1).await.as_deref(), Some("node-a"));
}

#[tokio::test]
async fn cluster_step_takes_rows_of_dead_or_missing_servers_and_leaves_their_heartbeat_rows() {
    let queue = cluster_queue(3).await;
    let config = node_config("node-a");
    let dead_at = now() - 10_000;
    put_node(&queue, "node-dead", dead_at).await;
    put_holder(&queue, 0, "node-dead").await;
    put_holder(&queue, 1, "node-gone").await;
    queue.heartbeat_cluster_worker(&config).await.unwrap();

    assert_eq!(
        queue
            .step_cluster_worker(&config, &BTreeSet::new())
            .await
            .unwrap(),
        partitions(0..3, [0, 1, 2])
    );
    for partition in 0..3 {
        assert_eq!(
            holder_of(&queue, partition).await.as_deref(),
            Some("node-a")
        );
    }
    assert_eq!(heartbeat_of(&queue, "node-dead").await, Some(dead_at));
    assert_eq!(heartbeat_of(&queue, "node-gone").await, None);
}

#[tokio::test]
async fn cluster_shares_split_partitions_and_no_server_takes_a_row_whose_holder_is_alive() {
    let queue = cluster_queue(32).await;
    let a = node_config("node-a");
    let b = node_config("node-b");
    let c = node_config("node-c");
    let none = BTreeSet::new();
    queue.heartbeat_cluster_worker(&a).await.unwrap();
    queue.heartbeat_cluster_worker(&b).await.unwrap();
    assert_eq!(
        queue.step_cluster_worker(&a, &none).await.unwrap(),
        partitions(0..16, 0..16)
    );
    assert_eq!(
        queue.step_cluster_worker(&b, &none).await.unwrap(),
        partitions(16..32, 16..32)
    );

    queue.heartbeat_cluster_worker(&c).await.unwrap();
    assert_eq!(
        queue.step_cluster_worker(&c, &none).await.unwrap(),
        partitions(21..32, [])
    );
    assert_eq!(
        queue
            .step_cluster_worker(&b, &BTreeSet::from([25]))
            .await
            .unwrap(),
        partitions(10..21, (16..21).chain([25]))
    );
    assert_eq!(
        queue.step_cluster_worker(&a, &none).await.unwrap(),
        partitions(0..10, 0..10)
    );
    assert_eq!(
        queue.step_cluster_worker(&c, &none).await.unwrap(),
        partitions(21..32, (21..25).chain(26..32))
    );
    assert_eq!(
        queue.step_cluster_worker(&b, &none).await.unwrap(),
        partitions(10..21, 10..21)
    );
    assert_eq!(
        queue.step_cluster_worker(&c, &none).await.unwrap(),
        partitions(21..32, 21..32)
    );

    for partition in 0..32 {
        let expected = match partition {
            0..=9 => "node-a",
            10..=20 => "node-b",
            _ => "node-c",
        };
        assert_eq!(
            holder_of(&queue, partition).await.as_deref(),
            Some(expected)
        );
    }
}

#[tokio::test]
async fn cluster_servers_that_take_the_same_row_at_once_leave_one_holder() {
    let none = BTreeSet::new();
    for _ in 0..5 {
        let queue = cluster_queue(2).await;
        let a = node_config("node-a");
        let mut b = node_config("node-b");
        b.lease_ttl = Duration::from_secs(60);
        put_node(&queue, "node-d", now() - 10_000).await;
        queue.heartbeat_cluster_worker(&a).await.unwrap();
        queue.heartbeat_cluster_worker(&b).await.unwrap();

        let (from_a, from_b) = tokio::join!(
            queue.step_cluster_worker(&a, &none),
            queue.step_cluster_worker(&b, &none)
        );
        let (from_a, from_b) = (from_a.unwrap(), from_b.unwrap());

        assert_eq!(from_a.share, vec![0]);
        assert_eq!(from_b.share, vec![0]);
        assert_eq!(from_a.held.len() + from_b.held.len(), 1);
        let holder = holder_of(&queue, 0)
            .await
            .expect("one Server holds the row");
        assert_eq!(from_a.held == vec![0], holder == "node-a");
        assert_eq!(from_b.held == vec![0], holder == "node-b");
        assert_eq!(holder_of(&queue, 1).await, None);
    }
}

#[tokio::test]
async fn cluster_handover_waits_for_the_event_in_progress_and_the_next_holder_runs_the_next_event_after_it(
) {
    let queue = cluster_queue(1).await;
    let log = Log::default();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    queue
        .publish(work("work:same", "first", now() - 1_000))
        .await
        .unwrap();
    let first_holder = {
        let log = log.clone();
        let started = started.clone();
        let release = release.clone();
        move |event: QueueEvent<Work>| -> WorkRun {
            let log = log.clone();
            let started = started.clone();
            let release = release.clone();
            Box::pin(async move {
                log.push("node-a", &event, Moment::Started);
                if event.payload.name == "first" {
                    started.notify_one();
                    release.notified().await;
                }
                log.push("node-a", &event, Moment::Finished);
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        }
    };
    let (stop_a, shutdown_a) = broadcast::channel(1);
    let worker_a = run_cluster_worker(
        queue.clone(),
        node_config("node-a"),
        first_holder,
        shutdown_a,
    )
    .await;
    timeout(Duration::from_secs(30), started.notified())
        .await
        .expect("node-a starts the first event");
    queue
        .publish(work("work:same", "second", now()))
        .await
        .unwrap();

    let (stop_b, shutdown_b) = broadcast::channel(1);
    let worker_b = run_cluster_worker(
        queue.clone(),
        node_config("node-b"),
        logging_handler("node-b", log.clone()),
        shutdown_b,
    )
    .await;
    wait_for_heartbeat(&queue, "node-b").await;
    sleep(Duration::from_millis(1_500)).await;
    assert_eq!(holder_of(&queue, 0).await.as_deref(), Some("node-a"));
    assert!(log.entries("node-b", Moment::Started).is_empty());

    release.notify_one();
    let second = wait_for_entries(&log, "node-b", Moment::Started, 1).await;
    let first_finished = log.entries("node-a", Moment::Finished);
    assert_eq!(first_finished.len(), 1);
    assert_eq!(first_finished[0].name, "first");
    assert_eq!(second[0].name, "second");
    assert!(second[0].at >= first_finished[0].at);
    assert_eq!(holder_of(&queue, 0).await.as_deref(), Some("node-b"));

    stop(stop_a, worker_a).await;
    stop(stop_b, worker_b).await;
    let first_runs = log.entries("node-a", Moment::Started);
    assert_eq!(first_runs.len(), 1);
    assert_eq!(first_runs[0].name, "first");
    let second_runs = log.entries("node-b", Moment::Started);
    assert_eq!(second_runs.len(), 1);
    assert_eq!(second_runs[0].name, "second");
    assert!(queue
        .scan_by_status::<Work>(None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn cluster_server_whose_heartbeat_fails_stops_its_workers_before_another_takes_its_partitions(
) {
    let queue = cluster_queue(1).await;
    let log = Log::default();
    let holder = node_config("node-z");
    let lease = millis(holder.lease_ttl);
    let stop_after = lease - 2 * millis(holder.heartbeat_interval);
    queue
        .publish(work("work:busy", "busy", now() - 1_000))
        .await
        .unwrap();
    let (_holder_shutdown, shutdown) = broadcast::channel(1);
    let holder_worker = run_cluster_worker(
        queue.clone(),
        holder.clone(),
        rescheduling_handler("node-z", log.clone()),
        shutdown,
    )
    .await;
    wait_for_entries(&log, "node-z", Moment::Started, 1).await;
    let (stop_taker, shutdown) = broadcast::channel(1);
    let taker_worker = run_cluster_worker(
        queue.clone(),
        node_config("node-a"),
        rescheduling_handler("node-a", log.clone()),
        shutdown,
    )
    .await;
    wait_for_heartbeat(&queue, "node-a").await;

    let mut lock = lock_heartbeat(&queue, "node-z").await;
    let last_heartbeat = heartbeat_of(&queue, "node-z").await.unwrap();
    sleep_until(last_heartbeat + stop_after + 500).await;
    let holder_runs = log.entries("node-z", Moment::Started);
    assert!(holder_runs
        .iter()
        .all(|entry| entry.at < last_heartbeat + stop_after + 250));
    sleep(Duration::from_secs(1)).await;
    assert_eq!(
        log.entries("node-z", Moment::Started).len(),
        holder_runs.len()
    );
    assert!(log.entries("node-a", Moment::Started).is_empty());

    holder_worker.abort();
    let _ = holder_worker.await;
    lock.rollback().await.unwrap();
    let taken_at = wait_for_holder(&queue, 0, "node-a").await;
    let taker_runs = wait_for_entries(&log, "node-a", Moment::Started, 1).await;
    assert!(taken_at >= last_heartbeat + lease);
    assert!(taker_runs[0].at >= last_heartbeat + lease);
    stop(stop_taker, taker_worker).await;
}

#[tokio::test]
async fn cluster_cleanly_stopped_server_hands_its_partitions_to_the_next_step_of_another() {
    let queue = cluster_queue(1).await;
    let log = Log::default();
    let taker = node_config("node-a");
    let (stop_holder, shutdown) = broadcast::channel(1);
    let holder_worker = run_cluster_worker(
        queue.clone(),
        node_config("node-z"),
        logging_handler("node-z", log.clone()),
        shutdown,
    )
    .await;
    wait_for_holder(&queue, 0, "node-z").await;
    let (stop_taker, shutdown) = broadcast::channel(1);
    let taker_worker = run_cluster_worker(
        queue.clone(),
        taker.clone(),
        logging_handler("node-a", log.clone()),
        shutdown,
    )
    .await;
    wait_for_heartbeat(&queue, "node-a").await;
    sleep(Duration::from_secs(1)).await;
    assert_eq!(holder_of(&queue, 0).await.as_deref(), Some("node-z"));

    stop(stop_holder, holder_worker).await;
    let stopped_at = now();
    assert_eq!(heartbeat_of(&queue, "node-z").await, None);
    assert_ne!(holder_of(&queue, 0).await.as_deref(), Some("node-z"));
    let taken_at = wait_for_holder(&queue, 0, "node-a").await;
    assert!(taken_at - stopped_at < millis(taker.heartbeat_interval) + 1_000);
    stop(stop_taker, taker_worker).await;
}

#[tokio::test]
async fn cluster_killed_server_loses_its_partitions_only_after_lease_ttl() {
    let queue = cluster_queue(1).await;
    let log = Log::default();
    let holder = node_config("node-z");
    let (_holder_shutdown, shutdown) = broadcast::channel(1);
    let holder_worker = run_cluster_worker(
        queue.clone(),
        holder.clone(),
        logging_handler("node-z", log.clone()),
        shutdown,
    )
    .await;
    wait_for_holder(&queue, 0, "node-z").await;
    let (stop_taker, shutdown) = broadcast::channel(1);
    let taker_worker = run_cluster_worker(
        queue.clone(),
        node_config("node-a"),
        logging_handler("node-a", log.clone()),
        shutdown,
    )
    .await;
    wait_for_heartbeat(&queue, "node-a").await;

    holder_worker.abort();
    let _ = holder_worker.await;
    let last_heartbeat = heartbeat_of(&queue, "node-z").await.unwrap();
    let taken_at = wait_for_holder(&queue, 0, "node-a").await;
    assert!(taken_at >= last_heartbeat + millis(holder.lease_ttl));
    stop(stop_taker, taker_worker).await;
}

#[tokio::test]
async fn cluster_worker_runs_a_chain_of_advances_without_waiting_its_poll_interval() {
    let queue = cluster_queue(1).await;
    let log = Log::default();
    let done = Arc::new(Notify::new());
    queue
        .publish(work("work:chain", "chain", now() - 1_000))
        .await
        .unwrap();
    let mut config = node_config("node-a");
    config.poll_interval = Duration::from_secs(60 * 60);
    let handler = {
        let log = log.clone();
        let done = done.clone();
        move |event: QueueEvent<Work>| -> WorkRun {
            let log = log.clone();
            let done = done.clone();
            Box::pin(async move {
                log.push("node-a", &event, Moment::Started);
                if event.payload.step < 5 {
                    let mut next = event.rescheduled(now());
                    next.payload.step += 1;
                    return Ok(QueueHandlerOutcome::Advance(next));
                }
                done.notify_one();
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        }
    };
    let (stop_tx, shutdown) = broadcast::channel(1);
    let worker = run_cluster_worker(queue.clone(), config, handler, shutdown).await;

    timeout(Duration::from_secs(30), done.notified())
        .await
        .expect("the chain ends without waiting the hour-long poll interval");
    stop(stop_tx, worker).await;
    assert_eq!(
        log.entries("node-a", Moment::Started)
            .iter()
            .map(|entry| entry.step)
            .collect::<Vec<_>>(),
        vec![0, 1, 2, 3, 4, 5]
    );
    assert!(queue
        .scan_by_status::<Work>(None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn cluster_worker_sleeps_its_poll_interval_between_passes_whose_events_all_fail() {
    let queue = cluster_queue(1).await;
    let log = Log::default();
    queue
        .publish(work("work:failing", "failing", now() - 1_000))
        .await
        .unwrap();
    let mut config = node_config("node-a");
    config.poll_interval = Duration::from_secs(1);
    let handler = {
        let log = log.clone();
        move |event: QueueEvent<Work>| -> WorkRun {
            let log = log.clone();
            Box::pin(async move {
                log.push("node-a", &event, Moment::Started);
                Err(TitoError::InvalidInput(
                    "the test handler always fails".to_string(),
                ))
            })
        }
    };
    let (stop_tx, shutdown) = broadcast::channel(1);
    let worker = run_cluster_worker(queue.clone(), config, handler, shutdown).await;

    wait_for_entries(&log, "node-a", Moment::Started, 1).await;
    sleep(Duration::from_millis(3_500)).await;
    let attempts = log.entries("node-a", Moment::Started).len();
    assert!(
        (2..=5).contains(&attempts),
        "{attempts} attempts in 3.5 seconds with a one-second poll interval"
    );
    stop(stop_tx, worker).await;
    let pending = queue.scan_by_status::<Work>(None, 10).await.unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.payload.name, "failing");
}

#[tokio::test]
async fn cluster_event_that_ran_on_two_servers_and_advanced_on_both_ends_with_one_successor() {
    let queue = cluster_queue(1).await;
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    queue
        .publish(work("work:twice", "twice", now() - 1_000))
        .await
        .unwrap();
    let handler = {
        let started = started.clone();
        let release = release.clone();
        move |event: QueueEvent<Work>| -> WorkRun {
            let started = started.clone();
            let release = release.clone();
            Box::pin(async move {
                started.notify_one();
                release.notified().await;
                let mut next = event.rescheduled(now() + 60 * 60 * 1_000);
                next.payload.step += 1;
                Ok(QueueHandlerOutcome::Advance(next))
            })
        }
    };
    let (stop_tx, shutdown) = broadcast::channel(1);
    let worker = run_cluster_worker(queue.clone(), node_config("node-a"), handler, shutdown).await;
    timeout(Duration::from_secs(30), started.notified())
        .await
        .expect("node-a starts the event");

    let (storage_key, delivered) = queue
        .pull::<Work>(0, None, 10)
        .await
        .unwrap()
        .events
        .remove(0);
    let mut successor = delivered.rescheduled(now() + 60 * 60 * 1_000);
    successor.payload.step += 1;
    queue.advance(&storage_key, successor).await.unwrap();
    release.notify_one();
    stop(stop_tx, worker).await;

    let rows = queue.scan_by_status::<Work>(None, 10).await.unwrap().events;
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1.id, delivered.id);
    assert_eq!(
        rows[0].1.payload,
        Work {
            name: "twice".to_string(),
            step: 1,
        }
    );
    assert_eq!(
        keys_with_prefix(&queue.engine, &format!("{}:", queue.config.name())).await,
        vec![rows[0].0.clone()]
    );
}

#[tokio::test]
async fn cluster_worker_contains_handler_panic_and_redelivers_pending_invocation() {
    let queue = cluster_queue(1).await;
    queue
        .publish(work("work:panic", "panic", now() - 10_000))
        .await
        .unwrap();
    let attempts = Arc::new(AtomicUsize::new(0));
    let processed = Arc::new(Notify::new());
    let handler = {
        let attempts = attempts.clone();
        let processed = processed.clone();
        move |_event: QueueEvent<Work>| -> WorkRun {
            let attempts = attempts.clone();
            let processed = processed.clone();
            Box::pin(async move {
                if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
                    panic!("simulated cluster handler panic");
                }
                processed.notify_one();
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        }
    };
    let (stop_tx, shutdown) = broadcast::channel(1);
    let worker = run_cluster_worker(queue.clone(), node_config("node-a"), handler, shutdown).await;

    timeout(Duration::from_secs(30), processed.notified())
        .await
        .expect("the worker runs the event again after the panic");
    stop(stop_tx, worker).await;
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
    assert!(queue
        .scan_by_status::<Work>(None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}

#[tokio::test]
async fn cluster_worker_shutdown_drains_its_started_handler_and_gives_its_partitions_back() {
    let queue = cluster_queue(1).await;
    let due = now() - 10_000;
    queue
        .publish(work("work:drain", "drain", due))
        .await
        .unwrap();
    queue
        .publish(work("work:already-pulled", "already-pulled", due))
        .await
        .unwrap();
    let log = Log::default();
    let started = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let handler = {
        let log = log.clone();
        let started = started.clone();
        let release = release.clone();
        move |event: QueueEvent<Work>| -> WorkRun {
            let log = log.clone();
            let started = started.clone();
            let release = release.clone();
            Box::pin(async move {
                log.push("node-a", &event, Moment::Started);
                started.notify_one();
                release.notified().await;
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        }
    };
    let (stop_tx, shutdown) = broadcast::channel(1);
    let mut worker =
        run_cluster_worker(queue.clone(), node_config("node-a"), handler, shutdown).await;

    timeout(Duration::from_secs(30), started.notified())
        .await
        .expect("the worker starts the first event");
    queue
        .publish(work("work:not-pulled", "not-pulled", now() - 10_000))
        .await
        .unwrap();
    let _ = stop_tx.send(());
    for _ in 0..10 {
        tokio::task::yield_now().await;
    }
    assert!(
        !worker.is_finished(),
        "cluster worker joined before its started handler drained"
    );

    release.notify_one();
    timeout(Duration::from_secs(30), &mut worker)
        .await
        .expect("the worker stops")
        .expect("the worker task ends without a panic");

    let runs = log.entries("node-a", Moment::Started);
    assert_eq!(runs.len(), 1, "shutdown allowed another handler to start");
    assert_eq!(runs[0].name, "drain");
    let pending = queue.scan_by_status::<Work>(None, 10).await.unwrap();
    assert_eq!(
        pending
            .events
            .into_iter()
            .map(|(_, event)| event.payload.name)
            .collect::<BTreeSet<_>>(),
        BTreeSet::from(["already-pulled".to_string(), "not-pulled".to_string()])
    );
    assert!(get_raw(&queue.engine, &queue.partition_key(0))
        .await
        .is_some());
    assert_eq!(holder_of(&queue, 0).await, None);
    assert_eq!(heartbeat_of(&queue, "node-a").await, None);
}

#[tokio::test]
async fn cluster_worker_shutdown_bounds_a_blocked_handler_by_its_timeout() {
    let queue = cluster_queue(1).await;
    queue
        .publish(work("work:timeout-drain", "timeout-drain", now() - 10_000))
        .await
        .unwrap();
    let mut config = node_config("node-a");
    config.handler_timeout = Duration::from_millis(25);
    let started = Arc::new(Notify::new());
    let handler = {
        let started = started.clone();
        move |_event: QueueEvent<Work>| -> WorkRun {
            let started = started.clone();
            Box::pin(async move {
                started.notify_one();
                std::future::pending::<()>().await;
                Ok(QueueHandlerOutcome::Acknowledge)
            })
        }
    };
    let (stop_tx, shutdown) = broadcast::channel(1);
    let worker = run_cluster_worker(queue.clone(), config, handler, shutdown).await;

    timeout(Duration::from_secs(30), started.notified())
        .await
        .expect("the worker starts the event");
    let _ = stop_tx.send(());
    timeout(Duration::from_secs(5), worker)
        .await
        .expect("the handler timeout bounds the shutdown")
        .expect("the worker task ends without a panic");

    let pending = queue.scan_by_status::<Work>(None, 10).await.unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.payload.name, "timeout-drain");
}

#[tokio::test]
async fn cluster_worker_observes_shutdown_sent_immediately_after_start() {
    let queue = cluster_queue(1).await;
    queue
        .publish(work("work:immediate", "immediate", now() - 10_000))
        .await
        .unwrap();
    let log = Log::default();
    let (stop_tx, shutdown) = broadcast::channel(1);
    let worker = run_cluster_worker(
        queue.clone(),
        node_config("node-a"),
        logging_handler("node-a", log.clone()),
        shutdown,
    )
    .await;

    let _ = stop_tx.send(());
    timeout(Duration::from_secs(5), worker)
        .await
        .expect("the worker stops")
        .expect("the worker task ends without a panic");

    assert!(
        log.entries("node-a", Moment::Started).is_empty(),
        "worker started a handler after shutdown"
    );
    assert_eq!(heartbeat_of(&queue, "node-a").await, None);
    let pending = queue.scan_by_status::<Work>(None, 10).await.unwrap();
    assert_eq!(pending.events.len(), 1);
    assert_eq!(pending.events[0].1.payload.name, "immediate");
}

#[tokio::test]
async fn cluster_partition_worker_pulls_the_next_page_without_waiting_for_its_poll_interval() {
    let queue = cluster_queue(1).await;
    let due = now() - 10_000;
    for index in 0..120 {
        let name = format!("paged-{index:03}");
        queue
            .publish(work(&format!("work:{name}"), &name, due))
            .await
            .unwrap();
    }
    let mut config = node_config("node-a");
    config.poll_interval = Duration::from_secs(60 * 60);
    let log = Log::default();
    let (stop_tx, shutdown) = broadcast::channel(1);
    let worker = run_cluster_worker(
        queue.clone(),
        config,
        logging_handler("node-a", log.clone()),
        shutdown,
    )
    .await;

    wait_for_entries(&log, "node-a", Moment::Started, 120).await;
    stop(stop_tx, worker).await;
    assert_eq!(log.entries("node-a", Moment::Started).len(), 120);
    assert!(queue
        .scan_by_status::<Work>(None, 10)
        .await
        .unwrap()
        .events
        .is_empty());
}
