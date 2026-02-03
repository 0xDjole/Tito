use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::time::sleep;

use super::{EventHandler, EventType, Queue, QueueEvent, RetryPolicy, WorkerConfig};
use crate::types::TitoEngine;
use crate::TitoError;

/// Run a worker that processes events using an EventHandler trait implementation
pub async fn run_worker_with_handler<E, T, H>(
    queue: Arc<Queue<E>>,
    config: WorkerConfig,
    handler: Arc<H>,
    ctx: H::Context,
    retry_policy: RetryPolicy,
    shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()>
where
    E: TitoEngine + 'static,
    T: EventType + Serialize + DeserializeOwned,
    H: EventHandler<T> + 'static,
{
    tokio::spawn(async move {
        let mut handles = Vec::new();

        for partition in config.partition_range.clone() {
            let q = queue.clone();
            let h = handler.clone();
            let ctx = ctx.clone();
            let event_type = config.event_type.clone();
            let retry_policy = retry_policy.clone();
            let mut rx = shutdown.resubscribe();

            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = rx.recv() => break,
                        _ = async {
                            match q.pull::<T>(&event_type, partition, 50).await {
                                Ok(jobs) if jobs.is_empty() => {
                                    sleep(Duration::from_millis(1000)).await;
                                }
                                Ok(jobs) => {
                                    for event in jobs {
                                        // Check if handler accepts this event
                                        if !h.accepts(&event.payload) {
                                            // Ack and skip
                                            let _ = q.ack(&event.key).await;
                                            continue;
                                        }

                                        let key = event.key.clone();
                                        match h.handle(&event, &ctx).await {
                                            Ok(_) => {
                                                // Success - ack the event
                                                let _ = q.ack(&key).await;
                                            }
                                            Err(err) => {
                                                // Handle retry logic
                                                let mut updated_event = event.clone();
                                                updated_event.retry_count += 1;
                                                updated_event.error = Some(err.to_string());

                                                if updated_event.retry_count > retry_policy.max_retries {
                                                    // Max retries exceeded - just ack (drop)
                                                    // TODO: Could move to DLQ here
                                                    let _ = q.ack(&key).await;
                                                } else {
                                                    // Reschedule with backoff
                                                    let backoff = retry_policy.backoff_seconds(updated_event.retry_count);
                                                    let new_scheduled_at = Utc::now().timestamp() + backoff;
                                                    let _ = q.reschedule(updated_event, new_scheduled_at).await;
                                                }
                                            }
                                        }
                                    }
                                }
                                Err(_) => {
                                    sleep(Duration::from_millis(500)).await;
                                }
                            }
                        } => {}
                    }
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }
    })
}

/// Run a worker with a closure handler (backward compatible API)
pub async fn run_worker<E, T, H>(
    queue: Arc<Queue<E>>,
    config: WorkerConfig,
    handler: H,
    shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()>
where
    E: TitoEngine + 'static,
    T: EventType + Serialize + DeserializeOwned,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, Result<(), TitoError>> + Clone + Send + Sync + 'static,
{
    tokio::spawn(async move {
        let mut handles = Vec::new();

        for partition in config.partition_range.clone() {
            let q = queue.clone();
            let h = handler.clone();
            let event_type = config.event_type.clone();
            let mut rx = shutdown.resubscribe();

            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = rx.recv() => break,
                        _ = async {
                            match q.pull::<T>(&event_type, partition, 50).await {
                                Ok(jobs) if jobs.is_empty() => {
                                    sleep(Duration::from_millis(1000)).await;
                                }
                                Ok(jobs) => {
                                    for event in jobs {
                                        let key = event.key.clone();
                                        match h(event).await {
                                            Ok(_) => { let _ = q.ack(&key).await; }
                                            Err(_) => { let _ = q.ack(&key).await; }
                                        }
                                    }
                                }
                                Err(_) => {
                                    sleep(Duration::from_millis(500)).await;
                                }
                            }
                        } => {}
                    }
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }
    })
}
