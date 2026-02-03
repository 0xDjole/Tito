use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::time::sleep;

use super::{EventType, Queue, QueueEvent};
use crate::types::TitoEngine;
use crate::TitoError;

/// Configuration for running a worker
#[derive(Debug, Clone)]
pub struct WorkerConfig {
    /// Event type to process (e.g., "events")
    pub event_type: String,
    /// Consumer name
    pub consumer: String,
    /// Partition range to process
    pub partition_range: std::ops::Range<u32>,
}

/// Run a worker with a closure handler
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
                                        match h(event.clone()).await {
                                            Ok(_) => {
                                                let _ = q.ack(&key).await;
                                            }
                                            Err(err) => {
                                                let mut retry_event = event.clone();
                                                retry_event.retry_count += 1;
                                                retry_event.error = Some(err.to_string());

                                                if retry_event.retry_count > retry_event.max_retries {
                                                    // Move to DLQ
                                                    let _ = q.move_to_dlq(retry_event).await;
                                                } else {
                                                    // Reschedule with backoff: 2^retry_count seconds
                                                    let backoff = 2_i64.pow(retry_event.retry_count);
                                                    let new_scheduled_at = Utc::now().timestamp() + backoff;
                                                    let _ = q.reschedule(retry_event, new_scheduled_at).await;
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
