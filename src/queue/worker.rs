use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::time::{sleep, timeout};

use super::{
    Queue, QueueEvent, QueueHandlerOutcome, QueuePullCursor, COMPLETED_EVENT_MAINTENANCE_INTERVAL,
};
use crate::types::TitoEngine;
use crate::TitoError;

const WORKER_POLL_INTERVAL: Duration = Duration::from_secs(1);
pub(crate) const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_secs(10 * 60);

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub partition_range: std::ops::Range<u32>,
    pub handler_timeout: Duration,
}

impl WorkerConfig {
    pub fn new(partition_range: std::ops::Range<u32>) -> Self {
        Self {
            partition_range,
            handler_timeout: DEFAULT_HANDLER_TIMEOUT,
        }
    }
}

pub(crate) enum HandlerExecution {
    Finished(Result<QueueHandlerOutcome, TitoError>),
    Panicked,
    TimedOut,
}

pub(crate) async fn execute_handler<T, H>(
    handler: &H,
    event: QueueEvent<T>,
    handler_timeout: Duration,
) -> HandlerExecution
where
    T: Send + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, Result<QueueHandlerOutcome, TitoError>>,
{
    let handler_future = match catch_unwind(AssertUnwindSafe(|| handler(event))) {
        Ok(handler_future) => handler_future,
        Err(_) => return HandlerExecution::Panicked,
    };

    match timeout(
        handler_timeout,
        AssertUnwindSafe(handler_future).catch_unwind(),
    )
    .await
    {
        Ok(Ok(result)) => HandlerExecution::Finished(result),
        Ok(Err(_)) => HandlerExecution::Panicked,
        Err(_) => HandlerExecution::TimedOut,
    }
}

pub async fn run_worker<E, T, H>(
    queue: Arc<Queue<E>>,
    config: WorkerConfig,
    handler: H,
    shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()>
where
    E: TitoEngine + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, Result<QueueHandlerOutcome, TitoError>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    tokio::spawn(async move {
        let mut handles = Vec::new();
        let maintenance_queue = queue.clone();
        let mut maintenance_shutdown = shutdown.resubscribe();
        handles.push(tokio::spawn(async move {
            let mut wait = Duration::ZERO;

            loop {
                tokio::select! {
                    _ = maintenance_shutdown.recv() => break,
                    _ = sleep(wait) => {
                        wait = match maintenance_queue
                            .maintain_completed_event_retention(chrono::Utc::now().timestamp())
                            .await
                        {
                            Ok(true) => {
                                tokio::task::yield_now().await;
                                Duration::ZERO
                            }
                            Ok(false) => COMPLETED_EVENT_MAINTENANCE_INTERVAL,
                            Err(error) => {
                                log::error!("Completed queue retention maintenance failed: {error}");
                                COMPLETED_EVENT_MAINTENANCE_INTERVAL
                            }
                        };
                    }
                }
            }
        }));

        for partition in config.partition_range.clone() {
            let q = queue.clone();
            let h = handler.clone();
            let mut rx = shutdown.resubscribe();
            let handler_timeout = config.handler_timeout;

            handles.push(tokio::spawn(async move {
                let mut cursor: Option<QueuePullCursor> = None;
                loop {
                    tokio::select! {
                        _ = rx.recv() => break,
                        _ = async {
                            match q.pull::<T>(partition, cursor.clone(), 50).await {
                                Ok(page) => {
                                    cursor = page.next_cursor;
                                    for (storage_key, event) in page.events {
                                        match execute_handler(
                                            &h,
                                            event.clone(),
                                            handler_timeout,
                                        )
                                        .await
                                        {
                                            HandlerExecution::Finished(Ok(outcome)) => {
                                                let result = match outcome {
                                                    QueueHandlerOutcome::Acknowledge => {
                                                        q.ack(&storage_key).await
                                                    }
                                                    QueueHandlerOutcome::ScheduleNextAt(
                                                        timestamp,
                                                    ) => q
                                                        .schedule_next_at::<T>(
                                                            &storage_key,
                                                            timestamp,
                                                        )
                                                        .await
                                                        .map(|_| ()),
                                                };
                                                if let Err(error) = result {
                                                    log::error!(
                                                        "Failed to apply queue outcome for event {} at {}: {}",
                                                        event.id,
                                                        storage_key,
                                                        error
                                                    );
                                                }
                                            }
                                            HandlerExecution::Finished(Err(error)) => {
                                                log::error!(
                                                    "Queue handler failed for event {} ({}); leaving it pending for redelivery: {}",
                                                    event.id,
                                                    event.key,
                                                    error
                                                );
                                            }
                                            HandlerExecution::Panicked => {
                                                log::error!(
                                                    "Queue handler panicked for event {} ({}); leaving it pending for redelivery",
                                                    event.id,
                                                    event.key
                                                );
                                            }
                                            HandlerExecution::TimedOut => {
                                                log::error!(
                                                    "Queue handler timed out after {:?} for event {} ({}); leaving it pending for redelivery",
                                                    handler_timeout,
                                                    event.id,
                                                    event.key
                                                );
                                            }
                                        }
                                    }
                                }
                                Err(error) => {
                                    log::error!(
                                        "Failed to pull queue partition {}: {}",
                                        partition,
                                        error
                                    );
                                }
                            }
                            sleep(WORKER_POLL_INTERVAL).await;
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
