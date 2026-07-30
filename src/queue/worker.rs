use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::sync::broadcast::error::TryRecvError;
use tokio::time::{sleep, timeout};

use super::{
    Queue, QueueEvent, QueueHandlerOutcome, QueuePullCursor, COMPLETED_EVENT_MAINTENANCE_INTERVAL,
};
use crate::types::TitoEngine;

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

pub(crate) enum HandlerExecution<T> {
    Finished(QueueHandlerOutcome<T>),
    Panicked,
    TimedOut,
}

pub(crate) async fn execute_handler<T, H>(
    handler: &H,
    event: QueueEvent<T>,
    handler_timeout: Duration,
) -> HandlerExecution<T>
where
    T: Send + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, QueueHandlerOutcome<T>>,
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
        Ok(Ok(outcome)) => HandlerExecution::Finished(outcome),
        Ok(Err(_)) => HandlerExecution::Panicked,
        Err(_) => HandlerExecution::TimedOut,
    }
}

pub(crate) fn handler_outcome_or_log<T>(
    execution: HandlerExecution<T>,
    event: &QueueEvent<T>,
    handler_timeout: Duration,
) -> Option<QueueHandlerOutcome<T>> {
    match execution {
        HandlerExecution::Finished(outcome) => Some(outcome),
        HandlerExecution::Panicked => {
            log::error!(
                "Queue handler panicked for event {} ({}); leaving it pending for redelivery",
                event.id,
                event.key
            );
            None
        }
        HandlerExecution::TimedOut => {
            log::error!(
                "Queue handler timed out after {:?} for event {} ({}); leaving it pending for redelivery",
                handler_timeout,
                event.id,
                event.key
            );
            None
        }
    }
}

pub(crate) async fn apply_handler_outcome<E, T>(
    queue: &Queue<E>,
    storage_key: &str,
    event: &QueueEvent<T>,
    outcome: QueueHandlerOutcome<T>,
) where
    E: TitoEngine,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
{
    let result = match outcome {
        QueueHandlerOutcome::Acknowledge => queue.ack(storage_key).await,
        QueueHandlerOutcome::Reschedule(next) => queue.reschedule(storage_key, next).await,
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

pub async fn run_worker<E, T, H>(
    queue: Arc<Queue<E>>,
    config: WorkerConfig,
    handler: H,
    shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()>
where
    E: TitoEngine + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, QueueHandlerOutcome<T>>
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
                    let page = tokio::select! {
                        _ = rx.recv() => break,
                        result = q.pull::<T>(partition, cursor.clone(), 50) => result,
                    };
                    match page {
                        Ok(page) => {
                            cursor = page.next_cursor;
                            let mut shutting_down = false;
                            for (storage_key, event) in page.events {
                                match rx.try_recv() {
                                    Ok(_) | Err(TryRecvError::Closed | TryRecvError::Lagged(_)) => {
                                        shutting_down = true;
                                        break;
                                    }
                                    Err(TryRecvError::Empty) => {}
                                }
                                let execution =
                                    execute_handler(&h, event.clone(), handler_timeout).await;
                                if let Some(outcome) =
                                    handler_outcome_or_log(execution, &event, handler_timeout)
                                {
                                    apply_handler_outcome(&q, &storage_key, &event, outcome).await;
                                }
                            }
                            if shutting_down {
                                break;
                            }
                        }
                        Err(error) => {
                            log::error!("Failed to pull queue partition {}: {}", partition, error);
                        }
                    }
                    tokio::select! {
                        _ = rx.recv() => break,
                        _ = sleep(WORKER_POLL_INTERVAL) => {}
                    }
                }
            }));
        }

        for h in handles {
            let _ = h.await;
        }
    })
}
