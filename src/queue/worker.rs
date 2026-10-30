use std::panic::{catch_unwind, AssertUnwindSafe};
use std::time::Duration;

use futures::future::BoxFuture;
use futures::FutureExt;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::time::timeout;

use super::{Queue, QueueEvent, QueueHandlerOutcome, QueueHandlerResult};
use crate::types::TitoEngine;

pub(crate) const DEFAULT_HANDLER_TIMEOUT: Duration = Duration::from_secs(10 * 60);

pub(crate) enum HandlerExecution<T> {
    Finished(QueueHandlerResult<T>),
    Panicked,
    TimedOut,
}

pub(crate) async fn execute_handler<T, H>(
    handler: &H,
    storage_key: &str,
    mut event: QueueEvent<T>,
    handler_timeout: Duration,
) -> HandlerExecution<T>
where
    T: Send + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, QueueHandlerResult<T>>,
{
    event.pending_key = Some(storage_key.to_string());
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

pub(crate) fn handler_outcome_or_log<T>(
    execution: HandlerExecution<T>,
    event: &QueueEvent<T>,
    handler_timeout: Duration,
) -> Option<QueueHandlerOutcome<T>> {
    match execution {
        HandlerExecution::Finished(Ok(outcome)) => Some(outcome),
        HandlerExecution::Finished(Err(error)) => {
            log::error!(
                "Queue handler failed for event {} ({}); leaving it pending for redelivery: {}",
                event.id,
                event.key,
                error
            );
            None
        }
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
        QueueHandlerOutcome::Advance(next) => queue.advance(storage_key, next).await,
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
