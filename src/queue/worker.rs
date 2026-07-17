use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::time::sleep;

use super::{Queue, QueueEvent};
use crate::types::TitoEngine;
use crate::TitoError;

#[derive(Debug, Clone)]
pub struct WorkerConfig {
    pub partition_range: std::ops::Range<u32>,
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
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, Result<(), TitoError>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    tokio::spawn(async move {
        let mut handles = Vec::new();

        for partition in config.partition_range.clone() {
            let q = queue.clone();
            let h = handler.clone();
            let mut rx = shutdown.resubscribe();

            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = rx.recv() => break,
                        _ = async {
                            match q.pull::<T>(partition, 50).await {
                                Ok(jobs) if jobs.is_empty() => {
                                    sleep(Duration::from_millis(1000)).await;
                                }
                                Ok(jobs) => {
                                    for (storage_key, event) in jobs {
                                        match h(event.clone()).await {
                                            Ok(_) => {
                                                if let Err(error) = q.ack(&storage_key).await {
                                                    log::error!(
                                                        "Failed to acknowledge queue event {} at {}: {}",
                                                        event.id,
                                                        storage_key,
                                                        error
                                                    );
                                                }
                                            }
                                            Err(err) => {
                                                q.retry_after_handler_error(
                                                    event,
                                                    &storage_key,
                                                    err.to_string(),
                                                )
                                                .await;
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
