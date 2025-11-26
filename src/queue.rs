use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;

use crate::types::{TitoEngine, TitoEvent, TitoTransaction};
use crate::TitoError;

#[derive(Clone)]
pub struct TitoQueue<E: TitoEngine> {
    pub engine: E,
}

impl<E: TitoEngine> TitoQueue<E> {
    pub async fn pull_partition(
        &self,
        consumer: String,
        event_type: String,
        partition: u32,
        limit: u32,
    ) -> Result<Vec<TitoEvent>, TitoError> {
        use crate::types::{PARTITION_DIGITS, QueueCheckpoint, QueueProgress};
        use chrono::Utc;

        self.engine
            .transaction(|tx| async move {
                let mut jobs = Vec::new();

                let checkpoint_key = format!(
                    "queue_checkpoint:{}:{}:{:0width$}",
                    consumer,
                    event_type,
                    partition,
                    width = PARTITION_DIGITS
                );

                let checkpoint = tx.get(checkpoint_key.as_bytes()).await
                    .map_err(|e| TitoError::QueryFailed(format!("Checkpoint read failed: {}", e)))?
                    .and_then(|bytes| serde_json::from_slice::<QueueCheckpoint>(&bytes).ok());

                let now = Utc::now().timestamp();

                let event_start_key = match &checkpoint {
                    Some(ckpt) => format!(
                        "event:{}:{:0pwidth$}:{}:{}\0",
                        event_type,
                        partition,
                        ckpt.timestamp,
                        ckpt.uuid,
                        pwidth = PARTITION_DIGITS,
                    ),
                    None => format!(
                        "event:{}:{:0pwidth$}:0",
                        event_type,
                        partition,
                        pwidth = PARTITION_DIGITS,
                    ),
                };
                let event_end_key = format!(
                    "event:{}:{:0pwidth$}:{}",
                    event_type,
                    partition,
                    now + 1,
                    pwidth = PARTITION_DIGITS,
                );

                let events = tx.scan(
                    event_start_key.as_bytes()..event_end_key.as_bytes(),
                    limit
                )
                .await
                .map_err(|e| TitoError::QueryFailed(format!("Event scan failed: {}", e)))?;

                for (event_key, event_bytes) in events {
                    if let Ok(event) = serde_json::from_slice::<TitoEvent>(&event_bytes) {
                        if event.timestamp > now {
                            continue;
                        }

                        let event_key_str = String::from_utf8_lossy(&event_key);
                        let parts: Vec<&str> = event_key_str.split(':').collect();
                        if parts.len() < 5 {
                            continue;
                        }
                        let partition_str = parts[2];
                        let timestamp_str = parts[3];
                        let uuid_str = parts[4];

                        let completed_key = format!(
                            "queue:{}:{}:completed:{}:{}:{}",
                            consumer, event_type, partition_str, timestamp_str, uuid_str
                        );
                        let progress_key = format!(
                            "queue:{}:{}:progress:{}:{}:{}",
                            consumer, event_type, partition_str, timestamp_str, uuid_str
                        );

                        let is_completed = tx.get(completed_key.as_bytes()).await
                            .map_err(|e| TitoError::QueryFailed(format!("Completed check failed: {}", e)))?
                            .is_some();

                        let is_in_progress = tx.get(progress_key.as_bytes()).await
                            .map_err(|e| TitoError::QueryFailed(format!("Progress check failed: {}", e)))?
                            .is_some();

                        if !is_completed && !is_in_progress {
                            let progress = QueueProgress {
                                retries: event.retries,
                                updated_at: Utc::now().timestamp(),
                            };
                            tx.put(progress_key.as_bytes(), serde_json::to_vec(&progress).unwrap())
                                .await
                                .map_err(|e| TitoError::UpdateFailed(format!("Progress put failed: {}", e)))?;

                            jobs.push(event);

                            if jobs.len() >= limit as usize {
                                break;
                            }
                        }
                    }
                }

                Ok::<_, TitoError>(jobs)
            })
            .await
    }

    pub async fn success_job(&self, consumer: String, job_id: String) -> Result<(), TitoError> {
        use crate::types::{QueueCompleted, QueueCheckpoint};
        use chrono::Utc;

        self.engine
            .transaction(|tx| async move {
                let parts: Vec<&str> = job_id.split(':').collect();
                if parts.len() < 5 {
                    return Err(TitoError::InvalidInput("Invalid event key format".to_string()));
                }
                let event_type = parts[1];
                let partition_str = parts[2];
                let timestamp_str = parts[3];
                let uuid_str = parts[4];
                let timestamp: i64 = timestamp_str.parse()
                    .map_err(|_| TitoError::InvalidInput("Invalid timestamp".to_string()))?;

                let progress_key = format!(
                    "queue:{}:{}:progress:{}:{}:{}",
                    consumer, event_type, partition_str, timestamp_str, uuid_str
                );
                tx.delete(progress_key.as_bytes())
                    .await
                    .map_err(|e| TitoError::DeleteFailed(format!("Delete progress failed: {}", e)))?;

                let completed_key = format!(
                    "queue:{}:{}:completed:{}:{}:{}",
                    consumer, event_type, partition_str, timestamp_str, uuid_str
                );
                let completed = QueueCompleted {
                    updated_at: Utc::now().timestamp(),
                };
                tx.put(completed_key.as_bytes(), serde_json::to_vec(&completed).unwrap())
                    .await
                    .map_err(|e| TitoError::UpdateFailed(format!("Put completed failed: {}", e)))?;

                let checkpoint_key = format!("queue_checkpoint:{}:{}:{}", consumer, event_type, partition_str);
                let current_checkpoint = tx.get(checkpoint_key.as_bytes()).await
                    .map_err(|e| TitoError::QueryFailed(format!("Checkpoint read failed: {}", e)))?
                    .and_then(|bytes| serde_json::from_slice::<QueueCheckpoint>(&bytes).ok());

                let should_advance = match &current_checkpoint {
                    Some(ckpt) => {
                        (timestamp, uuid_str) > (ckpt.timestamp, ckpt.uuid.as_str())
                    }
                    None => true,
                };

                if should_advance {
                    let new_checkpoint = QueueCheckpoint {
                        timestamp,
                        uuid: uuid_str.to_string(),
                    };
                    tx.put(checkpoint_key.as_bytes(), serde_json::to_vec(&new_checkpoint).unwrap())
                        .await
                        .map_err(|e| TitoError::UpdateFailed(format!("Checkpoint update failed: {}", e)))?;
                }

                Ok::<_, TitoError>(())
            })
            .await?;

        Ok(())
    }

    pub async fn fail_job(&self, consumer: String, job_id: String) -> Result<(), TitoError> {
        use crate::types::{QueueFailed, TitoEvent};
        use chrono::Utc;

        self.engine
            .transaction(|tx| async move {
                let parts: Vec<&str> = job_id.split(':').collect();
                if parts.len() < 5 {
                    return Err(TitoError::InvalidInput("Invalid event key format".to_string()));
                }
                let event_type = parts[1];
                let partition_str = parts[2];
                let timestamp_str = parts[3];
                let uuid_str = parts[4];

                let progress_key = format!(
                    "queue:{}:{}:progress:{}:{}:{}",
                    consumer, event_type, partition_str, timestamp_str, uuid_str
                );

                tx.delete(progress_key.as_bytes())
                    .await
                    .map_err(|e| TitoError::DeleteFailed(format!("Delete progress failed: {}", e)))?;

                let event_bytes = tx.get(job_id.as_bytes())
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Get event failed: {}", e)))?;

                if let Some(bytes) = event_bytes {
                    let mut event: TitoEvent = serde_json::from_slice(&bytes)
                        .map_err(|_| TitoError::DeserializationFailed(String::from("Failed to deserialize event")))?;

                    const MAX_RETRIES: u32 = 5;

                    if event.retries < MAX_RETRIES {
                        event.retries += 1;
                        let backoff_seconds = 2_i64.pow(event.retries);
                        let new_timestamp = Utc::now().timestamp() + backoff_seconds;
                        event.timestamp = new_timestamp;
                        event.updated_at = Utc::now().timestamp();

                        let new_key = format!(
                            "event:{}:{}:{}:{}",
                            event_type, partition_str, new_timestamp, uuid_str
                        );
                        event.key = new_key.clone();

                        tx.delete(job_id.as_bytes())
                            .await
                            .map_err(|e| TitoError::DeleteFailed(format!("Delete old event failed: {}", e)))?;

                        tx.put(new_key.as_bytes(), serde_json::to_vec(&event).unwrap())
                            .await
                            .map_err(|e| TitoError::UpdateFailed(format!("Put new event failed: {}", e)))?;
                    } else {
                        tx.delete(job_id.as_bytes())
                            .await
                            .map_err(|e| TitoError::DeleteFailed(format!("Delete event failed: {}", e)))?;

                        let failed_key = format!(
                            "queue:{}:{}:failed:{}:{}:{}",
                            consumer, event_type, partition_str, timestamp_str, uuid_str
                        );
                        let failed = QueueFailed {
                            retries: event.retries,
                            updated_at: Utc::now().timestamp(),
                            error: Some("Max retries exceeded".to_string()),
                        };

                        tx.put(failed_key.as_bytes(), serde_json::to_vec(&failed).unwrap())
                            .await
                            .map_err(|e| TitoError::UpdateFailed(format!("Put failed entry: {}", e)))?;
                    }
                }

                Ok::<_, TitoError>(())
            })
            .await?;

        Ok(())
    }

    pub async fn clear(&self) -> Result<(), TitoError> {
        Ok(())
    }
}

pub async fn run_worker<E: TitoEngine + 'static, H>(
    queue: Arc<TitoQueue<E>>,
    config: crate::types::WorkerConfig,
    handler: H,
    mut shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()>
where
    H: Fn(TitoEvent) -> futures::future::BoxFuture<'static, Result<(), TitoError>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = shutdown.recv() => {
                    break;
                }
                _ = async {
                    match queue.pull_partition(
                        config.consumer.clone(),
                        config.event_type.clone(),
                        config.partition,
                        1,
                    ).await {
                        Ok(jobs) => {
                            for job in jobs {
                                let result = handler(job.clone()).await;
                                let _ = match result {
                                    Ok(_) => queue.success_job(config.consumer.clone(), job.key).await,
                                    Err(_) => queue.fail_job(config.consumer.clone(), job.key).await,
                                };
                            }
                        }
                        Err(_) => {
                            sleep(Duration::from_millis(500)).await;
                        }
                    }
                    sleep(Duration::from_millis(125)).await;
                } => {}
            }
        }
    })
}
