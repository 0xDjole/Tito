use futures::{stream, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
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

    pub async fn pull_partition_range(
        &self,
        start_partition: u32,
        end_partition: u32,
        limit: u32,
        _reverse: bool, // Deprecated - always FIFO now
    ) -> Result<Vec<TitoEvent>, TitoError> {
        use crate::types::{PARTITION_DIGITS, SEQUENCE_DIGITS, QueueCheckpoint, QueueProgress};
        use chrono::Utc;

        self.engine
            .transaction(|tx| async move {
                let mut jobs = Vec::new();
                let mut jobs_collected = 0u32;

                // Process each partition in the range
                for partition in start_partition..end_partition {
                    if jobs_collected >= limit {
                        break;
                    }

                    let checkpoint_key = format!("queue_checkpoint:{:0width$}", partition, width = PARTITION_DIGITS);

                    // Read checkpoint for this partition
                    let last_sequence = match tx.get(checkpoint_key.as_bytes()).await
                        .map_err(|e| TitoError::QueryFailed(format!("Checkpoint read failed: {}", e)))? {
                        Some(bytes) => {
                            serde_json::from_slice::<QueueCheckpoint>(&bytes)
                                .map(|ckpt| ckpt.last_sequence)
                                .unwrap_or(0)
                        }
                        None => 0,
                    };

                    // Scan events in this partition from checkpoint onwards (FIFO)
                    let event_start_key = format!(
                        "event:{:0pwidth$}:{:0swidth$}",
                        partition,
                        last_sequence + 1,
                        pwidth = PARTITION_DIGITS,
                        swidth = SEQUENCE_DIGITS
                    );
                    let event_end_key = format!(
                        "event:{:0pwidth$}:",
                        partition + 1,
                        pwidth = PARTITION_DIGITS
                    );

                    let events = tx.scan(
                        event_start_key.as_bytes()..event_end_key.as_bytes(),
                        limit - jobs_collected
                    )
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Event scan failed: {}", e)))?;

                    for (event_key, event_bytes) in events {
                        if let Ok(event) = serde_json::from_slice::<TitoEvent>(&event_bytes) {
                            let event_key_str = String::from_utf8_lossy(&event_key);

                            // Extract sequence from event key
                            let parts: Vec<&str> = event_key_str.split(':').collect();
                            if parts.len() < 3 {
                                continue;
                            }
                            let sequence = parts[2];

                            // Check if already completed or in progress
                            let completed_key = format!("queue:COMPLETED:{:0pwidth$}:{}", partition, sequence, pwidth = PARTITION_DIGITS);
                            let progress_key = format!("queue:PROGRESS:{:0pwidth$}:{}", partition, sequence, pwidth = PARTITION_DIGITS);

                            let is_completed = tx.get(completed_key.as_bytes()).await
                                .map_err(|e| TitoError::QueryFailed(format!("Completed check failed: {}", e)))?
                                .is_some();

                            let is_in_progress = tx.get(progress_key.as_bytes()).await
                                .map_err(|e| TitoError::QueryFailed(format!("Progress check failed: {}", e)))?
                                .is_some();

                            if !is_completed && !is_in_progress {
                                // Create PROGRESS entry
                                let progress = QueueProgress {
                                    retries: event.retries,
                                    updated_at: Utc::now().timestamp(),
                                };
                                tx.put(progress_key.as_bytes(), serde_json::to_vec(&progress).unwrap())
                                    .await
                                    .map_err(|e| TitoError::UpdateFailed(format!("Progress put failed: {}", e)))?;

                                jobs.push(event);
                                jobs_collected += 1;

                                if jobs_collected >= limit {
                                    break;
                                }
                            }
                        }
                    }
                }

                Ok::<_, TitoError>(jobs)
            })
            .await
    }

    pub async fn success_job(&self, job_id: String) -> Result<(), TitoError> {
        use crate::types::{QueueCompleted, QueueCheckpoint};
        use chrono::Utc;

        self.engine
            .transaction(|tx| async move {
                // job_id is the event key: event:{partition}:{sequence}
                // Extract partition and sequence
                let parts: Vec<&str> = job_id.split(':').collect();
                if parts.len() < 3 {
                    return Err(TitoError::InvalidInput("Invalid event key format".to_string()));
                }
                let partition_str = parts[1];
                let sequence_str = parts[2];
                let sequence: i64 = sequence_str.parse()
                    .map_err(|_| TitoError::InvalidInput("Invalid sequence".to_string()))?;

                // Delete PROGRESS entry
                let progress_key = format!("queue:PROGRESS:{}:{}", partition_str, sequence_str);
                tx.delete(progress_key.as_bytes())
                    .await
                    .map_err(|e| TitoError::DeleteFailed(format!("Delete progress failed: {}", e)))?;

                // Create COMPLETED entry
                let completed_key = format!("queue:COMPLETED:{}:{}", partition_str, sequence_str);
                let completed = QueueCompleted {
                    updated_at: Utc::now().timestamp(),
                };
                tx.put(completed_key.as_bytes(), serde_json::to_vec(&completed).unwrap())
                    .await
                    .map_err(|e| TitoError::UpdateFailed(format!("Put completed failed: {}", e)))?;

                // Update checkpoint if this is higher than current
                let checkpoint_key = format!("queue_checkpoint:{}", partition_str);
                let current_checkpoint = match tx.get(checkpoint_key.as_bytes()).await
                    .map_err(|e| TitoError::QueryFailed(format!("Checkpoint read failed: {}", e)))? {
                    Some(bytes) => {
                        serde_json::from_slice::<QueueCheckpoint>(&bytes)
                            .map(|ckpt| ckpt.last_sequence)
                            .unwrap_or(0)
                    }
                    None => 0,
                };

                if sequence > current_checkpoint {
                    let new_checkpoint = QueueCheckpoint {
                        last_sequence: sequence,
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

    pub async fn fail_job(&self, job_id: String) -> Result<(), TitoError> {
        use crate::types::{QueueProgress, QueueFailed};
        use chrono::Utc;

        self.engine
            .transaction(|tx| async move {
                // job_id is the event key: event:{partition}:{sequence}
                // Extract partition and sequence
                let parts: Vec<&str> = job_id.split(':').collect();
                if parts.len() < 3 {
                    return Err(TitoError::InvalidInput("Invalid event key format".to_string()));
                }
                let partition_str = parts[1];
                let sequence_str = parts[2];

                // Read PROGRESS entry to get retry count
                let progress_key = format!("queue:PROGRESS:{}:{}", partition_str, sequence_str);
                let progress_data = tx.get(progress_key.as_bytes())
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Get progress failed: {}", e)))?;

                if let Some(progress_bytes) = progress_data {
                    let mut progress: QueueProgress = serde_json::from_slice(&progress_bytes)
                        .map_err(|_| TitoError::DeserializationFailed(String::from("Failed to deserialize progress")))?;

                    const MAX_RETRIES: u32 = 5;

                    if progress.retries < MAX_RETRIES {
                        // Increment retries and update PROGRESS entry
                        progress.retries += 1;
                        progress.updated_at = Utc::now().timestamp();

                        tx.put(progress_key.as_bytes(), serde_json::to_vec(&progress).unwrap())
                            .await
                            .map_err(|e| TitoError::UpdateFailed(format!("Update progress failed: {}", e)))?;
                    } else {
                        // Max retries reached - delete PROGRESS and create FAILED
                        tx.delete(progress_key.as_bytes())
                            .await
                            .map_err(|e| TitoError::DeleteFailed(format!("Delete progress failed: {}", e)))?;

                        let failed_key = format!("queue:FAILED:{}:{}", partition_str, sequence_str);
                        let failed = QueueFailed {
                            retries: progress.retries,
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
    handler: H,
    partition_config: crate::types::PartitionConfig,
    is_leader: Arc<AtomicBool>,
    concurrency: u32,
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
                    let is_leader_val = is_leader.load(Ordering::SeqCst);
                    if is_leader_val {
                        // Pull jobs from assigned partition range
                        // Use reverse=true for LIFO (newest first)
                        match queue.pull_partition_range(
                            partition_config.start,
                            partition_config.end,
                            20,
                            true
                        ).await {
                            Ok(jobs) => {
                                stream::iter(jobs.into_iter().map(|job| {
                                    let queue = Arc::clone(&queue);
                                    let handler = handler.clone();
                                    async move {
                                        let result = handler(job.clone()).await;
                                        let _ = match result {
                                            Ok(_) => queue.success_job(job.key).await,
                                            Err(_) => queue.fail_job(job.key).await,
                                        };
                                    }
                                }))
                                .for_each_concurrent(concurrency as usize, |job_future| job_future)
                                .await;
                            }
                            Err(_) => {
                                sleep(Duration::from_millis(500)).await;
                            }
                        }
                    }
                    sleep(Duration::from_millis(125)).await;
                } => {}
            }
        }
    })
}
