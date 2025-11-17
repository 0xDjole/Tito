use chrono::Utc;
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
        reverse: bool,
    ) -> Result<Vec<TitoEvent>, TitoError> {
        use crate::utils::next_string_lexicographically;
        use crate::types::{PARTITION_DIGITS};

        let start_key = format!("event:PENDING:{:0width$}:", start_partition, width = PARTITION_DIGITS);
        let end_key = format!("event:PENDING:{:0width$}:", end_partition, width = PARTITION_DIGITS);
        let end_key = next_string_lexicographically(end_key);

        self.engine
            .transaction(|tx| async move {
                let scan_stream = tx.scan_reverse(start_key.as_bytes()..end_key.as_bytes(), limit)
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

                let mut jobs = Vec::new();

                for item in scan_stream {
                    if let Ok(mut job) = serde_json::from_slice::<TitoEvent>(&item.1) {
                        // Atomically move PENDING -> PROGRESS
                        tx.delete(job.key.as_bytes())
                            .await
                            .map_err(|e| TitoError::DeleteFailed(format!("Delete failed: {}", e)))?;

                        job.status = String::from("PROGRESS");
                        let new_key = job.key.replace("PENDING", "PROGRESS");
                        job.key = new_key.clone();

                        tx.put(new_key.as_bytes(), serde_json::to_vec(&job).unwrap())
                            .await
                            .map_err(|e| TitoError::UpdateFailed(format!("Put failed: {}", e)))?;

                        jobs.push(job);
                    }
                }

                Ok::<_, TitoError>(jobs)
            })
            .await
    }

    pub async fn success_job(&self, job_id: String) -> Result<(), TitoError> {
        let new_key = job_id.replace("PROGRESS", "COMPLETED");

        self.engine
            .transaction(|tx| async move {
                let job = tx
                    .get(job_id.as_bytes())
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Get failed: {}", e)))?;

                if let Some(job_bytes) = job {
                    let mut job: TitoEvent = serde_json::from_slice(&job_bytes).map_err(|_| {
                        TitoError::DeserializationFailed(String::from("Failed job"))
                    })?;
                    job.status = "COMPLETED".to_string();
                    job.key = new_key.clone();

                    tx.delete(job_id.as_bytes())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete failed: {}", e)))?;
                    tx.put(new_key.as_bytes(), serde_json::to_vec(&job).unwrap())
                        .await
                        .map_err(|e| TitoError::UpdateFailed(format!("Put failed: {}", e)))?;
                }
                Ok::<_, TitoError>(())
            })
            .await?;

        Ok(())
    }

    pub async fn fail_job(&self, job_id: String) -> Result<(), TitoError> {
        self.engine
            .transaction(|tx| async move {
                let job = tx
                    .get(job_id.as_bytes())
                    .await
                    .map_err(|e| TitoError::QueryFailed(format!("Get failed: {}", e)))?;

                if let Some(job_bytes) = job {
                    let mut job: TitoEvent = serde_json::from_slice(&job_bytes).map_err(|_| {
                        TitoError::DeserializationFailed(String::from("Failed job"))
                    })?;

                    if job.retries < job.max_retries {
                        let additional_seconds = (job.retries * job.retries * 60) as i64;
                        job.scheduled_for = job.scheduled_for + additional_seconds;

                        let new_key = job_id.replace("PROGRESS", "PENDING");

                        let mut parts: Vec<&str> = new_key.split(':').collect();
                        if parts.len() >= 5 {
                            // Replace the timestamp (fourth element) with the new timestamp, keep status intact
                            let schedule_part = job.scheduled_for.to_string();
                            parts[3] = &schedule_part;
                            let new_key = parts.join(":");

                            job.key = new_key.clone();
                            job.retries += 1;
                            job.status = "PENDING".to_string();

                            tx.delete(job_id.as_bytes()).await.map_err(|e| {
                                TitoError::DeleteFailed(format!("Delete failed: {}", e))
                            })?;

                            tx.put(new_key.as_bytes(), serde_json::to_vec(&job).unwrap())
                                .await
                                .map_err(|e| {
                                    TitoError::UpdateFailed(format!("Put failed: {}", e))
                                })?;
                        }
                    } else {
                        let new_key = job_id.replace("PROGRESS", "FAILED");
                        job.key = new_key.clone();
                        job.status = "FAILED".to_string();

                        tx.delete(job_id.as_bytes()).await.map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete failed: {}", e))
                        })?;

                        tx.put(new_key.as_bytes(), serde_json::to_vec(&job).unwrap())
                            .await
                            .map_err(|e| TitoError::UpdateFailed(format!("Put failed: {}", e)))?;
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
