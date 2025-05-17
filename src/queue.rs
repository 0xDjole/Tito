use chrono::Utc;
use futures::{stream, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;

use crate::TitoError;
use crate::{
    transaction::TransactionManager,
    types::{TitoDatabase, TitoJob},
};

#[derive(Clone)]
pub struct TitoQueue {
    pub db: TitoDatabase,
    pub tx_manager: TransactionManager,
    pub table: String,
}

impl TitoQueue {
    pub async fn push(
        &self,
        _message: String,
        _date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), TitoError> {
        Ok(())
    }

    pub async fn pull(
        &self,
        limit: u32,
        _offset: u32,
        _group_id: Option<String>,
    ) -> Result<Vec<TitoJob>, TitoError> {
        let mut jobs: Vec<TitoJob> = Vec::new();

        let start_bound = format!("event:{}:PENDING", self.table);

        let current_time = Utc::now().timestamp();
        let end_bound = format!("event:{}:PENDING:{}", self.table, current_time);

        let jobs = self
            .tx_manager
            .transaction(|tx| async move {
                let scan_stream = tx.scan(start_bound..end_bound, limit).await?;

                for item in scan_stream {
                    if let Ok(job) = serde_json::from_slice::<TitoJob>(&item.1) {
                        jobs.push(job);
                    }
                }

                for job in jobs.iter_mut() {
                    tx.delete(job.key.clone()).await;

                    job.status = String::from("PROGRESS");
                    let new_key = job.key.replace("PENDING", "PROGRESS");
                    job.key = new_key.clone();

                    let json_job = serde_json::to_value(job.clone());

                    if let Ok(value) = json_job {
                        tx.put(new_key.clone(), serde_json::to_vec(&value).unwrap())
                            .await;
                    }
                }

                Ok::<_, TitoError>(jobs)
            })
            .await?;

        Ok(jobs.into_iter().map(|job| job.into()).collect())
    }

    pub async fn success_job(&self, job_id: String) -> Result<(), TitoError> {
        let new_key = job_id.replace("PROGRESS", "COMPLETED");

        self.tx_manager
            .transaction(|tx| async move {
                let job = tx.get(job_id.clone()).await?;

                if let Some(job_bytes) = job {
                    let mut job: TitoJob = serde_json::from_slice(&job_bytes).map_err(|_| {
                        TitoError::DeserializationFailed(String::from("Failed job"))
                    })?;
                    job.status = "COMPLETED".to_string();
                    job.key = new_key.clone();

                    tx.delete(job_id)
                        .await
                        .map_err(|_| TitoError::DeleteFailed(String::from("Failed job")))?;
                    tx.put(new_key.clone(), serde_json::to_vec(&job).unwrap())
                        .await
                        .map_err(|_| TitoError::UpdateFailed(String::from("Failed job")))?;
                }
                Ok::<_, TitoError>(())
            })
            .await;

        Ok(())
    }

    pub async fn fail_job(&self, job_id: String) -> Result<(), TitoError> {
        self.tx_manager
            .transaction(|tx| async move {
                let job = tx.get(job_id.clone()).await?;

                if let Some(job_bytes) = job {
                    let mut job: TitoJob = serde_json::from_slice(&job_bytes).map_err(|_| {
                        TitoError::DeserializationFailed(String::from("Failed job"))
                    })?;

                    if job.retries < job.max_retries {
                        let additional_seconds = (job.retries * job.retries * 60) as i64;
                        job.scheduled_for = job.scheduled_for + additional_seconds;

                        let new_key = job_id.replace("PROGRESS", "PENDING");

                        let mut parts: Vec<&str> = new_key.split(':').collect();
                        if parts.len() >= 4 {
                            // Replace the timestamp (third from last element) with the new timestamp
                            let schedule_part = job.scheduled_for.to_string();
                            parts[2] = &schedule_part;
                            let new_key = parts.join(":");

                            job.key = new_key.clone();
                            job.retries += 1;
                            job.status = "PENDING".to_string();

                            tx.delete(job_id)
                                .await
                                .map_err(|_| TitoError::DeleteFailed(String::from("Failed job")))?;

                            tx.put(new_key, serde_json::to_vec(&job).unwrap())
                                .await
                                .map_err(|_| TitoError::DeleteFailed(String::from("Failed job")))?;
                        }
                    } else {
                        let new_key = job_id.replace("PROGRESS", "FAILED");
                        job.key = new_key.clone();
                        job.status = "FAILED".to_string();

                        tx.delete(job_id)
                            .await
                            .map_err(|_| TitoError::DeleteFailed(String::from("Failed job")))?;

                        tx.put(new_key, serde_json::to_vec(&job).unwrap())
                            .await
                            .map_err(|_| TitoError::DeleteFailed(String::from("Failed job")))?;
                    }
                }

                Ok::<_, TitoError>(())
            })
            .await;

        Ok(())
    }

    pub async fn clear(&self) -> Result<(), TitoError> {
        Ok(())
    }
}

pub async fn run_worker<H>(
    queue: Arc<TitoQueue>,
    handler: H,
    is_leader: Arc<AtomicBool>,
    concurrency: u32,
    mut shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()>
where
    H: Fn(TitoJob) -> futures::future::BoxFuture<'static, Result<(), TitoError>>
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
                        // Attempt to pull jobs from the queue
                        match queue.pull(20, 0, None).await {
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
