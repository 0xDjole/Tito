use chrono::Utc;
use futures::{stream, StreamExt};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;

use crate::{
    transaction::TransactionManager,
    types::{TiKvDatabase, TiKvError, TiKvJob},
};

#[derive(Clone)]
pub struct TiKvQueue {
    pub db: TiKvDatabase,
    pub tx_manager: TransactionManager,
    pub table: String,
}

impl TiKvQueue {
    pub async fn push(
        &self,
        _message: String,
        _date: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<(), TiKvError> {
        Ok(())
    }

    pub async fn pull(
        &self,
        limit: u32,
        _offset: u32,
        _group_id: Option<String>,
    ) -> Result<Vec<TiKvJob>, TiKvError> {
        let mut jobs: Vec<TiKvJob> = Vec::new();

        let start_bound = format!("{}:PENDING", self.table);

        let current_time = Utc::now().timestamp();
        let end_bound = format!("{}:PENDING:{}", self.table, current_time);

        let jobs = self
            .tx_manager
            .transaction(|tx| async move {
                let scan_stream = tx.scan(start_bound..end_bound, limit).await?;

                for item in scan_stream {
                    if let Ok(job) = serde_json::from_slice::<TiKvJob>(&item.1) {
                        jobs.push(job);
                    }
                }

                let keys: Vec<_> = jobs.iter().map(|job| job.group_id.clone()).collect();

                for job in jobs.iter_mut() {
                    let key_by_entity = format!(
                        "{}_by_entity:{}:{}:{}:{}",
                        self.table, job.entity_id, job.action, job.scheduled_for, job.id
                    );

                    tx.delete(job.key.clone()).await;
                    tx.delete(key_by_entity).await;

                    job.status = String::from("PROGRESS");
                    let new_key = job.key.replace("PENDING", "PROGRESS");
                    job.key = new_key.clone();

                    let json_job = serde_json::to_value(job.clone());

                    let key_by_entity = format!(
                        "{}_by_entity:{}:{}:{}:{}",
                        self.table, job.entity_id, job.action, job.scheduled_for, job.id
                    );

                    if let Ok(value) = json_job {
                        tx.put(new_key.clone(), serde_json::to_vec(&value).unwrap())
                            .await;

                        tx.put(key_by_entity.clone(), serde_json::to_vec(&new_key).unwrap())
                            .await;
                    }
                }

                Ok::<_, TiKvError>(jobs)
            })
            .await?;

        Ok(jobs.into_iter().map(|job| job.into()).collect())
    }
    pub async fn success_job(&self, job_id: String) -> Result<(), TiKvError> {
        let new_key = job_id.replace("PROGRESS", "COMPLETED");

        self.tx_manager
            .transaction(|tx| async move {
                let job = tx.get(job_id.clone()).await?;

                if let Some(job_bytes) = job {
                    let mut job: TiKvJob = serde_json::from_slice(&job_bytes)
                        .map_err(|_| TiKvError::NotFound(String::from("Failed job")))?;
                    job.status = "COMPLETED".to_string();
                    job.key = new_key.clone();

                    tx.delete(job_id)
                        .await
                        .map_err(|_| TiKvError::TransactionFailed(String::from("Failed job")))?;
                    tx.put(new_key.clone(), serde_json::to_vec(&job).unwrap())
                        .await
                        .map_err(|_| TiKvError::Failed)?;

                    let key_by_entity = format!(
                        "{}_by_entity:{}:{}:{}:{}",
                        self.table, job.entity_id, job.action, job.scheduled_for, job.id
                    );

                    tx.put(key_by_entity, serde_json::to_vec(&new_key).unwrap())
                        .await
                        .map_err(|_| TiKvError::TransactionFailed(String::from("Failed job")))?;
                }
                Ok::<_, TiKvError>(())
            })
            .await;

        Ok(())
    }

    pub async fn fail_job(&self, job_id: String) -> Result<(), TiKvError> {
        self.tx_manager
            .transaction(|tx| async move {
                let job = tx.get(job_id.clone()).await?;

                if let Some(job_bytes) = job {
                    let mut job: TiKvJob = serde_json::from_slice(&job_bytes)
                        .map_err(|_| TiKvError::TransactionFailed(String::from("Failed job")))?;

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

                            tx.delete(job_id).await.map_err(|_| {
                                TiKvError::TransactionFailed(String::from("Failed job"))
                            })?;

                            tx.put(new_key, serde_json::to_vec(&job).unwrap())
                                .await
                                .map_err(|_| {
                                    TiKvError::TransactionFailed(String::from("Failed job"))
                                })?;
                        }
                    } else {
                        let new_key = job_id.replace("PROGRESS", "FAILED");
                        job.key = new_key.clone();
                        job.status = "FAILED".to_string();

                        tx.delete(job_id).await.map_err(|_| {
                            TiKvError::TransactionFailed(String::from("Failed job"))
                        })?;

                        tx.put(new_key, serde_json::to_vec(&job).unwrap())
                            .await
                            .map_err(|_| {
                                TiKvError::TransactionFailed(String::from("Failed job"))
                            })?;
                    }
                }

                Ok::<_, TiKvError>(())
            })
            .await;

        Ok(())
    }
    pub async fn clear(&self) -> Result<(), TiKvError> {
        Ok(())
    }
}

pub async fn run_worker<H>(
    queue: Arc<TiKvQueue>,
    handler: H,
    is_leader: Arc<AtomicBool>,
    concurrency: u32,
    mut shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()>
where
    H: Fn(TiKvJob) -> futures::future::BoxFuture<'static, Result<(), TiKvError>>
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
