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
        use crate::types::{PARTITION_DIGITS, QueueCheckpoint};
        use chrono::Utc;

        self.engine
            .transaction(|tx| {
                let consumer = consumer.clone();
                let event_type = event_type.clone();
                async move {
                let now = Utc::now().timestamp();

                // Get checkpoint
                let checkpoint_key = format!(
                    "queue_checkpoint:{}:{}:{:0width$}",
                    consumer, event_type, partition,
                    width = PARTITION_DIGITS
                );
                let checkpoint = tx.get(checkpoint_key.as_bytes()).await
                    .map_err(|e| TitoError::QueryFailed(format!("Checkpoint read failed: {}", e)))?
                    .and_then(|bytes| serde_json::from_slice::<QueueCheckpoint>(&bytes).ok());

                let start_ts = checkpoint.map(|c| c.timestamp).unwrap_or(0);

                // Scan from checkpoint
                let event_start = format!(
                    "event:{}:{:0pwidth$}:{}",
                    event_type, partition, start_ts,
                    pwidth = PARTITION_DIGITS,
                );
                let event_end = format!(
                    "event:{}:{:0pwidth$}:{}",
                    event_type, partition, now + 1,
                    pwidth = PARTITION_DIGITS,
                );

                let events = tx.scan(
                    event_start.as_bytes()..event_end.as_bytes(),
                    limit
                ).await.map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

                let mut jobs = Vec::new();
                let mut oldest_pending_ts: Option<i64> = None;

                for (_key, value) in events {
                    let event: TitoEvent = match serde_json::from_slice(&value) {
                        Ok(e) => e,
                        Err(_) => continue,
                    };

                    // Skip future events
                    if event.timestamp > now {
                        continue;
                    }

                    // Parse key parts
                    let parts: Vec<&str> = event.key.split(':').collect();
                    if parts.len() < 5 { continue; }
                    let partition_str = parts[2];
                    let timestamp_str = parts[3];
                    let uuid_str = parts[4];

                    // Check completed
                    let completed_key = format!(
                        "queue:{}:{}:completed:{}:{}:{}",
                        consumer, event_type, partition_str, timestamp_str, uuid_str
                    );
                    let is_completed = tx.get(completed_key.as_bytes()).await
                        .map_err(|e| TitoError::QueryFailed(format!("Completed check: {}", e)))?
                        .is_some();

                    if is_completed {
                        continue;
                    }

                    // Track oldest pending
                    if oldest_pending_ts.is_none() || event.timestamp < oldest_pending_ts.unwrap() {
                        oldest_pending_ts = Some(event.timestamp);
                    }

                    jobs.push(event);
                }

                // Checkpoint = oldest pending (so we retry from there)
                if let Some(ts) = oldest_pending_ts {
                    let new_ckpt = QueueCheckpoint { timestamp: ts, uuid: String::new() };
                    tx.put(checkpoint_key.as_bytes(), serde_json::to_vec(&new_ckpt).unwrap())
                        .await
                        .map_err(|e| TitoError::UpdateFailed(format!("Checkpoint update: {}", e)))?;
                }

                Ok::<_, TitoError>(jobs)
            }})
            .await
    }

    pub async fn success_job(&self, consumer: String, job_id: String) -> Result<(), TitoError> {
        use crate::types::QueueCompleted;
        use chrono::Utc;

        self.engine
            .transaction(|tx| {
                let consumer = consumer.clone();
                let job_id = job_id.clone();
                async move {
                let parts: Vec<&str> = job_id.split(':').collect();
                if parts.len() < 5 {
                    return Err(TitoError::InvalidInput("Invalid key".to_string()));
                }
                let event_type = parts[1];
                let partition_str = parts[2];
                let timestamp_str = parts[3];
                let uuid_str = parts[4];

                // Create completed key
                let completed_key = format!(
                    "queue:{}:{}:completed:{}:{}:{}",
                    consumer, event_type, partition_str, timestamp_str, uuid_str
                );
                let completed = QueueCompleted { updated_at: Utc::now().timestamp() };
                tx.put(completed_key.as_bytes(), serde_json::to_vec(&completed).unwrap())
                    .await
                    .map_err(|e| TitoError::UpdateFailed(format!("Put completed: {}", e)))?;

                Ok::<_, TitoError>(())
            }})
            .await
    }

    pub async fn fail_job(&self, consumer: String, job_id: String) -> Result<(), TitoError> {
        use crate::types::{QueueFailed, TitoEvent};
        use chrono::Utc;

        self.engine
            .transaction(|tx| {
                let consumer = consumer.clone();
                let job_id = job_id.clone();
                async move {
                let parts: Vec<&str> = job_id.split(':').collect();
                if parts.len() < 5 {
                    return Err(TitoError::InvalidInput("Invalid key".to_string()));
                }
                let event_type = parts[1];
                let partition_str = parts[2];
                let timestamp_str = parts[3];
                let uuid_str = parts[4];

                // Get event
                let event_bytes = tx.get(job_id.as_bytes()).await
                    .map_err(|e| TitoError::QueryFailed(format!("Get event: {}", e)))?;

                if let Some(bytes) = event_bytes {
                    let mut event: TitoEvent = serde_json::from_slice(&bytes)
                        .map_err(|_| TitoError::DeserializationFailed("Event".to_string()))?;

                    const MAX_RETRIES: u32 = 5;

                    if event.retries < MAX_RETRIES {
                        // Reschedule with backoff
                        event.retries += 1;
                        let backoff = 2_i64.pow(event.retries);
                        let new_ts = Utc::now().timestamp() + backoff;
                        event.timestamp = new_ts;
                        event.updated_at = Utc::now().timestamp();

                        let new_key = format!(
                            "event:{}:{}:{}:{}",
                            event_type, partition_str, new_ts, uuid_str
                        );
                        event.key = new_key.clone();

                        tx.delete(job_id.as_bytes()).await
                            .map_err(|e| TitoError::DeleteFailed(format!("Delete old: {}", e)))?;
                        tx.put(new_key.as_bytes(), serde_json::to_vec(&event).unwrap()).await
                            .map_err(|e| TitoError::UpdateFailed(format!("Put new: {}", e)))?;
                    } else {
                        // Max retries - move to failed
                        tx.delete(job_id.as_bytes()).await
                            .map_err(|e| TitoError::DeleteFailed(format!("Delete: {}", e)))?;

                        let failed_key = format!(
                            "queue:{}:{}:failed:{}:{}:{}",
                            consumer, event_type, partition_str, timestamp_str, uuid_str
                        );
                        let failed = QueueFailed {
                            retries: event.retries,
                            updated_at: Utc::now().timestamp(),
                            error: Some("Max retries".to_string()),
                        };
                        tx.put(failed_key.as_bytes(), serde_json::to_vec(&failed).unwrap()).await
                            .map_err(|e| TitoError::UpdateFailed(format!("Put failed: {}", e)))?;
                    }
                }

                Ok::<_, TitoError>(())
            }})
            .await
    }

    pub async fn clear(&self) -> Result<(), TitoError> {
        Ok(())
    }
}

/// Worker - one per partition, no competition
pub async fn run_worker<E: TitoEngine + 'static, H>(
    queue: Arc<TitoQueue<E>>,
    config: crate::types::WorkerConfig,
    handler: H,
    shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()>
where
    H: Fn(TitoEvent) -> futures::future::BoxFuture<'static, Result<(), TitoError>>
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
            let consumer = config.consumer.clone();
            let event_type = config.event_type.clone();
            let mut rx = shutdown.resubscribe();

            handles.push(tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = rx.recv() => break,
                        _ = async {
                            match q.pull_partition(consumer.clone(), event_type.clone(), partition, 50).await {
                                Ok(jobs) if jobs.is_empty() => {
                                    sleep(Duration::from_millis(1000)).await;
                                }
                                Ok(jobs) => {
                                    for event in jobs {
                                        let key = event.key.clone();
                                        match h(event).await {
                                            Ok(_) => { let _ = q.success_job(consumer.clone(), key).await; }
                                            Err(_) => { let _ = q.fail_job(consumer.clone(), key).await; }
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
