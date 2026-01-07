use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tokio::time::sleep;

use crate::types::{TitoEngine, TitoEvent, TitoTransaction};
use crate::utils::next_string_lexicographically;
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
        use crate::types::{QueueCheckpoint, PARTITION_DIGITS};
        use chrono::Utc;

        self.engine
            .transaction(|tx| {
                let consumer = consumer.clone();
                let event_type = event_type.clone();
                async move {
                    let now = Utc::now().timestamp();

                    let checkpoint_key = format!(
                        "queue_checkpoint:{}:{}:{:0width$}",
                        consumer,
                        event_type,
                        partition,
                        width = PARTITION_DIGITS
                    );
                    let checkpoint = tx
                        .get(checkpoint_key.as_bytes())
                        .await
                        .map_err(|e| {
                            TitoError::QueryFailed(format!("Checkpoint read failed: {}", e))
                        })?
                        .and_then(|bytes| serde_json::from_slice::<QueueCheckpoint>(&bytes).ok());

                    let (start_ts, start_uuid) = checkpoint
                        .map(|c| (c.timestamp, c.uuid))
                        .unwrap_or((0, String::new()));

                    // If we have a UUID, start AFTER that specific event to skip already-processed ones
                    let event_start = if !start_uuid.is_empty() {
                        let last_key = format!(
                            "event:{}:{:0pwidth$}:{}:{}",
                            event_type,
                            partition,
                            start_ts,
                            start_uuid,
                            pwidth = PARTITION_DIGITS,
                        );
                        next_string_lexicographically(last_key)
                    } else {
                        format!(
                            "event:{}:{:0pwidth$}:{}",
                            event_type,
                            partition,
                            start_ts,
                            pwidth = PARTITION_DIGITS,
                        )
                    };
                    let event_end = format!(
                        "event:{}:{:0pwidth$}:{}",
                        event_type,
                        partition,
                        now + 1,
                        pwidth = PARTITION_DIGITS,
                    );

                    let events = tx
                        .scan(event_start.as_bytes()..event_end.as_bytes(), limit)
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Scan failed: {}", e)))?;

                    let mut parsed: Vec<(TitoEvent, Vec<u8>)> = Vec::new();
                    for (_key, value) in events {
                        if let Ok(event) = serde_json::from_slice::<TitoEvent>(&value) {
                            if event.timestamp > now {
                                continue;
                            }
                            let parts: Vec<&str> = event.key.split(':').collect();
                            if parts.len() < 5 {
                                continue;
                            }
                            let completed_key = format!(
                                "queue:{}:{}:completed:{}:{}:{}",
                                consumer, event_type, parts[2], parts[3], parts[4]
                            )
                            .into_bytes();
                            parsed.push((event, completed_key));
                        }
                    }

                    let completed_keys: Vec<Vec<u8>> =
                        parsed.iter().map(|(_, k)| k.clone()).collect();
                    let completed: std::collections::HashSet<Vec<u8>> = tx
                        .batch_get(completed_keys)
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Batch check: {}", e)))?
                        .into_iter()
                        .map(|(k, _)| k)
                        .collect();

                    let mut jobs = Vec::new();
                    let mut oldest_pending_ts: Option<i64> = None;
                    let mut last_scanned_key: Option<String> = None;

                    for (event, completed_key) in &parsed {
                        last_scanned_key = Some(event.key.clone());

                        if completed.contains(completed_key) {
                            continue;
                        }
                        if oldest_pending_ts.is_none()
                            || event.timestamp < oldest_pending_ts.unwrap()
                        {
                            oldest_pending_ts = Some(event.timestamp);
                        }
                        jobs.push(event.clone());
                    }

                    if let Some(ts) = oldest_pending_ts {
                        let new_ckpt = QueueCheckpoint {
                            timestamp: ts,
                            uuid: String::new(),
                        };
                        tx.put(
                            checkpoint_key.as_bytes(),
                            serde_json::to_vec(&new_ckpt).unwrap(),
                        )
                        .await
                        .map_err(|e| {
                            TitoError::UpdateFailed(format!("Checkpoint update: {}", e))
                        })?;
                    } else if jobs.is_empty() && parsed.len() >= limit as usize {
                        if let Some(key) = last_scanned_key {
                            let parts: Vec<&str> = key.split(':').collect();
                            if parts.len() >= 5 {
                                if let Ok(ts) = parts[3].parse::<i64>() {
                                    let uuid = parts[4].to_string();
                                    let new_ckpt = QueueCheckpoint {
                                        timestamp: ts,
                                        uuid,
                                    };
                                    tx.put(
                                        checkpoint_key.as_bytes(),
                                        serde_json::to_vec(&new_ckpt).unwrap(),
                                    )
                                    .await
                                    .map_err(|e| {
                                        TitoError::UpdateFailed(format!("Checkpoint update: {}", e))
                                    })?;
                                }
                            }
                        }
                    }

                    Ok::<_, TitoError>(jobs)
                }
            })
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

                    let completed_key = format!(
                        "queue:{}:{}:completed:{}:{}:{}",
                        consumer, event_type, partition_str, timestamp_str, uuid_str
                    );
                    let completed = QueueCompleted {
                        updated_at: Utc::now().timestamp(),
                    };
                    tx.put(
                        completed_key.as_bytes(),
                        serde_json::to_vec(&completed).unwrap(),
                    )
                    .await
                    .map_err(|e| TitoError::UpdateFailed(format!("Put completed: {}", e)))?;

                    Ok::<_, TitoError>(())
                }
            })
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

                    let event_bytes = tx
                        .get(job_id.as_bytes())
                        .await
                        .map_err(|e| TitoError::QueryFailed(format!("Get event: {}", e)))?;

                    if let Some(bytes) = event_bytes {
                        let mut event: TitoEvent = serde_json::from_slice(&bytes)
                            .map_err(|_| TitoError::DeserializationFailed("Event".to_string()))?;

                        const MAX_RETRIES: u32 = 5;

                        if event.retries < MAX_RETRIES {
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

                            tx.delete(job_id.as_bytes()).await.map_err(|e| {
                                TitoError::DeleteFailed(format!("Delete old: {}", e))
                            })?;
                            tx.put(new_key.as_bytes(), serde_json::to_vec(&event).unwrap())
                                .await
                                .map_err(|e| TitoError::UpdateFailed(format!("Put new: {}", e)))?;
                        } else {
                            tx.delete(job_id.as_bytes())
                                .await
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
                            tx.put(failed_key.as_bytes(), serde_json::to_vec(&failed).unwrap())
                                .await
                                .map_err(|e| {
                                    TitoError::UpdateFailed(format!("Put failed: {}", e))
                                })?;
                        }
                    }

                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn clear(&self) -> Result<(), TitoError> {
        Ok(())
    }
}

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
