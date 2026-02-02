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
        _consumer: String,
        event_type: String,
        partition: u32,
        limit: u32,
    ) -> Result<Vec<TitoEvent>, TitoError> {
        use crate::types::PARTITION_DIGITS;
        use chrono::Utc;

        self.engine
            .transaction(|tx| {
                let event_type = event_type.clone();
                async move {
                    let now = Utc::now().timestamp();

                    let event_start = format!(
                        "event:{}:{:0pwidth$}:{}",
                        event_type,
                        partition,
                        0,
                        pwidth = PARTITION_DIGITS,
                    );
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

                    let mut jobs: Vec<TitoEvent> = Vec::new();
                    for (_key, value) in events.into_iter() {
                        if let Ok(event) = serde_json::from_slice::<TitoEvent>(&value) {
                            if event.timestamp <= now {
                                jobs.push(event);
                            }
                        }
                    }

                    Ok::<_, TitoError>(jobs)
                }
            })
            .await
    }

    pub async fn success_job(&self, _consumer: String, job_id: String) -> Result<(), TitoError> {
        self.engine
            .transaction(|tx| {
                let job_id = job_id.clone();
                async move {
                    tx.delete(job_id.as_bytes())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete event: {}", e)))?;
                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn fail_job(&self, _consumer: String, job_id: String) -> Result<(), TitoError> {
        self.engine
            .transaction(|tx| {
                let job_id = job_id.clone();
                async move {
                    tx.delete(job_id.as_bytes())
                        .await
                        .map_err(|e| TitoError::DeleteFailed(format!("Delete event: {}", e)))?;
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
