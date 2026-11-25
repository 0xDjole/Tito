use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};
use tokio::task::{AbortHandle, JoinSet};
use tokio::time::{sleep, timeout};

use super::worker::{
    apply_handler_outcome, execute_handler, handler_outcome_or_log, DEFAULT_HANDLER_TIMEOUT,
};
use super::{duration_millis, Queue, QueueEvent, QueueHandlerResult, QueuePullCursor};
use crate::key_encoder::encode_index_string;
use crate::types::{Sealed, TitoEngine, TitoTransaction, PARTITION_DIGITS};
use crate::TitoError;

#[derive(Debug, Clone)]
pub struct ClusterWorkerConfig {
    pub node_id: String,
    pub heartbeat_interval: Duration,
    pub poll_interval: Duration,
    pub handler_timeout: Duration,
    pub lease_ttl: Duration,
    pub stale_node_ttl: Duration,
}

impl ClusterWorkerConfig {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            heartbeat_interval: Duration::from_secs(5),
            poll_interval: Duration::from_millis(500),
            handler_timeout: DEFAULT_HANDLER_TIMEOUT,
            lease_ttl: Duration::from_secs(30),
            stale_node_ttl: Duration::from_secs(300),
        }
    }

    pub fn from_env() -> Self {
        let boot_id = uuid::Uuid::new_v4().to_string();
        let base = match (std::env::var("POD_NAME"), std::env::var("POD_UID")) {
            (Ok(name), Ok(uid)) if !name.is_empty() && !uid.is_empty() => {
                format!("{name}:{uid}")
            }
            _ => {
                let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "local".to_string());
                format!("{}:{}", host, std::process::id())
            }
        };

        Self::new(format!("{base}:{boot_id}"))
    }

    fn stop_after_millis(&self) -> i64 {
        duration_millis(self.lease_ttl)
            .saturating_sub(duration_millis(self.heartbeat_interval).saturating_mul(2))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterWorkerNode {
    pub node_id: String,
    pub heartbeat_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterPartitionAssignment {
    pub partition: u32,
    pub owner_node_id: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ClusterPartitions {
    pub share: Vec<u32>,
    pub held: Vec<u32>,
}

impl<E: TitoEngine> Queue<E> {
    fn cluster_prefix(&self) -> String {
        format!("tito:{}:cluster:", self.config.name())
    }

    fn nodes_prefix(&self) -> String {
        format!("{}nodes:", self.cluster_prefix())
    }

    pub(crate) fn node_key(&self, node_id: &str) -> String {
        format!("{}{}", self.nodes_prefix(), encode_index_string(node_id))
    }

    pub(crate) fn partition_key(&self, partition: u32) -> String {
        format!(
            "{}partitions:{:0pwidth$}",
            self.cluster_prefix(),
            partition,
            pwidth = PARTITION_DIGITS,
        )
    }

    fn decode_json<T: DeserializeOwned>(bytes: Option<Vec<u8>>) -> Result<Option<T>, TitoError> {
        let Some(bytes) = bytes else {
            return Ok(None);
        };

        serde_json::from_slice::<T>(&bytes)
            .map(Some)
            .map_err(|e| TitoError::DeserializationFailed(e.to_string()))
    }

    async fn put_json<T: Serialize>(
        tx: &E::Transaction,
        key: &str,
        value: &T,
    ) -> Result<(), TitoError> {
        let bytes =
            serde_json::to_vec(value).map_err(|e| TitoError::SerializationFailed(e.to_string()))?;
        tx.put(key.as_bytes(), bytes)
            .await
            .map_err(|e| TitoError::UpdateFailed(format!("Put cluster queue key: {}", e)))
    }

    async fn read_cluster_nodes(
        &self,
        tx: &E::Transaction,
    ) -> Result<Vec<ClusterWorkerNode>, TitoError> {
        let prefix = self.nodes_prefix();
        let entries = tx
            .scan(
                prefix.as_bytes()..Self::prefix_end(&prefix).as_slice(),
                u32::MAX,
            )
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Scan cluster queue nodes: {}", e)))?;

        let mut nodes = Vec::with_capacity(entries.len());
        for (key, value) in entries {
            let node = serde_json::from_slice::<ClusterWorkerNode>(&value)
                .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;
            if key != self.node_key(&node.node_id).into_bytes() {
                return Err(TitoError::DeserializationFailed(
                    "Cluster queue node row names another node".to_string(),
                ));
            }
            nodes.push(node);
        }
        Ok(nodes)
    }

    async fn read_cluster_partitions(
        &self,
        tx: &E::Transaction,
    ) -> Result<Vec<ClusterPartitionAssignment>, TitoError> {
        let partition_count = self.config.partition_count.max(1);
        let keys = (0..partition_count)
            .map(|partition| self.partition_key(partition))
            .collect::<Vec<_>>();
        let mut found: HashMap<_, _> = tx
            .get_keys(keys.iter().collect::<Vec<_>>(), Sealed)
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get cluster queue partitions: {}", e)))?
            .into_iter()
            .collect();

        let mut assignments = Vec::with_capacity(keys.len());
        for (partition, key) in (0..partition_count).zip(keys) {
            let assignment =
                Self::decode_json::<ClusterPartitionAssignment>(found.remove(key.as_bytes()))?
                    .unwrap_or(ClusterPartitionAssignment {
                        partition,
                        owner_node_id: None,
                    });
            if assignment.partition != partition {
                return Err(TitoError::DeserializationFailed(
                    "Cluster queue partition row names another partition".to_string(),
                ));
            }
            assignments.push(assignment);
        }
        Ok(assignments)
    }

    async fn fence_cluster_node(
        &self,
        tx: &E::Transaction,
        nodes: &[ClusterWorkerNode],
        node_id: &str,
    ) -> Result<(), TitoError> {
        let key = self.node_key(node_id);
        match nodes.iter().find(|node| node.node_id == node_id) {
            Some(node) => Self::put_json(tx, &key, node).await,
            None => tx
                .delete(key.as_bytes())
                .await
                .map_err(|e| TitoError::DeleteFailed(format!("Fence cluster queue node: {}", e))),
        }
    }

    pub async fn heartbeat_cluster_worker(
        &self,
        config: &ClusterWorkerConfig,
    ) -> Result<i64, TitoError> {
        let config = config.clone();
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    let node = ClusterWorkerNode {
                        node_id: config.node_id.clone(),
                        heartbeat_at: Utc::now().timestamp_millis(),
                    };
                    Self::put_json(&tx, &self.node_key(&config.node_id), &node).await?;
                    Ok::<_, TitoError>(node.heartbeat_at)
                }
            })
            .await
    }

    pub async fn step_cluster_worker(
        &self,
        config: &ClusterWorkerConfig,
        running_partitions: &BTreeSet<u32>,
    ) -> Result<ClusterPartitions, TitoError> {
        let config = config.clone();
        let running_partitions = running_partitions.clone();
        let (partitions, stale_nodes) = self
            .engine
            .transaction(|tx| {
                let config = config.clone();
                let running_partitions = running_partitions.clone();
                async move {
                    let now = Utc::now().timestamp_millis();
                    let live_after = now.saturating_sub(duration_millis(config.lease_ttl));
                    let remove_before = now.saturating_sub(duration_millis(config.stale_node_ttl));
                    let nodes = self.read_cluster_nodes(&tx).await?;
                    let stale_nodes = nodes.iter().any(|node| node.heartbeat_at < remove_before);
                    let mut live_nodes = nodes
                        .iter()
                        .filter(|node| node.heartbeat_at > live_after)
                        .cloned()
                        .collect::<Vec<_>>();
                    live_nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
                    let share =
                        desired_partition_owners(self.config.partition_count.max(1), &live_nodes)
                            .into_iter()
                            .filter(|(_, node_id)| *node_id == config.node_id)
                            .map(|(partition, _)| partition)
                            .collect::<BTreeSet<_>>();

                    let mut fenced = BTreeSet::new();
                    let mut held = Vec::new();
                    for mut assignment in self.read_cluster_partitions(&tx).await? {
                        let partition = assignment.partition;
                        if assignment.owner_node_id.as_deref() == Some(config.node_id.as_str()) {
                            if share.contains(&partition) || running_partitions.contains(&partition)
                            {
                                held.push(partition);
                            } else {
                                assignment.owner_node_id = None;
                                Self::put_json(&tx, &self.partition_key(partition), &assignment)
                                    .await?;
                            }
                            continue;
                        }
                        if !share.contains(&partition) {
                            continue;
                        }
                        if let Some(holder) = assignment.owner_node_id.clone() {
                            if live_nodes.iter().any(|node| node.node_id == holder) {
                                continue;
                            }
                            if fenced.insert(holder.clone()) {
                                self.fence_cluster_node(&tx, &nodes, &holder).await?;
                            }
                        }
                        assignment.owner_node_id = Some(config.node_id.clone());
                        Self::put_json(&tx, &self.partition_key(partition), &assignment).await?;
                        held.push(partition);
                    }

                    Ok::<_, TitoError>((
                        ClusterPartitions {
                            share: share.into_iter().collect(),
                            held,
                        },
                        stale_nodes,
                    ))
                }
            })
            .await?;

        if stale_nodes {
            if let Err(error) = self.delete_stale_cluster_nodes(&config).await {
                log::error!(
                    "Deleting stale queue cluster nodes failed for {}: {error}",
                    config.node_id
                );
            }
        }
        Ok(partitions)
    }

    async fn delete_stale_cluster_nodes(
        &self,
        config: &ClusterWorkerConfig,
    ) -> Result<(), TitoError> {
        let config = config.clone();
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    let remove_before = Utc::now()
                        .timestamp_millis()
                        .saturating_sub(duration_millis(config.stale_node_ttl));
                    for node in self.read_cluster_nodes(&tx).await? {
                        if node.heartbeat_at < remove_before {
                            tx.delete(self.node_key(&node.node_id).as_bytes())
                                .await
                                .map_err(|e| {
                                    TitoError::DeleteFailed(format!(
                                        "Delete stale cluster queue node: {}",
                                        e
                                    ))
                                })?;
                        }
                    }
                    Ok::<_, TitoError>(())
                }
            })
            .await
    }

    pub async fn leave_cluster(&self, config: &ClusterWorkerConfig) -> Result<(), TitoError> {
        let config = config.clone();
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    for mut assignment in self.read_cluster_partitions(&tx).await? {
                        if assignment.owner_node_id.as_deref() == Some(config.node_id.as_str()) {
                            assignment.owner_node_id = None;
                            Self::put_json(
                                &tx,
                                &self.partition_key(assignment.partition),
                                &assignment,
                            )
                            .await?;
                        }
                    }
                    tx.delete(self.node_key(&config.node_id).as_bytes())
                        .await
                        .map_err(|e| {
                            TitoError::DeleteFailed(format!("Delete cluster queue node: {}", e))
                        })?;
                    Ok::<_, TitoError>(())
                }
            })
            .await
    }
}

fn desired_partition_owners(
    partition_count: u32,
    nodes: &[ClusterWorkerNode],
) -> BTreeMap<u32, String> {
    let mut desired = BTreeMap::new();
    if nodes.is_empty() {
        return desired;
    }

    let node_count = nodes.len() as u64;

    for (index, node) in nodes.iter().enumerate() {
        let start = ((index as u64 * partition_count as u64) / node_count) as u32;
        let end = (((index as u64 + 1) * partition_count as u64) / node_count) as u32;
        for partition in start..end {
            desired.insert(partition, node.node_id.clone());
        }
    }

    desired
}

pub async fn run_cluster_worker<E, T, H>(
    queue: Arc<Queue<E>>,
    config: ClusterWorkerConfig,
    handler: H,
    shutdown: broadcast::Receiver<()>,
) -> tokio::task::JoinHandle<()>
where
    E: TitoEngine + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, QueueHandlerResult<T>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    tokio::spawn(async move {
        let stop_after = config.stop_after_millis();
        if stop_after <= 0 {
            log::error!(
                "Queue cluster worker {} needs a lease_ttl longer than two heartbeat intervals",
                config.node_id
            );
            return;
        }
        let mut shutdown = shutdown;
        let heartbeat = Arc::new(AtomicI64::new(i64::MIN));
        let mut tasks = JoinSet::new();
        let mut workers = BTreeMap::<u32, PartitionWorker>::new();

        loop {
            if shutdown_requested(&mut shutdown) {
                break;
            }
            while let Some(result) = tasks.try_join_next() {
                if let Err(error) = result {
                    log::error!(
                        "A queue partition worker of {} ended abnormally: {error}",
                        config.node_id
                    );
                }
            }
            workers.retain(|_, worker| !worker.task.is_finished());

            let heartbeat_at = tokio::select! {
                biased;
                _ = shutdown.recv() => break,
                heartbeat_at = queue.heartbeat_cluster_worker(&config) => heartbeat_at,
            };
            match heartbeat_at {
                Ok(heartbeat_at) => {
                    let previous = heartbeat.load(Ordering::Acquire);
                    if Utc::now().timestamp_millis().saturating_sub(previous) >= stop_after {
                        for worker in workers.values() {
                            let _ = worker.stop.send(true);
                        }
                    }
                    heartbeat.store(heartbeat_at, Ordering::Release);
                    let running = workers.keys().copied().collect::<BTreeSet<_>>();
                    let step = tokio::select! {
                        biased;
                        _ = shutdown.recv() => break,
                        step = queue.step_cluster_worker(&config, &running) => step,
                    };
                    match step {
                        Ok(partitions) => {
                            let run = partitions
                                .held
                                .iter()
                                .copied()
                                .filter(|partition| partitions.share.contains(partition))
                                .collect::<BTreeSet<_>>();
                            for (partition, worker) in &workers {
                                if !run.contains(partition) {
                                    let _ = worker.stop.send(true);
                                }
                            }
                            for partition in run {
                                if workers.contains_key(&partition) {
                                    continue;
                                }
                                let (stop, stop_rx) = watch::channel(false);
                                let task = tasks.spawn(run_partition_worker(
                                    queue.clone(),
                                    config.clone(),
                                    partition,
                                    handler.clone(),
                                    stop_rx,
                                    heartbeat.clone(),
                                    stop_after,
                                ));
                                workers.insert(partition, PartitionWorker { stop, task });
                            }
                        }
                        Err(error) => {
                            log::error!(
                                "Queue cluster step failed for {}: {error}",
                                config.node_id
                            );
                        }
                    }
                }
                Err(error) => {
                    log::error!("Queue heartbeat failed for {}: {error}", config.node_id);
                }
            }

            tokio::select! {
                biased;
                _ = shutdown.recv() => break,
                _ = sleep(config.heartbeat_interval) => {}
            }
        }

        for worker in workers.values() {
            let _ = worker.stop.send(true);
        }
        while tasks.join_next().await.is_some() {}
        match timeout(config.lease_ttl, queue.leave_cluster(&config)).await {
            Ok(Ok(())) => {}
            Ok(Err(error)) => {
                log::error!(
                    "Leaving the queue cluster failed for {}: {error}",
                    config.node_id
                );
            }
            Err(_) => {
                log::error!("Leaving the queue cluster timed out for {}", config.node_id);
            }
        }
    })
}

struct PartitionWorker {
    stop: watch::Sender<bool>,
    task: AbortHandle,
}

async fn run_partition_worker<E, T, H>(
    queue: Arc<Queue<E>>,
    config: ClusterWorkerConfig,
    partition: u32,
    handler: H,
    mut stop: watch::Receiver<bool>,
    heartbeat: Arc<AtomicI64>,
    stop_after: i64,
) where
    E: TitoEngine + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, QueueHandlerResult<T>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let mut cursor: Option<QueuePullCursor> = None;
    let mut finished_in_pass = false;

    loop {
        if !partition_still_held(&stop, &heartbeat, stop_after) {
            return;
        }

        let pull = tokio::select! {
            biased;
            _ = stop.changed() => return,
            pull = queue.pull::<T>(partition, cursor.clone(), 50) => pull,
        };

        if let Ok(page) = pull {
            cursor = page.next_cursor;
            for (storage_key, event) in page.events {
                if !partition_still_held(&stop, &heartbeat, stop_after) {
                    return;
                }
                let execution = execute_handler(
                    &handler,
                    &storage_key,
                    event.clone(),
                    config.handler_timeout,
                )
                .await;
                if let Some(outcome) =
                    handler_outcome_or_log(execution, &event, config.handler_timeout)
                {
                    if apply_handler_outcome(&queue, &storage_key, &event, outcome).await {
                        finished_in_pass = true;
                    }
                }
            }
            if cursor.is_some() || std::mem::take(&mut finished_in_pass) {
                continue;
            }
        }

        tokio::select! {
            biased;
            _ = stop.changed() => return,
            _ = sleep(config.poll_interval) => {}
        }
    }
}

fn partition_still_held(
    stop: &watch::Receiver<bool>,
    heartbeat: &AtomicI64,
    stop_after: i64,
) -> bool {
    !*stop.borrow()
        && Utc::now()
            .timestamp_millis()
            .saturating_sub(heartbeat.load(Ordering::Acquire))
            < stop_after
}

fn shutdown_requested(rx: &mut broadcast::Receiver<()>) -> bool {
    matches!(
        rx.try_recv(),
        Ok(_) | Err(broadcast::error::TryRecvError::Closed)
    )
}
