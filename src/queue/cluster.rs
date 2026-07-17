use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use chrono::Utc;
use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, watch};
use tokio::time::sleep;

use super::{Queue, QueueEvent};
use crate::key_encoder::safe_encode;
use crate::types::{TitoEngine, TitoTransaction, PARTITION_DIGITS};
use crate::TitoError;

#[derive(Debug, Clone)]
pub struct ClusterWorkerConfig {
    pub node_id: String,
    pub heartbeat_interval: Duration,
    pub poll_interval: Duration,
    pub lease_ttl: Duration,
    pub stale_node_ttl: Duration,
}

impl ClusterWorkerConfig {
    pub fn new(node_id: impl Into<String>) -> Self {
        Self {
            node_id: node_id.into(),
            heartbeat_interval: Duration::from_secs(5),
            poll_interval: Duration::from_millis(500),
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
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterWorkerNode {
    pub node_id: String,
    pub heartbeat_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterCoordinatorLease {
    pub owner_node_id: String,
    pub lease_until: i64,
    pub updated_at: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ClusterPartitionAssignment {
    pub partition: u32,
    pub desired_node_id: Option<String>,
    pub owner_node_id: Option<String>,
    pub lease_until: i64,
    pub generation: u64,
    pub updated_at: i64,
}

impl ClusterPartitionAssignment {
    fn new(partition: u32) -> Self {
        let now = Utc::now().timestamp();
        Self {
            partition,
            desired_node_id: None,
            owner_node_id: None,
            lease_until: 0,
            generation: 0,
            updated_at: now,
        }
    }

    fn owner_is_active(&self, active_nodes: &BTreeSet<String>, now: i64) -> bool {
        self.owner_node_id
            .as_ref()
            .is_some_and(|owner| active_nodes.contains(owner) && self.lease_until > now)
    }
}

impl<E: TitoEngine> Queue<E> {
    fn cluster_prefix() -> &'static str {
        "tito:queue:cluster:"
    }

    fn coordinator_key() -> String {
        format!("{}coordinator", Self::cluster_prefix())
    }

    fn nodes_prefix() -> String {
        format!("{}nodes:", Self::cluster_prefix())
    }

    fn node_key(node_id: &str) -> String {
        format!("{}{}", Self::nodes_prefix(), safe_encode(node_id))
    }

    fn partitions_prefix() -> String {
        format!("{}partitions:", Self::cluster_prefix())
    }

    fn partition_key(partition: u32) -> String {
        format!(
            "{}{:0pwidth$}",
            Self::partitions_prefix(),
            partition,
            pwidth = PARTITION_DIGITS,
        )
    }

    async fn read_json<T: DeserializeOwned>(
        tx: &E::Transaction,
        key: &str,
    ) -> Result<Option<T>, TitoError> {
        let Some(bytes) = tx
            .get(key.as_bytes())
            .await
            .map_err(|e| TitoError::QueryFailed(format!("Get cluster queue key: {}", e)))?
        else {
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

    pub async fn heartbeat_cluster_worker(
        &self,
        config: &ClusterWorkerConfig,
    ) -> Result<(), TitoError> {
        let config = config.clone();
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    let node = ClusterWorkerNode {
                        node_id: config.node_id.clone(),
                        heartbeat_at: Utc::now().timestamp(),
                    };
                    Self::put_json(&tx, &Self::node_key(&config.node_id), &node).await
                }
            })
            .await
    }

    pub async fn try_acquire_cluster_coordinator(
        &self,
        config: &ClusterWorkerConfig,
    ) -> Result<bool, TitoError> {
        let config = config.clone();
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    let now = Utc::now().timestamp();
                    let key = Self::coordinator_key();
                    let lease = Self::read_json::<ClusterCoordinatorLease>(&tx, &key).await?;
                    let can_claim = match lease {
                        None => true,
                        Some(ref lease) if lease.owner_node_id == config.node_id => true,
                        Some(ref lease) if lease.lease_until <= now => true,
                        Some(_) => false,
                    };

                    if !can_claim {
                        return Ok(false);
                    }

                    let lease = ClusterCoordinatorLease {
                        owner_node_id: config.node_id.clone(),
                        lease_until: now + config.lease_ttl.as_secs() as i64,
                        updated_at: now,
                    };
                    Self::put_json(&tx, &key, &lease).await?;
                    Ok(true)
                }
            })
            .await
    }

    pub async fn active_cluster_workers(
        &self,
        config: &ClusterWorkerConfig,
    ) -> Result<Vec<ClusterWorkerNode>, TitoError> {
        let config = config.clone();
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    let now = Utc::now().timestamp();
                    let stale_before = now - config.lease_ttl.as_secs() as i64;
                    let prefix = Self::nodes_prefix();
                    let entries = tx
                        .scan(
                            prefix.as_bytes()..Self::prefix_end(&prefix).as_slice(),
                            u32::MAX,
                        )
                        .await
                        .map_err(|e| {
                            TitoError::QueryFailed(format!("Scan cluster queue nodes: {}", e))
                        })?;

                    let mut nodes = Vec::new();
                    for (_, value) in entries {
                        let node = serde_json::from_slice::<ClusterWorkerNode>(&value)
                            .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;
                        if node.heartbeat_at >= stale_before {
                            nodes.push(node);
                        }
                    }
                    nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
                    Ok(nodes)
                }
            })
            .await
    }

    pub async fn rebalance_cluster_partitions(
        &self,
        config: &ClusterWorkerConfig,
    ) -> Result<Vec<ClusterPartitionAssignment>, TitoError> {
        let config = config.clone();
        let partition_count = self.config.partition_count.max(1);
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    let now = Utc::now().timestamp();
                    let nodes = {
                        let stale_before = now - config.lease_ttl.as_secs() as i64;
                        let remove_before = now - config.stale_node_ttl.as_secs() as i64;
                        let prefix = Self::nodes_prefix();
                        let entries = tx
                            .scan(
                                prefix.as_bytes()..Self::prefix_end(&prefix).as_slice(),
                                u32::MAX,
                            )
                            .await
                            .map_err(|e| {
                                TitoError::QueryFailed(format!("Scan cluster queue nodes: {}", e))
                            })?;

                        let mut nodes = Vec::new();
                        for (key, value) in entries {
                            let node = serde_json::from_slice::<ClusterWorkerNode>(&value)
                                .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;
                            if node.heartbeat_at >= stale_before {
                                nodes.push(node);
                            } else if node.heartbeat_at < remove_before {
                                tx.delete(key).await.map_err(|e| {
                                    TitoError::DeleteFailed(format!(
                                        "Delete stale cluster queue node: {}",
                                        e
                                    ))
                                })?;
                            }
                        }
                        nodes.sort_by(|a, b| a.node_id.cmp(&b.node_id));
                        nodes
                    };

                    if nodes.is_empty() {
                        return Ok(Vec::new());
                    }

                    let active_nodes = nodes
                        .iter()
                        .map(|node| node.node_id.clone())
                        .collect::<BTreeSet<_>>();
                    let desired = desired_partition_owners(partition_count, &nodes);
                    let mut assignments = Vec::new();

                    for partition in 0..partition_count {
                        let key = Self::partition_key(partition);
                        let mut assignment =
                            Self::read_json::<ClusterPartitionAssignment>(&tx, &key)
                                .await?
                                .unwrap_or_else(|| ClusterPartitionAssignment::new(partition));

                        let next_desired = desired.get(&partition).cloned();
                        let desired_changed = assignment.desired_node_id != next_desired;
                        assignment.desired_node_id = next_desired;

                        if !assignment.owner_is_active(&active_nodes, now) {
                            assignment.owner_node_id = None;
                            assignment.lease_until = 0;
                        }

                        if desired_changed {
                            assignment.generation = assignment.generation.saturating_add(1);
                        }

                        assignment.updated_at = now;
                        Self::put_json(&tx, &key, &assignment).await?;
                        assignments.push(assignment);
                    }

                    Ok(assignments)
                }
            })
            .await
    }

    pub async fn sync_cluster_partition_leases(
        &self,
        config: &ClusterWorkerConfig,
    ) -> Result<Vec<ClusterPartitionAssignment>, TitoError> {
        let config = config.clone();
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    let now = Utc::now().timestamp();
                    let prefix = Self::partitions_prefix();
                    let entries = tx
                        .scan(
                            prefix.as_bytes()..Self::prefix_end(&prefix).as_slice(),
                            u32::MAX,
                        )
                        .await
                        .map_err(|e| {
                            TitoError::QueryFailed(format!("Scan cluster queue partitions: {}", e))
                        })?;

                    let mut owned = Vec::new();
                    for (key_bytes, value) in entries {
                        let key = String::from_utf8(key_bytes).map_err(|_| {
                            TitoError::DeserializationFailed(
                                "Invalid cluster queue partition key".to_string(),
                            )
                        })?;
                        let mut assignment =
                            serde_json::from_slice::<ClusterPartitionAssignment>(&value)
                                .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;

                        let desired_is_me =
                            assignment.desired_node_id.as_ref() == Some(&config.node_id);
                        let owner_is_me =
                            assignment.owner_node_id.as_ref() == Some(&config.node_id);
                        let owner_expired = assignment.lease_until <= now;

                        if owner_is_me && !desired_is_me {
                            assignment.owner_node_id = None;
                            assignment.lease_until = 0;
                            assignment.generation = assignment.generation.saturating_add(1);
                            assignment.updated_at = now;
                            Self::put_json(&tx, &key, &assignment).await?;
                            continue;
                        }

                        if desired_is_me
                            && (owner_is_me || assignment.owner_node_id.is_none() || owner_expired)
                        {
                            if !owner_is_me {
                                assignment.generation = assignment.generation.saturating_add(1);
                            }
                            assignment.owner_node_id = Some(config.node_id.clone());
                            assignment.lease_until = now + config.lease_ttl.as_secs() as i64;
                            assignment.updated_at = now;
                            Self::put_json(&tx, &key, &assignment).await?;
                            owned.push(assignment);
                        }
                    }

                    owned.sort_by_key(|assignment| assignment.partition);
                    Ok(owned)
                }
            })
            .await
    }

    pub async fn owned_cluster_partitions(
        &self,
        config: &ClusterWorkerConfig,
    ) -> Result<Vec<u32>, TitoError> {
        Ok(self
            .owned_cluster_partition_assignments(config)
            .await?
            .into_iter()
            .map(|assignment| assignment.partition)
            .collect())
    }

    pub async fn owned_cluster_partition_assignments(
        &self,
        config: &ClusterWorkerConfig,
    ) -> Result<Vec<ClusterPartitionAssignment>, TitoError> {
        let config = config.clone();
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    let now = Utc::now().timestamp();
                    let prefix = Self::partitions_prefix();
                    let entries = tx
                        .scan(
                            prefix.as_bytes()..Self::prefix_end(&prefix).as_slice(),
                            u32::MAX,
                        )
                        .await
                        .map_err(|e| {
                            TitoError::QueryFailed(format!("Scan cluster queue partitions: {}", e))
                        })?;

                    let mut assignments = Vec::new();
                    for (_, value) in entries {
                        let assignment =
                            serde_json::from_slice::<ClusterPartitionAssignment>(&value)
                                .map_err(|e| TitoError::DeserializationFailed(e.to_string()))?;
                        if assignment.owner_node_id.as_ref() == Some(&config.node_id)
                            && assignment.desired_node_id.as_ref() == Some(&config.node_id)
                            && assignment.lease_until > now
                        {
                            assignments.push(assignment);
                        }
                    }
                    assignments.sort_by_key(|assignment| assignment.partition);
                    Ok(assignments)
                }
            })
            .await
    }

    async fn cluster_partition_lease_is_current(
        &self,
        config: &ClusterWorkerConfig,
        partition: u32,
        generation: u64,
    ) -> Result<bool, TitoError> {
        let config = config.clone();
        self.engine
            .transaction(|tx| {
                let config = config.clone();
                async move {
                    let now = Utc::now().timestamp();
                    let key = Self::partition_key(partition);
                    let Some(assignment) =
                        Self::read_json::<ClusterPartitionAssignment>(&tx, &key).await?
                    else {
                        return Ok(false);
                    };

                    Ok(assignment.generation == generation
                        && assignment.owner_node_id.as_ref() == Some(&config.node_id)
                        && assignment.desired_node_id.as_ref() == Some(&config.node_id)
                        && assignment.lease_until > now)
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
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, Result<(), TitoError>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    tokio::spawn(async move {
        let mut rx = shutdown.resubscribe();
        let mut partition_workers = BTreeMap::<u32, PartitionWorker>::new();
        let mut retired_partition_workers = Vec::<tokio::task::JoinHandle<()>>::new();
        let maintenance_queue = queue.clone();
        let maintenance_config = config.clone();
        let maintenance_shutdown = shutdown.resubscribe();
        let maintenance_handle = tokio::spawn(async move {
            maintain_cluster_worker(maintenance_queue, maintenance_config, maintenance_shutdown)
                .await;
        });

        loop {
            if shutdown_requested(&mut rx) {
                break;
            }

            reconcile_partition_workers(
                queue.clone(),
                config.clone(),
                handler.clone(),
                &mut partition_workers,
                &mut retired_partition_workers,
            )
            .await;
            reap_partition_workers(&mut retired_partition_workers).await;

            tokio::select! {
                _ = rx.recv() => break,
                _ = sleep(config.poll_interval) => {}
            }
        }

        stop_partition_workers(partition_workers, retired_partition_workers).await;
        let _ = maintenance_handle.await;
    })
}

struct PartitionWorker {
    generation: u64,
    stop: watch::Sender<bool>,
    handle: tokio::task::JoinHandle<()>,
}

async fn reconcile_partition_workers<E, T, H>(
    queue: Arc<Queue<E>>,
    config: ClusterWorkerConfig,
    handler: H,
    partition_workers: &mut BTreeMap<u32, PartitionWorker>,
    retired_partition_workers: &mut Vec<tokio::task::JoinHandle<()>>,
) where
    E: TitoEngine + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, Result<(), TitoError>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    let owned = match queue.owned_cluster_partition_assignments(&config).await {
        Ok(assignments) => assignments
            .into_iter()
            .map(|assignment| (assignment.partition, assignment))
            .collect::<BTreeMap<_, _>>(),
        Err(_) => return,
    };

    let stale_partitions = partition_workers
        .iter()
        .filter_map(|(partition, worker)| match owned.get(partition) {
            Some(assignment) if assignment.generation == worker.generation => None,
            _ => Some(*partition),
        })
        .collect::<Vec<_>>();

    for partition in stale_partitions {
        if let Some(worker) = partition_workers.remove(&partition) {
            let _ = worker.stop.send(true);
            retired_partition_workers.push(worker.handle);
        }
    }

    for assignment in owned.into_values() {
        if partition_workers.contains_key(&assignment.partition) {
            continue;
        }

        let (stop, stop_rx) = watch::channel(false);
        let q = queue.clone();
        let worker_config = config.clone();
        let h = handler.clone();
        let partition = assignment.partition;
        let generation = assignment.generation;
        let handle = tokio::spawn(async move {
            run_partition_worker(q, worker_config, partition, generation, h, stop_rx).await;
        });

        partition_workers.insert(
            partition,
            PartitionWorker {
                generation,
                stop,
                handle,
            },
        );
    }
}

async fn reap_partition_workers(partition_workers: &mut Vec<tokio::task::JoinHandle<()>>) {
    let mut index = 0;
    while index < partition_workers.len() {
        if partition_workers[index].is_finished() {
            let handle = partition_workers.swap_remove(index);
            let _ = handle.await;
        } else {
            index += 1;
        }
    }
}

async fn stop_partition_workers(
    partition_workers: BTreeMap<u32, PartitionWorker>,
    retired_partition_workers: Vec<tokio::task::JoinHandle<()>>,
) {
    for worker in partition_workers.values() {
        let _ = worker.stop.send(true);
    }

    for (_, worker) in partition_workers {
        let _ = worker.handle.await;
    }

    for handle in retired_partition_workers {
        let _ = handle.await;
    }
}

async fn run_partition_worker<E, T, H>(
    queue: Arc<Queue<E>>,
    config: ClusterWorkerConfig,
    partition: u32,
    generation: u64,
    handler: H,
    mut stop: watch::Receiver<bool>,
) where
    E: TitoEngine + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, Result<(), TitoError>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    loop {
        if *stop.borrow() {
            break;
        }

        let processed = process_partition_once(
            queue.clone(),
            config.clone(),
            partition,
            generation,
            handler.clone(),
        )
        .await;

        if !processed {
            tokio::select! {
                _ = stop.changed() => break,
                _ = sleep(config.poll_interval) => {}
            }
        }
    }
}

async fn maintain_cluster_worker<E>(
    queue: Arc<Queue<E>>,
    config: ClusterWorkerConfig,
    shutdown: broadcast::Receiver<()>,
) where
    E: TitoEngine + 'static,
{
    let mut rx = shutdown.resubscribe();

    loop {
        if shutdown_requested(&mut rx) {
            break;
        }

        if queue.heartbeat_cluster_worker(&config).await.is_ok() {
            if matches!(
                queue.try_acquire_cluster_coordinator(&config).await,
                Ok(true)
            ) {
                let _ = queue.rebalance_cluster_partitions(&config).await;
            }
            let _ = queue.sync_cluster_partition_leases(&config).await;
        }

        tokio::select! {
            _ = rx.recv() => break,
            _ = sleep(config.heartbeat_interval) => {}
        }
    }
}

fn shutdown_requested(rx: &mut broadcast::Receiver<()>) -> bool {
    matches!(
        rx.try_recv(),
        Ok(_) | Err(broadcast::error::TryRecvError::Closed)
    )
}

async fn process_partition_once<E, T, H>(
    queue: Arc<Queue<E>>,
    config: ClusterWorkerConfig,
    partition: u32,
    generation: u64,
    handler: H,
) -> bool
where
    E: TitoEngine + 'static,
    T: Serialize + DeserializeOwned + Clone + Send + Sync + 'static,
    H: Fn(QueueEvent<T>) -> BoxFuture<'static, Result<(), TitoError>>
        + Clone
        + Send
        + Sync
        + 'static,
{
    match queue.pull::<T>(partition, 50).await {
        Ok(jobs) if jobs.is_empty() => false,
        Ok(jobs) => {
            for (storage_key, event) in jobs {
                if !matches!(
                    queue
                        .cluster_partition_lease_is_current(&config, partition, generation)
                        .await,
                    Ok(true)
                ) {
                    continue;
                }

                match handler(event.clone()).await {
                    Ok(_) => {
                        if matches!(
                            queue
                                .cluster_partition_lease_is_current(&config, partition, generation)
                                .await,
                            Ok(true)
                        ) {
                            if let Err(error) = queue.ack(&storage_key).await {
                                log::error!(
                                    "Failed to acknowledge queue event {} at {}: {}",
                                    event.id,
                                    storage_key,
                                    error
                                );
                            }
                        }
                    }
                    Err(err) => {
                        if matches!(
                            queue
                                .cluster_partition_lease_is_current(&config, partition, generation)
                                .await,
                            Ok(true)
                        ) {
                            queue
                                .retry_after_handler_error(event, &storage_key, err)
                                .await;
                        }
                    }
                }
            }
            true
        }
        Err(_) => {
            sleep(Duration::from_millis(500)).await;
            false
        }
    }
}
