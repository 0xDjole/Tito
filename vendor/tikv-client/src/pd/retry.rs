// Copyright 2020 TiKV Project Authors. Licensed under Apache-2.0.

//! A utility module for managing and retrying PD requests.

use std::fmt;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use async_trait::async_trait;
use tokio::sync::{RwLock, RwLockWriteGuard};
use tokio::time::sleep;

use crate::pd::Cluster;
use crate::pd::Connection;
use crate::proto::keyspacepb;
use crate::proto::metapb;
use crate::proto::pdpb::Timestamp;
use crate::proto::pdpb::{self};
use crate::region::RegionId;
use crate::region::RegionWithLeader;
use crate::region::StoreId;
use crate::stats::pd_stats;
use crate::Error;
use crate::Result;
use crate::SecurityManager;

// FIXME: these numbers and how they are used are all just cargo-culted in, there
// may be more optimal values.
const RECONNECT_INTERVAL_SEC: u64 = 1;
const MAX_REQUEST_COUNT: usize = 5;
const LEADER_CHANGE_RETRY: usize = 10;

#[async_trait]
pub trait RetryClientTrait {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader>;

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader>;

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store>;

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>>;

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp>;

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool>;

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta>;
}

/// Client for communication with a PD cluster. Has the facility to reconnect to the cluster.
pub struct RetryClient<Cl = Cluster> {
    // Tuple is the cluster and the time of the cluster's last reconnect.
    cluster: RwLock<(Cl, Instant)>,
    connection: Connection,
    timeout: Duration,
}

impl<Cl> RetryClient<Cl> {
    async fn lock_for_reconnect(
        &self,
        interval_sec: u64,
        observed_generation: Instant,
    ) -> Option<RwLockWriteGuard<'_, (Cl, Instant)>> {
        loop {
            let lock = self.cluster.write().await;
            let last_connected = lock.1;
            if last_connected != observed_generation {
                return None;
            }
            let remaining = (last_connected + Duration::from_secs(interval_sec))
                .saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Some(lock);
            }
            drop(lock);
            sleep(remaining).await;
        }
    }
}

#[cfg(test)]
impl<Cl> RetryClient<Cl> {
    pub fn new_with_cluster(
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
        cluster: Cl,
    ) -> RetryClient<Cl> {
        let connection = Connection::new(security_mgr);
        RetryClient {
            cluster: RwLock::new((cluster, Instant::now())),
            connection,
            timeout,
        }
    }
}

macro_rules! retry_core {
    ($self: ident, $tag: literal, $call: expr) => {{
        let stats = pd_stats($tag);
        let mut last_err = Ok(());
        for _ in 0..LEADER_CHANGE_RETRY {
            let (res, observed_generation) = $call;

            match stats.done(res) {
                Ok(r) => return Ok(r),
                Err(e) => last_err = Err(e),
            }

            let mut reconnect_count = MAX_REQUEST_COUNT;
            while let Err(e) = $self
                .reconnect(RECONNECT_INTERVAL_SEC, observed_generation)
                .await
            {
                reconnect_count -= 1;
                if reconnect_count == 0 {
                    return Err(e);
                }
                sleep(Duration::from_secs(RECONNECT_INTERVAL_SEC)).await;
            }
        }

        last_err?;
        unreachable!();
    }};
}

macro_rules! retry_mut {
    ($self: ident, $tag: literal, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $tag, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let mut connection = $self.cluster.write().await;
            let observed_generation = connection.1;
            let $cluster = &mut connection.0;
            ($call.await, observed_generation)
        })
    }};
}

macro_rules! retry {
    ($self: ident, $tag: literal, |$cluster: ident| $call: expr) => {{
        retry_core!($self, $tag, {
            // use the block here to drop the guard of the lock,
            // otherwise `reconnect` will try to acquire the write lock and results in a deadlock
            let connection = $self.cluster.read().await;
            let observed_generation = connection.1;
            let $cluster = &connection.0;
            ($call.await, observed_generation)
        })
    }};
}

impl RetryClient<Cluster> {
    pub async fn connect(
        endpoints: &[String],
        security_mgr: Arc<SecurityManager>,
        timeout: Duration,
    ) -> Result<RetryClient> {
        let connection = Connection::new(security_mgr);
        let cluster = RwLock::new((
            connection.connect_cluster(endpoints, timeout).await?,
            Instant::now(),
        ));
        Ok(RetryClient {
            cluster,
            connection,
            timeout,
        })
    }
}

#[async_trait]
impl RetryClientTrait for RetryClient<Cluster> {
    // These get_* functions will try multiple times to make a request, reconnecting as necessary.
    // It does not know about encoding. Caller should take care of it.
    async fn get_region(self: Arc<Self>, key: Vec<u8>) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_region", |cluster| {
            let key = key.clone();
            async {
                cluster
                    .get_region(key.clone(), self.timeout)
                    .await
                    .and_then(|resp| {
                        region_from_response(resp, || Error::RegionForKeyNotFound { key })
                    })
            }
        })
    }

    async fn get_region_by_id(self: Arc<Self>, region_id: RegionId) -> Result<RegionWithLeader> {
        retry_mut!(self, "get_region_by_id", |cluster| async {
            cluster
                .get_region_by_id(region_id, self.timeout)
                .await
                .and_then(|resp| {
                    region_from_response(resp, || Error::RegionNotFoundInResponse { region_id })
                })
        })
    }

    async fn get_store(self: Arc<Self>, id: StoreId) -> Result<metapb::Store> {
        retry_mut!(self, "get_store", |cluster| async {
            cluster
                .get_store(id, self.timeout)
                .await
                .map(|resp| resp.store.unwrap())
        })
    }

    async fn get_all_stores(self: Arc<Self>) -> Result<Vec<metapb::Store>> {
        retry_mut!(self, "get_all_stores", |cluster| async {
            cluster
                .get_all_stores(self.timeout)
                .await
                .map(|resp| resp.stores)
        })
    }

    async fn get_timestamp(self: Arc<Self>) -> Result<Timestamp> {
        retry!(self, "get_timestamp", |cluster| cluster.get_timestamp())
    }

    async fn update_safepoint(self: Arc<Self>, safepoint: u64) -> Result<bool> {
        retry_mut!(self, "update_gc_safepoint", |cluster| async {
            cluster
                .update_safepoint(safepoint, self.timeout)
                .await
                .map(|resp| resp.new_safe_point == safepoint)
        })
    }

    async fn load_keyspace(&self, keyspace: &str) -> Result<keyspacepb::KeyspaceMeta> {
        retry_mut!(self, "load_keyspace", |cluster| async {
            cluster.load_keyspace(keyspace, self.timeout).await
        })
    }
}

impl fmt::Debug for RetryClient {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("pd::RetryClient")
            .field("timeout", &self.timeout)
            .finish()
    }
}

fn region_from_response(
    mut resp: pdpb::GetRegionResponse,
    err: impl FnOnce() -> Error,
) -> Result<RegionWithLeader> {
    let region = resp.region.take().ok_or_else(err)?;
    Ok(RegionWithLeader::new(region, resp.leader.take()))
}

// A node-like thing that can be connected to.
#[async_trait]
trait Reconnect {
    type Cl;
    async fn reconnect(&self, interval_sec: u64, observed_generation: Instant) -> Result<()>;
}

#[async_trait]
impl Reconnect for RetryClient<Cluster> {
    type Cl = Cluster;

    async fn reconnect(&self, interval_sec: u64, observed_generation: Instant) -> Result<()> {
        let Some(mut lock) = self
            .lock_for_reconnect(interval_sec, observed_generation)
            .await
        else {
            return Ok(());
        };
        let (cluster, last_connected) = &mut *lock;
        self.connection.reconnect(cluster, self.timeout).await?;
        *last_connected = Instant::now();
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering;
    use std::sync::Mutex;

    use futures::executor;
    use futures::future::ready;

    use super::*;
    use crate::internal_err;

    #[tokio::test(flavor = "multi_thread")]
    async fn test_reconnect() {
        struct MockClient {
            reconnect_count: AtomicUsize,
            cluster: RwLock<((), Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = ();

            async fn reconnect(&self, _: u64, _: Instant) -> Result<()> {
                self.reconnect_count
                    .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                // Not actually unimplemented, we just don't care about the error.
                Err(Error::Unimplemented)
            }
        }

        async fn retry_err(client: Arc<MockClient>) -> Result<()> {
            retry_mut!(client, "test", |_c| ready(Err(internal_err!("whoops"))))
        }

        async fn retry_ok(client: Arc<MockClient>) -> Result<()> {
            retry!(client, "test", |_c| ready(Ok::<_, Error>(())))
        }

        executor::block_on(async {
            let client = Arc::new(MockClient {
                reconnect_count: AtomicUsize::new(0),
                cluster: RwLock::new(((), Instant::now())),
            });

            assert!(retry_err(client.clone()).await.is_err());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                MAX_REQUEST_COUNT
            );

            client
                .reconnect_count
                .store(0, std::sync::atomic::Ordering::SeqCst);
            assert!(retry_ok(client.clone()).await.is_ok());
            assert_eq!(
                client
                    .reconnect_count
                    .load(std::sync::atomic::Ordering::SeqCst),
                0
            );
        })
    }

    #[test]
    fn test_retry() {
        struct MockClient {
            cluster: RwLock<(AtomicUsize, Instant)>,
        }

        #[async_trait]
        impl Reconnect for MockClient {
            type Cl = Mutex<usize>;

            async fn reconnect(&self, _: u64, _: Instant) -> Result<()> {
                Ok(())
            }
        }

        async fn retry_max_err(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry_mut!(client, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        async fn retry_max_ok(
            client: Arc<MockClient>,
            max_retries: Arc<AtomicUsize>,
        ) -> Result<()> {
            retry!(client, "test", |c| {
                c.fetch_add(1, std::sync::atomic::Ordering::SeqCst);

                let max_retries = max_retries.fetch_sub(1, Ordering::SeqCst) - 1;
                if max_retries == 0 {
                    ready(Ok(()))
                } else {
                    ready(Err(internal_err!("whoops")))
                }
            })
        }

        executor::block_on(async {
            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(1000));

            assert!(retry_max_err(client.clone(), max_retries).await.is_err());
            assert_eq!(
                client.cluster.read().await.0.load(Ordering::SeqCst),
                LEADER_CHANGE_RETRY
            );

            let client = Arc::new(MockClient {
                cluster: RwLock::new((AtomicUsize::new(0), Instant::now())),
            });
            let max_retries = Arc::new(AtomicUsize::new(2));

            assert!(retry_max_ok(client.clone(), max_retries).await.is_ok());
            assert_eq!(client.cluster.read().await.0.load(Ordering::SeqCst), 2);
        })
    }
}

#[cfg(test)]
mod reconnect_regression_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use futures::future::join_all;
    use tokio::sync::{oneshot, Notify};
    use tokio::time::timeout;

    use super::*;
    use crate::internal_err;

    #[derive(Clone, Copy)]
    enum WorkerFailure {
        StreamError,
        StreamEof,
        ClosedRequestChannel,
    }

    impl WorkerFailure {
        async fn result(self) -> Result<()> {
            match self {
                Self::StreamError => Err(tonic::Status::unavailable("PD leader changed").into()),
                Self::StreamEof => {
                    let (sender, receiver) = oneshot::channel::<()>();
                    drop(sender);
                    receiver.await.map_err(Into::into)
                }
                Self::ClosedRequestChannel => {
                    Err(internal_err!("TimestampRequest channel is closed"))
                }
            }
        }
    }

    #[derive(Clone, Copy, PartialEq)]
    enum Recovery {
        NextReconnect,
        EveryRequestFails,
        EveryConnectionFails,
    }

    #[derive(Default)]
    struct Observations {
        requests: AtomicUsize,
        connections: AtomicUsize,
        request_observed: Notify,
    }

    struct ProtocolCluster {
        failure: WorkerFailure,
        recovery: Recovery,
        ready: bool,
        observations: Arc<Observations>,
    }

    impl ProtocolCluster {
        async fn request(&self) -> Result<()> {
            self.observations.requests.fetch_add(1, Ordering::SeqCst);
            self.observations.request_observed.notify_one();
            if self.ready && self.recovery != Recovery::EveryRequestFails {
                return Ok(());
            }
            self.failure.result().await
        }
    }

    impl RetryClient<ProtocolCluster> {
        async fn read(self: Arc<Self>) -> Result<()> {
            retry!(self, "timestamp_recovery_regression", |cluster| cluster
                .request())
        }
    }

    #[async_trait]
    impl Reconnect for RetryClient<ProtocolCluster> {
        type Cl = ProtocolCluster;

        async fn reconnect(&self, interval_sec: u64, observed_generation: Instant) -> Result<()> {
            let Some(mut lock) = self
                .lock_for_reconnect(interval_sec, observed_generation)
                .await
            else {
                return Ok(());
            };
            let (cluster, last_connected) = &mut *lock;
            cluster
                .observations
                .connections
                .fetch_add(1, Ordering::SeqCst);
            if cluster.recovery == Recovery::EveryConnectionFails {
                return Err(Error::Unimplemented);
            }
            cluster.ready = true;
            *last_connected = Instant::now();
            Ok(())
        }
    }

    fn client(
        failure: WorkerFailure,
        recovery: Recovery,
        ready: bool,
    ) -> (Arc<RetryClient<ProtocolCluster>>, Arc<Observations>) {
        let observations = Arc::new(Observations::default());
        let cluster = ProtocolCluster {
            failure,
            recovery,
            ready,
            observations: observations.clone(),
        };
        let client = RetryClient::new_with_cluster(
            Arc::new(SecurityManager::default()),
            Duration::from_secs(5),
            cluster,
        );
        (Arc::new(client), observations)
    }

    async fn assert_recovers_after_throttle(failure: WorkerFailure) {
        let (client, observations) = client(failure, Recovery::NextReconnect, false);
        let generation = client.cluster.read().await.1;
        timeout(Duration::from_secs(5), client.clone().read())
            .await
            .unwrap()
            .unwrap();
        assert!(generation.elapsed() >= Duration::from_secs(RECONNECT_INTERVAL_SEC));
        assert_ne!(client.cluster.read().await.1, generation);
        assert_eq!(observations.requests.load(Ordering::SeqCst), 2);
        assert_eq!(observations.connections.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn a_closed_request_channel_waits_for_replacement_instead_of_exhausting_retries() {
        assert_recovers_after_throttle(WorkerFailure::ClosedRequestChannel).await;
    }

    #[tokio::test]
    async fn a_stream_error_waits_for_replacement_instead_of_exhausting_retries() {
        assert_recovers_after_throttle(WorkerFailure::StreamError).await;
    }

    #[tokio::test]
    async fn a_stream_eof_waits_for_replacement_instead_of_exhausting_retries() {
        assert_recovers_after_throttle(WorkerFailure::StreamEof).await;
    }

    #[tokio::test]
    async fn concurrent_failed_requests_share_one_replacement() {
        let (client, observations) = client(
            WorkerFailure::ClosedRequestChannel,
            Recovery::NextReconnect,
            false,
        );
        let generation = client.cluster.read().await.1;
        let results = timeout(
            Duration::from_secs(5),
            join_all((0..32).map(|_| client.clone().read())),
        )
        .await
        .unwrap();
        assert!(results.into_iter().all(|result| result.is_ok()));
        assert_eq!(observations.connections.load(Ordering::SeqCst), 1);
        assert_ne!(client.cluster.read().await.1, generation);
    }

    #[tokio::test]
    async fn a_throttled_wait_releases_the_lock_and_rechecks_a_concurrent_replacement() {
        let (client, observations) = client(
            WorkerFailure::ClosedRequestChannel,
            Recovery::NextReconnect,
            false,
        );
        let generation = client.cluster.read().await.1;
        let waiting = tokio::spawn(client.clone().read());
        observations.request_observed.notified().await;
        let mut connection = timeout(Duration::from_millis(250), client.cluster.write())
            .await
            .expect("the throttle must not retain the cluster lock");
        assert_eq!(connection.1, generation);
        connection.0.ready = true;
        connection.1 = Instant::now();
        let replacement = connection.1;
        drop(connection);
        timeout(Duration::from_secs(5), waiting)
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(client.cluster.read().await.1, replacement);
        assert_eq!(observations.connections.load(Ordering::SeqCst), 0);
        assert_eq!(observations.requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn healthy_idleness_does_not_replace_the_connection() {
        let (client, observations) = client(
            WorkerFailure::ClosedRequestChannel,
            Recovery::NextReconnect,
            true,
        );
        let generation = client.cluster.read().await.1;
        client.clone().read().await.unwrap();
        sleep(Duration::from_millis(1_100)).await;
        client.clone().read().await.unwrap();
        assert_eq!(client.cluster.read().await.1, generation);
        assert_eq!(observations.connections.load(Ordering::SeqCst), 0);
        assert_eq!(observations.requests.load(Ordering::SeqCst), 2);
    }

    #[tokio::test]
    async fn a_continued_stream_outage_keeps_the_existing_request_budget() {
        let (client, observations) = client(
            WorkerFailure::ClosedRequestChannel,
            Recovery::EveryRequestFails,
            false,
        );
        let error = timeout(Duration::from_secs(20), client.read())
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, Error::InternalError { .. }));
        assert_eq!(
            observations.requests.load(Ordering::SeqCst),
            LEADER_CHANGE_RETRY
        );
        assert_eq!(
            observations.connections.load(Ordering::SeqCst),
            LEADER_CHANGE_RETRY
        );
    }

    #[tokio::test]
    async fn a_continued_connection_outage_keeps_the_existing_connection_budget() {
        let (client, observations) = client(
            WorkerFailure::ClosedRequestChannel,
            Recovery::EveryConnectionFails,
            false,
        );
        let error = timeout(Duration::from_secs(15), client.read())
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(error, Error::Unimplemented));
        assert_eq!(observations.requests.load(Ordering::SeqCst), 1);
        assert_eq!(
            observations.connections.load(Ordering::SeqCst),
            MAX_REQUEST_COUNT
        );
    }
}

#[cfg(all(test, feature = "integration-tests"))]
#[path = "retry_native_tests.rs"]
mod native_recovery_tests;
