// Copyright 2019 TiKV Project Authors. Licensed under Apache-2.0.

use std::collections::BTreeSet;

use futures::future::join_all;
use serde_derive::Deserialize;
use tokio::time::{timeout, timeout_at};

use super::*;
use crate::TimestampExt;

#[derive(Debug, Deserialize, PartialEq, Eq)]
struct NativeMember {
    name: String,
    member_id: u64,
    client_urls: Vec<String>,
}

#[derive(Deserialize)]
struct NativeHeader {
    cluster_id: u64,
}

#[derive(Deserialize)]
struct NativeMembership {
    header: NativeHeader,
    members: Vec<NativeMember>,
    leader: NativeMember,
    etcd_leader: NativeMember,
}

fn loopback_endpoint(address: &str) -> reqwest::Url {
    let address = if address.starts_with("http://") {
        address.to_owned()
    } else {
        format!("http://{address}")
    };
    let endpoint = reqwest::Url::parse(&address).unwrap();
    assert_eq!(endpoint.scheme(), "http");
    assert_eq!(endpoint.host_str(), Some("127.0.0.1"));
    assert!(endpoint.port().is_some_and(|port| port != 0));
    assert!(endpoint.username().is_empty() && endpoint.password().is_none());
    assert_eq!(endpoint.path(), "/");
    assert!(endpoint.query().is_none() && endpoint.fragment().is_none());
    endpoint
}

fn owned_pd_endpoints() -> (Vec<String>, String) {
    let declared = std::env::var("PD_ADDRS")
        .expect("native TSO recovery requires three explicitly owned disposable PD endpoints");
    let endpoints = declared
        .split(',')
        .map(str::trim)
        .map(loopback_endpoint)
        .map(|endpoint| endpoint.to_string())
        .collect::<Vec<_>>();
    assert_eq!(endpoints.len(), 3);
    assert_eq!(endpoints.iter().collect::<BTreeSet<_>>().len(), 3);
    let run_id = std::env::var("TITO_NATIVE_RUN_ID")
        .expect("native TSO recovery requires the disposable run identity");
    assert!(
        run_id.starts_with("run-")
            && run_id.len() > 4
            && run_id.len() <= 64
            && run_id
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
    );
    (endpoints, run_id)
}

async fn membership(control: &reqwest::Client, endpoint: &str) -> NativeMembership {
    control
        .get(
            loopback_endpoint(endpoint)
                .join("/pd/api/v1/members")
                .unwrap(),
        )
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

fn assert_owned_membership(membership: &NativeMembership, endpoints: &[String], run_id: &str) {
    assert!(membership.header.cluster_id > 0);
    assert_eq!(membership.members.len(), 3);
    assert_eq!(
        membership
            .members
            .iter()
            .map(|member| member.name.clone())
            .collect::<BTreeSet<_>>(),
        (1..=3)
            .map(|index| format!("{run_id}-pd{index}"))
            .collect::<BTreeSet<_>>()
    );
    assert!(membership.members.iter().all(|member| member.member_id > 0));
    assert_eq!(
        membership
            .members
            .iter()
            .map(|member| member.member_id)
            .collect::<BTreeSet<_>>()
            .len(),
        3
    );
    assert!(membership
        .members
        .iter()
        .all(|member| member.client_urls.len() == 1));
    assert_eq!(
        membership
            .members
            .iter()
            .map(|member| loopback_endpoint(&member.client_urls[0]).to_string())
            .collect::<BTreeSet<_>>(),
        endpoints.iter().cloned().collect::<BTreeSet<_>>()
    );
    assert!(membership.members.contains(&membership.leader));
    assert!(membership.members.contains(&membership.etcd_leader));
}

async fn timestamp(client: &Arc<RetryClient>) -> u64 {
    timeout(Duration::from_secs(10), client.clone().get_timestamp())
        .await
        .unwrap()
        .unwrap()
        .version()
}

#[tokio::test]
#[serial_test::serial]
async fn actual_pd_leadership_transfer_recovers_the_same_closed_oracle_client_with_monotonic_timestamps(
) {
    let (endpoints, run_id) = owned_pd_endpoints();
    let client = Arc::new(
        timeout(
            Duration::from_secs(10),
            RetryClient::connect(
                &endpoints,
                Arc::new(SecurityManager::default()),
                Duration::from_secs(5),
            ),
        )
        .await
        .unwrap()
        .unwrap(),
    );
    let first = timestamp(&client).await;
    let idle_generation = client.cluster.read().await.1;
    sleep(Duration::from_millis(1_100)).await;
    let after_idle = timestamp(&client).await;
    assert!(after_idle > first);
    assert_eq!(client.cluster.read().await.1, idle_generation);

    let control = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(3))
        .build()
        .unwrap();
    let before = membership(&control, &endpoints[0]).await;
    assert_owned_membership(&before, &endpoints, &run_id);
    assert_eq!(before.leader, before.etcd_leader);
    let target = before
        .members
        .iter()
        .filter(|member| member.member_id != before.leader.member_id)
        .min_by(|left, right| left.name.cmp(&right.name))
        .unwrap();
    let target_membership = membership(&control, &target.client_urls[0]).await;
    assert_owned_membership(&target_membership, &endpoints, &run_id);
    assert_eq!(
        target_membership.header.cluster_id,
        before.header.cluster_id
    );
    assert_eq!(target_membership.leader, before.leader);
    assert_eq!(target_membership.etcd_leader, before.etcd_leader);
    let transfer = control
        .post(
            loopback_endpoint(&before.leader.client_urls[0])
                .join(&format!("/pd/api/v1/leader/transfer/{}", target.name))
                .unwrap(),
        )
        .build()
        .unwrap();

    timeout(
        Duration::from_secs(10),
        client.reconnect(0, idle_generation),
    )
    .await
    .unwrap()
    .unwrap();
    let generation = client.cluster.read().await.1;
    assert_ne!(generation, idle_generation);
    let mut last_before_recovery = timestamp(&client).await;
    assert!(last_before_recovery > after_idle);
    control
        .execute(transfer)
        .await
        .unwrap()
        .error_for_status()
        .unwrap();

    timeout_at(
        tokio::time::Instant::from_std(generation + Duration::from_secs(RECONNECT_INTERVAL_SEC)),
        async {
            loop {
                let outcome = {
                    let connection = client.cluster.read().await;
                    assert_eq!(connection.1, generation);
                    connection.0.get_timestamp().await
                };
                match outcome {
                    Ok(timestamp) => {
                        let version = timestamp.version();
                        assert!(version > last_before_recovery);
                        last_before_recovery = version;
                    }
                    Err(Error::InternalError { message })
                        if message.contains("TimestampRequest channel is closed") =>
                    {
                        break;
                    }
                    Err(Error::Channel(_)) => {}
                    Err(error) => panic!("unexpected native TSO termination: {error}"),
                }
                tokio::task::yield_now().await;
            }
        },
    )
    .await
    .expect("actual leadership transfer must close this worker inside its unchanged throttle");
    assert_eq!(client.cluster.read().await.1, generation);
    let results = timeout(Duration::from_secs(20), async {
        assert!(generation.elapsed() < Duration::from_secs(RECONNECT_INTERVAL_SEC));
        join_all((0..256).map(|_| client.clone().get_timestamp())).await
    })
    .await
    .unwrap();
    let versions = results
        .into_iter()
        .map(|result| result.unwrap().version())
        .collect::<BTreeSet<_>>();
    assert_eq!(versions.len(), 256);
    assert!(*versions.first().unwrap() > last_before_recovery);
    assert_ne!(client.cluster.read().await.1, generation);
    assert!(timestamp(&client).await > *versions.last().unwrap());

    let after = membership(&control, &target.client_urls[0]).await;
    assert_owned_membership(&after, &endpoints, &run_id);
    assert_eq!(after.header.cluster_id, before.header.cluster_id);
    assert_eq!(after.leader, *target);
    assert_eq!(after.etcd_leader, *target);
}
