use super::*;

#[tokio::test]
async fn cluster_coordinator_lease_blocks_other_nodes_until_expired() {
    let engine = engine();
    let queue = queue(engine.clone(), 2);
    let node_a = cluster_config("node-a");
    let node_b = cluster_config("node-b");

    assert!(queue
        .try_acquire_cluster_coordinator(&node_a)
        .await
        .unwrap());
    assert!(queue
        .try_acquire_cluster_coordinator(&node_a)
        .await
        .unwrap());
    assert!(!queue
        .try_acquire_cluster_coordinator(&node_b)
        .await
        .unwrap());

    let expired = ClusterCoordinatorLease {
        owner_node_id: "node-a".to_string(),
        lease_until: Utc::now().timestamp() - 1,
        updated_at: Utc::now().timestamp() - 2,
    };
    engine
        .put_json("tito:queue:cluster:coordinator", &json!(expired))
        .await;

    assert!(queue
        .try_acquire_cluster_coordinator(&node_b)
        .await
        .unwrap());
}

#[tokio::test]
async fn cluster_rebalance_assigns_and_syncs_partitions_to_active_nodes() {
    let engine = engine();
    let queue = queue(engine, 4);
    let node_a = cluster_config("node-a");
    let node_b = cluster_config("node-b");

    queue.heartbeat_cluster_worker(&node_b).await.unwrap();
    queue.heartbeat_cluster_worker(&node_a).await.unwrap();

    let active = queue.active_cluster_workers(&node_a).await.unwrap();
    assert_eq!(
        active
            .iter()
            .map(|node| node.node_id.as_str())
            .collect::<Vec<_>>(),
        vec!["node-a", "node-b"]
    );

    let assignments = queue.rebalance_cluster_partitions(&node_a).await.unwrap();
    assert_eq!(assignments.len(), 4);
    assert_eq!(assignments[0].desired_node_id.as_deref(), Some("node-a"));
    assert_eq!(assignments[1].desired_node_id.as_deref(), Some("node-a"));
    assert_eq!(assignments[2].desired_node_id.as_deref(), Some("node-b"));
    assert_eq!(assignments[3].desired_node_id.as_deref(), Some("node-b"));

    let owned_a = queue.sync_cluster_partition_leases(&node_a).await.unwrap();
    let owned_b = queue.sync_cluster_partition_leases(&node_b).await.unwrap();
    assert_eq!(
        owned_a
            .iter()
            .map(|assignment| assignment.partition)
            .collect::<Vec<_>>(),
        vec![0, 1]
    );
    assert_eq!(
        owned_b
            .iter()
            .map(|assignment| assignment.partition)
            .collect::<Vec<_>>(),
        vec![2, 3]
    );
    assert_eq!(
        queue.owned_cluster_partitions(&node_a).await.unwrap(),
        vec![0, 1]
    );
}
