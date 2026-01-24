use crate::{CacheConfig, DistributedCache, NodeId, RaftConfig};
use std::collections::HashSet;
use std::net::{SocketAddr, UdpSocket};
use std::time::{Duration, Instant};
use tokio::net::TcpListener;
use tokio::time::sleep;

/// Allocate OS-assigned ports by briefly binding to port 0.
/// Returns a vector of (NodeId, port) pairs.
pub(crate) async fn allocate_os_ports(node_ids: &[NodeId]) -> Vec<(NodeId, u16)> {
    let mut results = Vec::with_capacity(node_ids.len());
    for &node_id in node_ids {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener); // Release the port immediately
        results.push((node_id, port));
    }
    results
}

/// Allocate OS-assigned ports for both raft (TCP) and memberlist (UDP).
/// Returns a vector of (NodeId, raft_port, memberlist_port) tuples.
///
/// On Windows, TCP and UDP ports are in separate namespaces, and certain
/// port ranges may be excluded. This function allocates both ports from
/// the OS to ensure they are available.
pub(crate) async fn allocate_os_ports_with_memberlist(node_ids: &[NodeId]) -> Vec<(NodeId, u16, u16)> {
    let mut results = Vec::with_capacity(node_ids.len());
    for &node_id in node_ids {
        // Allocate raft port (TCP)
        let raft_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let raft_port = raft_listener.local_addr().unwrap().port();
        drop(raft_listener);

        // Allocate memberlist port (UDP) - memberlist uses UDP for gossip
        let memberlist_socket = UdpSocket::bind("127.0.0.1:0").unwrap();
        let memberlist_port = memberlist_socket.local_addr().unwrap().port();
        drop(memberlist_socket);

        results.push((node_id, raft_port, memberlist_port));
    }
    results
}
pub async fn wait_for_result<F, Fut, T, P>(
    mut action: F,
    predicate: P,
    timeout: Duration,
) -> Option<T>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = T>,
    P: Fn(&T) -> bool,
{
    let start = Instant::now();
    let interval = Duration::from_millis(100); // 每次重试间隔 100ms

    while start.elapsed() < timeout {
        let result = action().await;
        if predicate(&result) {
            return Some(result);
        }
        tokio::time::sleep(interval).await;
    }
    None // 超时未达成条件
}
/// Create a cache config for a node in a multi-node cluster using pre-allocated ports.
pub(crate) fn cluster_node_config(node_id: NodeId, port_configs: &[(NodeId, u16)]) -> CacheConfig {
    let my_port = port_configs
        .iter()
        .find(|(id, _)| *id == node_id)
        .map(|(_, port)| *port)
        .expect("node_id must exist in port_configs");

    let raft_addr: SocketAddr = format!("127.0.0.1:{}", my_port).parse().unwrap();

    let seed_nodes: Vec<(NodeId, SocketAddr)> = port_configs
        .iter()
        .filter(|(id, _)| *id != node_id)
        .map(|(id, port)| {
            let addr: SocketAddr = format!("127.0.0.1:{}", port).parse().unwrap();
            (*id, addr)
        })
        .collect();

    let base_election_tick = 10;

    let raft_config = RaftConfig {
        tick_interval_ms: 100,
        election_tick: base_election_tick + (node_id as usize * 10),
        heartbeat_tick: 2,
        max_size_per_msg: 1024 * 1024,
        max_inflight_msgs: 256,
        pre_vote: true,
        applied: 0,
        storage_type: crate::config::RaftStorageType::Memory,
    };

    CacheConfig::new(node_id, raft_addr)
        .with_seed_nodes(seed_nodes)
        .with_max_capacity(10_000)
        .with_default_ttl(Duration::from_secs(3600))
        .with_raft_config(raft_config)
}

/// Wait for a condition with timeout
pub(crate) async fn wait_for<F>(condition: F, timeout: Duration, check_interval: Duration) -> bool
where
    F: Fn() -> bool,
{
    let start = Instant::now();
    while start.elapsed() < timeout {
        if condition() {
            return true;
        }
        sleep(check_interval).await;
    }
    false
}

/// Wait for exactly one leader in the cluster
pub(crate) async fn wait_for_single_leader(
    caches: &[&DistributedCache],
    timeout: Duration,
) -> Option<NodeId> {
    let start = Instant::now();
    while start.elapsed() < timeout {
        let leaders: Vec<NodeId> = caches
            .iter()
            .filter(|c| c.is_leader())
            .map(|c| c.node_id())
            .collect();

        if leaders.len() == 1 {
            return Some(leaders[0]);
        }

        sleep(Duration::from_millis(50)).await;
    }
    None
}

/// Helper function to verify all nodes agree on leader
pub(crate) fn verify_leader_agreement(caches: &[&DistributedCache]) -> Option<NodeId> {
    let leader_ids: HashSet<Option<NodeId>> = caches.iter().map(|c| c.leader_id()).collect();

    if leader_ids.len() == 1 {
        *leader_ids.iter().next().unwrap()
    } else {
        None
    }
}
