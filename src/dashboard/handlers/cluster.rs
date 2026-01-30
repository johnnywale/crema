//! Cluster status handlers.

use axum::{extract::State, Json};

use crate::dashboard::handlers::AppState;
use crate::dashboard::types::{ClusterStatusResponse, HealthResponse, NodeInfo, NodesResponse};

/// GET /api/cluster/status
pub async fn get_cluster_status(State(cache): State<AppState>) -> Json<ClusterStatusResponse> {
    let status = cache.cluster_status();

    Json(ClusterStatusResponse {
        node_id: status.node_id,
        leader_id: status.leader_id,
        is_leader: status.is_leader,
        term: status.term,
        raft_peer_count: status.raft_peer_count,
        discovered_node_count: status.discovered_node_count,
        memberlist_node_count: status.memberlist_node_count,
        commit_index: status.commit_index,
        applied_index: status.applied_index,
    })
}

/// GET /api/cluster/nodes
pub async fn get_cluster_nodes(State(cache): State<AppState>) -> Json<NodesResponse> {
    let all_members = cache.discovery_members();
    let healthy_members = cache.discovery_healthy_members();

    let nodes: Vec<NodeInfo> = all_members
        .iter()
        .map(|&node_id| NodeInfo {
            node_id,
            is_healthy: healthy_members.contains(&node_id),
        })
        .collect();

    Json(NodesResponse { nodes })
}

/// GET /api/health
pub async fn health_check(State(cache): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok".to_string(),
        node_id: cache.node_id(),
        is_leader: cache.is_leader(),
    })
}
