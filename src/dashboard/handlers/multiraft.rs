//! Multi-Raft handlers.

use axum::{
    extract::{Path, State},
    Json,
};
use std::collections::HashMap;

use crate::dashboard::handlers::AppState;
use crate::dashboard::types::{
    AddShardResponse, ClusterShardsComparisonResponse, MultiRaftStatsResponse, NodeShardInfo,
    RemoveShardResponse, ShardDifference, ShardInfoResponse, ShardLeadersResponse,
    ShardRoutingResponse, ShardsResponse, SlotRoutingStatusResponse,
};
use crate::multiraft::TOTAL_SLOTS;

/// GET /api/multiraft/shards
pub async fn get_shards(State(cache): State<AppState>) -> Json<ShardsResponse> {
    // Run pending tasks to sync Moka's counters
    cache.run_pending_tasks().await;

    let coordinator = match cache.multiraft_coordinator() {
        Some(c) => c,
        None => {
            return Json(ShardsResponse {
                shards: vec![],
                total_shards: 0,
                total_entries: 0,
            });
        }
    };
    let shard_infos = coordinator.shard_info();

    // Get slot table snapshot for slot info (if slot routing is enabled)
    let slot_snapshot = cache.slot_table();

    // Calculate total entries for percentage
    let total_entries: u64 = shard_infos.iter().map(|s| s.entry_count).sum();

    let shards: Vec<ShardInfoResponse> = shard_infos
        .iter()
        .map(|info| {
            let percentage = if total_entries > 0 {
                (info.entry_count as f64 / total_entries as f64) * 100.0
            } else {
                0.0
            };

            // Get slot info from slot table snapshot
            let (slot_count, incoming_slots, outgoing_slots) =
                if let Some(ref snapshot) = slot_snapshot {
                    if let Some(shard_slot_info) = snapshot.shard_info.get(&info.shard_id) {
                        (
                            shard_slot_info.owned_slots.len(),
                            shard_slot_info.incoming_slots.len(),
                            shard_slot_info.outgoing_slots.len(),
                        )
                    } else {
                        (0, 0, 0)
                    }
                } else {
                    (0, 0, 0)
                };

            ShardInfoResponse {
                shard_id: info.shard_id,
                is_active: info.state == crate::multiraft::ShardState::Active,
                is_leader: info.leader == Some(cache.node_id()),
                leader_id: info.leader,
                entry_count: info.entry_count,
                percentage,
                term: info.term,
                slot_count,
                incoming_slots,
                outgoing_slots,
            }
        })
        .collect();

    Json(ShardsResponse {
        total_shards: shards.len() as u32,
        total_entries,
        shards,
    })
}

/// GET /api/multiraft/stats
pub async fn get_multiraft_stats(State(cache): State<AppState>) -> Json<MultiRaftStatsResponse> {
    let coordinator = match cache.multiraft_coordinator() {
        Some(c) => c,
        None => {
            return Json(MultiRaftStatsResponse {
                enabled: false,
                total_shards: 0,
                active_shards: 0,
                total_entries: 0,
                total_size_bytes: 0,
                local_leader_shards: 0,
                operations_total: 0,
                operations_per_sec: 0.0,
            });
        }
    };
    let stats = coordinator.stats();

    Json(MultiRaftStatsResponse {
        enabled: true,
        total_shards: stats.total_shards,
        active_shards: stats.active_shards,
        total_entries: stats.total_entries,
        total_size_bytes: stats.total_size_bytes,
        local_leader_shards: stats.local_leader_shards,
        operations_total: stats.operations_total,
        operations_per_sec: stats.operations_per_sec,
    })
}

/// GET /api/multiraft/leaders
pub async fn get_shard_leaders(State(cache): State<AppState>) -> Json<ShardLeadersResponse> {
    let coordinator = match cache.multiraft_coordinator() {
        Some(c) => c,
        None => {
            return Json(ShardLeadersResponse {
                leaders: Default::default(),
            });
        }
    };
    let leaders = coordinator.shard_leaders();

    Json(ShardLeadersResponse { leaders })
}

/// GET /api/multiraft/routing/{key}
pub async fn get_shard_routing(
    State(cache): State<AppState>,
    Path(key): Path<String>,
) -> Json<ShardRoutingResponse> {
    let shard_id = cache.shard_for_key(key.as_bytes()).unwrap_or(0);

    let leader_id = match cache.multiraft_coordinator() {
        Some(coordinator) => coordinator
            .shard_leaders()
            .get(&shard_id)
            .copied()
            .flatten(),
        None => cache.leader_id(),
    };

    Json(ShardRoutingResponse {
        key,
        shard_id,
        leader_id,
    })
}

/// GET /api/multiraft/slots/status
pub async fn get_slot_status(State(cache): State<AppState>) -> Json<SlotRoutingStatusResponse> {
    let enabled = cache.is_slot_routing_enabled();

    if !enabled {
        return Json(SlotRoutingStatusResponse {
            enabled: false,
            epoch: 0,
            total_slots: TOTAL_SLOTS,
            active_migrations: 0,
            completed_migrations: 0,
            failed_migrations: 0,
            total_keys_migrated: 0,
        });
    }

    let epoch = cache.slot_epoch().map(|e| e.value()).unwrap_or(0);

    let migration_status = cache.slot_migration_status();
    let (active_migrations, completed_migrations, failed_migrations, total_keys_migrated) =
        migration_status
            .map(|s| {
                (
                    s.active_migrations,
                    s.completed_migrations,
                    s.failed_migrations,
                    s.total_keys_migrated,
                )
            })
            .unwrap_or((0, 0, 0, 0));

    Json(SlotRoutingStatusResponse {
        enabled,
        epoch,
        total_slots: TOTAL_SLOTS,
        active_migrations,
        completed_migrations,
        failed_migrations,
        total_keys_migrated,
    })
}

/// POST /api/multiraft/shards/add
pub async fn add_shard(State(cache): State<AppState>) -> Json<AddShardResponse> {
    // Check if slot routing is enabled
    if !cache.is_slot_routing_enabled() {
        return Json(AddShardResponse {
            success: false,
            shard_id: None,
            slots_assigned: None,
            new_epoch: None,
            error: Some(
                "Slot routing is not enabled. Enable it first to add shards dynamically."
                    .to_string(),
            ),
        });
    }

    match cache.add_shard().await {
        Ok(result) => Json(AddShardResponse {
            success: true,
            shard_id: Some(result.shard_id),
            slots_assigned: Some(result.slots_assigned),
            new_epoch: Some(result.new_epoch.value()),
            error: None,
        }),
        Err(e) => Json(AddShardResponse {
            success: false,
            shard_id: None,
            slots_assigned: None,
            new_epoch: None,
            error: Some(e.to_string()),
        }),
    }
}

/// DELETE /api/multiraft/shards/:shard_id
pub async fn remove_shard(
    State(cache): State<AppState>,
    Path(shard_id): Path<u32>,
) -> Json<RemoveShardResponse> {
    // Check if slot routing is enabled
    if !cache.is_slot_routing_enabled() {
        return Json(RemoveShardResponse {
            success: false,
            shard_id: None,
            slots_redistributed: None,
            new_epoch: None,
            error: Some(
                "Slot routing is not enabled. Enable it first to remove shards dynamically."
                    .to_string(),
            ),
        });
    }

    match cache.remove_shard(shard_id).await {
        Ok(result) => Json(RemoveShardResponse {
            success: true,
            shard_id: Some(result.shard_id),
            slots_redistributed: Some(result.slots_to_redistribute),
            new_epoch: Some(result.new_epoch.value()),
            error: None,
        }),
        Err(e) => Json(RemoveShardResponse {
            success: false,
            shard_id: Some(shard_id),
            slots_redistributed: None,
            new_epoch: None,
            error: Some(e.to_string()),
        }),
    }
}

/// GET /api/multiraft/shards/compare
///
/// Returns shard information from all nodes in the cluster and highlights differences.
/// This is useful for detecting inconsistencies in shard state across nodes.
pub async fn get_cluster_shards_comparison(
    State(cache): State<AppState>,
) -> Json<ClusterShardsComparisonResponse> {
    let local_node_id = cache.node_id();

    // Get local shard information
    let local_shards = get_local_shards(&cache).await;
    let local_shard_info = NodeShardInfo {
        node_id: local_node_id,
        reachable: true,
        error: None,
        shards: local_shards.clone(),
    };

    let mut nodes = vec![local_shard_info];

    // Get all discovered nodes
    let all_members = cache.discovery_members();
    let healthy_members = cache.discovery_healthy_members();

    // For each remote node, we'll include placeholder info since we can't
    // directly fetch from other nodes without their dashboard addresses.
    // In a real deployment, this would require the cluster to share dashboard addresses
    // or use a shared registry. For now, we'll show what we know from discovery.
    for &member_id in &all_members {
        if member_id == local_node_id {
            continue;
        }

        let is_healthy = healthy_members.contains(&member_id);
        nodes.push(NodeShardInfo {
            node_id: member_id,
            reachable: is_healthy,
            error: if is_healthy {
                Some("Remote shard info not available (requires cluster coordination)".to_string())
            } else {
                Some("Node unreachable".to_string())
            },
            shards: vec![], // Would be populated if we had direct access
        });
    }

    // Compare shards and find differences
    let differences = find_shard_differences(&nodes);
    let is_consistent = differences.is_empty();

    let summary = if nodes.len() == 1 {
        "Single node mode - no comparison available".to_string()
    } else if is_consistent {
        format!(
            "All {} nodes report consistent shard state ({} shards)",
            nodes.len(),
            local_shards.len()
        )
    } else {
        format!(
            "{} differences detected across {} nodes",
            differences.len(),
            nodes.len()
        )
    };

    Json(ClusterShardsComparisonResponse {
        local_node_id,
        nodes,
        differences,
        is_consistent,
        summary,
    })
}

/// Get local shard information as ShardInfoResponse vec.
async fn get_local_shards(cache: &AppState) -> Vec<ShardInfoResponse> {
    cache.run_pending_tasks().await;

    let coordinator = match cache.multiraft_coordinator() {
        Some(c) => c,
        None => return vec![],
    };
    let shard_infos = coordinator.shard_info();
    let slot_snapshot = cache.slot_table();
    let total_entries: u64 = shard_infos.iter().map(|s| s.entry_count).sum();

    shard_infos
        .iter()
        .map(|info| {
            let percentage = if total_entries > 0 {
                (info.entry_count as f64 / total_entries as f64) * 100.0
            } else {
                0.0
            };

            let (slot_count, incoming_slots, outgoing_slots) =
                if let Some(ref snapshot) = slot_snapshot {
                    if let Some(shard_slot_info) = snapshot.shard_info.get(&info.shard_id) {
                        (
                            shard_slot_info.owned_slots.len(),
                            shard_slot_info.incoming_slots.len(),
                            shard_slot_info.outgoing_slots.len(),
                        )
                    } else {
                        (0, 0, 0)
                    }
                } else {
                    (0, 0, 0)
                };

            ShardInfoResponse {
                shard_id: info.shard_id,
                is_active: info.state == crate::multiraft::ShardState::Active,
                is_leader: info.leader == Some(cache.node_id()),
                leader_id: info.leader,
                entry_count: info.entry_count,
                percentage,
                term: info.term,
                slot_count,
                incoming_slots,
                outgoing_slots,
            }
        })
        .collect()
}

/// Find differences in shard information across nodes.
fn find_shard_differences(nodes: &[NodeShardInfo]) -> Vec<ShardDifference> {
    let mut differences = Vec::new();

    // Get all shard IDs from all nodes
    let mut all_shard_ids: std::collections::HashSet<u32> = std::collections::HashSet::new();
    for node in nodes {
        for shard in &node.shards {
            all_shard_ids.insert(shard.shard_id);
        }
    }

    // Build shard maps for each node
    let node_shard_maps: Vec<HashMap<u32, &ShardInfoResponse>> = nodes
        .iter()
        .map(|node| node.shards.iter().map(|s| (s.shard_id, s)).collect())
        .collect();

    // Compare shards across nodes
    for shard_id in all_shard_ids {
        // Check which nodes have this shard
        let mut nodes_with_shard: Vec<(usize, &ShardInfoResponse)> = Vec::new();
        let mut nodes_missing_shard: Vec<usize> = Vec::new();

        for (node_idx, shard_map) in node_shard_maps.iter().enumerate() {
            if let Some(shard) = shard_map.get(&shard_id) {
                nodes_with_shard.push((node_idx, shard));
            } else if nodes[node_idx].reachable && nodes[node_idx].error.is_none() {
                nodes_missing_shard.push(node_idx);
            }
        }

        // Report missing shards
        for node_idx in nodes_missing_shard {
            differences.push(ShardDifference {
                shard_id,
                diff_type: "missing_shard".to_string(),
                description: format!(
                    "Node {} is missing shard {}",
                    nodes[node_idx].node_id, shard_id
                ),
                node_a_value: None,
                node_b_value: Some(format!("Node {}", nodes[node_idx].node_id)),
            });
        }

        // Compare shard state across nodes that have it
        if nodes_with_shard.len() >= 2 {
            for i in 0..nodes_with_shard.len() - 1 {
                let (node_a_idx, shard_a) = nodes_with_shard[i];
                let (node_b_idx, shard_b) = nodes_with_shard[i + 1];
                let node_a_id = nodes[node_a_idx].node_id;
                let node_b_id = nodes[node_b_idx].node_id;

                // Compare leader
                if shard_a.leader_id != shard_b.leader_id {
                    differences.push(ShardDifference {
                        shard_id,
                        diff_type: "leader_mismatch".to_string(),
                        description: format!(
                            "Shard {} has different leader on nodes {} vs {}",
                            shard_id, node_a_id, node_b_id
                        ),
                        node_a_value: Some(format!("{:?}", shard_a.leader_id)),
                        node_b_value: Some(format!("{:?}", shard_b.leader_id)),
                    });
                }

                // Compare term
                if shard_a.term != shard_b.term {
                    differences.push(ShardDifference {
                        shard_id,
                        diff_type: "term_mismatch".to_string(),
                        description: format!(
                            "Shard {} has different term on nodes {} vs {}",
                            shard_id, node_a_id, node_b_id
                        ),
                        node_a_value: Some(format!("{}", shard_a.term)),
                        node_b_value: Some(format!("{}", shard_b.term)),
                    });
                }

                // Compare slot count (if slot routing enabled)
                if shard_a.slot_count != shard_b.slot_count
                    && (shard_a.slot_count > 0 || shard_b.slot_count > 0)
                {
                    differences.push(ShardDifference {
                        shard_id,
                        diff_type: "slot_count_mismatch".to_string(),
                        description: format!(
                            "Shard {} has different slot count on nodes {} vs {}",
                            shard_id, node_a_id, node_b_id
                        ),
                        node_a_value: Some(format!("{}", shard_a.slot_count)),
                        node_b_value: Some(format!("{}", shard_b.slot_count)),
                    });
                }

                // Compare active state
                if shard_a.is_active != shard_b.is_active {
                    differences.push(ShardDifference {
                        shard_id,
                        diff_type: "active_state_mismatch".to_string(),
                        description: format!(
                            "Shard {} has different active state on nodes {} vs {}",
                            shard_id, node_a_id, node_b_id
                        ),
                        node_a_value: Some(format!("{}", shard_a.is_active)),
                        node_b_value: Some(format!("{}", shard_b.is_active)),
                    });
                }
            }
        }
    }

    differences
}
