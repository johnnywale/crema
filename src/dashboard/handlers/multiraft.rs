//! Multi-Raft handlers.

use axum::{
    extract::{Path, State},
    Json,
};

use crate::dashboard::handlers::AppState;
use crate::dashboard::types::{
    AddShardResponse, MultiRaftStatsResponse, RemoveShardResponse, ShardInfoResponse,
    ShardLeadersResponse, ShardRoutingResponse, ShardsResponse, SlotRoutingStatusResponse,
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
