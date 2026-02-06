//! Server-Sent Events for real-time updates.

use axum::{
    extract::State,
    response::sse::{Event, Sse},
};
use futures::stream::{self, Stream};
use futures::StreamExt;
use std::convert::Infallible;
use std::sync::Arc;
use std::time::Duration;

use crate::cache::DistributedCache;
use crate::dashboard::types::{
    ClusterStatusResponse, MetricsResponse, ShardInfoResponse, ShardsResponse,
};

/// GET /api/events - Server-Sent Events stream
pub async fn events_stream(
    State(cache): State<Arc<DistributedCache>>,
) -> Sse<impl Stream<Item = Result<Event, Infallible>>> {
    let stream = stream::unfold(cache, |cache| async move {
        tokio::time::sleep(Duration::from_secs(1)).await;

        // Collect all events to send
        let mut events = Vec::new();

        // Metrics event
        let snapshot = cache.metrics().snapshot();
        let hit_rate = if snapshot.get_total > 0 {
            snapshot.get_hits as f64 / snapshot.get_total as f64
        } else {
            0.0
        };

        let metrics = MetricsResponse {
            get_total: snapshot.get_total,
            get_hits: snapshot.get_hits,
            get_misses: snapshot.get_misses,
            hit_rate,
            put_total: snapshot.put_total,
            put_success: snapshot.put_success,
            put_failures: snapshot.put_failures,
            delete_total: snapshot.delete_total,
            cache_entries: snapshot.cache_entries,
            is_leader: snapshot.is_leader,
            raft_term: snapshot.raft_term,
            avg_get_latency_ms: snapshot.get_latency.mean() * 1000.0,
            avg_put_latency_ms: snapshot.put_latency.mean() * 1000.0,
            get_latency_p50_ms: snapshot.get_latency.percentile(50.0) * 1000.0,
            get_latency_p90_ms: snapshot.get_latency.percentile(90.0) * 1000.0,
            get_latency_p99_ms: snapshot.get_latency.percentile(99.0) * 1000.0,
            put_latency_p50_ms: snapshot.put_latency.percentile(50.0) * 1000.0,
            put_latency_p90_ms: snapshot.put_latency.percentile(90.0) * 1000.0,
            put_latency_p99_ms: snapshot.put_latency.percentile(99.0) * 1000.0,
            forward_total: snapshot.forward_total,
            forward_success: snapshot.forward_success,
            forward_failures: snapshot.forward_failures,
            forward_timeouts: snapshot.forward_timeouts,
            forward_pending: snapshot.forward_pending,
        };

        if let Ok(json) = serde_json::to_string(&metrics) {
            events.push(Event::default().event("metrics").data(json));
        }

        // Cluster status event
        let status = cache.cluster_status();
        let cluster_status = ClusterStatusResponse {
            node_id: status.node_id,
            leader_id: status.leader_id,
            is_leader: status.is_leader,
            term: status.term,
            raft_peer_count: status.raft_peer_count,
            discovered_node_count: status.discovered_node_count,
            memberlist_node_count: status.memberlist_node_count,
            commit_index: status.commit_index,
            applied_index: status.applied_index,
        };

        if let Ok(json) = serde_json::to_string(&cluster_status) {
            events.push(Event::default().event("cluster_status").data(json));
        }

        // Shard update event (if Multi-Raft enabled)
        if cache.is_multiraft_enabled() {
            if let Some(coordinator) = cache.multiraft_coordinator() {
                // Run pending tasks to sync Moka's counters before reading stats
                cache.run_pending_tasks().await;

                let node_id = cache.node_id();
                let shard_infos = coordinator.shard_info();

                // Get slot table snapshot for slot info (if slot routing is enabled)
                let slot_snapshot = cache.slot_table();

                // Filter out shards that are being removed or stopped
                let visible_shard_infos: Vec<_> = shard_infos
                    .into_iter()
                    .filter(|info| {
                        !matches!(
                            info.state,
                            crate::multiraft::ShardState::Removing
                                | crate::multiraft::ShardState::Stopped
                        )
                    })
                    .collect();

                // Calculate total entries for percentage (only from visible shards)
                let total_entries: u64 = visible_shard_infos.iter().map(|s| s.entry_count).sum();

                let shards: Vec<ShardInfoResponse> = visible_shard_infos
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
                                if let Some(shard_slot_info) =
                                    snapshot.shard_info.get(&info.shard_id)
                                {
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
                            is_leader: info.leader == Some(node_id),
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

                let shards_response = ShardsResponse {
                    total_shards: shards.len() as u32,
                    total_entries,
                    shards,
                };

                if let Ok(json) = serde_json::to_string(&shards_response) {
                    events.push(Event::default().event("shard_update").data(json));
                }
            }
        }

        // Return events as a stream
        Some((stream::iter(events.into_iter().map(Ok)), cache))
    })
    .flatten();

    Sse::new(stream).keep_alive(
        axum::response::sse::KeepAlive::new()
            .interval(Duration::from_secs(15))
            .text("keep-alive"),
    )
}
