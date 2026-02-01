//! Metric descriptors defining all metric names, descriptions, and labels.
//!
//! This module centralizes all metric definitions for the Crema distributed cache.
//! All metric names use the `crema_` prefix for namespace isolation.

// ============================================================================
// Cache Module Metrics (14 metrics)
// ============================================================================

/// Cache GET operations counter.
pub const CACHE_GET_TOTAL: &str = "crema_cache_get_total";
pub const CACHE_GET_TOTAL_DESC: &str = "Total number of GET operations";

/// Cache GET operation latency histogram.
pub const CACHE_GET_DURATION_SECONDS: &str = "crema_cache_get_duration_seconds";
pub const CACHE_GET_DURATION_SECONDS_DESC: &str = "GET operation latency in seconds";

/// Cache PUT operations counter.
pub const CACHE_PUT_TOTAL: &str = "crema_cache_put_total";
pub const CACHE_PUT_TOTAL_DESC: &str = "Total number of PUT operations";

/// Cache PUT operation latency histogram.
pub const CACHE_PUT_DURATION_SECONDS: &str = "crema_cache_put_duration_seconds";
pub const CACHE_PUT_DURATION_SECONDS_DESC: &str = "PUT operation latency in seconds";

/// Cache DELETE operations counter.
pub const CACHE_DELETE_TOTAL: &str = "crema_cache_delete_total";
pub const CACHE_DELETE_TOTAL_DESC: &str = "Total number of DELETE operations";

/// Cache DELETE operation latency histogram.
pub const CACHE_DELETE_DURATION_SECONDS: &str = "crema_cache_delete_duration_seconds";
pub const CACHE_DELETE_DURATION_SECONDS_DESC: &str = "DELETE operation latency in seconds";

/// Cache CLEAR operations counter.
pub const CACHE_CLEAR_TOTAL: &str = "crema_cache_clear_total";
pub const CACHE_CLEAR_TOTAL_DESC: &str = "Total number of CLEAR operations";

/// Local PUT operations counter (bypassing Raft).
pub const CACHE_PUT_LOCAL_TOTAL: &str = "crema_cache_put_local_total";
pub const CACHE_PUT_LOCAL_TOTAL_DESC: &str = "Total number of local PUT operations";

/// Current number of cache entries gauge.
pub const CACHE_ENTRIES: &str = "crema_cache_entries";
pub const CACHE_ENTRIES_DESC: &str = "Current number of entries in the cache";

/// Current cache memory size gauge.
pub const CACHE_SIZE_BYTES: &str = "crema_cache_size_bytes";
pub const CACHE_SIZE_BYTES_DESC: &str = "Current cache memory size in bytes";

/// Cache evictions counter by reason.
pub const CACHE_EVICTIONS_TOTAL: &str = "crema_cache_evictions_total";
pub const CACHE_EVICTIONS_TOTAL_DESC: &str = "Total number of cache evictions";

/// Key size distribution histogram.
pub const CACHE_KEY_SIZE_BYTES: &str = "crema_cache_key_size_bytes";
pub const CACHE_KEY_SIZE_BYTES_DESC: &str = "Distribution of key sizes in bytes";

/// Value size distribution histogram.
pub const CACHE_VALUE_SIZE_BYTES: &str = "crema_cache_value_size_bytes";
pub const CACHE_VALUE_SIZE_BYTES_DESC: &str = "Distribution of value sizes in bytes";

/// TTL distribution histogram.
pub const CACHE_TTL_SECONDS: &str = "crema_cache_ttl_seconds";
pub const CACHE_TTL_SECONDS_DESC: &str = "Distribution of TTL values in seconds";

// ============================================================================
// Consensus/Raft Module Metrics (22 metrics)
// ============================================================================

/// Raft proposals counter.
pub const RAFT_PROPOSALS_TOTAL: &str = "crema_raft_proposals_total";
pub const RAFT_PROPOSALS_TOTAL_DESC: &str = "Total number of Raft proposals";

/// Raft proposal latency histogram.
pub const RAFT_PROPOSAL_DURATION_SECONDS: &str = "crema_raft_proposal_duration_seconds";
pub const RAFT_PROPOSAL_DURATION_SECONDS_DESC: &str = "Raft proposal latency in seconds";

/// Current Raft term gauge.
pub const RAFT_TERM: &str = "crema_raft_term";
pub const RAFT_TERM_DESC: &str = "Current Raft term number";

/// Current Raft commit index gauge.
pub const RAFT_COMMIT_INDEX: &str = "crema_raft_commit_index";
pub const RAFT_COMMIT_INDEX_DESC: &str = "Current Raft commit index";

/// Current Raft applied index gauge.
pub const RAFT_APPLIED_INDEX: &str = "crema_raft_applied_index";
pub const RAFT_APPLIED_INDEX_DESC: &str = "Current Raft applied index";

/// Gap between commit and applied index gauge.
pub const RAFT_COMMIT_APPLY_LAG: &str = "crema_raft_commit_apply_lag";
pub const RAFT_COMMIT_APPLY_LAG_DESC: &str = "Gap between commit and applied index";

/// Whether this node is the leader gauge.
pub const RAFT_IS_LEADER: &str = "crema_raft_is_leader";
pub const RAFT_IS_LEADER_DESC: &str = "Whether this node is the Raft leader (0 or 1)";

/// Current leader ID gauge.
pub const RAFT_LEADER_ID: &str = "crema_raft_leader_id";
pub const RAFT_LEADER_ID_DESC: &str = "Current Raft leader node ID";

/// Number of Raft peers gauge.
pub const RAFT_PEERS: &str = "crema_raft_peers";
pub const RAFT_PEERS_DESC: &str = "Number of Raft peer nodes";

/// Raft elections counter by result.
pub const RAFT_ELECTIONS_TOTAL: &str = "crema_raft_elections_total";
pub const RAFT_ELECTIONS_TOTAL_DESC: &str = "Total number of Raft elections";

/// Raft election duration histogram.
pub const RAFT_ELECTION_DURATION_SECONDS: &str = "crema_raft_election_duration_seconds";
pub const RAFT_ELECTION_DURATION_SECONDS_DESC: &str = "Raft election duration in seconds";

/// Raft messages counter by type and direction.
pub const RAFT_MESSAGES_TOTAL: &str = "crema_raft_messages_total";
pub const RAFT_MESSAGES_TOTAL_DESC: &str = "Total number of Raft messages";

/// Raft message bytes counter.
pub const RAFT_MESSAGES_BYTES_TOTAL: &str = "crema_raft_messages_bytes_total";
pub const RAFT_MESSAGES_BYTES_TOTAL_DESC: &str = "Total bytes of Raft messages";

/// Replication lag per peer gauge.
pub const RAFT_REPLICATION_LAG_ENTRIES: &str = "crema_raft_replication_lag_entries";
pub const RAFT_REPLICATION_LAG_ENTRIES_DESC: &str = "Replication lag in entries per peer";

/// Pending Raft proposals gauge.
pub const RAFT_PENDING_PROPOSALS: &str = "crema_raft_pending_proposals";
pub const RAFT_PENDING_PROPOSALS_DESC: &str = "Number of pending Raft proposals";

/// Raft queue depth gauge by queue type.
pub const RAFT_QUEUE_DEPTH: &str = "crema_raft_queue_depth";
pub const RAFT_QUEUE_DEPTH_DESC: &str = "Raft queue depth";

/// State machine apply duration histogram.
pub const RAFT_APPLY_DURATION_SECONDS: &str = "crema_raft_apply_duration_seconds";
pub const RAFT_APPLY_DURATION_SECONDS_DESC: &str = "State machine apply duration in seconds";

/// Raft step duration histogram.
pub const RAFT_STEP_DURATION_SECONDS: &str = "crema_raft_step_duration_seconds";
pub const RAFT_STEP_DURATION_SECONDS_DESC: &str = "Raft step processing duration in seconds";

/// Raft configuration changes counter.
pub const RAFT_CONF_CHANGE_TOTAL: &str = "crema_raft_conf_change_total";
pub const RAFT_CONF_CHANGE_TOTAL_DESC: &str = "Total number of Raft configuration changes";

/// Raft heartbeats sent counter.
pub const RAFT_HEARTBEAT_TOTAL: &str = "crema_raft_heartbeat_total";
pub const RAFT_HEARTBEAT_TOTAL_DESC: &str = "Total number of Raft heartbeats sent";

/// Raft heartbeat latency histogram.
pub const RAFT_HEARTBEAT_LATENCY_SECONDS: &str = "crema_raft_heartbeat_latency_seconds";
pub const RAFT_HEARTBEAT_LATENCY_SECONDS_DESC: &str = "Raft heartbeat round-trip time in seconds";

/// Raft log entries gauge.
pub const RAFT_LOG_ENTRIES: &str = "crema_raft_log_entries";
pub const RAFT_LOG_ENTRIES_DESC: &str = "Number of entries in the Raft log";

// ============================================================================
// Transport Module Metrics (15 metrics)
// ============================================================================

/// Transport messages sent counter.
pub const TRANSPORT_MESSAGES_SENT_TOTAL: &str = "crema_transport_messages_sent_total";
pub const TRANSPORT_MESSAGES_SENT_TOTAL_DESC: &str = "Total number of transport messages sent";

/// Transport message send failures counter.
pub const TRANSPORT_MESSAGES_FAILED_TOTAL: &str = "crema_transport_messages_failed_total";
pub const TRANSPORT_MESSAGES_FAILED_TOTAL_DESC: &str =
    "Total number of failed transport message sends";

/// Transport send latency histogram.
pub const TRANSPORT_SEND_DURATION_SECONDS: &str = "crema_transport_send_duration_seconds";
pub const TRANSPORT_SEND_DURATION_SECONDS_DESC: &str = "Transport message send latency in seconds";

/// Active transport connections gauge.
pub const TRANSPORT_CONNECTIONS_ACTIVE: &str = "crema_transport_connections_active";
pub const TRANSPORT_CONNECTIONS_ACTIVE_DESC: &str = "Number of active transport connections";

/// Transport connections created counter.
pub const TRANSPORT_CONNECTIONS_CREATED_TOTAL: &str = "crema_transport_connections_created_total";
pub const TRANSPORT_CONNECTIONS_CREATED_TOTAL_DESC: &str =
    "Total number of transport connections created";

/// Transport connection failures counter.
pub const TRANSPORT_CONNECTIONS_FAILED_TOTAL: &str = "crema_transport_connections_failed_total";
pub const TRANSPORT_CONNECTIONS_FAILED_TOTAL_DESC: &str =
    "Total number of transport connection failures";

/// Transport connection duration histogram.
pub const TRANSPORT_CONNECTION_DURATION_SECONDS: &str =
    "crema_transport_connection_duration_seconds";
pub const TRANSPORT_CONNECTION_DURATION_SECONDS_DESC: &str =
    "Transport connection establishment time in seconds";

/// Transport bytes sent counter.
pub const TRANSPORT_BYTES_SENT_TOTAL: &str = "crema_transport_bytes_sent_total";
pub const TRANSPORT_BYTES_SENT_TOTAL_DESC: &str = "Total bytes sent via transport";

/// Transport bytes received counter.
pub const TRANSPORT_BYTES_RECEIVED_TOTAL: &str = "crema_transport_bytes_received_total";
pub const TRANSPORT_BYTES_RECEIVED_TOTAL_DESC: &str = "Total bytes received via transport";

/// Transport queue full events counter.
pub const TRANSPORT_QUEUE_FULL_TOTAL: &str = "crema_transport_queue_full_total";
pub const TRANSPORT_QUEUE_FULL_TOTAL_DESC: &str = "Total number of queue full events";

/// Transport queue depth gauge.
pub const TRANSPORT_QUEUE_DEPTH: &str = "crema_transport_queue_depth";
pub const TRANSPORT_QUEUE_DEPTH_DESC: &str = "Current transport queue depth";

/// Transport retries counter.
pub const TRANSPORT_RETRIES_TOTAL: &str = "crema_transport_retries_total";
pub const TRANSPORT_RETRIES_TOTAL_DESC: &str = "Total number of transport retries";

/// Pending transport retries gauge.
pub const TRANSPORT_PENDING_RETRIES: &str = "crema_transport_pending_retries";
pub const TRANSPORT_PENDING_RETRIES_DESC: &str = "Number of pending transport retries";

/// Transport reconnection attempts counter.
pub const TRANSPORT_RECONNECTS_TOTAL: &str = "crema_transport_reconnects_total";
pub const TRANSPORT_RECONNECTS_TOTAL_DESC: &str = "Total number of transport reconnection attempts";

/// Transport message encoding duration histogram.
pub const TRANSPORT_ENCODE_DURATION_SECONDS: &str = "crema_transport_encode_duration_seconds";
pub const TRANSPORT_ENCODE_DURATION_SECONDS_DESC: &str =
    "Transport message encoding time in seconds";

// ============================================================================
// Cluster Module Metrics (12 metrics)
// ============================================================================

/// Total cluster nodes gauge.
pub const CLUSTER_NODES_TOTAL: &str = "crema_cluster_nodes_total";
pub const CLUSTER_NODES_TOTAL_DESC: &str = "Total number of nodes in the cluster";

/// Healthy cluster nodes gauge.
pub const CLUSTER_NODES_HEALTHY: &str = "crema_cluster_nodes_healthy";
pub const CLUSTER_NODES_HEALTHY_DESC: &str = "Number of healthy nodes in the cluster";

/// Node joins counter.
pub const CLUSTER_NODE_JOINS_TOTAL: &str = "crema_cluster_node_joins_total";
pub const CLUSTER_NODE_JOINS_TOTAL_DESC: &str = "Total number of node joins";

/// Node leaves counter.
pub const CLUSTER_NODE_LEAVES_TOTAL: &str = "crema_cluster_node_leaves_total";
pub const CLUSTER_NODE_LEAVES_TOTAL_DESC: &str = "Total number of node leaves";

/// Cluster join duration histogram.
pub const CLUSTER_JOIN_DURATION_SECONDS: &str = "crema_cluster_join_duration_seconds";
pub const CLUSTER_JOIN_DURATION_SECONDS_DESC: &str = "Cluster join duration in seconds";

/// Cluster health checks counter.
pub const CLUSTER_HEALTH_CHECK_TOTAL: &str = "crema_cluster_health_check_total";
pub const CLUSTER_HEALTH_CHECK_TOTAL_DESC: &str = "Total number of cluster health checks";

/// Health check duration histogram.
pub const CLUSTER_HEALTH_CHECK_DURATION_SECONDS: &str =
    "crema_cluster_health_check_duration_seconds";
pub const CLUSTER_HEALTH_CHECK_DURATION_SECONDS_DESC: &str =
    "Cluster health check duration in seconds";

/// Gossip rounds counter.
pub const CLUSTER_GOSSIP_ROUNDS_TOTAL: &str = "crema_cluster_gossip_rounds_total";
pub const CLUSTER_GOSSIP_ROUNDS_TOTAL_DESC: &str = "Total number of gossip rounds";

/// Gossip messages counter.
pub const CLUSTER_GOSSIP_MESSAGES_TOTAL: &str = "crema_cluster_gossip_messages_total";
pub const CLUSTER_GOSSIP_MESSAGES_TOTAL_DESC: &str = "Total number of gossip messages";

/// Cluster convergence duration histogram.
pub const CLUSTER_CONVERGENCE_DURATION_SECONDS: &str = "crema_cluster_convergence_duration_seconds";
pub const CLUSTER_CONVERGENCE_DURATION_SECONDS_DESC: &str =
    "Cluster convergence duration in seconds";

/// Discovery duration histogram.
pub const CLUSTER_DISCOVERY_DURATION_SECONDS: &str = "crema_cluster_discovery_duration_seconds";
pub const CLUSTER_DISCOVERY_DURATION_SECONDS_DESC: &str = "Cluster discovery duration in seconds";

/// Cluster events counter.
pub const CLUSTER_EVENTS_TOTAL: &str = "crema_cluster_events_total";
pub const CLUSTER_EVENTS_TOTAL_DESC: &str = "Total number of cluster events";

// ============================================================================
// Network Module Metrics (10 metrics)
// ============================================================================

/// Active network connections gauge.
pub const NETWORK_CONNECTIONS_ACTIVE: &str = "crema_network_connections_active";
pub const NETWORK_CONNECTIONS_ACTIVE_DESC: &str = "Number of active network connections";

/// Total network connections counter.
pub const NETWORK_CONNECTIONS_TOTAL: &str = "crema_network_connections_total";
pub const NETWORK_CONNECTIONS_TOTAL_DESC: &str = "Total number of network connections";

/// Network connection errors counter.
pub const NETWORK_CONNECTION_ERRORS_TOTAL: &str = "crema_network_connection_errors_total";
pub const NETWORK_CONNECTION_ERRORS_TOTAL_DESC: &str = "Total number of network connection errors";

/// Network requests counter.
pub const NETWORK_REQUESTS_TOTAL: &str = "crema_network_requests_total";
pub const NETWORK_REQUESTS_TOTAL_DESC: &str = "Total number of network requests";

/// Network request duration histogram.
pub const NETWORK_REQUEST_DURATION_SECONDS: &str = "crema_network_request_duration_seconds";
pub const NETWORK_REQUEST_DURATION_SECONDS_DESC: &str = "Network request duration in seconds";

/// Network bytes received counter.
pub const NETWORK_BYTES_RECEIVED_TOTAL: &str = "crema_network_bytes_received_total";
pub const NETWORK_BYTES_RECEIVED_TOTAL_DESC: &str = "Total bytes received from network";

/// Network bytes sent counter.
pub const NETWORK_BYTES_SENT_TOTAL: &str = "crema_network_bytes_sent_total";
pub const NETWORK_BYTES_SENT_TOTAL_DESC: &str = "Total bytes sent to network";

/// Network message size histogram.
pub const NETWORK_MESSAGE_SIZE_BYTES: &str = "crema_network_message_size_bytes";
pub const NETWORK_MESSAGE_SIZE_BYTES_DESC: &str = "Network message size distribution in bytes";

/// Network decode errors counter.
pub const NETWORK_DECODE_ERRORS_TOTAL: &str = "crema_network_decode_errors_total";
pub const NETWORK_DECODE_ERRORS_TOTAL_DESC: &str = "Total number of network decode errors";

/// Network protocol errors counter.
pub const NETWORK_PROTOCOL_ERRORS_TOTAL: &str = "crema_network_protocol_errors_total";
pub const NETWORK_PROTOCOL_ERRORS_TOTAL_DESC: &str = "Total number of network protocol errors";

// ============================================================================
// Multi-Raft Module Metrics (16 metrics)
// ============================================================================

/// Shard operations counter.
pub const SHARD_OPERATIONS_TOTAL: &str = "crema_shard_operations_total";
pub const SHARD_OPERATIONS_TOTAL_DESC: &str = "Total number of shard operations";

/// Shard operation duration histogram.
pub const SHARD_OPERATION_DURATION_SECONDS: &str = "crema_shard_operation_duration_seconds";
pub const SHARD_OPERATION_DURATION_SECONDS_DESC: &str = "Shard operation duration in seconds";

/// Entries per shard gauge.
pub const SHARD_ENTRIES: &str = "crema_shard_entries";
pub const SHARD_ENTRIES_DESC: &str = "Number of entries per shard";

/// Shard size gauge.
pub const SHARD_SIZE_BYTES: &str = "crema_shard_size_bytes";
pub const SHARD_SIZE_BYTES_DESC: &str = "Shard size in bytes";

/// Shard leader status gauge.
pub const SHARD_IS_LEADER: &str = "crema_shard_is_leader";
pub const SHARD_IS_LEADER_DESC: &str = "Whether this node is the shard leader (0 or 1)";

/// Shard leader changes counter.
pub const SHARD_LEADER_CHANGES_TOTAL: &str = "crema_shard_leader_changes_total";
pub const SHARD_LEADER_CHANGES_TOTAL_DESC: &str = "Total number of shard leader changes";

/// Shard leader tenure histogram.
pub const SHARD_LEADER_TENURE_SECONDS: &str = "crema_shard_leader_tenure_seconds";
pub const SHARD_LEADER_TENURE_SECONDS_DESC: &str = "Shard leadership tenure duration in seconds";

/// Shard forwards counter.
pub const SHARD_FORWARDS_TOTAL: &str = "crema_shard_forwards_total";
pub const SHARD_FORWARDS_TOTAL_DESC: &str = "Total number of shard forward requests";

/// Shard forward duration histogram.
pub const SHARD_FORWARD_DURATION_SECONDS: &str = "crema_shard_forward_duration_seconds";
pub const SHARD_FORWARD_DURATION_SECONDS_DESC: &str = "Shard forward request duration in seconds";

/// Pending shard forwards gauge.
pub const SHARD_FORWARD_PENDING: &str = "crema_shard_forward_pending";
pub const SHARD_FORWARD_PENDING_DESC: &str = "Number of pending shard forward requests";

/// Shard forward hops histogram.
pub const SHARD_FORWARD_HOPS: &str = "crema_shard_forward_hops";
pub const SHARD_FORWARD_HOPS_DESC: &str = "Distribution of forward hop counts";

/// Shard routing lookups counter.
pub const SHARD_ROUTING_TOTAL: &str = "crema_shard_routing_total";
pub const SHARD_ROUTING_TOTAL_DESC: &str = "Total number of shard routing lookups";

/// Shard routing cache hits counter.
pub const SHARD_ROUTING_CACHE_HITS_TOTAL: &str = "crema_shard_routing_cache_hits_total";
pub const SHARD_ROUTING_CACHE_HITS_TOTAL_DESC: &str = "Total number of shard routing cache hits";

/// Total shards gauge.
pub const SHARD_COUNT: &str = "crema_shard_count";
pub const SHARD_COUNT_DESC: &str = "Total number of shards";

/// Shards created counter.
pub const SHARD_CREATED_TOTAL: &str = "crema_shard_created_total";
pub const SHARD_CREATED_TOTAL_DESC: &str = "Total number of shards created";

/// Shards deleted counter.
pub const SHARD_DELETED_TOTAL: &str = "crema_shard_deleted_total";
pub const SHARD_DELETED_TOTAL_DESC: &str = "Total number of shards deleted";

// ============================================================================
// Migration Module Metrics (12 metrics)
// ============================================================================

/// Active migrations gauge.
pub const MIGRATION_ACTIVE: &str = "crema_migration_active";
pub const MIGRATION_ACTIVE_DESC: &str = "Number of currently active migrations";

/// Migrations counter by result.
pub const MIGRATION_TOTAL: &str = "crema_migration_total";
pub const MIGRATION_TOTAL_DESC: &str = "Total number of migrations";

/// Migration duration histogram.
pub const MIGRATION_DURATION_SECONDS: &str = "crema_migration_duration_seconds";
pub const MIGRATION_DURATION_SECONDS_DESC: &str = "Migration duration in seconds";

/// Migrated entries counter.
pub const MIGRATION_ENTRIES_TOTAL: &str = "crema_migration_entries_total";
pub const MIGRATION_ENTRIES_TOTAL_DESC: &str = "Total number of entries migrated";

/// Migrated bytes counter.
pub const MIGRATION_BYTES_TOTAL: &str = "crema_migration_bytes_total";
pub const MIGRATION_BYTES_TOTAL_DESC: &str = "Total bytes migrated";

/// Migration transfer rate gauge.
pub const MIGRATION_RATE_BYTES_PER_SECOND: &str = "crema_migration_rate_bytes_per_second";
pub const MIGRATION_RATE_BYTES_PER_SECOND_DESC: &str =
    "Migration transfer rate in bytes per second";

/// Migration phase gauge.
pub const MIGRATION_PHASE: &str = "crema_migration_phase";
pub const MIGRATION_PHASE_DESC: &str = "Current migration phase (encoded as number)";

/// Blocked writes during migration counter.
pub const MIGRATION_BLOCKED_WRITES_TOTAL: &str = "crema_migration_blocked_writes_total";
pub const MIGRATION_BLOCKED_WRITES_TOTAL_DESC: &str =
    "Total number of writes blocked during migration";

/// Dual writes during migration counter.
pub const MIGRATION_DUAL_WRITES_TOTAL: &str = "crema_migration_dual_writes_total";
pub const MIGRATION_DUAL_WRITES_TOTAL_DESC: &str = "Total number of dual writes during migration";

/// Migration catch-up lag gauge.
pub const MIGRATION_CATCHUP_LAG: &str = "crema_migration_catchup_lag";
pub const MIGRATION_CATCHUP_LAG_DESC: &str = "Migration catch-up lag in entries";

/// Migration retries counter.
pub const MIGRATION_RETRIES_TOTAL: &str = "crema_migration_retries_total";
pub const MIGRATION_RETRIES_TOTAL_DESC: &str = "Total number of migration retries";

/// Migration phase duration histogram.
pub const MIGRATION_PHASE_DURATION_SECONDS: &str = "crema_migration_phase_duration_seconds";
pub const MIGRATION_PHASE_DURATION_SECONDS_DESC: &str =
    "Duration of each migration phase in seconds";

// ============================================================================
// Checkpoint Module Metrics (10 metrics)
// ============================================================================

/// Checkpoints created counter.
pub const CHECKPOINT_CREATED_TOTAL: &str = "crema_checkpoint_created_total";
pub const CHECKPOINT_CREATED_TOTAL_DESC: &str = "Total number of checkpoints created";

/// Checkpoint creation duration histogram.
pub const CHECKPOINT_CREATE_DURATION_SECONDS: &str = "crema_checkpoint_create_duration_seconds";
pub const CHECKPOINT_CREATE_DURATION_SECONDS_DESC: &str = "Checkpoint creation duration in seconds";

/// Checkpoint size gauge.
pub const CHECKPOINT_SIZE_BYTES: &str = "crema_checkpoint_size_bytes";
pub const CHECKPOINT_SIZE_BYTES_DESC: &str = "Checkpoint size in bytes";

/// Checkpoint entries gauge.
pub const CHECKPOINT_ENTRIES: &str = "crema_checkpoint_entries";
pub const CHECKPOINT_ENTRIES_DESC: &str = "Number of entries in checkpoint";

/// Checkpoint compression ratio gauge.
pub const CHECKPOINT_COMPRESSION_RATIO: &str = "crema_checkpoint_compression_ratio";
pub const CHECKPOINT_COMPRESSION_RATIO_DESC: &str = "Checkpoint compression ratio";

/// Checkpoints loaded counter.
pub const CHECKPOINT_LOAD_TOTAL: &str = "crema_checkpoint_load_total";
pub const CHECKPOINT_LOAD_TOTAL_DESC: &str = "Total number of checkpoints loaded";

/// Checkpoint load duration histogram.
pub const CHECKPOINT_LOAD_DURATION_SECONDS: &str = "crema_checkpoint_load_duration_seconds";
pub const CHECKPOINT_LOAD_DURATION_SECONDS_DESC: &str = "Checkpoint load duration in seconds";

/// Entries since last checkpoint gauge.
pub const CHECKPOINT_ENTRIES_SINCE: &str = "crema_checkpoint_entries_since";
pub const CHECKPOINT_ENTRIES_SINCE_DESC: &str = "Number of entries since last checkpoint";

/// Checkpoint backpressure events counter.
pub const CHECKPOINT_BACKPRESSURE_TOTAL: &str = "crema_checkpoint_backpressure_total";
pub const CHECKPOINT_BACKPRESSURE_TOTAL_DESC: &str =
    "Total number of checkpoint backpressure events";

/// Checkpoint cleanup operations counter.
pub const CHECKPOINT_CLEANUP_TOTAL: &str = "crema_checkpoint_cleanup_total";
pub const CHECKPOINT_CLEANUP_TOTAL_DESC: &str = "Total number of checkpoint cleanup operations";

// ============================================================================
// Slot Routing Module Metrics (10 metrics)
// ============================================================================

/// Slot lookups counter.
pub const SLOT_LOOKUPS_TOTAL: &str = "crema_slot_lookups_total";
pub const SLOT_LOOKUPS_TOTAL_DESC: &str = "Total number of slot lookups";

/// Slot lookup duration histogram.
pub const SLOT_LOOKUP_DURATION_SECONDS: &str = "crema_slot_lookup_duration_seconds";
pub const SLOT_LOOKUP_DURATION_SECONDS_DESC: &str = "Slot lookup duration in seconds";

/// Current slot epoch gauge.
pub const SLOT_EPOCH: &str = "crema_slot_epoch";
pub const SLOT_EPOCH_DESC: &str = "Current slot routing epoch";

/// Slot reassignments counter.
pub const SLOT_REASSIGNMENTS_TOTAL: &str = "crema_slot_reassignments_total";
pub const SLOT_REASSIGNMENTS_TOTAL_DESC: &str = "Total number of slot reassignments";

/// Active slot migrations gauge.
pub const SLOT_MIGRATIONS_ACTIVE: &str = "crema_slot_migrations_active";
pub const SLOT_MIGRATIONS_ACTIVE_DESC: &str = "Number of active slot migrations";

/// Slot migrations counter by result.
pub const SLOT_MIGRATIONS_TOTAL: &str = "crema_slot_migrations_total";
pub const SLOT_MIGRATIONS_TOTAL_DESC: &str = "Total number of slot migrations";

/// Keys per slot counter.
pub const SLOT_MIGRATION_KEYS_TOTAL: &str = "crema_slot_migration_keys_total";
pub const SLOT_MIGRATION_KEYS_TOTAL_DESC: &str = "Total number of keys migrated per slot";

/// Slots per shard distribution gauge.
pub const SLOT_DISTRIBUTION: &str = "crema_slot_distribution";
pub const SLOT_DISTRIBUTION_DESC: &str = "Number of slots per shard";

/// Total slots gauge.
pub const SLOT_TOTAL: &str = "crema_slot_total";
pub const SLOT_TOTAL_DESC: &str = "Total number of slots (usually 1024)";

/// Owned slots per shard gauge.
pub const SLOT_OWNED: &str = "crema_slot_owned";
pub const SLOT_OWNED_DESC: &str = "Number of slots owned per shard";

// ============================================================================
// Error Tracking Metrics (3 metrics)
// ============================================================================

/// Errors counter by module and type.
pub const ERRORS_TOTAL: &str = "crema_errors_total";
pub const ERRORS_TOTAL_DESC: &str = "Total number of errors by module and type";

/// Retry attempts counter by operation.
pub const RETRIES_TOTAL: &str = "crema_retries_total";
pub const RETRIES_TOTAL_DESC: &str = "Total number of retry attempts";

/// Successful retries counter.
pub const RETRY_SUCCESS_TOTAL: &str = "crema_retry_success_total";
pub const RETRY_SUCCESS_TOTAL_DESC: &str = "Total number of successful retries";

// ============================================================================
// Common Labels
// ============================================================================

/// Node ID label.
pub const LABEL_NODE_ID: &str = "node_id";

/// Shard ID label.
pub const LABEL_SHARD_ID: &str = "shard_id";

/// Peer ID label.
pub const LABEL_PEER_ID: &str = "peer_id";

/// Operation result label (hit/miss, success/failure).
pub const LABEL_RESULT: &str = "result";

/// Success indicator label.
pub const LABEL_SUCCESS: &str = "success";

/// Operation type label.
pub const LABEL_OPERATION: &str = "operation";

/// Message type label.
pub const LABEL_TYPE: &str = "type";

/// Direction label (sent/received).
pub const LABEL_DIRECTION: &str = "direction";

/// Reason label (for leaves, failures, etc.).
pub const LABEL_REASON: &str = "reason";

/// Error type label.
pub const LABEL_ERROR_TYPE: &str = "error_type";

/// Module label.
pub const LABEL_MODULE: &str = "module";

/// Queue type label.
pub const LABEL_QUEUE: &str = "queue";

/// Priority label.
pub const LABEL_PRIORITY: &str = "priority";

/// Phase label (for migrations).
pub const LABEL_PHASE: &str = "phase";

/// Event type label.
pub const LABEL_EVENT_TYPE: &str = "event_type";

/// Has TTL indicator label.
pub const LABEL_HAS_TTL: &str = "has_ttl";

/// Command type label.
pub const LABEL_COMMAND_TYPE: &str = "command_type";

/// Slot ID label.
pub const LABEL_SLOT_ID: &str = "slot_id";

// ============================================================================
// Histogram Buckets
// ============================================================================

/// Latency buckets for cache operations (in seconds).
/// Optimized for sub-millisecond to 1 second range.
pub const CACHE_LATENCY_BUCKETS: &[f64] = &[
    0.0001, 0.0005, 0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0,
];

/// Latency buckets for Raft operations (in seconds).
/// Optimized for millisecond to 10 second range.
pub const RAFT_LATENCY_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// Latency buckets for network operations (in seconds).
pub const NETWORK_LATENCY_BUCKETS: &[f64] =
    &[0.0001, 0.0005, 0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0];

/// Size buckets for data (in bytes).
pub const SIZE_BUCKETS: &[f64] = &[
    64.0, 256.0, 1024.0, 4096.0, 16384.0, 65536.0, 262144.0, 1048576.0, 4194304.0,
];

/// Duration buckets for long-running operations like migrations (in seconds).
pub const MIGRATION_DURATION_BUCKETS: &[f64] = &[
    1.0, 5.0, 10.0, 30.0, 60.0, 120.0, 300.0, 600.0, 1800.0, 3600.0,
];

/// Tenure buckets for leadership duration (in seconds).
pub const TENURE_BUCKETS: &[f64] = &[
    1.0, 10.0, 60.0, 300.0, 600.0, 1800.0, 3600.0, 7200.0, 14400.0, 28800.0,
];

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_metric_names_have_prefix() {
        // Verify all metric names start with crema_
        assert!(CACHE_GET_TOTAL.starts_with("crema_"));
        assert!(RAFT_PROPOSALS_TOTAL.starts_with("crema_"));
        assert!(TRANSPORT_MESSAGES_SENT_TOTAL.starts_with("crema_"));
        assert!(CLUSTER_NODES_TOTAL.starts_with("crema_"));
        assert!(NETWORK_CONNECTIONS_ACTIVE.starts_with("crema_"));
        assert!(SHARD_OPERATIONS_TOTAL.starts_with("crema_"));
        assert!(MIGRATION_ACTIVE.starts_with("crema_"));
        assert!(CHECKPOINT_CREATED_TOTAL.starts_with("crema_"));
        assert!(SLOT_LOOKUPS_TOTAL.starts_with("crema_"));
        assert!(ERRORS_TOTAL.starts_with("crema_"));
    }

    #[test]
    fn test_buckets_are_sorted() {
        fn is_sorted(buckets: &[f64]) -> bool {
            buckets.windows(2).all(|w| w[0] < w[1])
        }

        assert!(is_sorted(CACHE_LATENCY_BUCKETS));
        assert!(is_sorted(RAFT_LATENCY_BUCKETS));
        assert!(is_sorted(NETWORK_LATENCY_BUCKETS));
        assert!(is_sorted(SIZE_BUCKETS));
        assert!(is_sorted(MIGRATION_DURATION_BUCKETS));
        assert!(is_sorted(TENURE_BUCKETS));
    }

    #[test]
    fn test_descriptions_not_empty() {
        assert!(!CACHE_GET_TOTAL_DESC.is_empty());
        assert!(!RAFT_PROPOSALS_TOTAL_DESC.is_empty());
        assert!(!ERRORS_TOTAL_DESC.is_empty());
    }
}
