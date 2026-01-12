//! Rebalancing coordinator for managing data transfer operations.
//!
//! The coordinator manages the lifecycle of rebalancing operations when
//! nodes join or leave the cluster.

use crate::partitioning::{HashRing, OwnershipTracker};
use crate::rebalancing::transfer::{TransferProgress, TransferRequest};
use crate::types::NodeId;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, error, info, warn};

/// State of a rebalancing operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceState {
    /// Rebalancing has been initiated but not started.
    Pending,
    /// Calculating ownership changes.
    Calculating,
    /// Streaming data between nodes.
    Streaming,
    /// Committing new hash ring.
    Committing,
    /// Rebalancing completed successfully.
    Complete,
    /// Rebalancing failed.
    Failed,
    /// Rebalancing was cancelled.
    Cancelled,
}

/// Type of rebalancing operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RebalanceType {
    /// Node is joining the cluster.
    NodeJoin(NodeId),
    /// Node is leaving the cluster.
    NodeLeave(NodeId),
    /// Manual rebalancing triggered by admin.
    Manual,
}

/// A rebalancing operation.
#[derive(Debug, Clone)]
pub struct RebalanceOperation {
    /// Unique operation ID.
    pub id: u64,

    /// Type of rebalancing.
    pub operation_type: RebalanceType,

    /// Current state.
    pub state: RebalanceState,

    /// When the operation started.
    pub started_at: Instant,

    /// When the operation completed (if complete).
    pub completed_at: Option<Instant>,

    /// Transfer progress for each (from, to) pair.
    pub transfers: HashMap<(NodeId, NodeId), TransferProgress>,

    /// Error message if failed.
    pub error: Option<String>,

    /// Hash ring snapshot before rebalancing.
    pub old_ring: HashRing,

    /// Hash ring after rebalancing (if available).
    pub new_ring: Option<HashRing>,
}

impl RebalanceOperation {
    /// Create a new rebalancing operation.
    pub fn new(id: u64, operation_type: RebalanceType, old_ring: HashRing) -> Self {
        Self {
            id,
            operation_type,
            state: RebalanceState::Pending,
            started_at: Instant::now(),
            completed_at: None,
            transfers: HashMap::new(),
            error: None,
            old_ring,
            new_ring: None,
        }
    }

    /// Get the duration of the operation so far.
    pub fn duration(&self) -> Duration {
        self.completed_at
            .map(|t| t.duration_since(self.started_at))
            .unwrap_or_else(|| self.started_at.elapsed())
    }

    /// Get overall progress percentage.
    pub fn progress_percentage(&self) -> f64 {
        if self.transfers.is_empty() {
            return if self.state == RebalanceState::Complete {
                100.0
            } else {
                0.0
            };
        }

        let total: f64 = self.transfers.values().map(|p| p.percentage()).sum();
        total / self.transfers.len() as f64
    }

    /// Check if all transfers are complete.
    pub fn all_transfers_complete(&self) -> bool {
        self.transfers.values().all(|p| p.complete)
    }

    /// Check if any transfer has failed.
    pub fn any_transfer_failed(&self) -> bool {
        self.transfers.values().any(|p| p.error.is_some())
    }
}

/// Configuration for rebalancing.
#[derive(Debug, Clone)]
pub struct RebalanceConfig {
    /// Maximum entries per transfer batch.
    pub batch_size: usize,

    /// Timeout for each batch transfer.
    pub batch_timeout: Duration,

    /// Maximum concurrent transfers.
    pub max_concurrent_transfers: usize,

    /// Whether to allow reads during rebalancing.
    pub allow_reads_during_rebalance: bool,

    /// Whether to allow writes during rebalancing.
    pub allow_writes_during_rebalance: bool,

    /// Throttle delay between batches.
    pub throttle_delay: Duration,
}

impl Default for RebalanceConfig {
    fn default() -> Self {
        Self {
            batch_size: 1000,
            batch_timeout: Duration::from_secs(30),
            max_concurrent_transfers: 4,
            allow_reads_during_rebalance: true,
            allow_writes_during_rebalance: false,
            throttle_delay: Duration::from_millis(10),
        }
    }
}

/// Coordinates rebalancing operations.
pub struct RebalanceCoordinator {
    /// Configuration.
    config: RebalanceConfig,

    /// Next operation ID.
    next_id: AtomicU64,

    /// Current active operation (if any).
    active_operation: RwLock<Option<RebalanceOperation>>,

    /// History of completed operations.
    history: RwLock<Vec<RebalanceOperation>>,

    /// Maximum history size.
    max_history: usize,

    /// Ownership tracker reference.
    ownership_tracker: Arc<OwnershipTracker>,
}

impl RebalanceCoordinator {
    /// Create a new rebalancing coordinator.
    pub fn new(ownership_tracker: Arc<OwnershipTracker>, config: RebalanceConfig) -> Self {
        Self {
            config,
            next_id: AtomicU64::new(1),
            active_operation: RwLock::new(None),
            history: RwLock::new(Vec::new()),
            max_history: 100,
            ownership_tracker,
        }
    }

    /// Create with default config.
    pub fn with_defaults(ownership_tracker: Arc<OwnershipTracker>) -> Self {
        Self::new(ownership_tracker, RebalanceConfig::default())
    }

    /// Get the configuration.
    pub fn config(&self) -> &RebalanceConfig {
        &self.config
    }

    /// Check if a rebalancing operation is in progress.
    pub fn is_rebalancing(&self) -> bool {
        self.active_operation.read().is_some()
    }

    /// Get the current operation state.
    pub fn current_state(&self) -> Option<RebalanceState> {
        self.active_operation.read().as_ref().map(|op| op.state)
    }

    /// Get current operation progress.
    pub fn current_progress(&self) -> Option<f64> {
        self.active_operation
            .read()
            .as_ref()
            .map(|op| op.progress_percentage())
    }

    /// Start a rebalancing operation for a node join.
    pub fn start_node_join(&self, node_id: NodeId) -> Result<u64, RebalanceError> {
        self.start_operation(RebalanceType::NodeJoin(node_id))
    }

    /// Start a rebalancing operation for a node leave.
    pub fn start_node_leave(&self, node_id: NodeId) -> Result<u64, RebalanceError> {
        self.start_operation(RebalanceType::NodeLeave(node_id))
    }

    /// Start a manual rebalancing operation.
    pub fn start_manual(&self) -> Result<u64, RebalanceError> {
        self.start_operation(RebalanceType::Manual)
    }

    /// Start a rebalancing operation.
    fn start_operation(&self, operation_type: RebalanceType) -> Result<u64, RebalanceError> {
        let mut active = self.active_operation.write();

        if active.is_some() {
            return Err(RebalanceError::AlreadyInProgress);
        }

        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let old_ring = self.ownership_tracker.ring().clone();

        let operation = RebalanceOperation::new(id, operation_type, old_ring);

        info!(
            id,
            ?operation_type,
            "Starting rebalancing operation"
        );

        *active = Some(operation);

        Ok(id)
    }

    /// Calculate transfers needed for the current operation.
    pub fn calculate_transfers(&self) -> Result<Vec<TransferRequest>, RebalanceError> {
        let mut active = self.active_operation.write();
        let operation = active.as_mut().ok_or(RebalanceError::NoActiveOperation)?;

        if operation.state != RebalanceState::Pending {
            return Err(RebalanceError::InvalidState(operation.state));
        }

        operation.state = RebalanceState::Calculating;

        let requests = match operation.operation_type {
            RebalanceType::NodeJoin(node_id) => {
                self.calculate_join_transfers(operation, node_id)
            }
            RebalanceType::NodeLeave(node_id) => {
                self.calculate_leave_transfers(operation, node_id)
            }
            RebalanceType::Manual => {
                // For manual rebalancing, we'd need more complex logic
                // For now, just return empty (no-op)
                Vec::new()
            }
        };

        operation.state = RebalanceState::Streaming;

        debug!(
            operation_id = operation.id,
            transfer_count = requests.len(),
            "Calculated transfers"
        );

        Ok(requests)
    }

    /// Calculate transfers for a node join.
    fn calculate_join_transfers(
        &self,
        operation: &mut RebalanceOperation,
        new_node: NodeId,
    ) -> Vec<TransferRequest> {
        let transfers = self.ownership_tracker.calculate_transfer_on_add(new_node);
        let mut requests = Vec::new();

        for ((from, to), count) in transfers {
            let progress = TransferProgress::new(operation.id, from, to, count as u64);
            operation.transfers.insert((from, to), progress);

            requests.push(TransferRequest::new(operation.id, from, to));
        }

        // Create new ring with the node added
        let mut new_ring = operation.old_ring.clone();
        new_ring.add_node(new_node);
        operation.new_ring = Some(new_ring);

        requests
    }

    /// Calculate transfers for a node leave.
    fn calculate_leave_transfers(
        &self,
        operation: &mut RebalanceOperation,
        leaving_node: NodeId,
    ) -> Vec<TransferRequest> {
        let transfers = self.ownership_tracker.calculate_transfer_on_remove(leaving_node);
        let mut requests = Vec::new();

        for ((from, to), count) in transfers {
            let progress = TransferProgress::new(operation.id, from, to, count as u64);
            operation.transfers.insert((from, to), progress);

            requests.push(TransferRequest::new(operation.id, from, to));
        }

        // Create new ring with the node removed
        let mut new_ring = operation.old_ring.clone();
        new_ring.remove_node(leaving_node);
        operation.new_ring = Some(new_ring);

        requests
    }

    /// Update progress for a transfer.
    pub fn update_transfer_progress(
        &self,
        from: NodeId,
        to: NodeId,
        entries: u64,
    ) -> Result<(), RebalanceError> {
        let mut active = self.active_operation.write();
        let operation = active.as_mut().ok_or(RebalanceError::NoActiveOperation)?;

        if let Some(progress) = operation.transfers.get_mut(&(from, to)) {
            progress.update(entries);
            debug!(
                operation_id = operation.id,
                from,
                to,
                entries,
                total = progress.transferred_entries,
                "Updated transfer progress"
            );
        }

        Ok(())
    }

    /// Mark a transfer as complete.
    pub fn complete_transfer(&self, from: NodeId, to: NodeId) -> Result<(), RebalanceError> {
        let mut active = self.active_operation.write();
        let operation = active.as_mut().ok_or(RebalanceError::NoActiveOperation)?;

        if let Some(progress) = operation.transfers.get_mut(&(from, to)) {
            progress.mark_complete();
            info!(
                operation_id = operation.id,
                from,
                to,
                entries = progress.transferred_entries,
                "Transfer complete"
            );
        }

        // Check if all transfers are complete
        if operation.all_transfers_complete() {
            operation.state = RebalanceState::Committing;
        }

        Ok(())
    }

    /// Mark a transfer as failed.
    pub fn fail_transfer(
        &self,
        from: NodeId,
        to: NodeId,
        error: impl Into<String>,
    ) -> Result<(), RebalanceError> {
        let mut active = self.active_operation.write();
        let operation = active.as_mut().ok_or(RebalanceError::NoActiveOperation)?;

        let error_str = error.into();
        if let Some(progress) = operation.transfers.get_mut(&(from, to)) {
            progress.mark_failed(&error_str);
            error!(
                operation_id = operation.id,
                from,
                to,
                error = %error_str,
                "Transfer failed"
            );
        }

        // Mark the whole operation as failed
        operation.state = RebalanceState::Failed;
        operation.error = Some(error_str);
        operation.completed_at = Some(Instant::now());

        Ok(())
    }

    /// Commit the new hash ring after successful transfers.
    pub fn commit(&self) -> Result<HashRing, RebalanceError> {
        let mut active = self.active_operation.write();
        let operation = active.as_mut().ok_or(RebalanceError::NoActiveOperation)?;

        if operation.state != RebalanceState::Committing {
            return Err(RebalanceError::InvalidState(operation.state));
        }

        let new_ring = operation
            .new_ring
            .clone()
            .ok_or(RebalanceError::NoNewRing)?;

        operation.state = RebalanceState::Complete;
        operation.completed_at = Some(Instant::now());

        info!(
            operation_id = operation.id,
            duration_ms = operation.duration().as_millis(),
            "Rebalancing complete"
        );

        Ok(new_ring)
    }

    /// Cancel the current operation.
    pub fn cancel(&self) -> Result<(), RebalanceError> {
        let mut active = self.active_operation.write();
        let operation = active.as_mut().ok_or(RebalanceError::NoActiveOperation)?;

        if operation.state == RebalanceState::Complete
            || operation.state == RebalanceState::Failed
        {
            return Err(RebalanceError::AlreadyComplete);
        }

        operation.state = RebalanceState::Cancelled;
        operation.completed_at = Some(Instant::now());

        warn!(operation_id = operation.id, "Rebalancing cancelled");

        Ok(())
    }

    /// Finish the current operation and move it to history.
    pub fn finish(&self) -> Result<RebalanceOperation, RebalanceError> {
        let mut active = self.active_operation.write();
        let operation = active.take().ok_or(RebalanceError::NoActiveOperation)?;

        if !matches!(
            operation.state,
            RebalanceState::Complete | RebalanceState::Failed | RebalanceState::Cancelled
        ) {
            // Put it back
            *active = Some(operation);
            return Err(RebalanceError::NotComplete);
        }

        // Add to history
        let mut history = self.history.write();
        history.push(operation.clone());

        // Trim history if needed
        while history.len() > self.max_history {
            history.remove(0);
        }

        drop(history);

        Ok(operation)
    }

    /// Get the history of completed operations.
    pub fn history(&self) -> Vec<RebalanceOperation> {
        self.history.read().clone()
    }

    /// Get info about the current operation.
    pub fn current_operation_info(&self) -> Option<RebalanceOperationInfo> {
        self.active_operation.read().as_ref().map(|op| {
            RebalanceOperationInfo {
                id: op.id,
                operation_type: op.operation_type,
                state: op.state,
                progress: op.progress_percentage(),
                duration: op.duration(),
                transfer_count: op.transfers.len(),
            }
        })
    }
}

impl std::fmt::Debug for RebalanceCoordinator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RebalanceCoordinator")
            .field("is_rebalancing", &self.is_rebalancing())
            .field("current_state", &self.current_state())
            .finish()
    }
}

/// Summary info about a rebalancing operation.
#[derive(Debug, Clone)]
pub struct RebalanceOperationInfo {
    /// Operation ID.
    pub id: u64,
    /// Type of operation.
    pub operation_type: RebalanceType,
    /// Current state.
    pub state: RebalanceState,
    /// Progress percentage.
    pub progress: f64,
    /// Duration so far.
    pub duration: Duration,
    /// Number of transfers.
    pub transfer_count: usize,
}

/// Errors that can occur during rebalancing.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RebalanceError {
    #[error("rebalancing already in progress")]
    AlreadyInProgress,

    #[error("no active rebalancing operation")]
    NoActiveOperation,

    #[error("invalid state for this operation: {0:?}")]
    InvalidState(RebalanceState),

    #[error("operation already complete")]
    AlreadyComplete,

    #[error("operation not complete")]
    NotComplete,

    #[error("no new ring calculated")]
    NoNewRing,

    #[error("transfer failed: {0}")]
    TransferFailed(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_coordinator() -> RebalanceCoordinator {
        let tracker = Arc::new(OwnershipTracker::new(1, 2));
        tracker.add_node(1);
        tracker.add_node(2);
        RebalanceCoordinator::with_defaults(tracker)
    }

    #[test]
    fn test_start_operation() {
        let coord = create_coordinator();

        let id = coord.start_node_join(3).unwrap();
        assert!(id > 0);
        assert!(coord.is_rebalancing());
        assert_eq!(coord.current_state(), Some(RebalanceState::Pending));
    }

    #[test]
    fn test_already_in_progress() {
        let coord = create_coordinator();

        coord.start_node_join(3).unwrap();

        // Second start should fail
        let result = coord.start_node_join(4);
        assert!(matches!(result, Err(RebalanceError::AlreadyInProgress)));
    }

    #[test]
    fn test_calculate_transfers() {
        let coord = create_coordinator();

        coord.start_node_join(3).unwrap();
        let requests = coord.calculate_transfers().unwrap();

        // Should have some transfers
        assert!(!requests.is_empty());
        assert_eq!(coord.current_state(), Some(RebalanceState::Streaming));
    }

    #[test]
    fn test_cancel_operation() {
        let coord = create_coordinator();

        coord.start_node_join(3).unwrap();
        coord.cancel().unwrap();

        assert_eq!(coord.current_state(), Some(RebalanceState::Cancelled));
    }

    #[test]
    fn test_operation_lifecycle() {
        let coord = create_coordinator();

        // Start
        coord.start_node_join(3).unwrap();

        // Calculate
        let requests = coord.calculate_transfers().unwrap();

        // Complete all transfers
        for req in &requests {
            coord.complete_transfer(req.from_node, req.to_node).unwrap();
        }

        // Should be in committing state
        assert_eq!(coord.current_state(), Some(RebalanceState::Committing));

        // Commit
        let new_ring = coord.commit().unwrap();
        assert!(new_ring.contains_node(3));

        // Finish
        let operation = coord.finish().unwrap();
        assert_eq!(operation.state, RebalanceState::Complete);
        assert!(!coord.is_rebalancing());
    }

    #[test]
    fn test_operation_info() {
        let coord = create_coordinator();

        coord.start_node_join(3).unwrap();

        let info = coord.current_operation_info().unwrap();
        assert!(matches!(info.operation_type, RebalanceType::NodeJoin(3)));
        assert_eq!(info.state, RebalanceState::Pending);
    }

    #[test]
    fn test_node_leave() {
        let coord = create_coordinator();

        coord.start_node_leave(2).unwrap();
        let requests = coord.calculate_transfers().unwrap();

        // All transfers should be FROM node 2
        for req in &requests {
            assert_eq!(req.from_node, 2);
        }
    }
}
