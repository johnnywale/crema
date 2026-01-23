//! Consistency checker for verifying data integrity after recovery.
//!
//! This module tracks expected state and verifies that nodes contain
//! the correct data after crash recovery scenarios.

use crate::error::{Error, Result};
use crate::types::NodeId;

use std::collections::{HashMap, HashSet};
use std::time::Duration;
use tracing::{debug, error, info, warn};

/// Tracks expected state and verifies consistency.
pub struct ConsistencyChecker {
    /// Expected key-value pairs.
    expected_state: HashMap<String, String>,

    /// Keys that have been deleted.
    deleted_keys: HashSet<String>,

    /// Keys with TTL (may expire).
    ttl_keys: HashSet<String>,

    /// Verification tolerance for TTL entries.
    ttl_tolerance: Duration,
}

impl Default for ConsistencyChecker {
    fn default() -> Self {
        Self::new()
    }
}

impl ConsistencyChecker {
    /// Create a new consistency checker.
    pub fn new() -> Self {
        Self {
            expected_state: HashMap::new(),
            deleted_keys: HashSet::new(),
            ttl_keys: HashSet::new(),
            ttl_tolerance: Duration::from_secs(5),
        }
    }

    /// Set TTL tolerance for verification.
    pub fn with_ttl_tolerance(mut self, tolerance: Duration) -> Self {
        self.ttl_tolerance = tolerance;
        self
    }

    /// Record a write operation.
    pub fn record_write(&mut self, key: &str, value: &str) {
        self.expected_state.insert(key.to_string(), value.to_string());
        self.deleted_keys.remove(key);
    }

    /// Record a write operation with TTL.
    pub fn record_write_with_ttl(&mut self, key: &str, value: &str) {
        self.expected_state.insert(key.to_string(), value.to_string());
        self.deleted_keys.remove(key);
        self.ttl_keys.insert(key.to_string());
    }

    /// Record a delete operation.
    pub fn record_delete(&mut self, key: &str) {
        self.expected_state.remove(key);
        self.deleted_keys.insert(key.to_string());
        self.ttl_keys.remove(key);
    }

    /// Record a clear operation.
    pub fn record_clear(&mut self) {
        self.expected_state.clear();
        self.deleted_keys.clear();
        self.ttl_keys.clear();
    }

    /// Get expected value for a key.
    pub fn expected_value(&self, key: &str) -> Option<&String> {
        if self.deleted_keys.contains(key) {
            None
        } else {
            self.expected_state.get(key)
        }
    }

    /// Check if a key has TTL.
    pub fn has_ttl(&self, key: &str) -> bool {
        self.ttl_keys.contains(key)
    }

    /// Get all expected keys.
    pub fn expected_keys(&self) -> Vec<&String> {
        self.expected_state.keys().collect()
    }

    /// Get the number of expected entries.
    pub fn expected_count(&self) -> usize {
        self.expected_state.len()
    }

    /// Verify a single node's data matches expected state.
    pub async fn verify_node(
        &self,
        cluster: &super::cluster::RecoveryTestCluster,
        node_id: NodeId,
    ) -> Result<()> {
        let mut errors = Vec::new();
        let mut checked = 0;
        let mut missing = 0;
        let mut mismatched = 0;
        let mut unexpected_present = 0;

        // Check all expected keys
        for (key, expected_value) in &self.expected_state {
            checked += 1;

            match cluster.read(node_id, key).await {
                Ok(Some(actual_value)) => {
                    if &actual_value != expected_value {
                        mismatched += 1;
                        errors.push(format!(
                            "Key '{}': expected '{}', got '{}'",
                            key, expected_value, actual_value
                        ));
                    }
                }
                Ok(None) => {
                    // TTL keys may have expired
                    if !self.ttl_keys.contains(key) {
                        missing += 1;
                        errors.push(format!(
                            "Key '{}': expected '{}', but key not found",
                            key, expected_value
                        ));
                    } else {
                        debug!(key, "TTL key may have expired (acceptable)");
                    }
                }
                Err(e) => {
                    errors.push(format!("Key '{}': read error: {}", key, e));
                }
            }
        }

        // Check that deleted keys are absent
        for key in &self.deleted_keys {
            match cluster.read(node_id, key).await {
                Ok(Some(value)) => {
                    unexpected_present += 1;
                    errors.push(format!(
                        "Key '{}': should be deleted but has value '{}'",
                        key, value
                    ));
                }
                Ok(None) => {
                    // Expected
                }
                Err(e) => {
                    errors.push(format!("Key '{}': read error checking deletion: {}", key, e));
                }
            }
        }

        if errors.is_empty() {
            info!(
                node_id,
                checked,
                "Node verification passed"
            );
            Ok(())
        } else {
            error!(
                node_id,
                missing,
                mismatched,
                unexpected_present,
                error_count = errors.len(),
                "Node verification failed"
            );

            // Log first few errors
            for (i, err) in errors.iter().take(5).enumerate() {
                error!(index = i, error = %err, "Verification error");
            }

            if errors.len() > 5 {
                error!(remaining = errors.len() - 5, "Additional errors omitted");
            }

            Err(Error::Internal(format!(
                "Consistency check failed on node {}: {} errors",
                node_id,
                errors.len()
            )))
        }
    }

    /// Verify all running nodes have consistent data.
    pub async fn verify_all_nodes(
        &self,
        cluster: &super::cluster::RecoveryTestCluster,
    ) -> Result<()> {
        let running_nodes = cluster.running_nodes();

        if running_nodes.is_empty() {
            warn!("No running nodes to verify");
            return Ok(());
        }

        info!(node_count = running_nodes.len(), "Verifying all nodes");

        let mut all_errors = Vec::new();

        for node_id in running_nodes {
            if let Err(e) = self.verify_node(cluster, node_id).await {
                all_errors.push((node_id, e));
            }
        }

        if all_errors.is_empty() {
            info!("All nodes verified successfully");
            Ok(())
        } else {
            let error_summary: String = all_errors
                .iter()
                .map(|(id, e)| format!("node {}: {}", id, e))
                .collect::<Vec<_>>()
                .join("; ");

            Err(Error::Internal(format!(
                "Consistency check failed: {}",
                error_summary
            )))
        }
    }

    /// Compare data between two nodes.
    pub async fn compare_nodes(
        &self,
        cluster: &super::cluster::RecoveryTestCluster,
        node_a: NodeId,
        node_b: NodeId,
    ) -> Result<()> {
        let mut differences = Vec::new();

        for key in self.expected_state.keys() {
            let value_a = cluster.read(node_a, key).await?;
            let value_b = cluster.read(node_b, key).await?;

            if value_a != value_b {
                differences.push(format!(
                    "Key '{}': node {} has {:?}, node {} has {:?}",
                    key, node_a, value_a, node_b, value_b
                ));
            }
        }

        if differences.is_empty() {
            info!(node_a, node_b, "Nodes have identical data");
            Ok(())
        } else {
            error!(
                node_a,
                node_b,
                diff_count = differences.len(),
                "Nodes have different data"
            );

            for diff in &differences {
                error!(diff = %diff, "Data difference");
            }

            Err(Error::Internal(format!(
                "Nodes {} and {} have {} differences",
                node_a,
                node_b,
                differences.len()
            )))
        }
    }

    /// Wait for eventual consistency across all nodes.
    pub async fn wait_for_consistency(
        &self,
        cluster: &super::cluster::RecoveryTestCluster,
        timeout: Duration,
    ) -> Result<()> {
        let start = std::time::Instant::now();

        while start.elapsed() < timeout {
            let result = self.verify_all_nodes(cluster).await;
            if result.is_ok() {
                return Ok(());
            }

            // Give nodes time to replicate
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // Final check
        self.verify_all_nodes(cluster).await
    }
}

/// Statistics from consistency verification.
#[derive(Debug, Clone)]
pub struct VerificationStats {
    /// Number of keys checked.
    pub keys_checked: usize,

    /// Number of keys that matched.
    pub keys_matched: usize,

    /// Number of keys that were missing.
    pub keys_missing: usize,

    /// Number of keys with wrong values.
    pub keys_mismatched: usize,

    /// Number of nodes verified.
    pub nodes_verified: usize,

    /// Number of nodes that passed verification.
    pub nodes_passed: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_checker_record_write() {
        let mut checker = ConsistencyChecker::new();

        checker.record_write("key1", "value1");
        checker.record_write("key2", "value2");

        assert_eq!(checker.expected_value("key1"), Some(&"value1".to_string()));
        assert_eq!(checker.expected_value("key2"), Some(&"value2".to_string()));
        assert_eq!(checker.expected_count(), 2);
    }

    #[test]
    fn test_checker_record_delete() {
        let mut checker = ConsistencyChecker::new();

        checker.record_write("key1", "value1");
        checker.record_delete("key1");

        assert_eq!(checker.expected_value("key1"), None);
        assert_eq!(checker.expected_count(), 0);
    }

    #[test]
    fn test_checker_record_clear() {
        let mut checker = ConsistencyChecker::new();

        checker.record_write("key1", "value1");
        checker.record_write("key2", "value2");
        checker.record_clear();

        assert_eq!(checker.expected_count(), 0);
    }

    #[test]
    fn test_checker_ttl_tracking() {
        let mut checker = ConsistencyChecker::new();

        checker.record_write_with_ttl("ttl_key", "value");

        assert!(checker.has_ttl("ttl_key"));
        assert!(!checker.has_ttl("other_key"));
    }

    #[test]
    fn test_checker_overwrite() {
        let mut checker = ConsistencyChecker::new();

        checker.record_write("key1", "value1");
        checker.record_write("key1", "value2");

        assert_eq!(checker.expected_value("key1"), Some(&"value2".to_string()));
        assert_eq!(checker.expected_count(), 1);
    }
}
