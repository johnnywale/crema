//! Data transfer types and utilities for rebalancing.
//!
//! This module defines the types used for transferring cache entries
//! between nodes during rebalancing operations.

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// A cache entry being transferred between nodes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferEntry {
    /// The cache key.
    pub key: Vec<u8>,

    /// The cache value.
    pub value: Vec<u8>,

    /// Absolute expiration time in nanoseconds since UNIX epoch.
    /// None means no expiration.
    pub expires_at_nanos: Option<u64>,
}

impl TransferEntry {
    /// Create a new transfer entry without expiration.
    pub fn new(key: impl Into<Vec<u8>>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            expires_at_nanos: None,
        }
    }

    /// Create a new transfer entry with expiration.
    pub fn with_expiration(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        expires_at: SystemTime,
    ) -> Self {
        let expires_at_nanos = expires_at
            .duration_since(UNIX_EPOCH)
            .ok()
            .map(|d| d.as_nanos() as u64);

        Self {
            key: key.into(),
            value: value.into(),
            expires_at_nanos,
        }
    }

    /// Create from a key-value pair with remaining TTL.
    pub fn with_ttl(
        key: impl Into<Vec<u8>>,
        value: impl Into<Vec<u8>>,
        ttl: Duration,
    ) -> Self {
        let expires_at = SystemTime::now() + ttl;
        Self::with_expiration(key, value, expires_at)
    }

    /// Check if the entry has expired.
    pub fn is_expired(&self) -> bool {
        self.expires_at_nanos.map_or(false, |expires| {
            let now_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;
            expires <= now_nanos
        })
    }

    /// Get the remaining TTL if the entry has an expiration.
    pub fn remaining_ttl(&self) -> Option<Duration> {
        self.expires_at_nanos.and_then(|expires| {
            let now_nanos = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos() as u64;

            if expires > now_nanos {
                Some(Duration::from_nanos(expires - now_nanos))
            } else {
                None
            }
        })
    }

    /// Get the key as bytes.
    pub fn key_bytes(&self) -> Bytes {
        Bytes::from(self.key.clone())
    }

    /// Get the value as bytes.
    pub fn value_bytes(&self) -> Bytes {
        Bytes::from(self.value.clone())
    }
}

/// A batch of entries being transferred.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferBatch {
    /// The batch sequence number.
    pub sequence: u64,

    /// Entries in this batch.
    pub entries: Vec<TransferEntry>,

    /// Whether this is the last batch.
    pub is_final: bool,
}

impl TransferBatch {
    /// Create a new transfer batch.
    pub fn new(sequence: u64, entries: Vec<TransferEntry>, is_final: bool) -> Self {
        Self {
            sequence,
            entries,
            is_final,
        }
    }

    /// Create an empty final batch.
    pub fn empty_final(sequence: u64) -> Self {
        Self {
            sequence,
            entries: Vec::new(),
            is_final: true,
        }
    }

    /// Get the number of entries in the batch.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Check if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Filter out expired entries.
    pub fn filter_expired(self) -> Self {
        Self {
            sequence: self.sequence,
            entries: self.entries.into_iter().filter(|e| !e.is_expired()).collect(),
            is_final: self.is_final,
        }
    }
}

/// Request to transfer data during rebalancing.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferRequest {
    /// The rebalancing operation ID.
    pub rebalance_id: u64,

    /// Source node ID.
    pub from_node: u64,

    /// Target node ID.
    pub to_node: u64,

    /// Hash range start (for targeted transfers).
    pub hash_start: Option<u64>,

    /// Hash range end (for targeted transfers).
    pub hash_end: Option<u64>,
}

impl TransferRequest {
    /// Create a new transfer request.
    pub fn new(rebalance_id: u64, from_node: u64, to_node: u64) -> Self {
        Self {
            rebalance_id,
            from_node,
            to_node,
            hash_start: None,
            hash_end: None,
        }
    }

    /// Set the hash range for targeted transfer.
    pub fn with_hash_range(mut self, start: u64, end: u64) -> Self {
        self.hash_start = Some(start);
        self.hash_end = Some(end);
        self
    }
}

/// Response to a transfer request.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferResponse {
    /// The rebalancing operation ID.
    pub rebalance_id: u64,

    /// Whether the transfer was successful.
    pub success: bool,

    /// Number of entries transferred.
    pub entries_transferred: u64,

    /// Error message if failed.
    pub error: Option<String>,
}

impl TransferResponse {
    /// Create a success response.
    pub fn success(rebalance_id: u64, entries_transferred: u64) -> Self {
        Self {
            rebalance_id,
            success: true,
            entries_transferred,
            error: None,
        }
    }

    /// Create a failure response.
    pub fn failure(rebalance_id: u64, error: impl Into<String>) -> Self {
        Self {
            rebalance_id,
            success: false,
            entries_transferred: 0,
            error: Some(error.into()),
        }
    }
}

/// Progress of an ongoing transfer.
#[derive(Debug, Clone)]
pub struct TransferProgress {
    /// The rebalancing operation ID.
    pub rebalance_id: u64,

    /// Source node ID.
    pub from_node: u64,

    /// Target node ID.
    pub to_node: u64,

    /// Total entries to transfer (estimated).
    pub total_entries: u64,

    /// Entries transferred so far.
    pub transferred_entries: u64,

    /// Batches sent.
    pub batches_sent: u64,

    /// Batches acknowledged.
    pub batches_acked: u64,

    /// Whether the transfer is complete.
    pub complete: bool,

    /// Error if the transfer failed.
    pub error: Option<String>,
}

impl TransferProgress {
    /// Create a new progress tracker.
    pub fn new(rebalance_id: u64, from_node: u64, to_node: u64, total_entries: u64) -> Self {
        Self {
            rebalance_id,
            from_node,
            to_node,
            total_entries,
            transferred_entries: 0,
            batches_sent: 0,
            batches_acked: 0,
            complete: false,
            error: None,
        }
    }

    /// Get the progress percentage.
    pub fn percentage(&self) -> f64 {
        if self.total_entries == 0 {
            100.0
        } else {
            (self.transferred_entries as f64 / self.total_entries as f64) * 100.0
        }
    }

    /// Mark as complete.
    pub fn mark_complete(&mut self) {
        self.complete = true;
    }

    /// Mark as failed.
    pub fn mark_failed(&mut self, error: impl Into<String>) {
        self.error = Some(error.into());
        self.complete = true;
    }

    /// Update progress.
    pub fn update(&mut self, entries: u64) {
        self.transferred_entries += entries;
        self.batches_acked += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transfer_entry_no_expiration() {
        let entry = TransferEntry::new(b"key".to_vec(), b"value".to_vec());

        assert!(!entry.is_expired());
        assert!(entry.remaining_ttl().is_none());
        assert_eq!(entry.key, b"key");
        assert_eq!(entry.value, b"value");
    }

    #[test]
    fn test_transfer_entry_with_ttl() {
        let entry = TransferEntry::with_ttl(
            b"key".to_vec(),
            b"value".to_vec(),
            Duration::from_secs(3600),
        );

        assert!(!entry.is_expired());
        let ttl = entry.remaining_ttl().unwrap();
        assert!(ttl.as_secs() > 3500 && ttl.as_secs() <= 3600);
    }

    #[test]
    fn test_transfer_entry_expired() {
        let past = SystemTime::now() - Duration::from_secs(3600);
        let entry = TransferEntry::with_expiration(b"key".to_vec(), b"value".to_vec(), past);

        assert!(entry.is_expired());
        assert!(entry.remaining_ttl().is_none());
    }

    #[test]
    fn test_transfer_batch() {
        let entries = vec![
            TransferEntry::new(b"k1".to_vec(), b"v1".to_vec()),
            TransferEntry::new(b"k2".to_vec(), b"v2".to_vec()),
        ];

        let batch = TransferBatch::new(1, entries, false);

        assert_eq!(batch.len(), 2);
        assert!(!batch.is_empty());
        assert!(!batch.is_final);
        assert_eq!(batch.sequence, 1);
    }

    #[test]
    fn test_transfer_batch_filter_expired() {
        let past = SystemTime::now() - Duration::from_secs(3600);
        let entries = vec![
            TransferEntry::new(b"valid".to_vec(), b"v1".to_vec()),
            TransferEntry::with_expiration(b"expired".to_vec(), b"v2".to_vec(), past),
        ];

        let batch = TransferBatch::new(1, entries, true);
        let filtered = batch.filter_expired();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered.entries[0].key, b"valid");
    }

    #[test]
    fn test_transfer_request() {
        let req = TransferRequest::new(1, 1, 2).with_hash_range(0, 1000);

        assert_eq!(req.rebalance_id, 1);
        assert_eq!(req.from_node, 1);
        assert_eq!(req.to_node, 2);
        assert_eq!(req.hash_start, Some(0));
        assert_eq!(req.hash_end, Some(1000));
    }

    #[test]
    fn test_transfer_response() {
        let success = TransferResponse::success(1, 100);
        assert!(success.success);
        assert_eq!(success.entries_transferred, 100);
        assert!(success.error.is_none());

        let failure = TransferResponse::failure(1, "network error");
        assert!(!failure.success);
        assert_eq!(failure.error, Some("network error".to_string()));
    }

    #[test]
    fn test_transfer_progress() {
        let mut progress = TransferProgress::new(1, 1, 2, 100);

        assert_eq!(progress.percentage(), 0.0);
        assert!(!progress.complete);

        progress.update(50);
        assert_eq!(progress.percentage(), 50.0);

        progress.update(50);
        progress.mark_complete();
        assert_eq!(progress.percentage(), 100.0);
        assert!(progress.complete);
    }
}
