//! Unit tests for the migration system.
//!
//! These tests cover all migration phases and steps using a mock data accessor
//! to simulate real data operations without requiring a full cluster setup.

#[cfg(test)]
mod tests {
    use crate::error::Result;
    use crate::multiraft::{
        MigrationDataAccessor, MigrationId, MigrationRaftCommand, ShardId, ShardRaftNode, SlotId,
        SlotLogEntry, SlotLogOperation, SlotMigrationPhase, SlotMigrationRecord, SlotMigrator,
        SlotMigratorConfig, SlotTable,
    };
    use bytes::Bytes;
    use parking_lot::RwLock;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    /// Get current time in milliseconds since Unix epoch.
    fn now_ms() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_millis() as u64)
            .unwrap_or(0)
    }

    /// Create a test migrator with default configuration.
    fn create_test_migrator() -> SlotMigrator {
        let slot_table = Arc::new(SlotTable::new(4));
        SlotMigrator::with_defaults(slot_table)
    }

    // ==================== Mock Data Accessor ====================

    /// Mock data accessor for testing migration phases with real data.
    #[derive(Debug)]
    struct MockDataAccessor {
        /// Keys stored per shard: shard_id -> slot_id -> Vec<(key, value)>
        data: RwLock<HashMap<ShardId, HashMap<SlotId, Vec<(Vec<u8>, Bytes)>>>>,
        /// Log entries per shard
        log_entries: RwLock<HashMap<ShardId, Vec<SlotLogEntry>>>,
        /// Current log index per shard
        log_index: RwLock<HashMap<ShardId, u64>>,
        /// Which node is the source leader (if any)
        source_leader: RwLock<Option<ShardId>>,
        /// Which node is the target leader (if any)
        target_leader: RwLock<Option<ShardId>>,
    }

    impl Default for MockDataAccessor {
        fn default() -> Self {
            Self::new()
        }
    }

    impl MockDataAccessor {
        fn new() -> Self {
            Self {
                data: RwLock::new(HashMap::new()),
                log_entries: RwLock::new(HashMap::new()),
                log_index: RwLock::new(HashMap::new()),
                source_leader: RwLock::new(None),
                target_leader: RwLock::new(None),
            }
        }

        /// Add test data for a specific shard and slot
        fn add_data(&self, shard_id: ShardId, slot_id: SlotId, keys: Vec<(Vec<u8>, Bytes)>) {
            let mut data = self.data.write();
            data.entry(shard_id)
                .or_insert_with(HashMap::new)
                .insert(slot_id, keys);
        }

        /// Add log entries for a shard
        fn add_log_entries(&self, shard_id: ShardId, entries: Vec<SlotLogEntry>) {
            self.log_entries.write().insert(shard_id, entries);
        }

        /// Set log index for a shard
        fn set_log_index(&self, shard_id: ShardId, index: u64) {
            self.log_index.write().insert(shard_id, index);
        }

        /// Set this accessor as source leader for a shard
        #[allow(dead_code)]
        fn set_source_leader(&self, shard_id: ShardId) {
            *self.source_leader.write() = Some(shard_id);
        }

        /// Set this accessor as target leader for a shard
        fn set_target_leader(&self, shard_id: ShardId) {
            *self.target_leader.write() = Some(shard_id);
        }

        /// Get data count for a shard/slot
        fn get_data_count(&self, shard_id: ShardId, slot_id: SlotId) -> usize {
            self.data
                .read()
                .get(&shard_id)
                .and_then(|s| s.get(&slot_id))
                .map(|v| v.len())
                .unwrap_or(0)
        }
    }

    #[async_trait::async_trait]
    impl MigrationDataAccessor for MockDataAccessor {
        async fn scan_slot_keys(
            &self,
            shard_id: ShardId,
            slot_id: SlotId,
            cursor: Option<&[u8]>,
            limit: usize,
        ) -> Result<(Vec<Vec<u8>>, Option<Vec<u8>>)> {
            let data = self.data.read();
            let empty = Vec::new();
            let slot_data = data
                .get(&shard_id)
                .and_then(|s| s.get(&slot_id))
                .unwrap_or(&empty);

            let mut keys = Vec::new();
            let mut next_cursor = None;

            for (key, _) in slot_data {
                // Skip until cursor
                if let Some(c) = cursor {
                    if key.as_slice() <= c {
                        continue;
                    }
                }

                keys.push(key.clone());
                if keys.len() >= limit {
                    next_cursor = Some(key.clone());
                    break;
                }
            }

            Ok((keys, next_cursor))
        }

        async fn get_keys(
            &self,
            shard_id: ShardId,
            keys: &[Vec<u8>],
        ) -> Result<Vec<(Vec<u8>, Option<Bytes>)>> {
            let data = self.data.read();
            let mut results = Vec::new();

            for key in keys {
                let mut found_value = None;
                if let Some(shard_data) = data.get(&shard_id) {
                    for slot_data in shard_data.values() {
                        for (k, v) in slot_data {
                            if k == key {
                                found_value = Some(v.clone());
                                break;
                            }
                        }
                    }
                }
                results.push((key.clone(), found_value));
            }

            Ok(results)
        }

        async fn import_keys(&self, shard_id: ShardId, data: &[(Vec<u8>, Bytes)]) -> Result<()> {
            let mut store = self.data.write();
            let shard_data = store.entry(shard_id).or_insert_with(HashMap::new);

            for (key, value) in data {
                // Calculate slot for key (simplified - just use slot 0 for tests)
                let slot_id = 0;
                let slot_data = shard_data.entry(slot_id).or_insert_with(Vec::new);

                // Check if key exists, update if so
                let mut found = false;
                for (k, v) in slot_data.iter_mut() {
                    if k == key {
                        *v = value.clone();
                        found = true;
                        break;
                    }
                }
                if !found {
                    slot_data.push((key.clone(), value.clone()));
                }
            }

            Ok(())
        }

        async fn current_log_index(&self, shard_id: ShardId) -> Result<u64> {
            Ok(*self.log_index.read().get(&shard_id).unwrap_or(&0))
        }

        async fn get_slot_log_entries(
            &self,
            shard_id: ShardId,
            _slot_id: SlotId,
            from_index: u64,
            limit: usize,
        ) -> Result<Vec<SlotLogEntry>> {
            let entries = self.log_entries.read();
            let shard_entries = entries.get(&shard_id).cloned().unwrap_or_default();

            let filtered: Vec<_> = shard_entries
                .into_iter()
                .filter(|e| e.index >= from_index)
                .take(limit)
                .collect();

            Ok(filtered)
        }

        async fn has_key(&self, shard_id: ShardId, key: &[u8]) -> Result<bool> {
            let data = self.data.read();
            if let Some(shard_data) = data.get(&shard_id) {
                for slot_data in shard_data.values() {
                    for (k, _) in slot_data {
                        if k.as_slice() == key {
                            return Ok(true);
                        }
                    }
                }
            }
            Ok(false)
        }

        async fn get_key(&self, shard_id: ShardId, key: &[u8]) -> Result<Option<Bytes>> {
            let data = self.data.read();
            if let Some(shard_data) = data.get(&shard_id) {
                for slot_data in shard_data.values() {
                    for (k, v) in slot_data {
                        if k.as_slice() == key {
                            return Ok(Some(v.clone()));
                        }
                    }
                }
            }
            Ok(None)
        }

        async fn put_key(&self, shard_id: ShardId, key: &[u8], value: &[u8]) -> Result<()> {
            self.import_keys(shard_id, &[(key.to_vec(), Bytes::copy_from_slice(value))])
                .await
        }

        async fn delete_slot_data(&self, shard_id: ShardId, slot_id: SlotId) -> Result<()> {
            let mut data = self.data.write();
            if let Some(shard_data) = data.get_mut(&shard_id) {
                shard_data.remove(&slot_id);
            }
            Ok(())
        }

        async fn count_keys_in_slot(&self, shard_id: ShardId, slot_id: SlotId) -> Result<u64> {
            Ok(self.get_data_count(shard_id, slot_id) as u64)
        }

        async fn checksum_slot(&self, shard_id: ShardId, slot_id: SlotId) -> Result<u64> {
            // Simple checksum: key count
            Ok(self.get_data_count(shard_id, slot_id) as u64)
        }

        fn is_source_shard_leader(&self, from_shard: ShardId) -> bool {
            self.source_leader
                .read()
                .map(|s| s == from_shard)
                .unwrap_or(true)
        }

        fn is_target_shard_leader(&self, to_shard: ShardId) -> bool {
            self.target_leader
                .read()
                .map(|s| s == to_shard)
                .unwrap_or(true)
        }

        fn get_shard_raft_node(&self, _shard_id: ShardId) -> Option<Arc<ShardRaftNode>> {
            None // No Raft node in mock
        }
    }

    // ==================== Phase Ordering Tests ====================

    #[test]
    fn test_phase_ordering() {
        let pending = SlotMigrationPhase::Pending;
        let claimed = SlotMigrationPhase::Claimed {
            owner_node: 1,
            claim_epoch: 1,
            claimed_at: 0,
        };
        let scanning = SlotMigrationPhase::Scanning {
            cursor: None,
            keys_found: 0,
        };
        let streaming = SlotMigrationPhase::Streaming {
            keys_total: 10,
            keys_transferred: 0,
            last_key: None,
        };
        let catching_up = SlotMigrationPhase::CatchingUp { from_log_index: 0 };
        let prepared = SlotMigrationPhase::Prepared {
            prepared_at: 0,
            target_commit_index: 100,
            validation_checksum: 0,
        };
        let completed = SlotMigrationPhase::Completed { completed_at: 0 };
        let cleaned = SlotMigrationPhase::Cleaned { cleaned_at: 0 };
        let failed = SlotMigrationPhase::Failed {
            error: "test".into(),
            failed_at: 0,
            retry_count: 0,
        };

        // Test ordinal progression
        assert!(claimed.is_more_advanced_than(&pending));
        assert!(scanning.is_more_advanced_than(&claimed));
        assert!(streaming.is_more_advanced_than(&scanning));
        assert!(catching_up.is_more_advanced_than(&streaming));
        assert!(prepared.is_more_advanced_than(&catching_up));
        assert!(completed.is_more_advanced_than(&prepared));
        assert!(cleaned.is_more_advanced_than(&completed));

        // Failed should never be more advanced
        assert!(!failed.is_more_advanced_than(&pending));
        assert!(!failed.is_more_advanced_than(&completed));

        // Same phase should not be more advanced
        assert!(!pending.is_more_advanced_than(&pending));
        assert!(!claimed.is_more_advanced_than(&claimed));
    }

    #[test]
    fn test_phase_actively_transferring() {
        let scanning = SlotMigrationPhase::Scanning {
            cursor: None,
            keys_found: 0,
        };
        let streaming = SlotMigrationPhase::Streaming {
            keys_total: 10,
            keys_transferred: 0,
            last_key: None,
        };
        let catching_up = SlotMigrationPhase::CatchingUp { from_log_index: 0 };
        let pending = SlotMigrationPhase::Pending;
        let prepared = SlotMigrationPhase::Prepared {
            prepared_at: 0,
            target_commit_index: 100,
            validation_checksum: 0,
        };

        assert!(scanning.is_actively_transferring());
        assert!(streaming.is_actively_transferring());
        assert!(catching_up.is_actively_transferring());
        assert!(!pending.is_actively_transferring());
        assert!(!prepared.is_actively_transferring());
    }

    // ==================== Process Scanning Tests ====================

    #[tokio::test]
    async fn test_process_scanning_with_data() {
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // Add test data to source shard (shard 0, slot 0)
        accessor.add_data(
            0,
            0,
            vec![
                (b"key1".to_vec(), Bytes::from("value1")),
                (b"key2".to_vec(), Bytes::from("value2")),
                (b"key3".to_vec(), Bytes::from("value3")),
            ],
        );

        // Register and transition to scanning
        migrator.register_migration(0, 0, 3);
        migrator.transition_to_scanning(0).await.unwrap();

        // Process scanning - finds all keys in one batch (< batch_size)
        // Should transition to Streaming since there's no cursor (all data scanned)
        migrator
            .process_scanning(0, None, 0, &accessor)
            .await
            .unwrap();

        // Should transition to Streaming with 3 keys
        let record = migrator.get_migration(0).unwrap();
        assert!(
            matches!(
                record.phase,
                SlotMigrationPhase::Streaming { keys_total: 3, .. }
            ),
            "Expected Streaming phase with 3 keys, got {:?}",
            record.phase
        );
    }

    #[tokio::test]
    async fn test_process_scanning_with_cursor() {
        let slot_table = Arc::new(SlotTable::new(4));
        let config = SlotMigratorConfig {
            scan_batch_size: 2, // Small batch to test cursor
            ..Default::default()
        };
        let migrator = SlotMigrator::new(0, slot_table, config);
        let accessor = Arc::new(MockDataAccessor::new());

        // Add more data than batch size
        accessor.add_data(
            0,
            0,
            vec![
                (b"key1".to_vec(), Bytes::from("value1")),
                (b"key2".to_vec(), Bytes::from("value2")),
                (b"key3".to_vec(), Bytes::from("value3")),
                (b"key4".to_vec(), Bytes::from("value4")),
            ],
        );

        migrator.register_migration(0, 0, 3);
        migrator.transition_to_scanning(0).await.unwrap();

        // First scan batch
        migrator
            .process_scanning(0, None, 0, &accessor)
            .await
            .unwrap();

        let record = migrator.get_migration(0).unwrap();
        // Should still be scanning with a cursor
        match &record.phase {
            SlotMigrationPhase::Scanning { cursor, keys_found } => {
                assert!(cursor.is_some(), "Should have a cursor for more scanning");
                assert_eq!(*keys_found, 2, "Should have found 2 keys in first batch");
            }
            _ => panic!("Expected Scanning phase, got {:?}", record.phase),
        }
    }

    #[tokio::test]
    async fn test_process_scanning_empty_source() {
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // No data in source shard

        migrator.register_migration(0, 0, 3);
        migrator.transition_to_scanning(0).await.unwrap();

        // Process scanning
        migrator
            .process_scanning(0, None, 0, &accessor)
            .await
            .unwrap();

        // Should transition to Streaming with 0 keys
        let record = migrator.get_migration(0).unwrap();
        assert!(
            matches!(
                record.phase,
                SlotMigrationPhase::Streaming { keys_total: 0, .. }
            ),
            "Expected Streaming phase with 0 keys, got {:?}",
            record.phase
        );
    }

    // ==================== Process Streaming Tests ====================

    #[tokio::test]
    async fn test_process_streaming_transfers_data() {
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // Add test data to source shard
        accessor.add_data(
            0,
            0,
            vec![
                (b"key1".to_vec(), Bytes::from("value1")),
                (b"key2".to_vec(), Bytes::from("value2")),
            ],
        );
        accessor.set_log_index(0, 100);

        // Register and manually set to Streaming phase
        migrator.register_migration(0, 0, 3);
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&0) {
                record.set_phase(SlotMigrationPhase::Streaming {
                    keys_total: 2,
                    keys_transferred: 0,
                    last_key: None,
                });
            }
        }

        // Process streaming
        migrator
            .process_streaming(0, 2, 0, None, &accessor)
            .await
            .unwrap();

        // Should have transferred keys to target shard
        assert_eq!(
            accessor.get_data_count(3, 0),
            2,
            "Target should have 2 keys"
        );
    }

    #[tokio::test]
    async fn test_process_streaming_transitions_to_catching_up() {
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // No data left to transfer (empty source)
        accessor.set_log_index(0, 50);

        migrator.register_migration(0, 0, 3);
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&0) {
                record.set_phase(SlotMigrationPhase::Streaming {
                    keys_total: 0,
                    keys_transferred: 0,
                    last_key: None,
                });
            }
        }

        // Process streaming with no keys to transfer
        migrator
            .process_streaming(0, 0, 0, None, &accessor)
            .await
            .unwrap();

        // Should transition to CatchingUp
        let record = migrator.get_migration(0).unwrap();
        match &record.phase {
            SlotMigrationPhase::CatchingUp { from_log_index } => {
                assert_eq!(
                    *from_log_index, 50,
                    "Should start catching up from index 50"
                );
            }
            _ => panic!("Expected CatchingUp phase, got {:?}", record.phase),
        }
    }

    // ==================== Process Catching Up Tests ====================

    #[tokio::test]
    async fn test_process_catching_up_replays_log_entries() {
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // Add log entries to replay
        accessor.add_log_entries(
            0,
            vec![
                SlotLogEntry {
                    index: 100,
                    key: b"newkey1".to_vec(),
                    operation: SlotLogOperation::Put,
                    value: Some(b"newvalue1".to_vec()),
                },
                SlotLogEntry {
                    index: 101,
                    key: b"newkey2".to_vec(),
                    operation: SlotLogOperation::Put,
                    value: Some(b"newvalue2".to_vec()),
                },
            ],
        );

        migrator.register_migration(0, 0, 3);
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&0) {
                record.set_phase(SlotMigrationPhase::CatchingUp {
                    from_log_index: 100,
                });
            }
        }

        // Process catching up
        migrator
            .process_catching_up(0, 100, &accessor, &None)
            .await
            .unwrap();

        // Should have replayed entries to target
        assert!(
            accessor.has_key(3, b"newkey1").await.unwrap(),
            "Should have replayed key1"
        );
        assert!(
            accessor.has_key(3, b"newkey2").await.unwrap(),
            "Should have replayed key2"
        );

        // from_log_index should be updated
        let record = migrator.get_migration(0).unwrap();
        match &record.phase {
            SlotMigrationPhase::CatchingUp { from_log_index } => {
                assert_eq!(*from_log_index, 102, "Should advance log index");
            }
            _ => panic!("Expected CatchingUp phase, got {:?}", record.phase),
        }
    }

    #[tokio::test]
    async fn test_process_catching_up_requires_raft_for_transition() {
        // After split-brain fix: transition to Prepared requires Raft consensus.
        // Without a Raft proposer, the transition should fail.
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // No more log entries (empty)
        accessor.add_log_entries(0, vec![]);
        accessor.set_log_index(3, 200); // Target commit index

        // Add same data to both source and target for validation
        accessor.add_data(0, 0, vec![(b"key1".to_vec(), Bytes::from("value1"))]);
        accessor.add_data(3, 0, vec![(b"key1".to_vec(), Bytes::from("value1"))]);

        migrator.register_migration(0, 0, 3);
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&0) {
                record.set_phase(SlotMigrationPhase::CatchingUp {
                    from_log_index: 100,
                });
            }
        }

        // Process catching up - should fail without Raft proposer
        let result = migrator.process_catching_up(0, 100, &accessor, &None).await;

        // Without Raft proposer, transition to Prepared should fail
        assert!(
            result.is_err(),
            "process_catching_up should fail without Raft proposer"
        );

        // Should still be in CatchingUp phase
        let record = migrator.get_migration(0).unwrap();
        assert!(
            matches!(record.phase, SlotMigrationPhase::CatchingUp { .. }),
            "Expected CatchingUp phase (no transition), got {:?}",
            record.phase
        );
    }

    // ==================== Validation Tests ====================

    #[tokio::test]
    async fn test_validate_migration_success() {
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // Same data on source and target
        let data = vec![
            (b"key1".to_vec(), Bytes::from("value1")),
            (b"key2".to_vec(), Bytes::from("value2")),
        ];
        accessor.add_data(0, 0, data.clone());
        accessor.add_data(3, 0, data);
        accessor.set_log_index(3, 100);

        let record = SlotMigrationRecord::new(0, 0, 3);
        let result = migrator.validate_migration(&record, &accessor).await;

        assert!(result.is_ok(), "Validation should succeed");
        let validation = result.unwrap();
        assert_eq!(validation.key_count, 2);
        assert_eq!(validation.raft_commit_index, 100);
    }

    #[tokio::test]
    async fn test_validate_migration_key_count_mismatch() {
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // Different key counts
        accessor.add_data(
            0,
            0,
            vec![
                (b"key1".to_vec(), Bytes::from("value1")),
                (b"key2".to_vec(), Bytes::from("value2")),
            ],
        );
        accessor.add_data(3, 0, vec![(b"key1".to_vec(), Bytes::from("value1"))]);

        let record = SlotMigrationRecord::new(0, 0, 3);
        let result = migrator.validate_migration(&record, &accessor).await;

        assert!(result.is_err(), "Validation should fail");
        let err = result.unwrap_err();
        assert!(
            err.to_string().contains("Key count mismatch"),
            "Error should mention key count: {}",
            err
        );
    }

    // ==================== Claim Migration Tests ====================

    #[tokio::test]
    async fn test_claim_migration_fails_without_raft_proposer() {
        // After split-brain fix: local fallback is removed.
        // Without a Raft proposer, claim_migration should fail instead of
        // silently falling back to local-only state changes.
        let slot_table = Arc::new(SlotTable::new(4));
        let migrator = SlotMigrator::new(1, slot_table, SlotMigratorConfig::default());
        let accessor = Arc::new(MockDataAccessor::new());
        accessor.set_target_leader(3);

        migrator.register_migration(0, 0, 3);

        let record = migrator.get_migration(0).unwrap();

        // Without a Raft proposer, this should fail rather than use local fallback
        let result = migrator.claim_migration(0, &record, &accessor).await;

        assert!(
            result.is_err(),
            "claim_migration should fail without Raft proposer (no local fallback)"
        );

        // Migration should still be in Pending state
        let record = migrator.get_migration(0).unwrap();
        assert!(
            matches!(record.phase, SlotMigrationPhase::Pending),
            "Migration should remain in Pending state when claim fails"
        );
    }

    #[tokio::test]
    async fn test_claim_migration_already_claimed_by_other() {
        let slot_table = Arc::new(SlotTable::new(4));
        let migrator = SlotMigrator::new(1, slot_table, SlotMigratorConfig::default());
        let accessor = Arc::new(MockDataAccessor::new());

        migrator.register_migration(0, 0, 3);

        // Pre-claim by node 2
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&0) {
                record.set_phase(SlotMigrationPhase::Claimed {
                    owner_node: 2,
                    claim_epoch: 1,
                    claimed_at: now_ms(),
                });
            }
        }

        let record = migrator.get_migration(0).unwrap();
        // Should not overwrite claim by other node
        migrator
            .claim_migration(0, &record, &accessor)
            .await
            .unwrap();

        let record = migrator.get_migration(0).unwrap();
        assert!(
            record.phase.is_claimed_by(2),
            "Should still be claimed by node 2"
        );
    }

    // ==================== Transition Tests ====================

    #[tokio::test]
    async fn test_transition_to_prepared_fails_without_raft() {
        // After split-brain fix: transition_to_prepared requires Raft consensus.
        // Without a Raft proposer configured, it should fail.
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // Set up matching data for validation
        accessor.add_data(0, 0, vec![(b"key1".to_vec(), Bytes::from("value1"))]);
        accessor.add_data(3, 0, vec![(b"key1".to_vec(), Bytes::from("value1"))]);
        accessor.set_log_index(3, 150);

        migrator.register_migration(0, 0, 3);
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&0) {
                record.set_phase(SlotMigrationPhase::CatchingUp {
                    from_log_index: 100,
                });
            }
        }

        // Transition to prepared should fail without Raft proposer
        let result = migrator.transition_to_prepared(0, &accessor).await;

        assert!(
            result.is_err(),
            "transition_to_prepared should fail without Raft proposer"
        );

        // Should remain in CatchingUp phase
        let record = migrator.get_migration(0).unwrap();
        assert!(
            matches!(record.phase, SlotMigrationPhase::CatchingUp { .. }),
            "Expected CatchingUp phase (no transition), got {:?}",
            record.phase
        );
    }

    #[tokio::test]
    async fn test_transition_to_completed_deletes_source() {
        let migrator = create_test_migrator();
        let accessor = Arc::new(MockDataAccessor::new());

        // Data on source to be deleted
        accessor.add_data(0, 0, vec![(b"key1".to_vec(), Bytes::from("value1"))]);

        migrator.register_migration(0, 0, 3);
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&0) {
                record.set_phase(SlotMigrationPhase::Prepared {
                    prepared_at: now_ms(),
                    target_commit_index: 100,
                    validation_checksum: 1,
                });
            }
        }

        // Transition to completed
        migrator
            .transition_to_completed(0, &accessor, &None)
            .await
            .unwrap();

        // Source data should be deleted
        assert_eq!(
            accessor.get_data_count(0, 0),
            0,
            "Source data should be deleted"
        );

        let record = migrator.get_migration(0).unwrap();
        assert!(
            record.phase.is_completed(),
            "Expected Completed phase, got {:?}",
            record.phase
        );
    }

    // ==================== Sync Tests ====================

    #[test]
    fn test_sync_from_peer_migrations() {
        let migrator = create_test_migrator();

        // No local migrations initially
        assert_eq!(migrator.active_migrations().len(), 0);

        // Peer has a claimed migration
        let mut peer_record = SlotMigrationRecord::new(42, 0, 3);
        peer_record.set_phase(SlotMigrationPhase::Claimed {
            owner_node: 2,
            claim_epoch: 1,
            claimed_at: now_ms(),
        });

        migrator.sync_from_peer_migrations(&[peer_record]);

        // Should have the migration now
        let record = migrator.get_migration(42);
        assert!(record.is_some(), "Should have synced migration");
        assert!(
            record.unwrap().phase.is_claimed_by(2),
            "Should preserve peer's phase"
        );
    }

    #[test]
    fn test_sync_from_peer_updates_phase() {
        let migrator = create_test_migrator();

        // Local migration in Pending
        migrator.register_migration(42, 0, 3);

        // Peer has more advanced phase
        let mut peer_record = SlotMigrationRecord::new(42, 0, 3);
        peer_record.set_phase(SlotMigrationPhase::Scanning {
            cursor: None,
            keys_found: 10,
        });

        migrator.sync_from_peer_migrations(&[peer_record]);

        // Should update to peer's phase
        let record = migrator.get_migration(42).unwrap();
        assert!(
            matches!(
                record.phase,
                SlotMigrationPhase::Scanning { keys_found: 10, .. }
            ),
            "Should update to peer's more advanced phase"
        );
    }

    #[test]
    fn test_sync_from_peer_does_not_regress_phase() {
        let migrator = create_test_migrator();

        // Local migration in Scanning
        migrator.register_migration(42, 0, 3);
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&42) {
                record.set_phase(SlotMigrationPhase::Scanning {
                    cursor: None,
                    keys_found: 10,
                });
            }
        }

        // Peer has less advanced phase (Pending)
        let peer_record = SlotMigrationRecord::new(42, 0, 3);

        migrator.sync_from_peer_migrations(&[peer_record]);

        // Should keep local more advanced phase
        let record = migrator.get_migration(42).unwrap();
        assert!(
            matches!(record.phase, SlotMigrationPhase::Scanning { .. }),
            "Should not regress to peer's less advanced phase"
        );
    }

    // ==================== Process Retries Tests ====================

    #[tokio::test]
    async fn test_process_retries_resets_to_pending() {
        let migrator = create_test_migrator();

        migrator.register_migration(0, 0, 3);

        // Mark as failed
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&0) {
                record.mark_failed("Test failure".into());
            }
        }

        // Process retries
        migrator.process_retries().await;

        let record = migrator.get_migration(0).unwrap();
        assert!(
            matches!(record.phase, SlotMigrationPhase::Pending),
            "Should reset to Pending after retry"
        );
    }

    #[tokio::test]
    async fn test_process_retries_respects_max_retries() {
        let slot_table = Arc::new(SlotTable::new(4));
        let config = SlotMigratorConfig {
            max_retries: 2,
            ..Default::default()
        };
        let migrator = SlotMigrator::new(0, slot_table, config);

        migrator.register_migration(0, 0, 3);

        // Fail 3 times (exceeds max_retries of 2)
        {
            let mut migrations = migrator.migrations_mut();
            if let Some(record) = migrations.get_mut(&0) {
                record.mark_failed("Fail 1".into());
                record.mark_failed("Fail 2".into());
                record.mark_failed("Fail 3".into()); // Now at retry_count = 3
            }
        }

        // Process retries should not reset
        migrator.process_retries().await;

        let record = migrator.get_migration(0).unwrap();
        assert!(
            record.phase.is_failed(),
            "Should remain failed after max retries exceeded"
        );
    }

    // ==================== Migrate Key On Demand Tests ====================

    #[tokio::test]
    async fn test_migrate_key_on_demand() {
        let migrator = create_test_migrator();
        let accessor = MockDataAccessor::new();

        // Key exists on source
        accessor.add_data(0, 0, vec![(b"mykey".to_vec(), Bytes::from("myvalue"))]);

        // Migrate key on demand
        migrator
            .migrate_key_on_demand(b"mykey", 0, 3, &accessor)
            .await
            .unwrap();

        // Key should now exist on target
        let value = accessor.get_key(3, b"mykey").await.unwrap();
        assert_eq!(value, Some(Bytes::from("myvalue")));
    }

    #[tokio::test]
    async fn test_migrate_key_on_demand_missing_key() {
        let migrator = create_test_migrator();
        let accessor = MockDataAccessor::new();

        // Key does not exist on source
        migrator
            .migrate_key_on_demand(b"nonexistent", 0, 3, &accessor)
            .await
            .unwrap();

        // Should not error, just do nothing
        let value = accessor.get_key(3, b"nonexistent").await.unwrap();
        assert!(value.is_none());
    }

    // ==================== Advance Migration Tests ====================

    #[tokio::test]
    async fn test_advance_migration_skips_when_not_target_leader() {
        let slot_table = Arc::new(SlotTable::new(4));
        let migrator = SlotMigrator::new(1, slot_table, SlotMigratorConfig::default());
        let accessor = Arc::new(MockDataAccessor::new());

        // This node is NOT the target leader
        accessor.set_target_leader(99); // Some other shard

        migrator.register_migration(0, 0, 3);

        // advance_migration should skip (not drive migration)
        let result = migrator.advance_migration(0, &accessor, &None).await;
        assert!(result.is_ok());

        // Should still be Pending (not advanced)
        let record = migrator.get_migration(0).unwrap();
        assert!(
            matches!(record.phase, SlotMigrationPhase::Pending),
            "Should not advance when not target leader"
        );
    }

    #[tokio::test]
    async fn test_advance_migration_requires_raft_when_target_leader() {
        // After split-brain fix: claiming requires Raft consensus.
        // Without a Raft proposer, advance_migration should fail.
        let slot_table = Arc::new(SlotTable::new(4));
        let migrator = SlotMigrator::new(1, slot_table, SlotMigratorConfig::default());
        let accessor = Arc::new(MockDataAccessor::new());

        // This node is the target leader
        accessor.set_target_leader(3);

        migrator.register_migration(0, 0, 3);

        // advance_migration should fail without Raft proposer (no local fallback)
        let result = migrator.advance_migration(0, &accessor, &None).await;
        assert!(
            result.is_err(),
            "advance_migration should fail without Raft proposer"
        );

        // Should remain in Pending (not claimed)
        let record = migrator.get_migration(0).unwrap();
        assert!(
            matches!(record.phase, SlotMigrationPhase::Pending),
            "Should remain Pending when no Raft proposer available"
        );
    }

    // ==================== MigrationRaftCommand Tests ====================

    #[test]
    fn test_migration_raft_command_name() {
        let claim = MigrationRaftCommand::Claim {
            migration_id: MigrationId::new(0, 1),
            leader_id: 1,
            proposed_at: 1000,
        };
        assert_eq!(claim.name(), "Claim");

        let prepared = MigrationRaftCommand::Prepared {
            migration_id: MigrationId::new(0, 1),
            target_commit_index: 100,
            validation_checksum: 0,
            proposed_at: 2000,
        };
        assert_eq!(prepared.name(), "Prepared");

        let completed = MigrationRaftCommand::Completed {
            migration_id: MigrationId::new(0, 1),
            proposed_at: 3000,
        };
        assert_eq!(completed.name(), "Completed");

        let cleaned = MigrationRaftCommand::Cleaned {
            migration_id: MigrationId::new(0, 1),
            proposed_at: 4000,
        };
        assert_eq!(cleaned.name(), "Cleaned");
    }

    #[test]
    fn test_migration_raft_command_migration_id() {
        let id = MigrationId::new(42, 5);

        let claim = MigrationRaftCommand::Claim {
            migration_id: id.clone(),
            leader_id: 1,
            proposed_at: 1000,
        };
        assert_eq!(claim.migration_id().slot_id, 42);
        assert_eq!(claim.migration_id().epoch, 5);

        let prepared = MigrationRaftCommand::Prepared {
            migration_id: id.clone(),
            target_commit_index: 100,
            validation_checksum: 0,
            proposed_at: 2000,
        };
        assert_eq!(prepared.migration_id().slot_id, 42);

        let completed = MigrationRaftCommand::Completed {
            migration_id: id.clone(),
            proposed_at: 3000,
        };
        assert_eq!(completed.migration_id().slot_id, 42);

        let cleaned = MigrationRaftCommand::Cleaned {
            migration_id: id,
            proposed_at: 4000,
        };
        assert_eq!(cleaned.migration_id().slot_id, 42);
    }

    // ==================== MigrationId Display Test ====================

    #[test]
    fn test_migration_id_display() {
        let id = MigrationId::new(42, 5);
        assert_eq!(format!("{}", id), "slot:42:epoch:5");
    }

    // ==================== SlotMigrationRecord Tests ====================

    #[test]
    fn test_slot_migration_record_with_epoch() {
        let record = SlotMigrationRecord::with_epoch(10, 0, 3, 5);
        assert_eq!(record.slot_id, 10);
        assert_eq!(record.from_shard, 0);
        assert_eq!(record.to_shard, 3);
        assert_eq!(record.id.epoch, 5);
        assert!(matches!(record.phase, SlotMigrationPhase::Pending));
    }

    #[test]
    fn test_slot_migration_record_touch_progress() {
        let mut record = SlotMigrationRecord::new(0, 0, 3);
        let original_progress = record.last_progress_at;

        // Wait a tiny bit and touch
        std::thread::sleep(std::time::Duration::from_millis(1));
        record.touch_progress();

        assert!(
            record.last_progress_at >= original_progress,
            "Progress timestamp should be updated"
        );
    }
}
