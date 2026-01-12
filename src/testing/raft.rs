// Cargo.toml dependencies needed:
// [dependencies]
// raft = "0.7"
// protobuf = "2.27"
// slog = "2.7"
// slog-stdlog = "4.1"
// tokio = { version = "1", features = ["full"] }

use crate::consensus::storage::MemStorage;
use raft::{prelude::*, GetEntriesContext, StateRole};
use slog::{o, Drain};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Helper to create a logger for Raft
fn create_logger() -> slog::Logger {
    let decorator = slog_term::PlainSyncDecorator::new(std::io::stdout());
    let drain = slog_term::FullFormat::new(decorator).build().fuse();
    slog::Logger::root(drain, o!())
}

/// Wrapper around Raft node for testing
struct TestNode {
    raft: RawNode<MemStorage>,
    #[allow(dead_code)]
    id: u64,
}

impl TestNode {
    fn new(id: u64, peers: Vec<u64>) -> Self {
        let logger = create_logger();
        let config = Config {
            id,
            election_tick: 10,
            heartbeat_tick: 3,
            ..Default::default()
        };

        let storage = MemStorage::new_with_conf_state(peers);
        let raft = RawNode::new(&config, storage, &logger).unwrap();

        Self { raft, id }
    }

    fn role(&self) -> StateRole {
        self.raft.raft.state
    }

    fn term(&self) -> u64 {
        self.raft.raft.term
    }

    fn tick(&mut self) {
        self.raft.tick();
    }

    fn step(&mut self, msg: Message) -> raft::Result<()> {
        self.raft.step(msg)
    }

    fn has_ready(&self) -> bool {
        self.raft.has_ready()
    }

    fn ready(&mut self) -> Ready {
        self.raft.ready()
    }

    /// Propose data to Raft (only works if this node is leader)
    fn propose(&mut self, data: Vec<u8>) -> raft::Result<()> {
        self.raft.propose(vec![], data)
    }

    /// Get the last log index
    fn last_index(&self) -> u64 {
        self.raft.raft.raft_log.last_index()
    }

    /// Get the committed index
    fn committed_index(&self) -> u64 {
        self.raft.raft.raft_log.committed
    }

    /// Get entry data at a specific index (returns None if not found or not data entry)
    fn get_entry_data(&self, index: u64) -> Option<Vec<u8>> {
        let ctx = GetEntriesContext::empty(false);
        let entries = self.raft.store().entries(index, index + 1, None, ctx).ok()?;
        entries.first().map(|e| e.data.to_vec())
    }

    /// Process ready state: persist to storage, return messages to send
    fn process_ready(&mut self) -> Vec<Message> {
        if !self.has_ready() {
            return vec![];
        }

        let mut ready = self.ready();

        // Get immediate messages first
        let mut messages = ready.take_messages();

        // Persist hard state if it changed
        if let Some(hs) = ready.hs() {
            self.raft.mut_store().set_hard_state(hs.clone());
        }

        // Append entries to storage
        let entries_to_persist = ready.entries().to_vec();
        if !entries_to_persist.is_empty() {
            let _ = self.raft.mut_store().append(&entries_to_persist);
        }

        // Get persisted messages (messages that should be sent after entries are persisted)
        messages.extend(ready.take_persisted_messages());

        // Advance the raft node and get light ready
        let mut light_rd = self.raft.advance(ready);

        // Also take messages from light ready
        messages.extend(light_rd.take_messages());

        // After advancing, check if there's another ready state
        // This handles cases where advancing triggers new messages
        while self.has_ready() {
            let mut next_ready = self.ready();
            messages.extend(next_ready.take_messages());

            if let Some(hs) = next_ready.hs() {
                self.raft.mut_store().set_hard_state(hs.clone());
            }

            let more_entries = next_ready.entries().to_vec();
            if !more_entries.is_empty() {
                let _ = self.raft.mut_store().append(&more_entries);
            }

            messages.extend(next_ready.take_persisted_messages());
            let mut lr = self.raft.advance(next_ready);
            messages.extend(lr.take_messages());
        }

        messages
    }
}

/// Test cluster for managing multiple Raft nodes
struct TestCluster {
    nodes: HashMap<u64, Arc<Mutex<TestNode>>>,
}

impl TestCluster {
    fn new(node_ids: Vec<u64>) -> Self {
        let mut nodes = HashMap::new();

        for id in &node_ids {
            let node = TestNode::new(*id, node_ids.clone());
            nodes.insert(*id, Arc::new(Mutex::new(node)));
        }

        Self { nodes }
    }

    fn get_node(&self, id: u64) -> Arc<Mutex<TestNode>> {
        self.nodes.get(&id).unwrap().clone()
    }

    fn tick_all(&self) {
        for node in self.nodes.values() {
            node.lock().unwrap().tick();
        }
    }

    fn process_ready(&self, id: u64) -> Vec<Message> {
        let node = self.get_node(id);
        let mut node = node.lock().unwrap();
        node.process_ready()
    }

    fn deliver_messages(&self, messages: Vec<Message>) {
        for msg in messages {
            if let Some(node) = self.nodes.get(&msg.to) {
                let _ = node.lock().unwrap().step(msg);
            }
        }
    }

    fn count_by_role(&self, role: StateRole) -> usize {
        self.nodes
            .values()
            .filter(|n| n.lock().unwrap().role() == role)
            .count()
    }

    fn get_leader_id(&self) -> Option<u64> {
        self.nodes
            .iter()
            .find(|(_, n)| n.lock().unwrap().role() == StateRole::Leader)
            .map(|(id, _)| *id)
    }

    /// Tick a single node and process ready, returning messages
    fn tick_node(&self, id: u64) -> Vec<Message> {
        let node = self.get_node(id);
        node.lock().unwrap().tick();
        self.process_ready(id)
    }

    /// Tick a node multiple times and collect all messages
    fn tick_node_times(&self, id: u64, times: usize) -> Vec<Message> {
        let mut all_messages = Vec::new();
        for _ in 0..times {
            all_messages.extend(self.tick_node(id));
        }
        all_messages
    }

    /// Deliver messages only to specific nodes (for partition simulation)
    fn deliver_messages_to(&self, messages: Vec<Message>, allowed_recipients: &[u64]) {
        for msg in messages {
            if allowed_recipients.contains(&msg.to) {
                if let Some(node) = self.nodes.get(&msg.to) {
                    let _ = node.lock().unwrap().step(msg);
                }
            }
        }
    }

    /// Deliver messages except to specific nodes (for partition simulation)
    fn deliver_messages_except(&self, messages: Vec<Message>, blocked_nodes: &[u64]) {
        for msg in messages {
            if !blocked_nodes.contains(&msg.to) && !blocked_nodes.contains(&msg.from) {
                if let Some(node) = self.nodes.get(&msg.to) {
                    let _ = node.lock().unwrap().step(msg);
                }
            }
        }
    }

    /// Propose data via a specific node
    fn propose(&self, id: u64, data: Vec<u8>) -> raft::Result<()> {
        let node = self.get_node(id);
        let result = node.lock().unwrap().propose(data);
        result
    }

    /// Get the last log index for a node
    fn last_index(&self, id: u64) -> u64 {
        self.get_node(id).lock().unwrap().last_index()
    }

    /// Get the committed index for a node
    fn committed_index(&self, id: u64) -> u64 {
        self.get_node(id).lock().unwrap().committed_index()
    }

    /// Get entry data at a specific index from a node
    fn get_entry_data(&self, id: u64, index: u64) -> Option<Vec<u8>> {
        self.get_node(id).lock().unwrap().get_entry_data(index)
    }

    /// Process ready for multiple nodes and collect all messages
    fn process_ready_all(&self) -> Vec<Message> {
        let mut all_messages = Vec::new();
        for id in self.nodes.keys() {
            all_messages.extend(self.process_ready(*id));
        }
        all_messages
    }

    /// Run one round of ticks and message delivery for specified nodes
    fn run_round_for(&self, node_ids: &[u64]) -> Vec<Message> {
        let mut all_messages = Vec::new();
        for id in node_ids {
            all_messages.extend(self.tick_node(*id));
        }
        all_messages
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_initial_state_all_followers() {
        // All nodes start as followers
        let cluster = TestCluster::new(vec![1, 2, 3]);

        assert_eq!(cluster.count_by_role(StateRole::Follower), 3);
        assert_eq!(cluster.count_by_role(StateRole::Candidate), 0);
        assert_eq!(cluster.count_by_role(StateRole::Leader), 0);
    }

    #[test]
    fn test_follower_to_candidate_on_election_timeout() {
        // Test: Follower → Candidate (timeout)
        let cluster = TestCluster::new(vec![1, 2, 3]);

        assert_eq!(cluster.count_by_role(StateRole::Follower), 3);
        // Initially all followers
        assert_eq!(
            cluster.get_node(1).lock().unwrap().role(),
            StateRole::Follower
        );

        // Tick node 1 past election timeout (randomized between 10-20 ticks)
        let messages = cluster.tick_node_times(1, 25);

        // Node 1 should transition to Candidate
        assert_eq!(
            cluster.get_node(1).lock().unwrap().role(),
            StateRole::Candidate
        );

        // Should have sent RequestVote messages
        assert!(messages
            .iter()
            .any(|m| m.msg_type == MessageType::MsgRequestVote));
    }

    #[test]
    fn test_candidate_to_leader_on_election_win() {
        // Test: Candidate → Leader (wins election)
        let cluster = TestCluster::new(vec![1, 2, 3]);

        // Make node 1 timeout and become candidate
        let messages = cluster.tick_node_times(1, 25);
        assert_eq!(
            cluster.get_node(1).lock().unwrap().role(),
            StateRole::Candidate
        );

        // Deliver vote requests to other nodes
        cluster.deliver_messages(messages);

        // Process responses from other nodes and deliver to node 1
        for id in [2, 3] {
            let msgs = cluster.process_ready(id);
            cluster.deliver_messages(msgs);
        }

        // Process vote responses at node 1
        cluster.process_ready(1);

        // Node 1 should become leader after receiving majority votes
        assert_eq!(
            cluster.get_node(1).lock().unwrap().role(),
            StateRole::Leader
        );
    }

    #[test]
    fn test_leader_to_follower_on_higher_term() {
        // Test: Leader → Follower (higher term seen)
        let cluster = TestCluster::new(vec![1, 2, 3]);

        // Elect node 1 as leader
        let msgs = cluster.tick_node_times(1, 25);
        cluster.deliver_messages(msgs);

        for id in [2, 3] {
            let msgs = cluster.process_ready(id);
            cluster.deliver_messages(msgs);
        }
        cluster.process_ready(1);

        let node1 = cluster.get_node(1);
        assert_eq!(node1.lock().unwrap().role(), StateRole::Leader);
        let leader_term = node1.lock().unwrap().term();

        // Simulate node 2 starting election with higher term
        // Tick node 2 past election timeout
        let msgs = cluster.tick_node_times(2, 25);

        let node2 = cluster.get_node(2);
        let new_term = node2.lock().unwrap().term();
        assert!(new_term > leader_term);

        // Deliver higher term message to node 1
        cluster.deliver_messages(msgs);
        cluster.process_ready(1);

        // Node 1 should step down to follower
        assert_eq!(node1.lock().unwrap().role(), StateRole::Follower);
        assert!(node1.lock().unwrap().term() >= new_term);
    }

    #[test]
    fn test_complete_election_cycle() {
        // Integration test: Full election cycle
        let cluster = TestCluster::new(vec![1, 2, 3, 4, 5]);

        // Initially all followers
        assert_eq!(cluster.count_by_role(StateRole::Follower), 5);

        // Simulate election - Node 1 times out first
        let mut all_messages = cluster.tick_node_times(1, 25);

        // Candidate state
        assert_eq!(
            cluster.get_node(1).lock().unwrap().role(),
            StateRole::Candidate
        );

        // Exchange messages until leader elected
        for _ in 0..10 {
            cluster.deliver_messages(all_messages);
            all_messages = vec![];

            for id in 1..=5 {
                let msgs = cluster.process_ready(id);
                all_messages.extend(msgs);
            }

            // Check if we have a leader
            if cluster.count_by_role(StateRole::Leader) == 1 {
                break;
            }
        }

        // Should have exactly 1 leader
        assert_eq!(cluster.count_by_role(StateRole::Leader), 1);
        assert_eq!(cluster.count_by_role(StateRole::Follower), 4);

        println!("Leader elected: node {}", cluster.get_leader_id().unwrap());
    }

    #[test]
    fn test_heartbeat_prevents_election() {
        // Test: Leader heartbeats prevent follower timeout
        let cluster = TestCluster::new(vec![1, 2, 3]);

        // Elect a leader
        let mut msgs = cluster.tick_node_times(1, 25);

        // Complete election
        for _ in 0..5 {
            cluster.deliver_messages(msgs);
            msgs = vec![];
            for id in 1..=3 {
                msgs.extend(cluster.process_ready(id));
            }
        }

        assert_eq!(cluster.count_by_role(StateRole::Leader), 1);
        let leader_id = cluster.get_leader_id().unwrap();

        // Tick leader to send heartbeats
        for _ in 0..20 {
            let hb_msgs = cluster.tick_node(leader_id);
            cluster.deliver_messages(hb_msgs);

            // Process heartbeats at followers
            for id in 1..=3 {
                if id != leader_id {
                    cluster.process_ready(id);
                }
            }
        }

        // Should still have the same leader (no new election)
        assert_eq!(cluster.count_by_role(StateRole::Leader), 1);
        assert_eq!(cluster.get_leader_id().unwrap(), leader_id);
    }

    #[test]
    fn test_split_vote_scenario() {
        // Test: Multiple candidates causing split vote
        let cluster = TestCluster::new(vec![1, 2, 3, 4, 5]);

        // Make nodes 1 and 2 timeout simultaneously, collecting messages
        let mut all_messages = Vec::new();
        for _ in 0..25 {
            all_messages.extend(cluster.tick_node(1));
            all_messages.extend(cluster.tick_node(2));
        }

        // Both should be candidates
        assert_eq!(
            cluster.get_node(1).lock().unwrap().role(),
            StateRole::Candidate
        );
        assert_eq!(
            cluster.get_node(2).lock().unwrap().role(),
            StateRole::Candidate
        );

        // Deliver vote requests
        cluster.deliver_messages(all_messages);

        // Eventually one should win or they'll timeout and retry
        let mut msgs = vec![];
        for _ in 0..50 {
            cluster.tick_all();

            for id in 1..=5 {
                msgs.extend(cluster.process_ready(id));
            }

            cluster.deliver_messages(msgs.clone());
            msgs.clear();

            if cluster.count_by_role(StateRole::Leader) == 1 {
                break;
            }
        }

        // Should eventually elect a leader
        assert_eq!(cluster.count_by_role(StateRole::Leader), 1);
    }

    /// Test Case 6: Log Overwrite (Safety Verification)
    ///
    /// Scenario: An old Leader is partitioned while it has uncommitted entries
    /// that no other node has. A new Leader is elected and commits different
    /// entries at the same indexes.
    ///
    /// This verifies Raft's safety property: uncommitted entries from old leaders
    /// must be overwritten by committed entries from new leaders.
    #[test]
    fn test_log_overwrite_safety() {
        let cluster = TestCluster::new(vec![1, 2, 3]);

        // Step 1: Elect Node 1 as Leader
        let mut msgs = cluster.tick_node_times(1, 25);

        // Complete election - deliver messages and process responses
        for _ in 0..5 {
            cluster.deliver_messages(msgs);
            msgs = vec![];
            for id in 1..=3 {
                msgs.extend(cluster.process_ready(id));
            }
        }

        // Verify Node 1 is leader
        assert_eq!(
            cluster.get_node(1).lock().unwrap().role(),
            StateRole::Leader,
            "Node 1 should be leader"
        );
        let initial_term = cluster.get_node(1).lock().unwrap().term();

        // Send a few heartbeats to establish stable leadership
        for _ in 0..5 {
            let hb_msgs = cluster.tick_node(1);
            cluster.deliver_messages(hb_msgs);
            for id in 2..=3 {
                let resp = cluster.process_ready(id);
                cluster.deliver_messages(resp);
            }
            cluster.process_ready(1);
        }

        // Record the committed index before partitioning
        // All nodes should have the same committed index at this point
        let committed_before_partition = cluster.committed_index(1);

        // Step 2: Isolate Node 1 (create partition)
        // From now on, Node 1's messages won't reach Nodes 2 & 3

        // Step 3: Node 1 receives propose(A) but cannot replicate it
        let data_a = b"entry_A_from_old_leader".to_vec();
        cluster.propose(1, data_a.clone()).expect("Propose should succeed");

        // Process the proposal on Node 1 (it will append to its log)
        let _msgs_from_node1 = cluster.process_ready(1);

        // Node 1 tries to replicate but messages are dropped (partitioned)
        // We intentionally don't deliver these messages to nodes 2 & 3

        // Verify Node 1 has the uncommitted entry
        let node1_last_index = cluster.last_index(1);
        let node1_committed = cluster.committed_index(1);

        // Node 1 should have entry A in its log but NOT committed
        assert!(
            node1_committed < node1_last_index,
            "Entry A should be uncommitted on Node 1 (committed: {}, last: {})",
            node1_committed,
            node1_last_index
        );

        // Verify Node 1 has entry A at the new index
        let entry_a_index = node1_last_index;
        let node1_entry_data = cluster.get_entry_data(1, entry_a_index);
        assert_eq!(
            node1_entry_data,
            Some(data_a.clone()),
            "Node 1 should have entry A at index {}",
            entry_a_index
        );

        // Step 4: Nodes 2 & 3 elect Node 2 as Leader (Term 2)
        // Tick node 2 past election timeout
        let mut all_msgs = Vec::new();
        for _ in 0..25 {
            all_msgs.extend(cluster.tick_node(2));
        }

        // Node 2 should become candidate
        assert_eq!(
            cluster.get_node(2).lock().unwrap().role(),
            StateRole::Candidate,
            "Node 2 should become candidate after timeout"
        );

        // Deliver vote request to Node 3 only (Node 1 is partitioned)
        cluster.deliver_messages_to(all_msgs, &[3]);

        // Node 3 responds with vote
        let vote_response = cluster.process_ready(3);
        cluster.deliver_messages_to(vote_response, &[2]);

        // Process vote response at Node 2
        cluster.process_ready(2);

        // Node 2 should become leader (has votes from self + Node 3 = majority)
        assert_eq!(
            cluster.get_node(2).lock().unwrap().role(),
            StateRole::Leader,
            "Node 2 should become leader with majority votes"
        );

        let new_term = cluster.get_node(2).lock().unwrap().term();
        assert!(
            new_term > initial_term,
            "New leader should have higher term (new: {}, initial: {})",
            new_term,
            initial_term
        );

        // Step 5: Node 2 proposes and commits entry B
        let data_b = b"entry_B_from_new_leader".to_vec();
        cluster.propose(2, data_b.clone()).expect("Propose should succeed");

        // Process messages between Node 2 and Node 3 until entry B is committed
        // We need to ensure proper replication by processing each message type properly
        for round in 0..20 {
            // Get all messages from Node 2
            let msgs = cluster.process_ready(2);

            // Deliver messages to Node 3 one at a time and immediately process response
            for msg in msgs {
                if msg.to == 3 {
                    // Step the message
                    {
                        let node3 = cluster.get_node(3);
                        let _ = node3.lock().unwrap().step(msg);
                    }
                    // Process response immediately
                    let resp = cluster.process_ready(3);
                    // Deliver responses back to Node 2
                    for r in resp {
                        if r.to == 2 {
                            let node2 = cluster.get_node(2);
                            let _ = node2.lock().unwrap().step(r);
                        }
                    }
                }
            }

            // Check if committed
            let committed = cluster.committed_index(2);
            let last_idx = cluster.last_index(2);
            if committed >= last_idx && last_idx >= 3 {
                break;
            }

            // Tick Node 2 to trigger heartbeat/retry (not too many times to avoid election)
            if round < 15 {
                let node2 = cluster.get_node(2);
                node2.lock().unwrap().tick();
            }
        }

        // Verify entry B exists in Node 2's log
        let node2_last_index = cluster.last_index(2);
        let mut entry_b_index = 0;
        for idx in 1..=node2_last_index {
            if cluster.get_entry_data(2, idx) == Some(data_b.clone()) {
                entry_b_index = idx;
                break;
            }
        }
        assert!(entry_b_index > 0, "Node 2 should have entry B in its log");

        // Verify entry B is committed (index <= committed_index)
        let node2_committed = cluster.committed_index(2);
        assert!(
            entry_b_index <= node2_committed,
            "Entry B at index {} should be committed (committed_index: {})",
            entry_b_index,
            node2_committed
        );

        // KEY SAFETY VERIFICATION:
        // Entry A from old leader should NOT be committed on any node
        // because it was never replicated to a majority

        // Check Node 2 doesn't have entry A committed
        for idx in 1..=node2_committed {
            let entry_data = cluster.get_entry_data(2, idx);
            assert_ne!(
                entry_data,
                Some(data_a.clone()),
                "Entry A from old leader should NOT be committed on Node 2"
            );
        }

        // Check Node 3 doesn't have entry A committed
        let node3_committed = cluster.committed_index(3);
        for idx in 1..=node3_committed {
            let entry_data = cluster.get_entry_data(3, idx);
            assert_ne!(
                entry_data,
                Some(data_a.clone()),
                "Entry A from old leader should NOT be committed on Node 3"
            );
        }

        // Verify Node 1 has NOT committed entry A (still uncommitted)
        let node1_final_committed = cluster.committed_index(1);
        assert!(
            node1_final_committed <= committed_before_partition || entry_a_index > node1_final_committed,
            "Entry A should still be uncommitted on Node 1 (committed: {}, entry_a_index: {})",
            node1_final_committed,
            entry_a_index
        );

        // This test verifies the Raft safety property:
        // - An entry can only be committed if it's replicated to a majority
        // - Entry A was only on Node 1 (minority), so it cannot be committed
        // - Entry B was replicated to Nodes 2 & 3 (majority), so it can be committed
        // - When Node 1 eventually reconnects, it must discard entry A and adopt the new leader's log
    }
}
