//! Flow control for backpressure handling and rate limiting.
//!
//! This module provides mechanisms to handle network congestion and
//! prevent message loss through backpressure feedback and rate limiting.

use crate::consensus::transport::BackpressureEvent;
use crate::error::{RaftError, Result};
use crate::types::NodeId;
use parking_lot::{Mutex, RwLock};
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Pressure level for backpressure events.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PressureLevel {
    /// Queue is at or above high watermark (80%)
    High,
    /// Queue is at normal levels (below 50%)
    Normal,
    /// Queue is full, messages being dropped
    Critical,
}

impl From<&BackpressureEvent> for PressureLevel {
    fn from(event: &BackpressureEvent) -> Self {
        match event {
            BackpressureEvent::QueueFull { .. } => PressureLevel::Critical,
            BackpressureEvent::QueueHighWatermark { .. } => PressureLevel::High,
            BackpressureEvent::QueueNormal { .. } => PressureLevel::Normal,
        }
    }
}

/// Flow control for managing backpressure and rate limiting.
///
/// This struct tracks unreachable peers and adjusts inflight message limits
/// based on backpressure feedback from the transport layer.
pub struct FlowControl {
    /// Maximum inflight messages per peer
    max_inflight: RwLock<HashMap<NodeId, usize>>,
    /// Default maximum inflight messages
    default_max_inflight: usize,
    /// Propose rate limiter
    propose_rate_limiter: RateLimiter,
    /// Set of peers marked as unreachable due to backpressure
    unreachable_peers: RwLock<HashSet<NodeId>>,
    /// Timestamp when each peer was last marked unreachable
    unreachable_since: RwLock<HashMap<NodeId, Instant>>,
}

impl FlowControl {
    /// Create a new FlowControl instance.
    pub fn new(default_max_inflight: usize) -> Self {
        Self {
            max_inflight: RwLock::new(HashMap::new()),
            default_max_inflight,
            propose_rate_limiter: RateLimiter::new(1000), // 1000 proposals/sec default
            unreachable_peers: RwLock::new(HashSet::new()),
            unreachable_since: RwLock::new(HashMap::new()),
        }
    }

    /// Create a new FlowControl with custom rate limit.
    pub fn with_rate_limit(default_max_inflight: usize, proposals_per_sec: u64) -> Self {
        Self {
            max_inflight: RwLock::new(HashMap::new()),
            default_max_inflight,
            propose_rate_limiter: RateLimiter::new(proposals_per_sec),
            unreachable_peers: RwLock::new(HashSet::new()),
            unreachable_since: RwLock::new(HashMap::new()),
        }
    }

    /// Handle a backpressure event from the transport layer.
    pub fn handle_backpressure(&self, peer_id: NodeId, level: PressureLevel) {
        match level {
            PressureLevel::Critical | PressureLevel::High => {
                // Strategy 1: Mark peer as unreachable in Raft
                self.unreachable_peers.write().insert(peer_id);
                self.unreachable_since.write().insert(peer_id, Instant::now());

                // Strategy 2: Reduce inflight messages for this peer by 50%
                let mut inflight = self.max_inflight.write();
                let current = inflight
                    .get(&peer_id)
                    .copied()
                    .unwrap_or(self.default_max_inflight);
                let new_limit = (current / 2).max(1); // Don't go below 1
                inflight.insert(peer_id, new_limit);

                // Strategy 3: Throttle propose rate globally
                let factor = if level == PressureLevel::Critical {
                    0.5 // Reduce by 50% for critical
                } else {
                    0.7 // Reduce by 30% for high
                };
                self.propose_rate_limiter.reduce_rate(factor);

                warn!(
                    peer_id,
                    new_limit,
                    "Applied backpressure: reduced inflight, throttled propose rate"
                );
            }
            PressureLevel::Normal => {
                // Remove from unreachable set
                self.unreachable_peers.write().remove(&peer_id);
                self.unreachable_since.write().remove(&peer_id);

                // Gradually restore inflight capacity
                let mut inflight = self.max_inflight.write();
                let current = inflight
                    .get(&peer_id)
                    .copied()
                    .unwrap_or(self.default_max_inflight / 2);
                let new_limit = current
                    .saturating_add(current / 4)
                    .min(self.default_max_inflight);
                inflight.insert(peer_id, new_limit);

                // Restore propose rate
                self.propose_rate_limiter.increase_rate(1.1); // Increase by 10%

                info!(
                    peer_id,
                    new_limit, "Backpressure relieved: restored inflight"
                );
            }
        }
    }

    /// Handle a BackpressureEvent directly from transport.
    pub fn handle_backpressure_event(&self, event: BackpressureEvent) {
        let peer_id = match &event {
            BackpressureEvent::QueueFull { peer_id, .. } => *peer_id,
            BackpressureEvent::QueueHighWatermark { peer_id, .. } => *peer_id,
            BackpressureEvent::QueueNormal { peer_id, .. } => *peer_id,
        };
        let level = PressureLevel::from(&event);
        self.handle_backpressure(peer_id, level);
    }

    /// Get the maximum inflight messages allowed for a peer.
    pub fn get_max_inflight(&self, peer_id: NodeId) -> usize {
        self.max_inflight
            .read()
            .get(&peer_id)
            .copied()
            .unwrap_or(self.default_max_inflight)
    }

    /// Check if a peer is marked as unreachable.
    pub fn is_unreachable(&self, peer_id: NodeId) -> bool {
        self.unreachable_peers.read().contains(&peer_id)
    }

    /// Get all unreachable peers.
    pub fn unreachable_peers(&self) -> Vec<NodeId> {
        self.unreachable_peers.read().iter().copied().collect()
    }

    /// Clear a peer from the unreachable set.
    pub fn mark_reachable(&self, peer_id: NodeId) {
        self.unreachable_peers.write().remove(&peer_id);
        self.unreachable_since.write().remove(&peer_id);
        debug!(peer_id, "Peer marked as reachable");
    }

    /// Check propose rate limit before proposing.
    ///
    /// Returns Ok(()) if the proposal is allowed, or waits until a permit is available.
    pub async fn check_propose_rate(&self) -> Result<()> {
        if !self.propose_rate_limiter.acquire().await {
            return Err(RaftError::Internal("rate limited".to_string()).into());
        }
        Ok(())
    }

    /// Try to check propose rate without blocking.
    ///
    /// Returns true if a permit was acquired, false otherwise.
    pub fn try_check_propose_rate(&self) -> bool {
        self.propose_rate_limiter.try_acquire()
    }

    /// Get the current propose rate limit.
    pub fn current_propose_rate(&self) -> u64 {
        self.propose_rate_limiter.current_rate()
    }

    /// Reset flow control state for a peer.
    pub fn reset_peer(&self, peer_id: NodeId) {
        self.max_inflight.write().remove(&peer_id);
        self.unreachable_peers.write().remove(&peer_id);
        self.unreachable_since.write().remove(&peer_id);
    }

    /// Reset all flow control state.
    pub fn reset_all(&self) {
        self.max_inflight.write().clear();
        self.unreachable_peers.write().clear();
        self.unreachable_since.write().clear();
        self.propose_rate_limiter.reset();
    }
}

impl Default for FlowControl {
    fn default() -> Self {
        Self::new(256) // Default max inflight
    }
}

impl std::fmt::Debug for FlowControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("FlowControl")
            .field("default_max_inflight", &self.default_max_inflight)
            .field("unreachable_peers_count", &self.unreachable_peers.read().len())
            .field("current_propose_rate", &self.current_propose_rate())
            .finish()
    }
}

/// Token bucket rate limiter for controlling proposal rate.
pub struct RateLimiter {
    /// Maximum permits per second
    permits_per_second: AtomicU64,
    /// Last refill timestamp
    last_refill: Mutex<Instant>,
    /// Available permits
    available_permits: AtomicU64,
    /// Initial rate (for reset)
    initial_rate: u64,
}

impl RateLimiter {
    /// Create a new rate limiter with the given rate.
    pub fn new(rate: u64) -> Self {
        Self {
            permits_per_second: AtomicU64::new(rate),
            last_refill: Mutex::new(Instant::now()),
            available_permits: AtomicU64::new(rate),
            initial_rate: rate,
        }
    }

    /// Acquire a permit, waiting if necessary.
    ///
    /// Returns true when a permit is acquired.
    pub async fn acquire(&self) -> bool {
        self.refill();

        // Try to acquire with limited retries to avoid infinite loop
        for _ in 0..1000 {
            let available = self.available_permits.load(Ordering::Acquire);
            if available > 0 {
                if self
                    .available_permits
                    .compare_exchange(available, available - 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }
            } else {
                // Wait a bit and retry
                tokio::time::sleep(Duration::from_millis(1)).await;
                self.refill();
            }
        }
        true // Eventually allow to prevent deadlock
    }

    /// Try to acquire a permit without waiting.
    pub fn try_acquire(&self) -> bool {
        self.refill();

        loop {
            let available = self.available_permits.load(Ordering::Acquire);
            if available > 0 {
                if self
                    .available_permits
                    .compare_exchange(available, available - 1, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
                {
                    return true;
                }
                // CAS failed, retry
            } else {
                return false;
            }
        }
    }

    /// Refill permits based on elapsed time.
    fn refill(&self) {
        let mut last_refill = self.last_refill.lock();
        let now = Instant::now();
        let elapsed = now.duration_since(*last_refill);

        // Refill every 100ms
        if elapsed >= Duration::from_millis(100) {
            let rate = self.permits_per_second.load(Ordering::Relaxed);
            let to_add = (rate as f64 * elapsed.as_secs_f64()) as u64;

            if to_add > 0 {
                let current = self.available_permits.load(Ordering::Relaxed);
                let new_available = current.saturating_add(to_add).min(rate);
                self.available_permits.store(new_available, Ordering::Release);
                *last_refill = now;
            }
        }
    }

    /// Reduce the rate by a factor (e.g., 0.7 means reduce by 30%).
    pub fn reduce_rate(&self, factor: f64) {
        let current = self.permits_per_second.load(Ordering::Relaxed);
        let new_rate = ((current as f64) * factor).max(10.0) as u64; // Min 10/sec
        self.permits_per_second.store(new_rate, Ordering::Relaxed);
        debug!(current, new_rate, factor, "Reduced propose rate");
    }

    /// Increase the rate by a factor (e.g., 1.1 means increase by 10%).
    pub fn increase_rate(&self, factor: f64) {
        let current = self.permits_per_second.load(Ordering::Relaxed);
        let new_rate = ((current as f64) * factor).min(self.initial_rate as f64) as u64;
        self.permits_per_second.store(new_rate, Ordering::Relaxed);
        debug!(current, new_rate, factor, "Increased propose rate");
    }

    /// Get the current rate.
    pub fn current_rate(&self) -> u64 {
        self.permits_per_second.load(Ordering::Relaxed)
    }

    /// Reset to initial rate.
    pub fn reset(&self) {
        self.permits_per_second
            .store(self.initial_rate, Ordering::Relaxed);
        self.available_permits
            .store(self.initial_rate, Ordering::Release);
        *self.last_refill.lock() = Instant::now();
    }
}

impl std::fmt::Debug for RateLimiter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RateLimiter")
            .field("permits_per_second", &self.permits_per_second.load(Ordering::Relaxed))
            .field("available_permits", &self.available_permits.load(Ordering::Relaxed))
            .field("initial_rate", &self.initial_rate)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_flow_control_creation() {
        let fc = FlowControl::new(128);
        assert_eq!(fc.default_max_inflight, 128);
        assert!(!fc.is_unreachable(1));
        assert_eq!(fc.get_max_inflight(1), 128);
    }

    #[test]
    fn test_flow_control_backpressure_high() {
        let fc = FlowControl::new(128);

        fc.handle_backpressure(1, PressureLevel::High);

        assert!(fc.is_unreachable(1));
        assert_eq!(fc.get_max_inflight(1), 64); // Reduced by half
    }

    #[test]
    fn test_flow_control_backpressure_normal() {
        let fc = FlowControl::new(128);

        // First apply high pressure
        fc.handle_backpressure(1, PressureLevel::High);
        assert!(fc.is_unreachable(1));
        assert_eq!(fc.get_max_inflight(1), 64);

        // Then release pressure
        fc.handle_backpressure(1, PressureLevel::Normal);
        assert!(!fc.is_unreachable(1));
        assert!(fc.get_max_inflight(1) > 64); // Should increase
    }

    #[test]
    fn test_rate_limiter_try_acquire() {
        let limiter = RateLimiter::new(100);

        // Should be able to acquire permits
        for _ in 0..100 {
            assert!(limiter.try_acquire());
        }

        // Should fail when exhausted
        assert!(!limiter.try_acquire());
    }

    #[test]
    fn test_rate_limiter_reduce_increase() {
        let limiter = RateLimiter::new(1000);

        limiter.reduce_rate(0.5);
        assert_eq!(limiter.current_rate(), 500);

        limiter.increase_rate(1.5);
        assert_eq!(limiter.current_rate(), 750);

        limiter.reset();
        assert_eq!(limiter.current_rate(), 1000);
    }
}
