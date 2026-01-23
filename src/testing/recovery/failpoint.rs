//! Failpoint injection for testing crash recovery scenarios.
//!
//! This module provides a mechanism to inject failures at specific points in the code
//! to test recovery behavior. Failpoints are thread-safe and can be enabled/disabled
//! dynamically during test execution.
//!
//! # Example
//!
//! ```rust,ignore
//! use distributed_cache::testing::recovery::{FailpointRegistry, FailpointAction};
//!
//! let registry = FailpointRegistry::new();
//!
//! // Enable a failpoint that will panic
//! registry.enable("checkpoint_before_write", FailpointAction::Panic);
//!
//! // In production code, use fail_point! macro
//! // fail_point!(registry, "checkpoint_before_write");
//!
//! registry.disable("checkpoint_before_write");
//! ```

use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

/// Global failpoint registry for tests.
/// This is lazily initialized and should only be used in test code.
static GLOBAL_REGISTRY: once_cell::sync::Lazy<Arc<FailpointRegistry>> =
    once_cell::sync::Lazy::new(|| Arc::new(FailpointRegistry::new()));

/// Get the global failpoint registry.
pub fn global_registry() -> Arc<FailpointRegistry> {
    GLOBAL_REGISTRY.clone()
}

/// Action to take when a failpoint is triggered.
#[derive(Clone)]
pub enum FailpointAction {
    /// Panic with the failpoint name.
    Panic,

    /// Sleep for the specified duration.
    Sleep(Duration),

    /// Return early (for functions that can bail out).
    Return,

    /// Execute a custom callback.
    Callback(Arc<dyn Fn() + Send + Sync>),

    /// Only trigger after N hits (countdown).
    CountdownPanic(u64),

    /// Only trigger on every Nth hit.
    EveryNPanic(u64),

    /// Trigger with a probability (0.0 - 1.0).
    ProbabilityPanic(f64),

    /// Disable after triggering once.
    OncePanic,
}

impl std::fmt::Debug for FailpointAction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Panic => write!(f, "Panic"),
            Self::Sleep(d) => write!(f, "Sleep({:?})", d),
            Self::Return => write!(f, "Return"),
            Self::Callback(_) => write!(f, "Callback(...)"),
            Self::CountdownPanic(n) => write!(f, "CountdownPanic({})", n),
            Self::EveryNPanic(n) => write!(f, "EveryNPanic({})", n),
            Self::ProbabilityPanic(p) => write!(f, "ProbabilityPanic({})", p),
            Self::OncePanic => write!(f, "OncePanic"),
        }
    }
}

/// Internal state for a failpoint.
struct FailpointState {
    action: FailpointAction,
    hit_count: AtomicU64,
    triggered_count: AtomicU64,
    disabled: AtomicBool,
}

/// Registry for managing failpoints.
pub struct FailpointRegistry {
    /// Active failpoints.
    failpoints: RwLock<HashMap<String, Arc<FailpointState>>>,

    /// Global enable/disable flag.
    enabled: AtomicBool,

    /// Total hits across all failpoints.
    total_hits: AtomicU64,
}

impl Default for FailpointRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl FailpointRegistry {
    /// Create a new failpoint registry.
    pub fn new() -> Self {
        Self {
            failpoints: RwLock::new(HashMap::new()),
            enabled: AtomicBool::new(true),
            total_hits: AtomicU64::new(0),
        }
    }

    /// Enable a failpoint with the given action.
    pub fn enable(&self, name: &str, action: FailpointAction) {
        let state = Arc::new(FailpointState {
            action,
            hit_count: AtomicU64::new(0),
            triggered_count: AtomicU64::new(0),
            disabled: AtomicBool::new(false),
        });
        self.failpoints.write().insert(name.to_string(), state);
    }

    /// Disable a specific failpoint.
    pub fn disable(&self, name: &str) {
        self.failpoints.write().remove(name);
    }

    /// Disable all failpoints.
    pub fn disable_all(&self) {
        self.failpoints.write().clear();
    }

    /// Globally enable/disable all failpoints.
    pub fn set_enabled(&self, enabled: bool) {
        self.enabled.store(enabled, Ordering::SeqCst);
    }

    /// Check if globally enabled.
    pub fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::SeqCst)
    }

    /// Check a failpoint and execute its action if enabled.
    ///
    /// Returns `true` if the failpoint triggered an action that should cause early return.
    pub fn check(&self, name: &str) -> FailpointResult {
        if !self.enabled.load(Ordering::Relaxed) {
            return FailpointResult::Continue;
        }

        let failpoints = self.failpoints.read();
        let state = match failpoints.get(name) {
            Some(s) => s.clone(),
            None => return FailpointResult::Continue,
        };
        drop(failpoints);

        if state.disabled.load(Ordering::Relaxed) {
            return FailpointResult::Continue;
        }

        let hit = state.hit_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.total_hits.fetch_add(1, Ordering::Relaxed);

        match &state.action {
            FailpointAction::Panic => {
                state.triggered_count.fetch_add(1, Ordering::Relaxed);
                panic!("failpoint triggered: {}", name);
            }

            FailpointAction::Sleep(duration) => {
                state.triggered_count.fetch_add(1, Ordering::Relaxed);
                FailpointResult::Sleep(*duration)
            }

            FailpointAction::Return => {
                state.triggered_count.fetch_add(1, Ordering::Relaxed);
                FailpointResult::Return
            }

            FailpointAction::Callback(cb) => {
                state.triggered_count.fetch_add(1, Ordering::Relaxed);
                cb();
                FailpointResult::Continue
            }

            FailpointAction::CountdownPanic(countdown) => {
                if hit >= *countdown {
                    state.triggered_count.fetch_add(1, Ordering::Relaxed);
                    panic!("failpoint triggered after {} hits: {}", hit, name);
                }
                FailpointResult::Continue
            }

            FailpointAction::EveryNPanic(n) => {
                if hit % n == 0 {
                    state.triggered_count.fetch_add(1, Ordering::Relaxed);
                    panic!("failpoint triggered on hit {}: {}", hit, name);
                }
                FailpointResult::Continue
            }

            FailpointAction::ProbabilityPanic(prob) => {
                let rng: f64 = rand::random();
                if rng < *prob {
                    state.triggered_count.fetch_add(1, Ordering::Relaxed);
                    panic!("failpoint triggered with probability {}: {}", prob, name);
                }
                FailpointResult::Continue
            }

            FailpointAction::OncePanic => {
                if hit == 1 {
                    state.disabled.store(true, Ordering::Relaxed);
                    state.triggered_count.fetch_add(1, Ordering::Relaxed);
                    panic!("failpoint triggered once: {}", name);
                }
                FailpointResult::Continue
            }
        }
    }

    /// Check a failpoint asynchronously (handles Sleep action).
    pub async fn check_async(&self, name: &str) -> FailpointResult {
        match self.check(name) {
            FailpointResult::Sleep(d) => {
                tokio::time::sleep(d).await;
                FailpointResult::Continue
            }
            other => other,
        }
    }

    /// Get statistics for a failpoint.
    pub fn stats(&self, name: &str) -> Option<FailpointStats> {
        self.failpoints.read().get(name).map(|state| FailpointStats {
            hit_count: state.hit_count.load(Ordering::Relaxed),
            triggered_count: state.triggered_count.load(Ordering::Relaxed),
        })
    }

    /// Get total hits across all failpoints.
    pub fn total_hits(&self) -> u64 {
        self.total_hits.load(Ordering::Relaxed)
    }

    /// List all active failpoints.
    pub fn list(&self) -> Vec<String> {
        self.failpoints.read().keys().cloned().collect()
    }
}

/// Result of checking a failpoint.
#[derive(Debug, Clone, PartialEq)]
pub enum FailpointResult {
    /// Continue execution normally.
    Continue,

    /// Return early from the function.
    Return,

    /// Sleep for the specified duration.
    Sleep(Duration),
}

/// Statistics for a failpoint.
#[derive(Debug, Clone)]
pub struct FailpointStats {
    /// Number of times the failpoint was hit.
    pub hit_count: u64,

    /// Number of times the failpoint actually triggered an action.
    pub triggered_count: u64,
}

/// Macro for checking a failpoint.
///
/// Usage:
/// ```rust,ignore
/// // With explicit registry
/// fail_point!(registry, "my_failpoint");
///
/// // With global registry
/// fail_point!("my_failpoint");
///
/// // With return on trigger
/// fail_point!(registry, "my_failpoint", return);
///
/// // With custom return value
/// fail_point!(registry, "my_failpoint", return Err(Error::Interrupted));
/// ```
#[macro_export]
macro_rules! fail_point {
    ($registry:expr, $name:expr) => {
        if cfg!(feature = "failpoints") || cfg!(test) {
            let _ = $registry.check($name);
        }
    };

    ($registry:expr, $name:expr, return) => {
        if cfg!(feature = "failpoints") || cfg!(test) {
            if $registry.check($name) == $crate::testing::recovery::FailpointResult::Return {
                return;
            }
        }
    };

    ($registry:expr, $name:expr, return $val:expr) => {
        if cfg!(feature = "failpoints") || cfg!(test) {
            if $registry.check($name) == $crate::testing::recovery::FailpointResult::Return {
                return $val;
            }
        }
    };

    ($name:expr) => {
        if cfg!(feature = "failpoints") || cfg!(test) {
            let _ = $crate::testing::recovery::global_registry().check($name);
        }
    };
}

/// Async version of the fail_point macro.
#[macro_export]
macro_rules! fail_point_async {
    ($registry:expr, $name:expr) => {
        if cfg!(feature = "failpoints") || cfg!(test) {
            let _ = $registry.check_async($name).await;
        }
    };

    ($name:expr) => {
        if cfg!(feature = "failpoints") || cfg!(test) {
            let _ = $crate::testing::recovery::global_registry()
                .check_async($name)
                .await;
        }
    };
}

pub use fail_point;
pub use fail_point_async;

#[cfg(test)]
mod tests {
    use super::*;
    use std::panic::AssertUnwindSafe;
    use std::sync::atomic::AtomicUsize;

    #[test]
    fn test_failpoint_panic() {
        let registry = FailpointRegistry::new();
        registry.enable("test_panic", FailpointAction::Panic);

        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            registry.check("test_panic");
        }));

        assert!(result.is_err());
    }

    #[test]
    fn test_failpoint_disabled() {
        let registry = FailpointRegistry::new();
        registry.enable("test_disabled", FailpointAction::Panic);
        registry.disable("test_disabled");

        // Should not panic
        let result = registry.check("test_disabled");
        assert_eq!(result, FailpointResult::Continue);
    }

    #[test]
    fn test_failpoint_globally_disabled() {
        let registry = FailpointRegistry::new();
        registry.enable("test_global", FailpointAction::Panic);
        registry.set_enabled(false);

        // Should not panic
        let result = registry.check("test_global");
        assert_eq!(result, FailpointResult::Continue);
    }

    #[test]
    fn test_failpoint_callback() {
        let registry = FailpointRegistry::new();
        let counter = Arc::new(AtomicUsize::new(0));
        let counter_clone = counter.clone();

        registry.enable(
            "test_callback",
            FailpointAction::Callback(Arc::new(move || {
                counter_clone.fetch_add(1, Ordering::Relaxed);
            })),
        );

        registry.check("test_callback");
        registry.check("test_callback");

        assert_eq!(counter.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn test_failpoint_countdown() {
        let registry = FailpointRegistry::new();
        registry.enable("test_countdown", FailpointAction::CountdownPanic(3));

        // First two hits should not panic
        assert_eq!(registry.check("test_countdown"), FailpointResult::Continue);
        assert_eq!(registry.check("test_countdown"), FailpointResult::Continue);

        // Third hit should panic
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            registry.check("test_countdown");
        }));
        assert!(result.is_err());
    }

    #[test]
    fn test_failpoint_once() {
        let registry = FailpointRegistry::new();
        registry.enable("test_once", FailpointAction::OncePanic);

        // First hit should panic
        let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
            registry.check("test_once");
        }));
        assert!(result.is_err());

        // Re-enable for second test
        registry.enable("test_once2", FailpointAction::OncePanic);

        // After panic, should be disabled
        // (Note: in real code, we'd need to recover from the panic)
    }

    #[test]
    fn test_failpoint_stats() {
        let registry = FailpointRegistry::new();
        registry.enable("test_stats", FailpointAction::Return);

        registry.check("test_stats");
        registry.check("test_stats");
        registry.check("test_stats");

        let stats = registry.stats("test_stats").unwrap();
        assert_eq!(stats.hit_count, 3);
        assert_eq!(stats.triggered_count, 3);
    }

    #[test]
    fn test_failpoint_return() {
        let registry = FailpointRegistry::new();
        registry.enable("test_return", FailpointAction::Return);

        let result = registry.check("test_return");
        assert_eq!(result, FailpointResult::Return);
    }

    #[test]
    fn test_failpoint_sleep() {
        let registry = FailpointRegistry::new();
        registry.enable("test_sleep", FailpointAction::Sleep(Duration::from_millis(10)));

        let result = registry.check("test_sleep");
        assert!(matches!(result, FailpointResult::Sleep(_)));
    }

    #[tokio::test]
    async fn test_failpoint_sleep_async() {
        let registry = FailpointRegistry::new();
        registry.enable("test_sleep_async", FailpointAction::Sleep(Duration::from_millis(10)));

        let start = std::time::Instant::now();
        let result = registry.check_async("test_sleep_async").await;
        let elapsed = start.elapsed();

        assert_eq!(result, FailpointResult::Continue);
        assert!(elapsed >= Duration::from_millis(10));
    }
}
