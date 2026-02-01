//! Facade module providing a unified metrics API with conditional compilation.
//!
//! This module provides macros and helper functions that work both with and without
//! the `metrics` feature enabled. When the feature is disabled, all operations are
//! no-ops with zero runtime overhead.

use std::time::{Duration, Instant};

/// A guard that records duration when dropped (for timing operations).
///
/// This is a simple timer that measures elapsed time. When dropped, it can
/// optionally record the duration to a metric (when the `metrics` feature is enabled).
pub struct TimerGuard {
    start: Instant,
    #[cfg(feature = "metrics")]
    metric_name: &'static str,
    #[cfg(feature = "metrics")]
    recorded: bool,
}

impl TimerGuard {
    /// Create a new timer guard.
    #[cfg(feature = "metrics")]
    pub fn new(metric_name: &'static str, _labels: Vec<(&'static str, String)>) -> Self {
        Self {
            start: Instant::now(),
            metric_name,
            recorded: false,
        }
    }

    #[cfg(not(feature = "metrics"))]
    pub fn new(_metric_name: &'static str, _labels: Vec<(&'static str, String)>) -> Self {
        Self {
            start: Instant::now(),
        }
    }

    /// Get elapsed time without recording.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }

    /// Stop the timer and return elapsed duration without recording.
    #[cfg(feature = "metrics")]
    pub fn stop(mut self) -> Duration {
        self.recorded = true; // Mark as recorded to prevent drop from recording
        self.start.elapsed()
    }

    #[cfg(not(feature = "metrics"))]
    pub fn stop(self) -> Duration {
        self.start.elapsed()
    }

    /// Stop and record manually (useful when you need the duration value).
    #[cfg(feature = "metrics")]
    pub fn stop_and_record(mut self) -> Duration {
        let duration = self.start.elapsed();
        metrics::histogram!(self.metric_name).record(duration.as_secs_f64());
        self.recorded = true;
        duration
    }

    #[cfg(not(feature = "metrics"))]
    pub fn stop_and_record(self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for TimerGuard {
    #[cfg(feature = "metrics")]
    fn drop(&mut self) {
        if !self.recorded {
            let duration = self.start.elapsed();
            metrics::histogram!(self.metric_name).record(duration.as_secs_f64());
        }
    }

    #[cfg(not(feature = "metrics"))]
    fn drop(&mut self) {
        // No-op
    }
}

// ============================================================================
// Counter Macros
// ============================================================================

/// Increment a counter by 1.
///
/// # Examples
/// ```ignore
/// counter_inc!("crema_cache_get_total");
/// counter_inc!("crema_cache_get_total", "node_id" => "1", "result" => "hit");
/// ```
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! counter_inc {
    ($name:expr) => {
        metrics::counter!($name).increment(1)
    };
    ($name:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {
        metrics::counter!($name, $($label_key => $label_value),+).increment(1)
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! counter_inc {
    ($name:expr) => {};
    ($name:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {};
}

/// Increment a counter by a specific amount.
///
/// # Examples
/// ```ignore
/// counter_add!("crema_cache_bytes_written", 1024);
/// counter_add!("crema_cache_bytes_written", 1024, "node_id" => "1");
/// ```
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! counter_add {
    ($name:expr, $value:expr) => {
        metrics::counter!($name).increment($value)
    };
    ($name:expr, $value:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {
        metrics::counter!($name, $($label_key => $label_value),+).increment($value)
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! counter_add {
    ($name:expr, $value:expr) => {};
    ($name:expr, $value:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {};
}

// ============================================================================
// Gauge Macros
// ============================================================================

/// Set a gauge to an absolute value.
///
/// # Examples
/// ```ignore
/// gauge_set!("crema_cache_entries", 1000);
/// gauge_set!("crema_cache_entries", 1000, "node_id" => "1");
/// ```
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! gauge_set {
    ($name:expr, $value:expr) => {
        metrics::gauge!($name).set($value as f64)
    };
    ($name:expr, $value:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {
        metrics::gauge!($name, $($label_key => $label_value),+).set($value as f64)
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! gauge_set {
    ($name:expr, $value:expr) => {};
    ($name:expr, $value:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {};
}

/// Increment a gauge by a value.
///
/// # Examples
/// ```ignore
/// gauge_inc!("crema_connections_active");
/// gauge_inc!("crema_connections_active", "node_id" => "1");
/// ```
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! gauge_inc {
    ($name:expr) => {
        metrics::gauge!($name).increment(1.0)
    };
    ($name:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {
        metrics::gauge!($name, $($label_key => $label_value),+).increment(1.0)
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! gauge_inc {
    ($name:expr) => {};
    ($name:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {};
}

/// Decrement a gauge by a value.
///
/// # Examples
/// ```ignore
/// gauge_dec!("crema_connections_active");
/// gauge_dec!("crema_connections_active", "node_id" => "1");
/// ```
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! gauge_dec {
    ($name:expr) => {
        metrics::gauge!($name).decrement(1.0)
    };
    ($name:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {
        metrics::gauge!($name, $($label_key => $label_value),+).decrement(1.0)
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! gauge_dec {
    ($name:expr) => {};
    ($name:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {};
}

// ============================================================================
// Histogram Macros
// ============================================================================

/// Record a value in a histogram.
///
/// # Examples
/// ```ignore
/// histogram_record!("crema_cache_get_duration_seconds", 0.005);
/// histogram_record!("crema_cache_get_duration_seconds", 0.005, "node_id" => "1");
/// ```
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! histogram_record {
    ($name:expr, $value:expr) => {
        metrics::histogram!($name).record($value)
    };
    ($name:expr, $value:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {
        metrics::histogram!($name, $($label_key => $label_value),+).record($value)
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! histogram_record {
    ($name:expr, $value:expr) => {};
    ($name:expr, $value:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {};
}

/// Record a duration in a histogram (converts to seconds).
///
/// # Examples
/// ```ignore
/// histogram_record_duration!("crema_cache_get_duration_seconds", duration);
/// histogram_record_duration!("crema_cache_get_duration_seconds", duration, "node_id" => "1");
/// ```
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! histogram_record_duration {
    ($name:expr, $duration:expr) => {
        metrics::histogram!($name).record($duration.as_secs_f64())
    };
    ($name:expr, $duration:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {
        metrics::histogram!($name, $($label_key => $label_value),+).record($duration.as_secs_f64())
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! histogram_record_duration {
    ($name:expr, $duration:expr) => {};
    ($name:expr, $duration:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {};
}

/// Start a timer that records duration on drop.
///
/// # Examples
/// ```ignore
/// let _timer = timer_start!("crema_cache_get_duration_seconds");
/// let _timer = timer_start!("crema_cache_get_duration_seconds", "node_id" => node_id.to_string());
/// ```
#[macro_export]
macro_rules! timer_start {
    ($name:expr) => {
        $crate::metrics::facade::TimerGuard::new($name, vec![])
    };
    ($name:expr, $($label_key:expr => $label_value:expr),+ $(,)?) => {
        $crate::metrics::facade::TimerGuard::new(
            $name,
            vec![$(($label_key, $label_value.to_string())),+]
        )
    };
}

// ============================================================================
// Describe Macros (for registering metric metadata)
// ============================================================================

/// Describe a counter metric.
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! describe_counter {
    ($name:expr, $description:expr) => {
        metrics::describe_counter!($name, $description)
    };
    ($name:expr, $unit:expr, $description:expr) => {
        metrics::describe_counter!($name, $unit, $description)
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! describe_counter {
    ($name:expr, $description:expr) => {};
    ($name:expr, $unit:expr, $description:expr) => {};
}

/// Describe a gauge metric.
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! describe_gauge {
    ($name:expr, $description:expr) => {
        metrics::describe_gauge!($name, $description)
    };
    ($name:expr, $unit:expr, $description:expr) => {
        metrics::describe_gauge!($name, $unit, $description)
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! describe_gauge {
    ($name:expr, $description:expr) => {};
    ($name:expr, $unit:expr, $description:expr) => {};
}

/// Describe a histogram metric.
#[macro_export]
#[cfg(feature = "metrics")]
macro_rules! describe_histogram {
    ($name:expr, $description:expr) => {
        metrics::describe_histogram!($name, $description)
    };
    ($name:expr, $unit:expr, $description:expr) => {
        metrics::describe_histogram!($name, $unit, $description)
    };
}

#[macro_export]
#[cfg(not(feature = "metrics"))]
macro_rules! describe_histogram {
    ($name:expr, $description:expr) => {};
    ($name:expr, $unit:expr, $description:expr) => {};
}

// Re-export macros at module level for convenience
pub use counter_add;
pub use counter_inc;
pub use describe_counter;
pub use describe_gauge;
pub use describe_histogram;
pub use gauge_dec;
pub use gauge_inc;
pub use gauge_set;
pub use histogram_record;
pub use histogram_record_duration;
pub use timer_start;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_timer_guard_elapsed() {
        let guard = TimerGuard::new("test_metric", vec![]);
        std::thread::sleep(Duration::from_millis(10));
        assert!(guard.elapsed() >= Duration::from_millis(10));
    }

    #[test]
    fn test_timer_guard_stop() {
        let guard = TimerGuard::new("test_metric", vec![]);
        std::thread::sleep(Duration::from_millis(10));
        let duration = guard.stop();
        assert!(duration >= Duration::from_millis(10));
    }

    #[test]
    fn test_counter_macros() {
        // These should compile and not panic
        counter_inc!("test_counter");
        counter_inc!("test_counter", "label" => "value");
        counter_add!("test_counter", 5);
        counter_add!("test_counter", 5, "label" => "value");
    }

    #[test]
    fn test_gauge_macros() {
        gauge_set!("test_gauge", 100);
        gauge_set!("test_gauge", 100, "label" => "value");
        gauge_inc!("test_gauge");
        gauge_dec!("test_gauge");
    }

    #[test]
    fn test_histogram_macros() {
        histogram_record!("test_histogram", 0.5);
        histogram_record!("test_histogram", 0.5, "label" => "value");
        histogram_record_duration!("test_histogram", Duration::from_millis(100));
    }

    #[test]
    fn test_timer_macro() {
        let _timer = timer_start!("test_timer");
        let _timer2 = timer_start!("test_timer", "node_id" => "1");
    }
}
