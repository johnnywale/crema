//! Gauge metrics for values that can increase or decrease.

use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// A gauge that can increase or decrease.
#[derive(Debug)]
pub struct Gauge {
    name: &'static str,
    help: &'static str,
    value: AtomicI64,
}

impl Gauge {
    /// Create a new gauge.
    pub const fn new(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            value: AtomicI64::new(0),
        }
    }

    /// Get the gauge name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the gauge help text.
    pub fn help(&self) -> &'static str {
        self.help
    }

    /// Set the gauge to a specific value.
    pub fn set(&self, value: i64) {
        self.value.store(value, Ordering::Relaxed);
    }

    /// Increment the gauge by 1.
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Decrement the gauge by 1.
    pub fn dec(&self) {
        self.value.fetch_sub(1, Ordering::Relaxed);
    }

    /// Add to the gauge.
    pub fn add(&self, n: i64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Subtract from the gauge.
    pub fn sub(&self, n: i64) {
        self.value.fetch_sub(n, Ordering::Relaxed);
    }

    /// Get the current value.
    pub fn get(&self) -> i64 {
        self.value.load(Ordering::Relaxed)
    }
}

/// A gauge for floating-point values.
#[derive(Debug)]
pub struct FloatGauge {
    name: &'static str,
    help: &'static str,
    // Store as bits for atomic operations
    value: AtomicU64,
}

impl FloatGauge {
    /// Create a new float gauge.
    pub const fn new(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            value: AtomicU64::new(0),
        }
    }

    /// Get the gauge name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the gauge help text.
    pub fn help(&self) -> &'static str {
        self.help
    }

    /// Set the gauge to a specific value.
    pub fn set(&self, value: f64) {
        self.value.store(value.to_bits(), Ordering::Relaxed);
    }

    /// Get the current value.
    pub fn get(&self) -> f64 {
        f64::from_bits(self.value.load(Ordering::Relaxed))
    }

    /// Add to the gauge (not atomic, use with caution in concurrent scenarios).
    pub fn add(&self, n: f64) {
        loop {
            let current = self.value.load(Ordering::Relaxed);
            let new_value = f64::from_bits(current) + n;
            if self
                .value
                .compare_exchange_weak(
                    current,
                    new_value.to_bits(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }
    }
}

/// A gauge with labels for dimensional metrics.
#[derive(Debug)]
pub struct LabeledGauge<const N: usize> {
    name: &'static str,
    help: &'static str,
    label_names: [&'static str; N],
    gauges: parking_lot::RwLock<std::collections::HashMap<[String; N], AtomicI64>>,
}

impl<const N: usize> LabeledGauge<N> {
    /// Create a new labeled gauge.
    pub fn new(
        name: &'static str,
        help: &'static str,
        label_names: [&'static str; N],
    ) -> Self {
        Self {
            name,
            help,
            label_names,
            gauges: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Get the gauge name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the gauge help text.
    pub fn help(&self) -> &'static str {
        self.help
    }

    /// Get the label names.
    pub fn label_names(&self) -> &[&'static str; N] {
        &self.label_names
    }

    /// Set the gauge with the given labels.
    pub fn set(&self, labels: [&str; N], value: i64) {
        let key: [String; N] = labels.map(|s| s.to_string());
        let mut gauges = self.gauges.write();
        gauges
            .entry(key)
            .or_insert_with(|| AtomicI64::new(0))
            .store(value, Ordering::Relaxed);
    }

    /// Increment the gauge with the given labels.
    pub fn inc(&self, labels: [&str; N]) {
        self.add(labels, 1);
    }

    /// Decrement the gauge with the given labels.
    pub fn dec(&self, labels: [&str; N]) {
        self.add(labels, -1);
    }

    /// Add to the gauge with the given labels.
    pub fn add(&self, labels: [&str; N], n: i64) {
        let key: [String; N] = labels.map(|s| s.to_string());

        {
            let gauges = self.gauges.read();
            if let Some(gauge) = gauges.get(&key) {
                gauge.fetch_add(n, Ordering::Relaxed);
                return;
            }
        }

        let mut gauges = self.gauges.write();
        let gauge = gauges.entry(key).or_insert_with(|| AtomicI64::new(0));
        gauge.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the value for specific labels.
    pub fn get(&self, labels: [&str; N]) -> i64 {
        let key: [String; N] = labels.map(|s| s.to_string());
        self.gauges
            .read()
            .get(&key)
            .map(|g| g.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get all values with their labels.
    pub fn get_all(&self) -> Vec<([String; N], i64)> {
        self.gauges
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect()
    }

    /// Remove a gauge with specific labels.
    pub fn remove(&self, labels: [&str; N]) {
        let key: [String; N] = labels.map(|s| s.to_string());
        self.gauges.write().remove(&key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_gauge() {
        let gauge = Gauge::new("test_gauge", "A test gauge");

        assert_eq!(gauge.get(), 0);

        gauge.set(10);
        assert_eq!(gauge.get(), 10);

        gauge.inc();
        assert_eq!(gauge.get(), 11);

        gauge.dec();
        assert_eq!(gauge.get(), 10);

        gauge.add(5);
        assert_eq!(gauge.get(), 15);

        gauge.sub(3);
        assert_eq!(gauge.get(), 12);
    }

    #[test]
    fn test_float_gauge() {
        let gauge = FloatGauge::new("test_float", "A float gauge");

        assert_eq!(gauge.get(), 0.0);

        gauge.set(3.14);
        assert!((gauge.get() - 3.14).abs() < 0.001);

        gauge.add(1.0);
        assert!((gauge.get() - 4.14).abs() < 0.001);
    }

    #[test]
    fn test_labeled_gauge() {
        let gauge = LabeledGauge::<1>::new("test_labeled", "A labeled gauge", ["node"]);

        gauge.set(["node1"], 10);
        gauge.set(["node2"], 20);

        assert_eq!(gauge.get(["node1"]), 10);
        assert_eq!(gauge.get(["node2"]), 20);

        gauge.inc(["node1"]);
        assert_eq!(gauge.get(["node1"]), 11);

        gauge.remove(["node2"]);
        assert_eq!(gauge.get(["node2"]), 0);
    }
}
