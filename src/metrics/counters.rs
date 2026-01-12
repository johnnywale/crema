//! Counter metrics for monotonically increasing values.

use std::sync::atomic::{AtomicU64, Ordering};

/// A monotonically increasing counter.
#[derive(Debug)]
pub struct Counter {
    name: &'static str,
    help: &'static str,
    value: AtomicU64,
}

impl Counter {
    /// Create a new counter.
    pub const fn new(name: &'static str, help: &'static str) -> Self {
        Self {
            name,
            help,
            value: AtomicU64::new(0),
        }
    }

    /// Get the counter name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the counter help text.
    pub fn help(&self) -> &'static str {
        self.help
    }

    /// Increment the counter by 1.
    pub fn inc(&self) {
        self.value.fetch_add(1, Ordering::Relaxed);
    }

    /// Increment the counter by a specific amount.
    pub fn inc_by(&self, n: u64) {
        self.value.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the current value.
    pub fn get(&self) -> u64 {
        self.value.load(Ordering::Relaxed)
    }

    /// Reset the counter to zero.
    pub fn reset(&self) {
        self.value.store(0, Ordering::Relaxed);
    }
}

/// A counter with labels for dimensional metrics.
#[derive(Debug)]
pub struct LabeledCounter<const N: usize> {
    name: &'static str,
    help: &'static str,
    label_names: [&'static str; N],
    counters: parking_lot::RwLock<std::collections::HashMap<[String; N], AtomicU64>>,
}

impl<const N: usize> LabeledCounter<N> {
    /// Create a new labeled counter.
    pub fn new(
        name: &'static str,
        help: &'static str,
        label_names: [&'static str; N],
    ) -> Self {
        Self {
            name,
            help,
            label_names,
            counters: parking_lot::RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Get the counter name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the counter help text.
    pub fn help(&self) -> &'static str {
        self.help
    }

    /// Get the label names.
    pub fn label_names(&self) -> &[&'static str; N] {
        &self.label_names
    }

    /// Increment the counter with the given labels.
    pub fn inc(&self, labels: [&str; N]) {
        self.inc_by(labels, 1);
    }

    /// Increment the counter by a specific amount with the given labels.
    pub fn inc_by(&self, labels: [&str; N], n: u64) {
        let key: [String; N] = labels.map(|s| s.to_string());

        // Try to read first
        {
            let counters = self.counters.read();
            if let Some(counter) = counters.get(&key) {
                counter.fetch_add(n, Ordering::Relaxed);
                return;
            }
        }

        // Need to insert
        let mut counters = self.counters.write();
        let counter = counters
            .entry(key)
            .or_insert_with(|| AtomicU64::new(0));
        counter.fetch_add(n, Ordering::Relaxed);
    }

    /// Get the value for specific labels.
    pub fn get(&self, labels: [&str; N]) -> u64 {
        let key: [String; N] = labels.map(|s| s.to_string());
        self.counters
            .read()
            .get(&key)
            .map(|c| c.load(Ordering::Relaxed))
            .unwrap_or(0)
    }

    /// Get all values with their labels.
    pub fn get_all(&self) -> Vec<([String; N], u64)> {
        self.counters
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.load(Ordering::Relaxed)))
            .collect()
    }

    /// Reset all counters.
    pub fn reset(&self) {
        self.counters.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_counter() {
        let counter = Counter::new("test_counter", "A test counter");

        assert_eq!(counter.get(), 0);

        counter.inc();
        assert_eq!(counter.get(), 1);

        counter.inc_by(5);
        assert_eq!(counter.get(), 6);

        counter.reset();
        assert_eq!(counter.get(), 0);
    }

    #[test]
    fn test_labeled_counter() {
        let counter = LabeledCounter::<2>::new(
            "test_labeled",
            "A labeled counter",
            ["method", "status"],
        );

        counter.inc(["GET", "200"]);
        counter.inc(["GET", "200"]);
        counter.inc(["POST", "201"]);

        assert_eq!(counter.get(["GET", "200"]), 2);
        assert_eq!(counter.get(["POST", "201"]), 1);
        assert_eq!(counter.get(["GET", "404"]), 0);

        let all = counter.get_all();
        assert_eq!(all.len(), 2);
    }
}
