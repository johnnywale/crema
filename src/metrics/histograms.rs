//! Histogram metrics for measuring distributions of values.

use parking_lot::RwLock;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Default histogram buckets (in seconds) for latency measurements.
pub const DEFAULT_BUCKETS: &[f64] = &[
    0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
];

/// A histogram for measuring distributions.
#[derive(Debug)]
pub struct Histogram {
    name: &'static str,
    help: &'static str,
    buckets: Vec<f64>,
    bucket_counts: Vec<AtomicU64>,
    sum: AtomicU64, // Store as bits
    count: AtomicU64,
}

impl Histogram {
    /// Create a new histogram with default buckets.
    pub fn new(name: &'static str, help: &'static str) -> Self {
        Self::with_buckets(name, help, DEFAULT_BUCKETS.to_vec())
    }

    /// Create a new histogram with custom buckets.
    pub fn with_buckets(name: &'static str, help: &'static str, mut buckets: Vec<f64>) -> Self {
        buckets.sort_by(|a, b| a.partial_cmp(b).unwrap());

        let bucket_counts = buckets.iter().map(|_| AtomicU64::new(0)).collect();

        Self {
            name,
            help,
            buckets,
            bucket_counts,
            sum: AtomicU64::new(0),
            count: AtomicU64::new(0),
        }
    }

    /// Get the histogram name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the histogram help text.
    pub fn help(&self) -> &'static str {
        self.help
    }

    /// Get the bucket boundaries.
    pub fn buckets(&self) -> &[f64] {
        &self.buckets
    }

    /// Observe a value.
    pub fn observe(&self, value: f64) {
        // Update count
        self.count.fetch_add(1, Ordering::Relaxed);

        // Update sum (using CAS for float)
        loop {
            let current = self.sum.load(Ordering::Relaxed);
            let new_sum = f64::from_bits(current) + value;
            if self
                .sum
                .compare_exchange_weak(
                    current,
                    new_sum.to_bits(),
                    Ordering::Relaxed,
                    Ordering::Relaxed,
                )
                .is_ok()
            {
                break;
            }
        }

        // Update buckets
        for (i, &upper) in self.buckets.iter().enumerate() {
            if value <= upper {
                self.bucket_counts[i].fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Observe a duration in seconds.
    pub fn observe_duration(&self, duration: Duration) {
        self.observe(duration.as_secs_f64());
    }

    /// Start a timer that will observe when dropped.
    pub fn start_timer(&self) -> HistogramTimer<'_> {
        HistogramTimer {
            histogram: self,
            start: Instant::now(),
        }
    }

    /// Get the total count of observations.
    pub fn get_count(&self) -> u64 {
        self.count.load(Ordering::Relaxed)
    }

    /// Get the sum of all observations.
    pub fn get_sum(&self) -> f64 {
        f64::from_bits(self.sum.load(Ordering::Relaxed))
    }

    /// Get the bucket counts.
    pub fn get_bucket_counts(&self) -> Vec<u64> {
        self.bucket_counts
            .iter()
            .map(|c| c.load(Ordering::Relaxed))
            .collect()
    }

    /// Get a snapshot of the histogram data.
    pub fn snapshot(&self) -> HistogramSnapshot {
        HistogramSnapshot {
            buckets: self.buckets.clone(),
            bucket_counts: self.get_bucket_counts(),
            sum: self.get_sum(),
            count: self.get_count(),
        }
    }

    /// Reset the histogram.
    pub fn reset(&self) {
        self.count.store(0, Ordering::Relaxed);
        self.sum.store(0, Ordering::Relaxed);
        for bucket in &self.bucket_counts {
            bucket.store(0, Ordering::Relaxed);
        }
    }
}

/// A timer that observes duration when dropped.
pub struct HistogramTimer<'a> {
    histogram: &'a Histogram,
    start: Instant,
}

impl<'a> HistogramTimer<'a> {
    /// Stop the timer and observe the duration.
    pub fn observe(self) -> Duration {
        let duration = self.start.elapsed();
        self.histogram.observe_duration(duration);
        std::mem::forget(self); // Don't observe twice
        duration
    }

    /// Get elapsed time without observing.
    pub fn elapsed(&self) -> Duration {
        self.start.elapsed()
    }
}

impl Drop for HistogramTimer<'_> {
    fn drop(&mut self) {
        self.histogram.observe_duration(self.start.elapsed());
    }
}

/// A snapshot of histogram data.
#[derive(Debug, Clone)]
pub struct HistogramSnapshot {
    /// Bucket boundaries.
    pub buckets: Vec<f64>,
    /// Count of observations <= each bucket boundary.
    pub bucket_counts: Vec<u64>,
    /// Sum of all observations.
    pub sum: f64,
    /// Total count of observations.
    pub count: u64,
}

impl HistogramSnapshot {
    /// Calculate the mean.
    pub fn mean(&self) -> f64 {
        if self.count == 0 {
            0.0
        } else {
            self.sum / self.count as f64
        }
    }

    /// Estimate a percentile (approximate).
    pub fn percentile(&self, p: f64) -> f64 {
        if self.count == 0 {
            return 0.0;
        }

        let target = (self.count as f64 * p / 100.0).ceil() as u64;
        let mut prev_count = 0;
        let mut prev_bucket = 0.0;

        for (i, &count) in self.bucket_counts.iter().enumerate() {
            if count >= target {
                // Linear interpolation within bucket
                let bucket_range = self.buckets[i] - prev_bucket;
                let bucket_count = count - prev_count;
                if bucket_count == 0 {
                    return self.buckets[i];
                }
                let position = (target - prev_count) as f64 / bucket_count as f64;
                return prev_bucket + position * bucket_range;
            }
            prev_count = count;
            prev_bucket = self.buckets[i];
        }

        // Return the highest bucket
        *self.buckets.last().unwrap_or(&0.0)
    }
}

/// A histogram with labels.
#[derive(Debug)]
pub struct LabeledHistogram<const N: usize> {
    name: &'static str,
    help: &'static str,
    label_names: [&'static str; N],
    buckets: Vec<f64>,
    histograms: RwLock<std::collections::HashMap<[String; N], Histogram>>,
}

impl<const N: usize> LabeledHistogram<N> {
    /// Create a new labeled histogram with default buckets.
    pub fn new(name: &'static str, help: &'static str, label_names: [&'static str; N]) -> Self {
        Self::with_buckets(name, help, label_names, DEFAULT_BUCKETS.to_vec())
    }

    /// Create a new labeled histogram with custom buckets.
    pub fn with_buckets(
        name: &'static str,
        help: &'static str,
        label_names: [&'static str; N],
        buckets: Vec<f64>,
    ) -> Self {
        Self {
            name,
            help,
            label_names,
            buckets,
            histograms: RwLock::new(std::collections::HashMap::new()),
        }
    }

    /// Get the histogram name.
    pub fn name(&self) -> &'static str {
        self.name
    }

    /// Get the histogram help text.
    pub fn help(&self) -> &'static str {
        self.help
    }

    /// Observe a value with the given labels.
    pub fn observe(&self, labels: [&str; N], value: f64) {
        let key: [String; N] = labels.map(|s| s.to_string());

        {
            let histograms = self.histograms.read();
            if let Some(h) = histograms.get(&key) {
                h.observe(value);
                return;
            }
        }

        let mut histograms = self.histograms.write();
        let histogram = histograms.entry(key).or_insert_with(|| {
            Histogram::with_buckets(self.name, self.help, self.buckets.clone())
        });
        histogram.observe(value);
    }

    /// Observe a duration with the given labels.
    pub fn observe_duration(&self, labels: [&str; N], duration: Duration) {
        self.observe(labels, duration.as_secs_f64());
    }

    /// Get a snapshot for specific labels.
    pub fn snapshot(&self, labels: [&str; N]) -> Option<HistogramSnapshot> {
        let key: [String; N] = labels.map(|s| s.to_string());
        self.histograms.read().get(&key).map(|h| h.snapshot())
    }

    /// Get all snapshots with their labels.
    pub fn get_all(&self) -> Vec<([String; N], HistogramSnapshot)> {
        self.histograms
            .read()
            .iter()
            .map(|(k, v)| (k.clone(), v.snapshot()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_histogram() {
        let hist = Histogram::new("test_hist", "A test histogram");

        hist.observe(0.005);
        hist.observe(0.05);
        hist.observe(0.5);
        hist.observe(5.0);

        assert_eq!(hist.get_count(), 4);
        assert!((hist.get_sum() - 5.555).abs() < 0.001);

        let snapshot = hist.snapshot();
        assert_eq!(snapshot.count, 4);
    }

    #[test]
    fn test_histogram_timer() {
        let hist = Histogram::new("test_timer", "Timer test");

        {
            let _timer = hist.start_timer();
            std::thread::sleep(Duration::from_millis(10));
        }

        assert_eq!(hist.get_count(), 1);
        assert!(hist.get_sum() > 0.01);
    }

    #[test]
    fn test_histogram_percentile() {
        let hist = Histogram::with_buckets(
            "test_pct",
            "Percentile test",
            vec![1.0, 5.0, 10.0, 50.0, 100.0],
        );

        for i in 1..=100 {
            hist.observe(i as f64);
        }

        let snapshot = hist.snapshot();
        let p50 = snapshot.percentile(50.0);
        let p99 = snapshot.percentile(99.0);

        // These are approximate due to bucketing
        assert!(p50 > 40.0 && p50 < 60.0);
        assert!(p99 > 90.0);
    }

    #[test]
    fn test_labeled_histogram() {
        let hist = LabeledHistogram::<1>::new("test_labeled", "Labeled test", ["operation"]);

        hist.observe(["read"], 0.001);
        hist.observe(["read"], 0.002);
        hist.observe(["write"], 0.01);

        let read_snapshot = hist.snapshot(["read"]).unwrap();
        assert_eq!(read_snapshot.count, 2);

        let write_snapshot = hist.snapshot(["write"]).unwrap();
        assert_eq!(write_snapshot.count, 1);
    }
}
