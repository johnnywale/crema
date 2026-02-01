//! Prometheus exporter setup and utilities.
//!
//! This module provides integration with `metrics-exporter-prometheus` for
//! exposing metrics in Prometheus format.

use std::net::SocketAddr;

/// Result type for Prometheus operations.
pub type PrometheusResult<T> = Result<T, PrometheusError>;

/// Error type for Prometheus operations.
#[derive(Debug)]
pub enum PrometheusError {
    /// Failed to install the Prometheus recorder.
    InstallFailed(String),
    /// Failed to bind to address.
    BindFailed(String),
    /// Exporter not available (feature disabled).
    NotAvailable,
}

impl std::fmt::Display for PrometheusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            PrometheusError::InstallFailed(msg) => {
                write!(f, "Failed to install Prometheus recorder: {}", msg)
            }
            PrometheusError::BindFailed(msg) => {
                write!(f, "Failed to bind Prometheus exporter: {}", msg)
            }
            PrometheusError::NotAvailable => {
                write!(
                    f,
                    "Prometheus exporter not available (metrics feature disabled)"
                )
            }
        }
    }
}

impl std::error::Error for PrometheusError {}

/// Configuration for the Prometheus exporter.
#[derive(Debug, Clone)]
pub struct PrometheusConfig {
    /// Address to listen on for the metrics endpoint.
    pub listen_addr: SocketAddr,
    /// Endpoint path for metrics (default: "/metrics").
    pub endpoint: String,
    /// Whether to include process metrics.
    pub include_process_metrics: bool,
    /// Idle timeout for histogram buckets (in seconds).
    pub idle_timeout_secs: Option<u64>,
    /// Upkeep interval for cleaning up stale metrics (in seconds).
    pub upkeep_interval_secs: u64,
}

impl Default for PrometheusConfig {
    fn default() -> Self {
        Self {
            listen_addr: ([0, 0, 0, 0], 9090).into(),
            endpoint: "/metrics".to_string(),
            include_process_metrics: true,
            idle_timeout_secs: Some(600), // 10 minutes
            upkeep_interval_secs: 30,
        }
    }
}

impl PrometheusConfig {
    /// Create a new Prometheus configuration.
    pub fn new(listen_addr: SocketAddr) -> Self {
        Self {
            listen_addr,
            ..Default::default()
        }
    }

    /// Set the endpoint path.
    pub fn with_endpoint(mut self, endpoint: impl Into<String>) -> Self {
        self.endpoint = endpoint.into();
        self
    }

    /// Disable process metrics.
    pub fn without_process_metrics(mut self) -> Self {
        self.include_process_metrics = false;
        self
    }

    /// Set the idle timeout for buckets.
    pub fn with_idle_timeout(mut self, secs: u64) -> Self {
        self.idle_timeout_secs = Some(secs);
        self
    }

    /// Disable idle timeout.
    pub fn without_idle_timeout(mut self) -> Self {
        self.idle_timeout_secs = None;
        self
    }

    /// Set the upkeep interval.
    pub fn with_upkeep_interval(mut self, secs: u64) -> Self {
        self.upkeep_interval_secs = secs;
        self
    }
}

/// Handle to the Prometheus exporter.
///
/// When dropped, the exporter will be stopped.
#[cfg(feature = "metrics")]
pub struct PrometheusHandle {
    handle: metrics_exporter_prometheus::PrometheusHandle,
}

#[cfg(not(feature = "metrics"))]
pub struct PrometheusHandle;

#[cfg(feature = "metrics")]
impl PrometheusHandle {
    /// Render metrics in Prometheus text format.
    ///
    /// This is useful for serving metrics via a custom HTTP endpoint
    /// (e.g., in an existing web server like the dashboard).
    pub fn render(&self) -> String {
        self.handle.render()
    }
}

#[cfg(not(feature = "metrics"))]
impl PrometheusHandle {
    /// Render metrics (no-op when feature is disabled).
    pub fn render(&self) -> String {
        String::new()
    }
}

/// Install the Prometheus recorder and return a handle.
///
/// This installs the `metrics-exporter-prometheus` recorder as the global
/// metrics recorder. The handle can be used to render metrics on demand.
///
/// # Example
///
/// ```ignore
/// use crema::metrics::prometheus::{install_recorder, PrometheusConfig};
///
/// let config = PrometheusConfig::default();
/// let handle = install_recorder(config)?;
///
/// // Later, render metrics for an HTTP response:
/// let metrics_text = handle.render();
/// ```
#[cfg(feature = "metrics")]
pub fn install_recorder(_config: PrometheusConfig) -> PrometheusResult<PrometheusHandle> {
    use metrics_exporter_prometheus::PrometheusBuilder;

    let builder = PrometheusBuilder::new();

    // Set up bucket configuration for histograms
    // Use descriptors buckets for latency metrics
    let builder = builder
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Suffix("_duration_seconds".to_string()),
            crate::metrics::descriptors::RAFT_LATENCY_BUCKETS,
        )
        .map_err(|e| PrometheusError::InstallFailed(e.to_string()))?
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Suffix("_size_bytes".to_string()),
            crate::metrics::descriptors::SIZE_BUCKETS,
        )
        .map_err(|e| PrometheusError::InstallFailed(e.to_string()))?;

    // Build the recorder and get the handle
    let handle = builder
        .install_recorder()
        .map_err(|e| PrometheusError::InstallFailed(e.to_string()))?;

    Ok(PrometheusHandle { handle })
}

#[cfg(not(feature = "metrics"))]
pub fn install_recorder(_config: PrometheusConfig) -> PrometheusResult<PrometheusHandle> {
    Err(PrometheusError::NotAvailable)
}

/// Start the Prometheus HTTP server for scraping.
///
/// This starts a simple HTTP server that serves metrics at the configured endpoint.
/// The server runs in the background and will be stopped when the handle is dropped.
///
/// **Note:** If you're using the dashboard feature, you may want to use `install_recorder`
/// instead and serve metrics through the dashboard's existing HTTP server.
#[cfg(feature = "metrics")]
pub async fn start_http_server(config: PrometheusConfig) -> PrometheusResult<()> {
    use metrics_exporter_prometheus::PrometheusBuilder;

    let builder = PrometheusBuilder::new()
        .with_http_listener(config.listen_addr)
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Suffix("_duration_seconds".to_string()),
            crate::metrics::descriptors::RAFT_LATENCY_BUCKETS,
        )
        .map_err(|e| PrometheusError::InstallFailed(e.to_string()))?
        .set_buckets_for_metric(
            metrics_exporter_prometheus::Matcher::Suffix("_size_bytes".to_string()),
            crate::metrics::descriptors::SIZE_BUCKETS,
        )
        .map_err(|e| PrometheusError::InstallFailed(e.to_string()))?;

    builder
        .install()
        .map_err(|e| PrometheusError::InstallFailed(e.to_string()))?;

    Ok(())
}

#[cfg(not(feature = "metrics"))]
pub async fn start_http_server(_config: PrometheusConfig) -> PrometheusResult<()> {
    Err(PrometheusError::NotAvailable)
}

/// Global Prometheus handle for rendering metrics.
static PROMETHEUS_HANDLE: once_cell::sync::OnceCell<PrometheusHandle> =
    once_cell::sync::OnceCell::new();

/// Install the Prometheus recorder globally.
///
/// This is a convenience function that installs the recorder and stores the handle
/// globally so it can be accessed from anywhere (e.g., HTTP handlers).
#[cfg(feature = "metrics")]
pub fn install_global_recorder(config: PrometheusConfig) -> PrometheusResult<()> {
    let handle = install_recorder(config)?;
    PROMETHEUS_HANDLE
        .set(handle)
        .map_err(|_| PrometheusError::InstallFailed("Already installed".to_string()))?;
    Ok(())
}

#[cfg(not(feature = "metrics"))]
pub fn install_global_recorder(_config: PrometheusConfig) -> PrometheusResult<()> {
    Err(PrometheusError::NotAvailable)
}

/// Get the global Prometheus handle.
///
/// Returns `None` if `install_global_recorder` hasn't been called.
pub fn global_handle() -> Option<&'static PrometheusHandle> {
    PROMETHEUS_HANDLE.get()
}

/// Render metrics from the global handle.
///
/// Returns an empty string if the Prometheus recorder hasn't been installed.
pub fn render_metrics() -> String {
    global_handle().map(|h| h.render()).unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_prometheus_config_builder() {
        let config = PrometheusConfig::new(([127, 0, 0, 1], 9091).into())
            .with_endpoint("/custom/metrics")
            .without_process_metrics()
            .with_idle_timeout(300);

        assert_eq!(config.listen_addr, ([127, 0, 0, 1], 9091).into());
        assert_eq!(config.endpoint, "/custom/metrics");
        assert!(!config.include_process_metrics);
        assert_eq!(config.idle_timeout_secs, Some(300));
    }

    #[test]
    fn test_default_config() {
        let config = PrometheusConfig::default();
        assert_eq!(config.endpoint, "/metrics");
        assert!(config.include_process_metrics);
    }
}
