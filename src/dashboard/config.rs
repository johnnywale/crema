//! Dashboard configuration.

use std::net::SocketAddr;

/// Configuration for the dashboard server.
#[derive(Debug, Clone)]
pub struct DashboardConfig {
    /// Whether the dashboard is enabled.
    pub enabled: bool,
    /// Port to bind the dashboard server to.
    pub port: u16,
    /// Address to bind to.
    pub bind_addr: String,
    /// Interval for SSE updates in milliseconds.
    pub update_interval_ms: u64,
}

impl Default for DashboardConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            port: 8080,
            bind_addr: "0.0.0.0".to_string(),
            update_interval_ms: 1000,
        }
    }
}

impl DashboardConfig {
    /// Create a new dashboard config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the port.
    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    /// Set the bind address.
    pub fn with_bind_addr(mut self, addr: impl Into<String>) -> Self {
        self.bind_addr = addr.into();
        self
    }

    /// Set the SSE update interval in milliseconds.
    pub fn with_update_interval_ms(mut self, ms: u64) -> Self {
        self.update_interval_ms = ms;
        self
    }

    /// Enable or disable the dashboard.
    pub fn with_enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// Get the socket address to bind to.
    /// Returns an error if the address is invalid.
    pub fn socket_addr(&self) -> Result<SocketAddr, std::net::AddrParseError> {
        format!("{}:{}", self.bind_addr, self.port).parse()
    }

    /// Get the socket address, panicking if invalid.
    /// Use this only in contexts where invalid config would be a bug.
    pub fn socket_addr_or_panic(&self) -> SocketAddr {
        self.socket_addr().unwrap_or_else(|e| {
            panic!(
                "Invalid dashboard address '{}:{}': {}",
                self.bind_addr, self.port, e
            )
        })
    }
}
