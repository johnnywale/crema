//! Dashboard HTTP server.

use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::cache::DistributedCache;
use crate::dashboard::config::DashboardConfig;
use crate::dashboard::routes::build_router;

/// Dashboard server that serves the web UI and API.
pub struct DashboardServer {
    cache: Arc<DistributedCache>,
    config: DashboardConfig,
}

impl DashboardServer {
    /// Create a new dashboard server.
    pub fn new(cache: Arc<DistributedCache>, config: DashboardConfig) -> Self {
        Self { cache, config }
    }

    /// Start the dashboard server.
    ///
    /// This method will block until the server is shut down.
    pub async fn start(&self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.config.enabled {
            info!("Dashboard is disabled");
            return Ok(());
        }

        let addr = self.config.socket_addr().map_err(|e| {
            error!(
                "Invalid dashboard address '{}:{}': {}",
                self.config.bind_addr, self.config.port, e
            );
            Box::new(e) as Box<dyn std::error::Error + Send + Sync>
        })?;
        let router = build_router(self.cache.clone());

        info!("Starting dashboard server on http://{}", addr);

        let listener = TcpListener::bind(addr).await?;
        axum::serve(listener, router).await?;

        Ok(())
    }

    /// Start the dashboard server in a background task.
    ///
    /// Returns immediately after spawning the server task.
    pub fn start_background(self) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            if let Err(e) = self.start().await {
                error!("Dashboard server error: {}", e);
            }
        })
    }

    /// Get the dashboard URL.
    /// Returns None if the configured address is invalid.
    pub fn url(&self) -> Option<String> {
        self.config
            .socket_addr()
            .ok()
            .map(|addr| format!("http://{}", addr))
    }
}
