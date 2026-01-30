//! Static file serving using rust-embed.

use axum::{
    body::Body,
    http::{header, Response, StatusCode},
    response::IntoResponse,
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "static/"]
pub struct StaticAssets;

/// Serve a static file from embedded assets.
pub async fn serve_static(path: &str) -> impl IntoResponse {
    let path = if path.is_empty() || path == "/" {
        "index.html"
    } else {
        path.trim_start_matches('/')
    };

    match StaticAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path)
                .first_or_octet_stream()
                .to_string();

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, mime)
                .header(header::CACHE_CONTROL, "no-cache")
                .body(Body::from(content.data.to_vec()))
                .expect("static response headers are valid")
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(Body::from("Not Found"))
            .expect("static response headers are valid"),
    }
}
