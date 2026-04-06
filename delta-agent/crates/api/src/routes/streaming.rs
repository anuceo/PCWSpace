use std::sync::Arc;

use axum::{response::IntoResponse, routing::get, Router};

use crate::pipeline::PipelineState;

pub fn router() -> Router<Arc<PipelineState>> {
    Router::new().route("/streaming/ping", get(ping))
}

async fn ping() -> impl IntoResponse {
    "streaming-ok"
}
