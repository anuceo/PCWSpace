use std::sync::Arc;

use axum::Router;

use crate::pipeline::PipelineState;

pub fn router() -> Router<Arc<PipelineState>> {
    Router::new()
}
