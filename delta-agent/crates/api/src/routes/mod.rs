pub mod branches;
pub mod sessions;
pub mod streaming;

use axum::Router;

use crate::pipeline::PipelineState;

pub fn router(state: PipelineState) -> Router {
    Router::new()
        .merge(sessions::router())
        .merge(branches::router())
        .merge(streaming::router())
        .with_state(std::sync::Arc::new(state))
}
