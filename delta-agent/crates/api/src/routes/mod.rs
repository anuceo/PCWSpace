pub mod api_v1;

use axum::Router;

use crate::pipeline::PipelineState;

pub fn router(state: PipelineState) -> Router {
    Router::new()
        .nest("/api/v1", api_v1::router())
        .with_state(std::sync::Arc::new(state))
}
