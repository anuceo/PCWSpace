pub mod handlers;
pub mod middleware;
pub mod pipeline;
pub mod routes;

use axum::Router;
use pipeline::PipelineState;

pub fn router(state: PipelineState) -> Router {
    routes::router(state)
}

pub fn router_with_pipeline(state: PipelineState) -> Router {
    router(state)
}
