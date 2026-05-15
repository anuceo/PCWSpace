pub mod api_v1;

use axum::middleware;
use axum::Router;

use crate::middleware::auth::{authentication_middleware, AuthConfig};
use crate::pipeline::PipelineState;

pub fn router(state: PipelineState) -> Router {
    let auth_config = AuthConfig::from_env();
    let api_router = api_v1::router().route_layer(middleware::from_fn_with_state(
        auth_config,
        authentication_middleware,
    ));

    Router::new()
        .nest("/api/v1", api_router)
        .with_state(std::sync::Arc::new(state))
}
