use axum::{
    middleware,
    routing::{get, post},
    Router,
};
use crate::{
    handlers::{artifacts, deltashots, metrics, sessions, workflows},
    middleware::require_api_key,
};

pub fn build_router() -> Router {
    let protected = Router::new()
        // Workspaces
        .route("/workspaces", post(sessions::create_workspace))
        // Sessions
        .route("/sessions", post(sessions::create_session))
        .route("/sessions/:id", get(sessions::get_session))
        .route("/sessions/:id/close", post(sessions::close_session))
        .route("/sessions/:id/agent", post(sessions::agent_call))
        // Artifacts
        .route("/artifacts", post(artifacts::create_artifact))
        .route("/artifacts/:id", get(artifacts::get_artifact))
        .route("/artifacts/:id/versions", get(artifacts::list_versions))
        .route("/artifacts/:id/versions", post(artifacts::new_artifact_version))
        .route("/sessions/:id/artifacts", get(artifacts::list_session_artifacts))
        // Workflows
        .route("/workflows", post(workflows::start_workflow))
        .route("/workflows/:id", get(workflows::get_workflow))
        .route("/workflow-definitions", get(workflows::list_definitions))
        // DeltaShots / Replay
        .route("/sessions/:id/deltashots/count", get(deltashots::get_deltashots_count))
        .route("/sessions/:id/replay", post(deltashots::replay))
        .route("/sessions/:id/rollback-points", get(deltashots::rollback_points))
        .route("/sessions/:id/fork", post(deltashots::fork_session))
        // Metrics
        .route("/metrics", get(metrics::get_metrics))
        .layer(middleware::from_fn(require_api_key));

    Router::new()
        .route("/health", get(health))
        .nest("/api/v1", protected)
        .fallback(handler_404)
}

async fn handler_404() -> axum::response::Response {
    use axum::response::IntoResponse;
    (
        axum::http::StatusCode::NOT_FOUND,
        axum::Json(serde_json::json!({"ok": false, "error": "not found"})),
    )
        .into_response()
}

async fn health() -> axum::Json<serde_json::Value> {
    let redis_ok = infra::redis_client::ping().await;
    axum::Json(serde_json::json!({
        "status": if redis_ok { "ok" } else { "degraded" },
        "redis": redis_ok,
        "version": env!("CARGO_PKG_VERSION"),
    }))
}
