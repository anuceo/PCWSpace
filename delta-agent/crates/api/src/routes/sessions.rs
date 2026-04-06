use std::sync::Arc;

use axum::{extract::Path, routing::get, Json, Router};

use crate::pipeline::{HandleMessageRequest, HandleMessageResponse, PipelineState, SessionView};

pub fn router() -> Router<Arc<PipelineState>> {
    Router::new()
        .route("/sessions/{id}", get(get_session))
        .route("/sessions/{id}/message", axum::routing::post(post_message))
}

async fn get_session(
    Path(id): Path<String>,
    axum::extract::State(state): axum::extract::State<Arc<PipelineState>>,
) -> Json<SessionView> {
    Json(state.get_or_create_session_view(&id).await)
}

async fn post_message(
    Path(id): Path<String>,
    axum::extract::State(state): axum::extract::State<Arc<PipelineState>>,
    Json(request): Json<HandleMessageRequest>,
) -> Json<HandleMessageResponse> {
    match state.handle_message(&id, request).await {
        Ok(response) => Json(response),
        Err(error) => Json(HandleMessageResponse {
            message: format!("pipeline_error: {error}"),
            artifacts: Vec::new(),
            deltashot_id: "none".to_owned(),
            state_version: 0,
        }),
    }
}
