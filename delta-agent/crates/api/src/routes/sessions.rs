use axum::{extract::Path, routing::get, Json, Router};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct SessionResponse {
    session_id: String,
    status: String,
}

pub fn router() -> Router {
    Router::new().route("/sessions/{id}", get(get_session))
}

async fn get_session(Path(id): Path<String>) -> Json<SessionResponse> {
    Json(SessionResponse {
        session_id: id,
        status: "ok".to_owned(),
    })
}
