use axum::{extract::Path, Json};
use infra::{metrics, redis_client::get_multiplexed_connection};
use deltashots::{replay::{replay_session, list_rollback_points}, store::count_shots};
use serde::Deserialize;
use super::{ok, map_err, ApiResult};

#[derive(Deserialize)]
pub struct ReplayQuery {
    pub up_to_sequence: Option<u64>,
}

pub async fn get_deltashots_count(Path(session_id): Path<String>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let count = count_shots(&session_id, &mut conn)
        .await
        .map_err(map_err)?;
    ok(serde_json::json!({"session_id": session_id, "count": count}))
}

pub async fn replay(
    Path(session_id): Path<String>,
    Json(body): Json<ReplayQuery>,
) -> ApiResult {
    metrics::global().increment(metrics::names::REPLAY_CALLS);
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let state = replay_session(&session_id, body.up_to_sequence, &mut conn)
        .await
        .map_err(map_err)?;
    ok(state)
}

pub async fn rollback_points(Path(session_id): Path<String>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let points = list_rollback_points(&session_id, &mut conn)
        .await
        .map_err(map_err)?;
    ok(points)
}

pub async fn fork_session(
    Path(session_id): Path<String>,
    Json(body): Json<serde_json::Value>,
) -> ApiResult {
    let seq = body["fork_at_sequence"]
        .as_u64()
        .ok_or_else(|| (axum::http::StatusCode::BAD_REQUEST, axum::Json(serde_json::json!({"ok":false,"error":"fork_at_sequence required"}))))?;

    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let forked = timeline::branch::fork_session(&session_id, seq, &mut conn)
        .await
        .map_err(map_err)?;
    ok(forked)
}
