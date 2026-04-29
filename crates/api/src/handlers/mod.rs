pub mod artifacts;
pub mod deltashots;
pub mod sessions;
pub mod workflows;

use axum::{http::StatusCode, Json};
use serde_json::{json, Value};

pub type ApiResult = Result<Json<Value>, (StatusCode, Json<Value>)>;

pub fn ok(data: impl serde::Serialize) -> ApiResult {
    Ok(Json(json!({"ok": true, "data": data})))
}

pub fn err(status: StatusCode, msg: &str) -> ApiResult {
    Err((status, Json(json!({"ok": false, "error": msg}))))
}

pub fn map_err(e: pcw_core::errors::PcwError) -> (StatusCode, Json<Value>) {
    use pcw_core::errors::PcwError::*;
    let status = match &e {
        SessionNotFound(_) | ArtifactNotFound(_) | WorkflowNotFound(_)
        | WorkflowDefNotFound(_) | DeltaShotNotFound(_) | WorkspaceNotFound(_) => {
            StatusCode::NOT_FOUND
        }
        InvalidInput(_) | InvalidTransition { .. } => StatusCode::BAD_REQUEST,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    };
    (status, Json(json!({"ok": false, "error": e.to_string()})))
}
