use axum::{extract::Path, Json};
use infra::{metrics, redis_client::get_multiplexed_connection};
use workflows::{definitions::get_definition, engine::WorkflowEngine, worker::enqueue};
use serde::Deserialize;
use serde_json::json;
use super::{ok, err, map_err, ApiResult};
use axum::http::StatusCode;
use std::collections::HashMap;

#[derive(Deserialize)]
pub struct StartWorkflowBody {
    pub session_id: String,
    pub definition_name: String,
    pub context: Option<HashMap<String, serde_json::Value>>,
}

pub async fn start_workflow(Json(body): Json<StartWorkflowBody>) -> ApiResult {
    let def = match get_definition(&body.definition_name) {
        Some(d) => d,
        None => return err(StatusCode::NOT_FOUND, &format!("Workflow '{}' not found", body.definition_name)),
    };

    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let ctx = body.context.unwrap_or_default();
    let state = WorkflowEngine::start(&def, &body.session_id, ctx, &mut conn)
        .await
        .map_err(map_err)?;

    // Enqueue first step for async processing
    enqueue(&state.workflow_id, &mut conn).await.map_err(map_err)?;
    metrics::global().increment(metrics::names::WORKFLOWS_STARTED);

    ok(state)
}

pub async fn get_workflow(Path(workflow_id): Path<String>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let state = WorkflowEngine::get_state(&workflow_id, &mut conn)
        .await
        .map_err(map_err)?;
    ok(state)
}

pub async fn list_definitions() -> ApiResult {
    ok(json!([
        {"name": "client_outreach", "description": "Research, draft, and refine outreach emails"},
        {"name": "content_creation", "description": "Research, outline, write, and edit content"}
    ]))
}
