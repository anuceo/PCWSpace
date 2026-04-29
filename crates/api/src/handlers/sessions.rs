use axum::{extract::Path, Json};
use pcw_core::models::{AgentType, Workspace};
use infra::redis_client::{get_multiplexed_connection, key_workspace};
use redis::AsyncCommands;
use runtime::orchestrator::Orchestrator;
use serde::Deserialize;
use serde_json::json;
use super::{ok, map_err, ApiResult};

#[derive(Deserialize)]
pub struct CreateSessionBody {
    pub workspace_id: String,
}

#[derive(Deserialize)]
pub struct AgentCallBody {
    pub message: String,
    pub agent: Option<String>,
    pub system_prompt: Option<String>,
}

#[derive(Deserialize)]
pub struct CreateWorkspaceBody {
    pub name: String,
}

pub async fn create_workspace(Json(body): Json<CreateWorkspaceBody>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let ws = Workspace::new(body.name);
    let key = key_workspace(&ws.workspace_id);
    let json_str = serde_json::to_string(&ws)
        .map_err(|e| map_err(pcw_core::errors::PcwError::SerializationError(e.to_string())))?;
    let _: () = conn
        .set::<_, _, ()>(&key, json_str)
        .await
        .map_err(|e| map_err(pcw_core::errors::PcwError::RedisError(e.to_string())))?;
    ok(ws)
}

pub async fn create_session(Json(body): Json<CreateSessionBody>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let session = Orchestrator::create_session(&body.workspace_id, &mut conn)
        .await
        .map_err(map_err)?;
    ok(session)
}

pub async fn get_session(Path(session_id): Path<String>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let session = Orchestrator::load_session(&session_id, &mut conn)
        .await
        .map_err(map_err)?;
    ok(session)
}

pub async fn close_session(Path(session_id): Path<String>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    Orchestrator::close_session(&session_id, &mut conn)
        .await
        .map_err(map_err)?;
    ok(json!({"session_id": session_id, "status": "closed"}))
}

pub async fn agent_call(
    Path(session_id): Path<String>,
    Json(body): Json<AgentCallBody>,
) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;

    let agent_override = body.agent.as_deref().and_then(|a| match a {
        "claude" => Some(AgentType::Claude),
        "deepseek" => Some(AgentType::DeepSeek),
        _ => None,
    });

    let orch = Orchestrator::new();
    let result = orch
        .call_agent(
            &session_id,
            &body.message,
            agent_override,
            body.system_prompt.as_deref(),
            &mut conn,
        )
        .await
        .map_err(map_err)?;

    ok(result)
}
