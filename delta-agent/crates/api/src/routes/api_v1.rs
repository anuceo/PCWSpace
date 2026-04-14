use std::sync::Arc;
use std::time::Instant;

use axum::{
    extract::{Path, Query, State},
    http::HeaderMap,
    http::StatusCode,
    response::{
        sse::{Event, KeepAlive, Sse},
        IntoResponse,
    },
    routing::{get, post},
    Json, Router,
};
use futures_util::stream;
use serde::Deserialize;

use crate::pipeline::{
    ApiErrorBody, ApiErrorEnvelope, ApiResponseMeta, ArtifactVersionView, ArtifactWriteRequest,
    BranchAuditResponse, BranchCreateRequest, BranchListResponse, BranchMergeRequest,
    BranchMergeResponse, BranchSwitchRequest, BranchSwitchResponse, CreateSessionRequest,
    CreateWorkspaceRequest, DeltashotView, ExecutionTrace, ForceAgentRequest, MessageRecord,
    PipelineError, PipelineState, RollbackRequest, SendMessageRequestV1, SessionView,
    StreamMessageRequestV1, WorkflowStartRequest, WorkflowStepRequest,
};

#[derive(Debug, Deserialize)]
struct WorkspaceSessionQuery {
    limit: Option<usize>,
    cursor: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct SessionMessagesQuery {
    limit: Option<usize>,
    cursor: Option<String>,
}

pub fn router() -> Router<Arc<PipelineState>> {
    Router::new()
        .route("/workspaces", post(create_workspace))
        .route("/workspaces/{workspaceId}", get(get_workspace))
        .route(
            "/workspaces/{workspaceId}/sessions",
            get(list_workspace_sessions),
        )
        .route("/sessions", post(create_session))
        .route("/sessions/{sessionId}/messages", post(send_message))
        .route("/sessions/{sessionId}/state", get(get_session_state))
        .route("/sessions/{sessionId}/messages", get(get_session_messages))
        .route("/sessions/{sessionId}/deltashots", get(list_deltashots))
        .route("/sessions/{sessionId}/rollback", post(rollback_session))
        .route("/deltashots/{deltashotId}", get(get_deltashot))
        .route("/artifacts", post(write_artifact))
        .route("/artifacts/{artifactId}", get(get_artifact))
        .route(
            "/artifacts/{artifactId}/versions",
            get(get_artifact_versions),
        )
        .route(
            "/artifacts/{artifactId}/versions/{version}",
            get(get_artifact_version),
        )
        .route("/workflows/start", post(start_workflow))
        .route("/workflows/execute-next", post(execute_next_workflow_job))
        .route("/workflows/{workflowId}/state", get(get_workflow_state))
        .route("/workflows/{workflowId}/step", post(advance_workflow_step))
        .route("/sessions/{sessionId}/agent", post(force_agent_selection))
        .route("/sessions/{sessionId}/agents/logs", get(get_agent_logs))
        .route("/sessions/{sessionId}/trace", get(get_trace))
        .route("/debug/branches/{branchId}/audit", post(run_branch_audit))
        .route("/sessions/{sessionId}/branch", post(create_branch))
        .route("/sessions/{sessionId}/branches", get(list_branches))
        .route("/sessions/{sessionId}/branch/switch", post(switch_branch))
        .route("/sessions/{sessionId}/branch/merge", post(merge_branch))
        .route(
            "/sessions/{sessionId}/messages/stream",
            post(stream_message),
        )
        .route("/health", get(health))
}

async fn create_workspace(
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<CreateWorkspaceRequest>,
) -> impl IntoResponse {
    with_meta(async move { state.create_workspace(request).await }).await
}

async fn get_workspace(
    Path(workspace_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        state.get_workspace(&workspace_id).await.ok_or_else(|| {
            PipelineError::NotFound(format!("workspace '{}' not found", workspace_id))
        })
    })
    .await
}

async fn list_workspace_sessions(
    Path(workspace_id): Path<String>,
    Query(query): Query<WorkspaceSessionQuery>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(20);
    let cursor = query.cursor;
    with_meta(async move {
        let result = state
            .list_workspace_sessions(&workspace_id, limit, cursor)
            .await;
        Ok::<Vec<SessionView>, PipelineError>(result)
    })
    .await
}

async fn create_session(
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<CreateSessionRequest>,
) -> impl IntoResponse {
    with_meta(async move { state.create_session(request).await }).await
}

async fn send_message(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<SendMessageRequestV1>,
) -> impl IntoResponse {
    with_meta(async move { state.handle_message_v1(&session_id, request).await }).await
}

async fn get_session_state(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        state
            .get_session_state(&session_id)
            .await
            .ok_or_else(|| PipelineError::NotFound(format!("session '{}' not found", session_id)))
    })
    .await
}

async fn get_session_messages(
    Path(session_id): Path<String>,
    Query(query): Query<SessionMessagesQuery>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    let limit = query.limit.unwrap_or(50);
    let cursor = query.cursor;
    with_meta(async move {
        let data = state.get_session_messages(&session_id, limit, cursor).await;
        Ok::<Vec<MessageRecord>, PipelineError>(data)
    })
    .await
}

async fn list_deltashots(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        let data = state.list_deltashots(&session_id).await;
        Ok::<Vec<DeltashotView>, PipelineError>(data)
    })
    .await
}

async fn get_deltashot(
    Path(deltashot_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        state.get_deltashot(&deltashot_id).await.ok_or_else(|| {
            PipelineError::NotFound(format!("deltashot '{}' not found", deltashot_id))
        })
    })
    .await
}

async fn rollback_session(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<RollbackRequest>,
) -> impl IntoResponse {
    with_meta(async move { state.rollback_session(&session_id, request).await }).await
}

async fn write_artifact(
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<ArtifactWriteRequest>,
) -> impl IntoResponse {
    with_meta(async move { state.create_or_update_artifact_api(request).await }).await
}

async fn get_artifact(
    Path(artifact_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        state
            .get_artifact(&artifact_id)
            .await
            .ok_or_else(|| PipelineError::NotFound(format!("artifact '{}' not found", artifact_id)))
    })
    .await
}

async fn get_artifact_versions(
    Path(artifact_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        let data = state.get_artifact_versions(&artifact_id).await;
        Ok::<Vec<ArtifactVersionView>, PipelineError>(data)
    })
    .await
}

async fn get_artifact_version(
    Path((artifact_id, version)): Path<(String, u64)>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        state
            .get_artifact_version(&artifact_id, version)
            .await
            .ok_or_else(|| {
                PipelineError::NotFound(format!(
                    "artifact '{}' version '{}' not found",
                    artifact_id, version
                ))
            })
    })
    .await
}

async fn start_workflow(
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<WorkflowStartRequest>,
) -> impl IntoResponse {
    with_meta(async move { state.start_workflow(request).await }).await
}

async fn execute_next_workflow_job(State(state): State<Arc<PipelineState>>) -> impl IntoResponse {
    with_meta(async move {
        state.execute_next_workflow_job().await?.ok_or_else(|| {
            PipelineError::NotFound("no workflow job available for execution".to_owned())
        })
    })
    .await
}

async fn get_workflow_state(
    Path(workflow_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        state
            .get_workflow_state(&workflow_id)
            .await
            .ok_or_else(|| PipelineError::NotFound(format!("workflow '{}' not found", workflow_id)))
    })
    .await
}

async fn advance_workflow_step(
    Path(workflow_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<WorkflowStepRequest>,
) -> impl IntoResponse {
    with_meta(async move { state.advance_workflow_step(&workflow_id, request).await }).await
}

async fn force_agent_selection(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<ForceAgentRequest>,
) -> impl IntoResponse {
    with_meta(async move { state.force_agent_selection(&session_id, request).await }).await
}

async fn get_agent_logs(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        let data = state.get_agent_logs(&session_id).await;
        Ok::<_, PipelineError>(data)
    })
    .await
}

async fn get_trace(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        let trace = state.get_trace(&session_id).await;
        Ok::<ExecutionTrace, PipelineError>(trace)
    })
    .await
}

async fn run_branch_audit(
    Path(branch_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        let report = state.debug_audit_branch(&branch_id).await?;
        Ok::<BranchAuditResponse, PipelineError>(report)
    })
    .await
}

async fn health(State(state): State<Arc<PipelineState>>) -> impl IntoResponse {
    with_meta(async move { Ok::<_, PipelineError>(state.health()) }).await
}

async fn create_branch(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<BranchCreateRequest>,
) -> impl IntoResponse {
    with_meta(async move { state.create_branch(&session_id, request).await }).await
}

async fn list_branches(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
) -> impl IntoResponse {
    with_meta(async move {
        let branches = state.list_branches(&session_id).await?;
        Ok::<BranchListResponse, PipelineError>(branches)
    })
    .await
}

async fn switch_branch(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<BranchSwitchRequest>,
) -> impl IntoResponse {
    with_meta(async move {
        let switched = state.switch_branch(&session_id, request).await?;
        Ok::<BranchSwitchResponse, PipelineError>(switched)
    })
    .await
}

async fn merge_branch(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
    Json(request): Json<BranchMergeRequest>,
) -> impl IntoResponse {
    with_meta(async move {
        let merged = state.merge_branch(&session_id, request).await?;
        Ok::<BranchMergeResponse, PipelineError>(merged)
    })
    .await
}

async fn stream_message(
    Path(session_id): Path<String>,
    State(state): State<Arc<PipelineState>>,
    headers: HeaderMap,
    Json(request): Json<StreamMessageRequestV1>,
) -> impl IntoResponse {
    let idempotency_key = headers
        .get("Idempotency-Key")
        .and_then(|value| value.to_str().ok())
        .map(ToOwned::to_owned);
    let events = state
        .stream_message_v1(&session_id, request, idempotency_key)
        .await;

    let sse_stream = stream::iter(events.into_iter().map(|envelope| {
        let payload = serde_json::to_string(&envelope.data).unwrap_or_else(|_| {
            "{\"code\":\"SERIALIZATION_ERROR\",\"retryable\":false}".to_owned()
        });
        Ok::<Event, std::convert::Infallible>(Event::default().event(envelope.event).data(payload))
    }));

    Sse::new(sse_stream)
        .keep_alive(KeepAlive::new().interval(std::time::Duration::from_secs(15)))
        .into_response()
}

async fn with_meta<T, F>(fut: F) -> impl IntoResponse
where
    T: serde::Serialize,
    F: std::future::Future<Output = Result<T, PipelineError>>,
{
    let started = Instant::now();
    let request_id = format!("req_{}", uuid::Uuid::new_v4().simple());
    let now = now_ms();
    match fut.await {
        Ok(data) => {
            let payload = serde_json::to_value(data).unwrap_or_else(|_| serde_json::json!({}));
            let meta = serde_json::to_value(ApiResponseMeta {
                request_id,
                timestamp: now,
                latency_ms: started.elapsed().as_millis() as u64,
            })
            .unwrap_or_else(|_| serde_json::json!({}));

            let response = match payload {
                serde_json::Value::Object(mut object) => {
                    object.insert("meta".to_owned(), meta);
                    serde_json::Value::Object(object)
                }
                other => serde_json::json!({
                    "value": other,
                    "meta": meta,
                }),
            };

            (StatusCode::OK, Json(response)).into_response()
        }
        Err(error) => {
            let status = match error {
                PipelineError::InvalidInput(_) => StatusCode::BAD_REQUEST,
                PipelineError::NotFound(_) => StatusCode::NOT_FOUND,
                PipelineError::LockUnavailable => StatusCode::CONFLICT,
                PipelineError::Serialization(_) | PipelineError::Internal(_) => {
                    StatusCode::INTERNAL_SERVER_ERROR
                }
            };
            let envelope = ApiErrorEnvelope {
                error: ApiErrorBody {
                    code: error.code().to_owned(),
                    message: error.to_string(),
                    retryable: error.retryable(),
                },
                meta: ApiResponseMeta {
                    request_id,
                    timestamp: now,
                    latency_ms: started.elapsed().as_millis() as u64,
                },
            };
            (status, Json(envelope)).into_response()
        }
    }
}

fn now_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_millis() as u64
}
