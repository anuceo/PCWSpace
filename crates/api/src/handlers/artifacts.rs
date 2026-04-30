use axum::{extract::Path, Json};
use pcw_core::models::ArtifactType;
use infra::redis_client::get_multiplexed_connection;
use artifacts::{service::ArtifactService, versions};
use serde::Deserialize;
use tracing::info;
use super::{ok, map_err, ApiResult};

#[derive(Deserialize)]
pub struct CreateArtifactBody {
    pub name: String,
    pub artifact_type: String,
    pub content: String,
    pub session_id: Option<String>,
}

#[derive(Deserialize)]
pub struct NewVersionBody {
    pub content: String,
    pub session_id: Option<String>,
}

fn parse_type(t: &str) -> ArtifactType {
    match t {
        "code" => ArtifactType::Code,
        "doc"  => ArtifactType::Doc,
        "design" => ArtifactType::Design,
        "dataset" => ArtifactType::Dataset,
        _ => ArtifactType::Doc,
    }
}

pub async fn create_artifact(Json(body): Json<CreateArtifactBody>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let artifact = ArtifactService::create(
        &body.name,
        parse_type(&body.artifact_type),
        &body.content,
        body.session_id.as_deref(),
        None,
        None,
        &mut conn,
    )
    .await
    .map_err(map_err)?;

    // Fire-and-forget Notion sync — does not block the response
    let snapshot = artifact.clone();
    tokio::spawn(async move {
        if let Some(page_id) = infra::notion::sync_artifact(&snapshot).await {
            info!(artifact_id = %snapshot.artifact_id, notion_page = %page_id, "Artifact synced to Notion");
        }
    });

    ok(artifact)
}

pub async fn get_artifact(Path(artifact_id): Path<String>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let artifact = ArtifactService::get(&artifact_id, &mut conn)
        .await
        .map_err(map_err)?;
    ok(artifact)
}

pub async fn list_session_artifacts(Path(session_id): Path<String>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let artifacts = ArtifactService::list_for_session(&session_id, &mut conn)
        .await
        .map_err(map_err)?;
    ok(artifacts)
}

pub async fn new_artifact_version(
    Path(artifact_id): Path<String>,
    Json(body): Json<NewVersionBody>,
) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let artifact = versions::new_version(&artifact_id, &body.content, None, None, body.session_id.as_deref(), &mut conn)
        .await
        .map_err(map_err)?;
    ok(artifact)
}

pub async fn list_versions(Path(artifact_id): Path<String>) -> ApiResult {
    let mut conn = get_multiplexed_connection().await.map_err(map_err)?;
    let ver_ids = versions::list_versions(&artifact_id, &mut conn)
        .await
        .map_err(map_err)?;
    ok(ver_ids)
}
