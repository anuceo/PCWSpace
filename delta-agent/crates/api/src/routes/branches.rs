use axum::{extract::Path, routing::get, Json, Router};
use serde::Serialize;

#[derive(Debug, Serialize)]
struct BranchResponse {
    branch_id: String,
    queue_depth: usize,
}

pub fn router() -> Router {
    Router::new().route("/branches/{id}", get(get_branch))
}

async fn get_branch(Path(id): Path<String>) -> Json<BranchResponse> {
    Json(BranchResponse {
        branch_id: id,
        queue_depth: 0,
    })
}
