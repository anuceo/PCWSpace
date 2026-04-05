use axum::{response::IntoResponse, routing::get, Router};

pub fn router() -> Router {
    Router::new().route("/streaming/ping", get(ping))
}

async fn ping() -> impl IntoResponse {
    "streaming-ok"
}
