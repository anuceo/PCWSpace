use axum::{
    extract::Request,
    http::{HeaderMap, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
    Json,
};
use pcw_core::config::get_settings;
use serde_json::json;

pub async fn require_api_key(
    headers: HeaderMap,
    request: Request,
    next: Next,
) -> Response {
    let expected = &get_settings().pcw_api_key;

    let provided = headers
        .get("x-api-key")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    if provided != expected {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error": "Invalid or missing API key"})),
        )
            .into_response();
    }

    next.run(request).await
}
