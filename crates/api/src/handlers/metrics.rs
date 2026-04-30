use axum::Json;
use serde_json::Value;
use infra::metrics;

pub async fn get_metrics() -> Json<Value> {
    let snapshot = metrics::global().snapshot();
    axum::Json(serde_json::json!({
        "ok": true,
        "data": snapshot,
    }))
}
