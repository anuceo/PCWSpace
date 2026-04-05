pub mod branches;
pub mod sessions;
pub mod streaming;

use axum::Router;

pub fn router() -> Router {
    Router::new()
        .merge(sessions::router())
        .merge(branches::router())
        .merge(streaming::router())
}
