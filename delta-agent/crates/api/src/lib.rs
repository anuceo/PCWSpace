pub mod handlers;
pub mod middleware;
pub mod routes;

use axum::Router;

pub fn router() -> Router {
    routes::router()
}
