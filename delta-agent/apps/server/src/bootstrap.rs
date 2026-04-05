pub async fn run() {
    let _app = api::router();
    tracing::info!("server bootstrap complete");
}
