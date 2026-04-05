mod bootstrap;

#[tokio::main]
async fn main() {
    bootstrap::run().await;
}
