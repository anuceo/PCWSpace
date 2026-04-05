mod scheduler_loop;

#[tokio::main]
async fn main() {
    scheduler_loop::run().await;
}
