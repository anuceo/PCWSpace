mod config;
mod bootstrap;

#[tokio::main]
async fn main() {
    if let Err(error) = bootstrap::run().await {
        eprintln!("server failed: {error:?}");
        std::process::exit(1);
    }
}
