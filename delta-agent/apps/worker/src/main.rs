mod config;
mod scheduler_loop;

#[tokio::main]
async fn main() {
    if let Err(error) = scheduler_loop::run().await {
        eprintln!("worker failed: {error:?}");
        std::process::exit(1);
    }
}
