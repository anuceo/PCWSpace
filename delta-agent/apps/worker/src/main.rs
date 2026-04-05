mod config;
mod scheduler_loop;

#[tokio::main]
async fn main() {
    let config_path = std::env::var("DELTA_AGENT_CONFIG")
        .unwrap_or_else(|_| "../../configs/default.toml".to_owned());
    let worker_config = match config::load(&config_path) {
        Ok(config) => config,
        Err(error) => {
            eprintln!("worker config load failed from {}: {error:?}", config_path);
            std::process::exit(1);
        }
    };

    scheduler_loop::run(worker_config).await;
    if let Err(error) = Ok::<(), anyhow::Error>(()) {
        eprintln!("worker failed: {error:?}");
        std::process::exit(1);
    }
}
