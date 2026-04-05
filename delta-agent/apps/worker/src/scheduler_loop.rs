use scheduler::queue::ScheduledTask;
use scheduler::worker::SchedulerWorker;
use tokio::time::{sleep, Duration};
use tracing::{info, Level};

use crate::config::WorkerConfig;

pub async fn run(config: WorkerConfig) {
    init_tracing();
    let mut worker = SchedulerWorker::default();

    info!(
        max_concurrency = config.max_concurrency,
        poll_interval_ms = config.poll_interval_ms,
        "worker starting"
    );

    let poll_interval = Duration::from_millis(config.poll_interval_ms);

    loop {
        worker.enqueue(ScheduledTask {
            id: "task-1".to_owned(),
            branch_id: "main".to_owned(),
            priority_score: 100,
        });

        let _ = worker.run_once().await;
        info!("worker loop tick complete");
        sleep(poll_interval).await;
    }
}

fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_max_level(Level::INFO)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "worker=info".into()),
        )
        .try_init();
}
