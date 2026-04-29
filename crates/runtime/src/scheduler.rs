use pcw_core::errors::PcwResult;
use tracing::{debug, info};

pub struct ScheduledTask {
    pub name: String,
    pub interval_secs: u64,
}

/// Run a background loop that processes workflow jobs at a fixed interval.
pub async fn run_workflow_worker_loop(
    _redis_url: &str,
    interval_secs: u64,
) -> PcwResult<()> {
    use infra::redis_client::get_multiplexed_connection;
    use workflows::worker::process_next;

    info!("Starting workflow worker loop (interval={}s)", interval_secs);

    loop {
        let mut conn = get_multiplexed_connection().await?;
        match process_next(&mut conn).await {
            Ok(true)  => debug!("Processed a workflow job"),
            Ok(false) => {}
            Err(e)    => tracing::warn!(error = %e, "Workflow worker error"),
        }

        if interval_secs > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        }
    }
}
