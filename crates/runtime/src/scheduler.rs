/// PCW scheduler — Storm topology lifecycle management.
///
/// Previously this ran a Redis XREADGROUP polling loop. Now it:
///   1. Submits the PCW topology to Storm Nimbus on startup.
///   2. Falls back to the in-process Redis poller if Storm is unavailable
///      (degraded mode), so the system still works without a Storm cluster.
use pcw_core::errors::PcwResult;
use storm::NimbusClient;
use tracing::{info, warn};

/// Attempt to submit the PCW topology to Storm Nimbus.
///
/// Returns `Ok(true)` if Storm accepted the topology, `Ok(false)` if Nimbus
/// was unreachable (caller should fall back to degraded mode).
pub async fn submit_storm_topology(nimbus_url: &str, worker_binary: &str) -> bool {
    let client   = NimbusClient::new(nimbus_url);
    let topology = crate::topology::pcw_topology(worker_binary, 2);

    match client.submit(&topology).await {
        Ok(()) => {
            info!(nimbus_url, "PCW topology submitted to Storm");
            true
        }
        Err(e) => {
            warn!(error = %e, "Storm Nimbus unreachable — falling back to in-process worker");
            false
        }
    }
}

/// Degraded-mode worker loop — used when Storm is not available.
///
/// Polls Redis streams directly (the original behaviour), so the API works
/// correctly in local development without a Storm cluster.
pub async fn run_workflow_worker_loop(
    _redis_url: &str,
    interval_secs: u64,
) -> PcwResult<()> {
    use infra::redis_client::get_multiplexed_connection;
    use workflows::worker::process_next;

    info!("Starting in-process workflow worker loop (interval={}s)", interval_secs);

    loop {
        let mut conn = get_multiplexed_connection().await?;
        match process_next(&mut conn).await {
            Ok(true) => {
                infra::metrics::global().increment(infra::metrics::names::WORKFLOWS_COMPLETED);
                tracing::debug!("Processed a workflow job");
            }
            Ok(false) => {}
            Err(e) => tracing::warn!(error = %e, "Workflow worker error"),
        }

        if interval_secs > 0 {
            tokio::time::sleep(std::time::Duration::from_secs(interval_secs)).await;
        }
    }
}
