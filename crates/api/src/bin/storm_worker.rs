/// PCW Storm worker dispatcher.
///
/// Storm spawns this binary with argv[1] set to the component role.
/// A single binary serves all roles — Storm passes the role as the first arg.
///
/// Usage (Storm executes these via Multilang shebang):
///   pcw-storm-worker workflow-spout
///   pcw-storm-worker workflow-bolt
///   pcw-storm-worker deltashot-bolt
use std::process;
use tracing::error;

#[tokio::main]
async fn main() {
    // Initialise logging to stderr so Storm can capture it without confusing stdout
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "info".into())
        )
        .init();

    let role = std::env::args().nth(1).unwrap_or_default();

    match role.as_str() {
        "workflow-spout" => {
            use runtime::topology::WorkflowSpout;
            storm::run_spout(WorkflowSpout::new()).await;
        }
        "workflow-bolt" => {
            use runtime::topology::WorkflowBolt;
            storm::run_bolt(WorkflowBolt::new()).await;
        }
        "deltashot-bolt" => {
            use runtime::topology::DeltaShotBolt;
            storm::run_bolt(DeltaShotBolt::new()).await;
        }
        other => {
            error!("Unknown storm worker role: '{other}'");
            error!("Valid roles: workflow-spout, workflow-bolt, deltashot-bolt");
            process::exit(1);
        }
    }
}
