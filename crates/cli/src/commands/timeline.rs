use crate::client::PcwClient;
use clap::Subcommand;
use colored::Colorize;
use serde_json::json;

#[derive(Subcommand)]
pub enum TimelineCmd {
    /// Get DeltaShot count for a session
    Count {
        /// Session ID
        session: String,
    },
    /// List rollback points
    Rollback {
        /// Session ID
        session: String,
    },
    /// Replay session state to a specific sequence
    Replay {
        /// Session ID
        #[arg(short, long)]
        session: String,

        /// Sequence number to replay to
        #[arg(short = 'n', long)]
        sequence: u64,
    },
    /// Fork a session at a specific point
    Fork {
        /// Source session ID
        #[arg(short, long)]
        session: String,

        /// Sequence number to fork at
        #[arg(short = 'n', long)]
        sequence: u64,
    },
}

pub async fn run(client: &PcwClient, cmd: TimelineCmd) -> Result<(), String> {
    match cmd {
        TimelineCmd::Count { session } => {
            let resp = client
                .get(&format!("/api/v1/sessions/{session}/deltashots/count"))
                .await?;
            let count = resp["data"]["count"].as_u64().unwrap_or(0);
            println!("{} {}", "DeltaShot count:".bold(), count);
        }
        TimelineCmd::Rollback { session } => {
            let resp = client
                .get(&format!("/api/v1/sessions/{session}/rollback-points"))
                .await?;
            let points = resp["data"].as_array();

            println!("{}", "Rollback Points".bold());
            match points {
                Some(pts) if !pts.is_empty() => {
                    for p in pts {
                        let seq = p["sequence"].as_u64().unwrap_or(0);
                        let action = p["action"].as_str().unwrap_or("?");
                        let ts = p["timestamp"].as_str().unwrap_or("?");
                        println!("  seq {}: {} ({})", seq.to_string().cyan(), action, ts.dimmed());
                    }
                }
                _ => println!("  {}", "No rollback points yet".dimmed()),
            }
        }
        TimelineCmd::Replay { session, sequence } => {
            let resp = client
                .post(
                    &format!("/api/v1/sessions/{session}/replay"),
                    &json!({"sequence": sequence}),
                )
                .await?;

            println!("{}", "State replayed".green().bold());
            println!("  Replayed to sequence: {}", sequence);
            if let Some(state) = resp["data"].as_object() {
                println!("  State keys: {:?}", state.keys().collect::<Vec<_>>());
            }
        }
        TimelineCmd::Fork { session, sequence } => {
            let resp = client
                .post(
                    &format!("/api/v1/sessions/{session}/fork"),
                    &json!({"fork_at_sequence": sequence}),
                )
                .await?;

            let data = &resp["data"];
            println!("{}", "Session forked".green().bold());
            println!("  New session:  {}", data["session_id"].as_str().unwrap_or("?"));
            println!("  Forked from:  {}", session);
            println!("  At sequence:  {}", sequence);
            println!("  Status:       {}", data["status"].as_str().unwrap_or("?"));
        }
    }
    Ok(())
}
