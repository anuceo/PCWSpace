use crate::client::PcwClient;
use clap::Subcommand;
use colored::Colorize;
use serde_json::json;

#[derive(Subcommand)]
pub enum SessionCmd {
    /// Create a new session in a workspace
    Create {
        /// Workspace ID
        #[arg(short, long)]
        workspace: String,
    },
    /// Get session details
    Get {
        /// Session ID
        id: String,
    },
    /// Close a session
    Close {
        /// Session ID
        id: String,
    },
    /// List artifacts in a session
    Artifacts {
        /// Session ID
        id: String,
    },
}

pub async fn run(client: &PcwClient, cmd: SessionCmd) -> Result<(), String> {
    match cmd {
        SessionCmd::Create { workspace } => {
            let resp = client
                .post("/api/v1/sessions", &json!({"workspace_id": workspace}))
                .await?;

            let data = &resp["data"];
            println!("{}", "Session created".green().bold());
            println!("  ID:        {}", data["session_id"].as_str().unwrap_or("?"));
            println!("  Workspace: {}", data["workspace_id"].as_str().unwrap_or("?"));
            println!("  Status:    {}", data["status"].as_str().unwrap_or("?"));
        }
        SessionCmd::Get { id } => {
            let resp = client.get(&format!("/api/v1/sessions/{id}")).await?;
            let data = &resp["data"];

            println!("{}", "Session".bold());
            println!("  ID:        {}", data["session_id"].as_str().unwrap_or("?"));
            println!("  Workspace: {}", data["workspace_id"].as_str().unwrap_or("?"));
            println!("  Status:    {}", format_status(data["status"].as_str().unwrap_or("?")));
            println!("  Created:   {}", data["created_at"].as_str().unwrap_or("?"));
            if let Some(closed) = data["closed_at"].as_str() {
                println!("  Closed:    {}", closed);
            }
            if let Some(wf) = data["workflow_id"].as_str() {
                println!("  Workflow:  {}", wf);
            }
        }
        SessionCmd::Close { id } => {
            let resp = client
                .post(&format!("/api/v1/sessions/{id}/close"), &json!({}))
                .await?;

            let data = &resp["data"];
            println!("{}", "Session closed".green().bold());
            println!("  ID:     {}", data["session_id"].as_str().unwrap_or("?"));
            println!("  Status: {}", "closed".yellow());
        }
        SessionCmd::Artifacts { id } => {
            let resp = client.get(&format!("/api/v1/sessions/{id}/artifacts")).await?;
            let data = resp["data"].as_array();

            match data {
                Some(artifacts) if !artifacts.is_empty() => {
                    println!("{} ({} artifacts)", "Session Artifacts".bold(), artifacts.len());
                    println!();
                    for a in artifacts {
                        let atype = a["artifact_type"].as_str().unwrap_or("?");
                        let name = a["name"].as_str().unwrap_or("?");
                        let version = a["version"].as_u64().unwrap_or(0);
                        let id = a["artifact_id"].as_str().unwrap_or("?");
                        println!("  {} {} (v{})", format!("[{atype}]").dimmed(), name.bold(), version);
                        println!("        {}", id.dimmed());
                    }
                }
                _ => println!("{}", "No artifacts in this session".dimmed()),
            }
        }
    }
    Ok(())
}

fn format_status(status: &str) -> String {
    match status {
        "active" => status.green().to_string(),
        "closed" => status.yellow().to_string(),
        _ => status.to_string(),
    }
}
