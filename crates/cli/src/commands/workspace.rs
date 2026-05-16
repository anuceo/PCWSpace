use crate::client::PcwClient;
use clap::Subcommand;
use colored::Colorize;
use serde_json::json;

#[derive(Subcommand)]
pub enum WorkspaceCmd {
    /// Create a new workspace
    Create {
        /// Workspace name
        name: String,
    },
}

pub async fn run(client: &PcwClient, cmd: WorkspaceCmd) -> Result<(), String> {
    match cmd {
        WorkspaceCmd::Create { name } => {
            let resp = client
                .post("/api/v1/workspaces", &json!({"name": name}))
                .await?;

            let data = &resp["data"];
            println!("{}", "Workspace created".green().bold());
            println!("  ID:      {}", data["workspace_id"].as_str().unwrap_or("?"));
            println!("  Name:    {}", data["name"].as_str().unwrap_or("?"));
            println!("  Created: {}", data["created_at"].as_str().unwrap_or("?"));
        }
    }
    Ok(())
}
