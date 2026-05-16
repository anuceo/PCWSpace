use crate::client::PcwClient;
use clap::Subcommand;
use colored::Colorize;
use serde_json::json;

#[derive(Subcommand)]
pub enum WorkflowCmd {
    /// List available workflow definitions
    List,
    /// Start a workflow
    Start {
        /// Workflow definition name (client_outreach or content_creation)
        #[arg(short, long)]
        definition: String,

        /// Session ID
        #[arg(short, long)]
        session: String,

        /// Input as JSON string (e.g. '{"topic":"AI","audience":"devs"}')
        #[arg(short, long, default_value = "{}")]
        input: String,
    },
    /// Get workflow status
    Get {
        /// Workflow ID
        id: String,
    },
}

pub async fn run(client: &PcwClient, cmd: WorkflowCmd) -> Result<(), String> {
    match cmd {
        WorkflowCmd::List => {
            let resp = client.get("/api/v1/workflow-definitions").await?;
            let defs = resp["data"].as_array();

            println!("{}", "Workflow Definitions".bold());
            println!();
            if let Some(defs) = defs {
                for d in defs {
                    let name = d["name"].as_str().unwrap_or("?");
                    let desc = d["description"].as_str().unwrap_or("");
                    println!("  {} {}", name.cyan().bold(), desc.dimmed());
                }
            }
        }
        WorkflowCmd::Start {
            definition,
            session,
            input,
        } => {
            let input_val: serde_json::Value =
                serde_json::from_str(&input).map_err(|e| format!("Invalid JSON input: {e}"))?;

            let resp = client
                .post(
                    "/api/v1/workflows",
                    &json!({
                        "definition_name": definition,
                        "session_id": session,
                        "input": input_val,
                    }),
                )
                .await?;

            let data = &resp["data"];
            println!("{}", "Workflow started".green().bold());
            println!("  ID:           {}", data["workflow_id"].as_str().unwrap_or("?"));
            println!("  Definition:   {}", definition);
            println!("  Status:       {}", data["status"].as_str().unwrap_or("?"));
            println!("  Current step: {}", data["current_step"].as_str().unwrap_or("?"));
        }
        WorkflowCmd::Get { id } => {
            let resp = client.get(&format!("/api/v1/workflows/{id}")).await?;
            let data = &resp["data"];

            let status = data["status"].as_str().unwrap_or("?");
            let step = data["current_step"].as_str().unwrap_or("?");
            let step_status = data["step_status"].as_str().unwrap_or("?");
            let retries = data["retry_count"].as_u64().unwrap_or(0);

            println!("{}", "Workflow Status".bold());
            println!("  ID:           {}", data["workflow_id"].as_str().unwrap_or("?"));
            println!("  Status:       {}", format_wf_status(status));
            println!("  Current step: {}", step.cyan());
            println!("  Step status:  {}", step_status);
            println!("  Retries:      {}", retries);
            if let Some(err) = data["error"].as_str() {
                println!("  Error:        {}", err.red());
            }
            if let Some(completed) = data["completed_at"].as_str() {
                println!("  Completed:    {}", completed);
            }
        }
    }
    Ok(())
}

fn format_wf_status(s: &str) -> String {
    match s {
        "running" => s.green().to_string(),
        "completed" => s.cyan().to_string(),
        "failed" => s.red().to_string(),
        _ => s.to_string(),
    }
}
