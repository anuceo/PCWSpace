use crate::client::PcwClient;
use clap::Subcommand;
use colored::Colorize;
use serde_json::json;

#[derive(Subcommand)]
pub enum AgentCmd {
    /// Send a message to the AI agent
    Ask {
        /// Session ID
        #[arg(short, long)]
        session: String,

        /// Message to send
        message: String,

        /// Force a specific agent (claude or deepseek)
        #[arg(short, long)]
        agent: Option<String>,

        /// Custom system prompt
        #[arg(long)]
        system_prompt: Option<String>,
    },
}

pub async fn run(client: &PcwClient, cmd: AgentCmd) -> Result<(), String> {
    match cmd {
        AgentCmd::Ask {
            session,
            message,
            agent,
            system_prompt,
        } => {
            let mut body = json!({ "message": message });
            if let Some(a) = &agent {
                body["agent"] = json!(a);
            }
            if let Some(sp) = &system_prompt {
                body["system_prompt"] = json!(sp);
            }

            println!("{}", "Calling agent...".dimmed());

            let resp = client
                .post(&format!("/api/v1/sessions/{session}/agent"), &body)
                .await?;

            let data = &resp["data"];
            let agent_type = data["agent_type"].as_str().unwrap_or("unknown");
            let response = data["response"].as_str().unwrap_or("");
            let input_tokens = data["input_tokens"].as_u64().unwrap_or(0);
            let output_tokens = data["output_tokens"].as_u64().unwrap_or(0);
            let shot_id = data["shot_id"].as_str().unwrap_or("none");

            println!();
            println!("{} ({})", "Agent Response".green().bold(), agent_type.cyan());
            println!("{}", "─".repeat(60).dimmed());
            println!("{response}");
            println!("{}", "─".repeat(60).dimmed());
            println!(
                "  {} in: {} | out: {} | shot: {}",
                "tokens".dimmed(),
                input_tokens,
                output_tokens,
                &shot_id[..8.min(shot_id.len())]
            );
        }
    }
    Ok(())
}
