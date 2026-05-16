use crate::client::PcwClient;
use clap::Subcommand;
use colored::Colorize;
use serde_json::json;

#[derive(Subcommand)]
pub enum ArtifactCmd {
    /// Create a new artifact
    Create {
        /// Session ID
        #[arg(short, long)]
        session: String,

        /// Artifact name
        #[arg(short, long)]
        name: String,

        /// Type: doc, code, design, dataset
        #[arg(short = 't', long, default_value = "doc")]
        artifact_type: String,

        /// Content (or use --file to read from file)
        #[arg(short, long)]
        content: Option<String>,

        /// Read content from file
        #[arg(short, long)]
        file: Option<String>,
    },
    /// Get an artifact
    Get {
        /// Artifact ID
        id: String,
    },
    /// Create a new version of an artifact
    Update {
        /// Artifact ID (root)
        id: String,

        /// New content (or use --file)
        #[arg(short, long)]
        content: Option<String>,

        /// Read content from file
        #[arg(short, long)]
        file: Option<String>,
    },
    /// List version history
    Versions {
        /// Artifact ID (root)
        id: String,
    },
}

pub async fn run(client: &PcwClient, cmd: ArtifactCmd) -> Result<(), String> {
    match cmd {
        ArtifactCmd::Create {
            session,
            name,
            artifact_type,
            content,
            file,
        } => {
            let content = resolve_content(content, file)?;
            let resp = client
                .post(
                    "/api/v1/artifacts",
                    &json!({
                        "session_id": session,
                        "name": name,
                        "artifact_type": artifact_type,
                        "content": content,
                    }),
                )
                .await?;

            let data = &resp["data"];
            println!("{}", "Artifact created".green().bold());
            println!("  ID:      {}", data["artifact_id"].as_str().unwrap_or("?"));
            println!("  Name:    {}", data["name"].as_str().unwrap_or("?"));
            println!("  Type:    {}", data["artifact_type"].as_str().unwrap_or("?"));
            println!("  Version: {}", data["version"].as_u64().unwrap_or(0));
            println!("  Hash:    {}", data["content_hash"].as_str().unwrap_or("?"));
        }
        ArtifactCmd::Get { id } => {
            let resp = client.get(&format!("/api/v1/artifacts/{id}")).await?;
            let data = &resp["data"];

            let name = data["name"].as_str().unwrap_or("?");
            let atype = data["artifact_type"].as_str().unwrap_or("?");
            let version = data["version"].as_u64().unwrap_or(0);
            let content = data["content"].as_str().unwrap_or("");
            let hash = data["content_hash"].as_str().unwrap_or("?");

            println!("{} {} (v{}) [{}]", "Artifact:".bold(), name, version, atype);
            println!("{} {}", "Hash:".dimmed(), hash);
            println!("{}", "─".repeat(60).dimmed());
            println!("{content}");
        }
        ArtifactCmd::Update { id, content, file } => {
            let content = resolve_content(content, file)?;
            let resp = client
                .post(
                    &format!("/api/v1/artifacts/{id}/versions"),
                    &json!({"content": content}),
                )
                .await?;

            let data = &resp["data"];
            println!("{}", "New version created".green().bold());
            println!("  ID:      {}", data["artifact_id"].as_str().unwrap_or("?"));
            println!("  Version: {}", data["version"].as_u64().unwrap_or(0));
            println!("  Hash:    {}", data["content_hash"].as_str().unwrap_or("?"));
        }
        ArtifactCmd::Versions { id } => {
            let resp = client.get(&format!("/api/v1/artifacts/{id}/versions")).await?;
            let versions = resp["data"].as_array();

            match versions {
                Some(v) => {
                    println!("{} ({} versions)", "Version History".bold(), v.len());
                    for (i, vid) in v.iter().enumerate() {
                        let marker = if i == v.len() - 1 { "→" } else { " " };
                        println!("  {} v{}: {}", marker, i + 1, vid.as_str().unwrap_or("?"));
                    }
                    println!();
                    println!("  {} = latest", "→".green());
                }
                None => println!("{}", "No versions found".dimmed()),
            }
        }
    }
    Ok(())
}

fn resolve_content(content: Option<String>, file: Option<String>) -> Result<String, String> {
    match (content, file) {
        (Some(c), _) => Ok(c),
        (None, Some(f)) => std::fs::read_to_string(&f).map_err(|e| format!("Cannot read file '{f}': {e}")),
        (None, None) => Err("Provide --content or --file".to_string()),
    }
}
