use clap::{Parser, Subcommand};

mod client;
mod commands;

#[derive(Parser)]
#[command(name = "pcw")]
#[command(about = "PCWSpace CLI — Persistent Cognitive Workspace", long_about = None)]
#[command(version)]
struct Cli {
    /// PCW server URL (default: http://localhost:8000)
    #[arg(long, env = "PCW_URL", default_value = "http://localhost:8000")]
    url: String,

    /// API key for authentication
    #[arg(long, env = "PCW_API_KEY", default_value = "dev-insecure")]
    api_key: String,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Check server health
    Health,

    /// Manage workspaces
    #[command(subcommand)]
    Workspace(commands::workspace::WorkspaceCmd),

    /// Manage sessions
    #[command(subcommand)]
    Session(commands::session::SessionCmd),

    /// Call AI agents
    #[command(subcommand)]
    Agent(commands::agent::AgentCmd),

    /// Manage artifacts
    #[command(subcommand)]
    Artifact(commands::artifact::ArtifactCmd),

    /// Manage workflows
    #[command(subcommand)]
    Workflow(commands::workflow::WorkflowCmd),

    /// DeltaShot and timeline operations
    #[command(subcommand)]
    Timeline(commands::timeline::TimelineCmd),
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let client = client::PcwClient::new(&cli.url, &cli.api_key);

    let result = match cli.command {
        Commands::Health => commands::health::run(&client).await,
        Commands::Workspace(cmd) => commands::workspace::run(&client, cmd).await,
        Commands::Session(cmd) => commands::session::run(&client, cmd).await,
        Commands::Agent(cmd) => commands::agent::run(&client, cmd).await,
        Commands::Artifact(cmd) => commands::artifact::run(&client, cmd).await,
        Commands::Workflow(cmd) => commands::workflow::run(&client, cmd).await,
        Commands::Timeline(cmd) => commands::timeline::run(&client, cmd).await,
    };

    if let Err(e) = result {
        eprintln!("{} {}", colored::Colorize::red("error:"), e);
        std::process::exit(1);
    }
}
