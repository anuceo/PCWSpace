use crate::client::PcwClient;
use colored::Colorize;

pub async fn run(client: &PcwClient) -> Result<(), String> {
    let resp = client.health().await?;

    let status = resp["status"].as_str().unwrap_or("unknown");
    let redis = resp["redis"].as_bool().unwrap_or(false);
    let version = resp["version"].as_str().unwrap_or("?");

    println!("{}", "PCWSpace Health Check".bold());
    println!("  Status:  {}", if status == "ok" { status.green().to_string() } else { status.yellow().to_string() });
    println!("  Redis:   {}", if redis { "connected".green().to_string() } else { "disconnected".red().to_string() });
    println!("  Version: {}", version);

    Ok(())
}
