use std::fs;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

#[derive(Debug, Clone, Deserialize)]
struct RootConfig {
    server: ServerConfig,
}

pub fn load(path: impl AsRef<Path>) -> anyhow::Result<ServerConfig> {
    let path_ref = path.as_ref();
    let contents = fs::read_to_string(path_ref)
        .with_context(|| format!("failed reading config {}", path_ref.display()))?;
    let parsed: RootConfig = toml::from_str(&contents)
        .with_context(|| format!("failed parsing config {}", path_ref.display()))?;
    Ok(parsed.server)
}
