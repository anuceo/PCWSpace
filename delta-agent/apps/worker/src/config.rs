use std::fs;
use std::path::Path;

use anyhow::Context;
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct WorkerConfig {
    pub max_concurrency: usize,
    pub poll_interval_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
struct RootConfig {
    worker: WorkerConfig,
}

pub fn load(path: impl AsRef<Path>) -> anyhow::Result<WorkerConfig> {
    let path_ref = path.as_ref();
    let raw = fs::read_to_string(path_ref)
        .with_context(|| format!("failed to read config file {}", path_ref.display()))?;
    let parsed: RootConfig = toml::from_str(&raw)
        .with_context(|| format!("failed to parse TOML config {}", path_ref.display()))?;
    Ok(parsed.worker)
}
