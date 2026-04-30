/// Async wrappers around the `podman` CLI.
/// All functions return (success, human-readable output).
use tokio::process::Command;

async fn run(args: &[&str]) -> (bool, String) {
    match Command::new("podman").args(args).output().await {
        Ok(o) => {
            let out = format!("{}{}", String::from_utf8_lossy(&o.stdout), String::from_utf8_lossy(&o.stderr));
            (o.status.success(), out.trim().to_string())
        }
        Err(e) => (false, format!("exec error: {e}")),
    }
}

// ── Podman presence ───────────────────────────────────────────────────────────

/// Returns (installed, version_string)
pub async fn check_installed() -> (bool, String) {
    let (ok, out) = run(&["version", "--format", "{{.Client.Version}}"]).await;
    (ok, out)
}

/// Returns true if the daemon/rootless session is reachable.
pub async fn check_rootless() -> bool {
    let (ok, _) = run(&["info", "--format", "{{.Host.Security.Rootless}}"]).await;
    ok
}

// ── Network ───────────────────────────────────────────────────────────────────

#[derive(Debug, Default)]
pub struct NetworkInspect {
    pub exists:  bool,
    pub subnet:  String,
    pub gateway: String,
    pub driver:  String,
}

pub async fn inspect_network(name: &str) -> NetworkInspect {
    let (ok, out) = run(&[
        "network", "inspect", name,
        "--format", "{{.Driver}}|{{range .Subnets}}{{.Subnet}}|{{.Gateway}}{{end}}",
    ]).await;
    if !ok { return NetworkInspect::default(); }
    let parts: Vec<&str> = out.splitn(3, '|').collect();
    NetworkInspect {
        exists:  true,
        driver:  parts.first().copied().unwrap_or("bridge").to_string(),
        subnet:  parts.get(1).copied().unwrap_or("").to_string(),
        gateway: parts.get(2).copied().unwrap_or("").to_string(),
    }
}

pub async fn create_network(name: &str, subnet: &str, gateway: &str) -> (bool, String) {
    run(&[
        "network", "create",
        "--driver", "bridge",
        "--subnet", subnet,
        "--gateway", gateway,
        name,
    ]).await
}

// ── Image management ──────────────────────────────────────────────────────────

pub async fn image_exists(image: &str) -> bool {
    let (ok, _) = run(&["image", "exists", image]).await;
    ok
}

pub async fn pull_image(image: &str) -> (bool, String) {
    run(&["pull", image]).await
}

pub async fn image_version(image: &str) -> String {
    // Ask for the RepoTags label to get a usable tag/digest string
    let (ok, out) = run(&["image", "inspect", image,
        "--format", "{{index .RepoTags 0}}"]).await;
    if ok { out } else { String::new() }
}

// ── Container lifecycle ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum CtrStatus { NotFound, Running, Exited, Other(String) }

pub async fn container_status(name: &str) -> CtrStatus {
    let (ok, out) = run(&["inspect", name, "--format", "{{.State.Status}}"]).await;
    if !ok { return CtrStatus::NotFound; }
    match out.trim() {
        "running" => CtrStatus::Running,
        "exited"  => CtrStatus::Exited,
        other     => CtrStatus::Other(other.to_string()),
    }
}

pub async fn start_container(name: &str) -> (bool, String) {
    run(&["start", name]).await
}

pub async fn stop_container(name: &str) -> (bool, String) {
    run(&["stop", name]).await
}

/// Pull the latest image tag and recreate the container if running.
pub async fn update_image(image: &str, container: &str) -> (bool, String) {
    let (ok, out) = pull_image(image).await;
    if !ok { return (false, out); }
    // Stop + remove old container so next start picks the fresh image
    let _ = run(&["stop", container]).await;
    let _ = run(&["rm", container]).await;
    (true, format!("{out}\nContainer removed — restart with [Start] to apply update."))
}

// ── Service-specific helpers ──────────────────────────────────────────────────

pub const REDIS_IMAGE:  &str = "docker.io/redis:7-alpine";
pub const REDIS_CTR:    &str = "pcw-redis";
pub const ZK_IMAGE:     &str = "docker.io/zookeeper:3.9";
pub const ZK_CTR:       &str = "pcw-zookeeper";
pub const NIMBUS_IMAGE: &str = "docker.io/storm:2.6";
pub const NIMBUS_CTR:   &str = "pcw-storm-nimbus";
pub const SUPER_CTR:    &str = "pcw-storm-supervisor";

/// Start Redis with the provided config flags via a one-shot `podman run`.
/// If the container already exists it is started directly.
pub async fn start_redis(port: &str, max_mem: &str, persist_flags: &str, network: &str) -> (bool, String) {
    // Try simple start first
    let status = container_status(REDIS_CTR).await;
    if status == CtrStatus::Running {
        return (true, "Redis is already running.".into());
    }
    if status == CtrStatus::Exited {
        return start_container(REDIS_CTR).await;
    }
    // Run fresh
    run(&[
        "run", "--detach",
        "--name", REDIS_CTR,
        "--network", network,
        "--publish", &format!("{port}:{port}"),
        REDIS_IMAGE,
        "redis-server",
        "--maxmemory", max_mem,
        "--maxmemory-policy", "allkeys-lru",
        persist_flags,
    ]).await
}

/// Start all three Storm components in order: ZooKeeper → Nimbus → Supervisor.
pub async fn start_storm(nimbus_url: &str, network: &str) -> (bool, String) {
    // ZooKeeper
    if container_status(ZK_CTR).await != CtrStatus::Running {
        let (ok, out) = run(&[
            "run", "--detach", "--name", ZK_CTR,
            "--network", network, ZK_IMAGE,
        ]).await;
        if !ok { return (false, format!("ZooKeeper start failed:\n{out}")); }
    }

    tokio::time::sleep(std::time::Duration::from_secs(3)).await;

    // Nimbus
    if container_status(NIMBUS_CTR).await != CtrStatus::Running {
        let (ok, out) = run(&[
            "run", "--detach", "--name", NIMBUS_CTR,
            "--network", network,
            "--publish", "6627:6627", "--publish", "8080:8080",
            NIMBUS_IMAGE, "storm", "nimbus",
        ]).await;
        if !ok { return (false, format!("Nimbus start failed:\n{out}")); }
    }

    tokio::time::sleep(std::time::Duration::from_secs(2)).await;

    // Supervisor
    if container_status(SUPER_CTR).await != CtrStatus::Running {
        let (ok, out) = run(&[
            "run", "--detach", "--name", SUPER_CTR,
            "--network", network,
            NIMBUS_IMAGE, "storm", "supervisor",
        ]).await;
        if !ok { return (false, format!("Supervisor start failed:\n{out}")); }
    }

    let _ = nimbus_url;
    (true, "ZooKeeper + Nimbus + Supervisor started.".into())
}
