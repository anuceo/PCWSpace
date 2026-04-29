use std::collections::HashMap;
use std::io::{self, Write};
use std::time::Duration;

// ── ANSI colours ──────────────────────────────────────────────────────────────

const GREEN:  &str = "\x1b[32m";
const RED:    &str = "\x1b[31m";
const YELLOW: &str = "\x1b[33m";
const CYAN:   &str = "\x1b[36m";
const BOLD:   &str = "\x1b[1m";
const DIM:    &str = "\x1b[2m";
const RESET:  &str = "\x1b[0m";

fn ok(msg: &str)   { println!("  {GREEN}✓{RESET} {msg}"); }
fn fail(msg: &str) { println!("  {RED}✗{RESET} {msg}"); }
fn warn(msg: &str) { println!("  {YELLOW}⚠{RESET} {msg}"); }
fn info(msg: &str) { println!("  {CYAN}→{RESET} {msg}"); }

// ── Banner ────────────────────────────────────────────────────────────────────

pub fn print_banner() {
    println!();
    println!("{BOLD}{CYAN}╔═══════════════════════════════════════════╗{RESET}");
    println!("{BOLD}{CYAN}║   PCW  —  Persistent Cognitive Workspace  ║{RESET}");
    println!("{BOLD}{CYAN}╚═══════════════════════════════════════════╝{RESET}");
    println!("{DIM}  One-man business OS · event sourcing · multi-agent{RESET}");
    println!();
}

// ── Stdin helpers ─────────────────────────────────────────────────────────────

fn prompt(label: &str) -> String {
    print!("  {BOLD}{label}{RESET} ");
    io::stdout().flush().unwrap();
    let mut buf = String::new();
    io::stdin().read_line(&mut buf).unwrap();
    buf.trim().to_string()
}

fn prompt_secret(label: &str) -> String {
    // Use rpassword if available; fall back to plain stdin with a note
    print!("  {BOLD}{label}{RESET} ");
    io::stdout().flush().unwrap();
    match rpassword_read() {
        Some(s) => s,
        None => {
            let mut buf = String::new();
            io::stdin().read_line(&mut buf).unwrap();
            buf.trim().to_string()
        }
    }
}

fn rpassword_read() -> Option<String> {
    // Attempt to read without echo via termios (best-effort — no extra dep)
    use std::process::Command;
    let result = Command::new("sh")
        .arg("-c")
        .arg("stty -echo 2>/dev/null; IFS= read -r line; stty echo 2>/dev/null; echo \"$line\"")
        .stdin(std::process::Stdio::inherit())
        .output();
    match result {
        Ok(o) if o.status.success() => {
            let s = String::from_utf8_lossy(&o.stdout).trim().to_string();
            if !s.is_empty() { Some(s) } else { None }
        }
        _ => None,
    }
}

// ── .env persistence ──────────────────────────────────────────────────────────

fn load_env_map(path: &str) -> HashMap<String, String> {
    let mut map = HashMap::new();
    if let Ok(contents) = std::fs::read_to_string(path) {
        for line in contents.lines() {
            let line = line.trim();
            if line.starts_with('#') || line.is_empty() { continue; }
            if let Some((k, v)) = line.split_once('=') {
                map.insert(k.trim().to_string(), v.trim().to_string());
            }
        }
    }
    map
}

fn save_env_map(path: &str, map: &HashMap<String, String>) {
    // Preserve comments + order; just append/replace key=value lines
    let existing = std::fs::read_to_string(path).unwrap_or_default();
    let mut written_keys: std::collections::HashSet<String> = std::collections::HashSet::new();
    let mut output = String::new();

    for line in existing.lines() {
        let trimmed = line.trim();
        if !trimmed.starts_with('#') && !trimmed.is_empty() {
            if let Some((k, _)) = trimmed.split_once('=') {
                let k = k.trim().to_string();
                if let Some(v) = map.get(&k) {
                    output.push_str(&format!("{k}={v}\n"));
                    written_keys.insert(k);
                    continue;
                }
            }
        }
        output.push_str(line);
        output.push('\n');
    }

    // Append any new keys not in original file
    for (k, v) in map {
        if !written_keys.contains(k) {
            output.push_str(&format!("{k}={v}\n"));
        }
    }

    let _ = std::fs::write(path, output);
}

const SKIPPED: &str = "__SKIPPED__";

fn get_env(key: &str, map: &HashMap<String, String>) -> String {
    std::env::var(key)
        .ok()
        .filter(|v| !v.is_empty() && v != SKIPPED)
        .or_else(|| map.get(key).filter(|v| !v.is_empty() && v.as_str() != SKIPPED).cloned())
        .unwrap_or_default()
}

// ── Network check ─────────────────────────────────────────────────────────────

async fn check_internet() -> bool {
    // Try connecting to Cloudflare DNS and Google DNS
    for target in &["1.1.1.1:80", "8.8.8.8:53", "9.9.9.9:53"] {
        if let Ok(Ok(_)) = tokio::time::timeout(
            Duration::from_secs(3),
            tokio::net::TcpStream::connect(target),
        )
        .await
        {
            return true;
        }
    }
    false
}

fn list_wifi_networks() -> Vec<String> {
    let output = std::process::Command::new("nmcli")
        .args(["-t", "-f", "SSID,SIGNAL,SECURITY", "device", "wifi", "list"])
        .output();

    match output {
        Ok(o) if o.status.success() => {
            String::from_utf8_lossy(&o.stdout)
                .lines()
                .filter(|l| !l.trim().is_empty())
                .map(|l| l.to_string())
                .collect()
        }
        _ => vec![],
    }
}

fn connect_wifi(ssid: &str, password: &str) -> bool {
    let status = if password.is_empty() {
        std::process::Command::new("nmcli")
            .args(["device", "wifi", "connect", ssid])
            .status()
    } else {
        std::process::Command::new("nmcli")
            .args(["device", "wifi", "connect", ssid, "password", password])
            .status()
    };
    status.map(|s| s.success()).unwrap_or(false)
}

async fn network_setup_flow() {
    if check_internet().await {
        ok("Internet reachable");
        return;
    }

    fail("No internet connection detected");

    // Try nmcli for wifi
    let networks = list_wifi_networks();
    if networks.is_empty() {
        warn("nmcli not available or no wifi adapter found");
        println!();
        println!("  Please connect to the internet manually, then press Enter to retry.");
        let _ = prompt("Press Enter when connected:");
        if check_internet().await {
            ok("Internet reachable");
        } else {
            fail("Still no connection — continuing anyway (some features will be unavailable)");
        }
        return;
    }

    println!();
    println!("  {BOLD}Available Wi-Fi networks:{RESET}");
    println!("  {DIM}{:<4} {:<32} {:<8} {}{RESET}", "#", "SSID", "Signal", "Security");
    println!("  {DIM}{}{RESET}", "─".repeat(60));

    for (i, net) in networks.iter().enumerate() {
        let parts: Vec<&str> = net.splitn(3, ':').collect();
        let ssid = parts.first().copied().unwrap_or("(hidden)");
        let signal = parts.get(1).copied().unwrap_or("?");
        let security = parts.get(2).copied().unwrap_or("none");
        println!("  {CYAN}{:<4}{RESET} {:<32} {:<8} {}", i + 1, ssid, signal, security);
    }

    println!();
    let choice = prompt("Enter network number (or press Enter to skip):");

    if choice.is_empty() {
        warn("Skipping wifi setup — some features unavailable");
        return;
    }

    let idx: usize = match choice.parse::<usize>() {
        Ok(n) if n >= 1 && n <= networks.len() => n - 1,
        _ => {
            fail("Invalid selection");
            return;
        }
    };

    let ssid = networks[idx].splitn(2, ':').next().unwrap_or("").to_string();
    let password = prompt_secret(&format!("Password for '{ssid}' (Enter if open):"));

    info(&format!("Connecting to '{ssid}'..."));
    if connect_wifi(&ssid, &password) {
        tokio::time::sleep(Duration::from_secs(2)).await;
        if check_internet().await {
            ok(&format!("Connected to '{ssid}'"));
        } else {
            fail("Connected to wifi but internet still unreachable — check router/firewall");
        }
    } else {
        fail(&format!("Failed to connect to '{ssid}' — check password or try manually"));
    }
}

// ── API key verification ──────────────────────────────────────────────────────

async fn verify_redis(url: &str) -> Result<String, String> {
    let client = redis::Client::open(url).map_err(|e| e.to_string())?;
    let mut conn = client
        .get_multiplexed_async_connection()
        .await
        .map_err(|e| e.to_string())?;
    let pong: String = redis::cmd("PING")
        .query_async(&mut conn)
        .await
        .map_err(|e| e.to_string())?;
    if pong == "PONG" {
        Ok(format!("connected ({})", url.split('@').last().unwrap_or(url)))
    } else {
        Err(format!("unexpected response: {pong}"))
    }
}

async fn verify_anthropic(api_key: &str) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let resp = client
        .get("https://api.anthropic.com/v1/models")
        .header("x-api-key", api_key)
        .header("anthropic-version", "2023-06-01")
        .send()
        .await
        .map_err(|e| format!("network error: {e}"))?;

    match resp.status().as_u16() {
        200 => {
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            let count = body["data"].as_array().map(|a| a.len()).unwrap_or(0);
            Ok(format!("{count} models available"))
        }
        401 => Err("invalid API key (401 Unauthorized)".into()),
        403 => Err("forbidden — check your account permissions (403)".into()),
        code => Err(format!("HTTP {code}")),
    }
}

async fn verify_deepseek(api_key: &str, base_url: &str) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let url = format!("{base_url}/v1/models");
    let resp = client
        .get(&url)
        .header("Authorization", format!("Bearer {api_key}"))
        .send()
        .await
        .map_err(|e| format!("network error: {e}"))?;

    match resp.status().as_u16() {
        200 => Ok("key valid".into()),
        401 => Err("invalid API key (401)".into()),
        code => Err(format!("HTTP {code}")),
    }
}

async fn verify_notion(token: &str) -> Result<String, String> {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();

    let resp = client
        .get("https://api.notion.com/v1/users/me")
        .header("Authorization", format!("Bearer {token}"))
        .header("Notion-Version", "2022-06-28")
        .send()
        .await
        .map_err(|e| format!("network error: {e}"))?;

    match resp.status().as_u16() {
        200 => {
            let body: serde_json::Value = resp.json().await.unwrap_or_default();
            let name = body["name"].as_str().unwrap_or("unknown");
            Ok(format!("connected as '{name}'"))
        }
        401 => Err("invalid token (401)".into()),
        code => Err(format!("HTTP {code}")),
    }
}

// ── Required-key prompting ────────────────────────────────────────────────────

struct KeySpec {
    env_key:  &'static str,
    label:    &'static str,
    required: bool,
    secret:   bool,
    hint:     &'static str,
}

const KEY_SPECS: &[KeySpec] = &[
    KeySpec { env_key: "REDIS_URL",         label: "Redis URL",           required: true,  secret: false, hint: "redis://127.0.0.1:6379  or  rediss://:password@host:port" },
    KeySpec { env_key: "ANTHROPIC_API_KEY", label: "Anthropic API key",   required: true,  secret: true,  hint: "sk-ant-api03-..." },
    KeySpec { env_key: "PCW_API_KEY",       label: "PCW gateway API key", required: true,  secret: false, hint: "any string — used to authenticate requests to this server" },
    KeySpec { env_key: "DEEPSEEK_API_KEY",  label: "DeepSeek API key",    required: false, secret: true,  hint: "sk-...  (press Enter to skip)" },
    KeySpec { env_key: "NOTION_TOKEN",      label: "Notion integration token", required: false, secret: true, hint: "secret_...  (press Enter to skip)" },
];

fn collect_missing_keys(env_map: &mut HashMap<String, String>) -> bool {
    let mut any_prompted = false;

    println!("{BOLD}Configuration{RESET}");

    for spec in KEY_SPECS {
        let raw_stored = env_map.get(spec.env_key).map(|s| s.as_str()).unwrap_or("");
        if raw_stored == SKIPPED {
            println!("  {DIM}skipped           {:<28}{RESET}", spec.env_key);
            continue;
        }

        let current = get_env(spec.env_key, env_map);
        if !current.is_empty() {
            let display = if spec.secret {
                format!("{}...{}", &current[..4.min(current.len())], &current[current.len().saturating_sub(4)..])
            } else {
                current.clone()
            };
            ok(&format!("{:<28} {DIM}{display}{RESET}", spec.env_key));
            continue;
        }

        if spec.required {
            println!();
            println!("  {YELLOW}missing required:{RESET} {BOLD}{}{RESET}", spec.env_key);
            println!("  {DIM}hint: {}{RESET}", spec.hint);
        } else {
            println!();
            println!("  {DIM}optional:{RESET} {BOLD}{}{RESET}", spec.env_key);
            println!("  {DIM}hint: {}{RESET}", spec.hint);
        }

        let value = if spec.secret {
            prompt_secret(&format!("Enter {}: ", spec.label))
        } else {
            prompt(&format!("Enter {}: ", spec.label))
        };

        if !value.is_empty() {
            env_map.insert(spec.env_key.to_string(), value.clone());
            // Also set in current process env so get_settings() picks it up
            std::env::set_var(spec.env_key, &value);
            any_prompted = true;
        } else if spec.required {
            warn(&format!("{} is required — some features will fail", spec.env_key));
        } else {
            // Record that the user explicitly skipped this optional key so we
            // don't prompt again on the next run.
            env_map.insert(spec.env_key.to_string(), SKIPPED.to_string());
            any_prompted = true;
        }
    }

    any_prompted
}

// ── Live verification ─────────────────────────────────────────────────────────

async fn verify_all_keys(env_map: &HashMap<String, String>) -> bool {
    println!();
    println!("{BOLD}Verifying API keys{RESET}");

    let mut all_ok = true;

    // Redis
    let redis_url = get_env("REDIS_URL", env_map);
    if !redis_url.is_empty() {
        print!("  {DIM}Redis ......................{RESET} ");
        io::stdout().flush().unwrap();
        match verify_redis(&redis_url).await {
            Ok(msg) => println!("{GREEN}✓{RESET} {msg}"),
            Err(e)  => { println!("{RED}✗{RESET} {e}"); all_ok = false; }
        }
    } else {
        println!("  {DIM}Redis ......................{RESET} {YELLOW}⚠{RESET} no URL set");
        all_ok = false;
    }

    // Anthropic
    let anthropic_key = get_env("ANTHROPIC_API_KEY", env_map);
    if !anthropic_key.is_empty() {
        print!("  {DIM}Anthropic Claude ...........{RESET} ");
        io::stdout().flush().unwrap();
        match verify_anthropic(&anthropic_key).await {
            Ok(msg) => println!("{GREEN}✓{RESET} {msg}"),
            Err(e)  => { println!("{RED}✗{RESET} {e}"); all_ok = false; }
        }
    } else {
        println!("  {DIM}Anthropic Claude ...........{RESET} {YELLOW}⚠{RESET} key not set");
    }

    // DeepSeek (optional)
    let deepseek_key = get_env("DEEPSEEK_API_KEY", env_map);
    let deepseek_url = get_env("DEEPSEEK_BASE_URL", env_map);
    let deepseek_base = if deepseek_url.is_empty() { "https://api.deepseek.com".to_string() } else { deepseek_url };
    if !deepseek_key.is_empty() {
        print!("  {DIM}DeepSeek ...................{RESET} ");
        io::stdout().flush().unwrap();
        match verify_deepseek(&deepseek_key, &deepseek_base).await {
            Ok(msg) => println!("{GREEN}✓{RESET} {msg}"),
            Err(e)  => println!("{YELLOW}⚠{RESET} {e}"),
        }
    } else {
        println!("  {DIM}DeepSeek ...................{RESET} {DIM}skipped (no key){RESET}");
    }

    // Notion (optional)
    let notion_token = get_env("NOTION_TOKEN", env_map);
    if !notion_token.is_empty() {
        print!("  {DIM}Notion .....................{RESET} ");
        io::stdout().flush().unwrap();
        match verify_notion(&notion_token).await {
            Ok(msg) => println!("{GREEN}✓{RESET} {msg}"),
            Err(e)  => println!("{YELLOW}⚠{RESET} {e}"),
        }
    } else {
        println!("  {DIM}Notion .....................{RESET} {DIM}skipped (no token){RESET}");
    }

    all_ok
}

// ── Main entry point ──────────────────────────────────────────────────────────

/// Run all startup checks. Returns false only if a required service is
/// unreachable AND the user did not abort — caller can still choose to
/// proceed in degraded mode.
pub async fn run_checks(env_path: &str) -> bool {
    print_banner();

    // ── 1. Network ────────────────────────────────────────────────────────────
    println!("{BOLD}Network{RESET}");
    network_setup_flow().await;
    println!();

    // ── 2. Load .env ──────────────────────────────────────────────────────────
    let _ = dotenvy::from_path(env_path); // load into process env
    let mut env_map = load_env_map(env_path);

    // ── 3. Collect missing keys ───────────────────────────────────────────────
    let prompted = collect_missing_keys(&mut env_map);

    // ── 4. Save back any newly entered keys ──────────────────────────────────
    if prompted {
        save_env_map(env_path, &env_map);
        println!();
        ok(&format!("Keys saved to {env_path}"));
    }

    // ── 5. Live verification ──────────────────────────────────────────────────
    let all_ok = verify_all_keys(&env_map).await;

    println!();
    if all_ok {
        ok("All required services verified — starting PCW server");
    } else {
        warn("Some services are unavailable — starting in degraded mode");
    }
    println!();

    all_ok
}
