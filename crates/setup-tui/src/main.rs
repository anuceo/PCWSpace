mod app;
mod podman;
mod ui;

use std::io;
use std::time::Duration;

use crossterm::{
    event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode, KeyModifiers},
    execute,
    terminal::{disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen},
};
use ratatui::{backend::CrosstermBackend, Terminal};
use tokio::sync::mpsc;

use app::{
    App, AppEvent, ConfigFocus, ContainerStatus, NetworkInfo, Screen, ServiceState,
    ServiceTab, StepStatus, MAX_MEMORY_OPTIONS, PERSISTENCE_OPTIONS, REDIS_MODE_OPTIONS,
};
use podman::{
    CtrStatus, NIMBUS_IMAGE, REDIS_CTR, REDIS_IMAGE, NIMBUS_CTR, ZK_IMAGE, ZK_CTR,
};

const TICK_MS: u64 = 80;

// ── Terminal lifecycle ────────────────────────────────────────────────────────

fn setup_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    enable_raw_mode()?;
    let mut stdout = io::stdout();
    execute!(stdout, EnterAlternateScreen, EnableMouseCapture)?;
    let backend = CrosstermBackend::new(stdout);
    Terminal::new(backend)
}

fn restore_terminal(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    disable_raw_mode()?;
    execute!(terminal.backend_mut(), LeaveAlternateScreen, DisableMouseCapture)?;
    terminal.show_cursor()
}

// ── Entry point ───────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> io::Result<()> {
    let mut terminal = setup_terminal()?;
    let result = run(&mut terminal).await;
    restore_terminal(&mut terminal)?;
    result
}

// ── Main event loop ───────────────────────────────────────────────────────────

async fn run(terminal: &mut Terminal<CrosstermBackend<io::Stdout>>) -> io::Result<()> {
    let (tx, mut rx) = mpsc::channel::<AppEvent>(64);
    let mut app = App::new();

    // Kick off the loading sequence in a background task
    {
        let tx2 = tx.clone();
        tokio::spawn(loading_sequence(tx2));
    }

    // Tick sender
    {
        let tx2 = tx.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(TICK_MS)).await;
                if tx2.send(AppEvent::Tick).await.is_err() { break; }
            }
        });
    }

    loop {
        terminal.draw(|f| ui::draw(f, &app))?;

        // Non-blocking crossterm event check
        if event::poll(Duration::from_millis(0))? {
            if let Event::Key(key) = event::read()? {
                if let Some(ev) = handle_key(&mut app, key.code, key.modifiers, &tx) {
                    if matches!(ev, AppEvent::Quit) { break; }
                }
            }
        }

        // Drain async events
        while let Ok(ev) = rx.try_recv() {
            match ev {
                AppEvent::Tick => { app.tick = app.tick.wrapping_add(1); }
                AppEvent::Quit => break,

                AppEvent::StepUpdate { index, status } => {
                    if let Some(step) = app.steps.get_mut(index) {
                        step.status = status;
                    }
                    // When all steps are done, transition to NetworkPopup
                    if app.steps.iter().all(|s| {
                        matches!(s.status, StepStatus::Done | StepStatus::Failed(_))
                    }) {
                        app.loading_done = true;
                        if app.screen == Screen::Loading {
                            app.screen = Screen::NetworkPopup;
                        }
                    }
                }

                AppEvent::NetworkReady(net) => {
                    app.network = net;
                    if app.screen == Screen::Loading {
                        app.screen = Screen::NetworkPopup;
                    }
                }

                AppEvent::ServiceStatus { service, state } => {
                    match service {
                        "redis" => app.redis_state = state,
                        "storm" => app.storm_state = state,
                        _       => {}
                    }
                }

                AppEvent::CommandOutput(line) => {
                    app.push_log(line);
                }
            }
        }

        // Done screen → exit after a brief pause
        if app.screen == Screen::Done {
            app.write_env();
            terminal.draw(|f| ui::draw(f, &app))?;
            tokio::time::sleep(Duration::from_millis(1500)).await;
            break;
        }
    }

    Ok(())
}

// ── Keyboard handling ─────────────────────────────────────────────────────────

fn handle_key(app: &mut App, code: KeyCode, modifiers: KeyModifiers, tx: &mpsc::Sender<AppEvent>) -> Option<AppEvent> {
    // Global quit
    if code == KeyCode::Char('q') || (code == KeyCode::Char('c') && modifiers.contains(KeyModifiers::CONTROL)) {
        return Some(AppEvent::Quit);
    }

    match app.screen {
        Screen::Loading => None,

        Screen::NetworkPopup => {
            if code == KeyCode::Enter {
                app.screen = Screen::ServiceConfig;
            }
            None
        }

        Screen::ServiceConfig => {
            match code {
                KeyCode::Tab | KeyCode::Right => { app.next_tab(); None }
                KeyCode::BackTab | KeyCode::Left => { app.prev_tab(); None }

                KeyCode::Down => { app.focus_next(); None }
                KeyCode::Up   => { app.focus_prev(); None }

                // Cycle select fields with ← / → when focused on a Field(n)
                KeyCode::Char(' ') | KeyCode::Char(']') => {
                    cycle_field(app, 1);
                    None
                }
                KeyCode::Char('[') => {
                    cycle_field(app, -1);
                    None
                }

                KeyCode::Enter => {
                    if app.active_tab == ServiceTab::Finish {
                        app.screen = Screen::Done;
                        return None;
                    }
                    // Spawn the action as a background task so the TUI stays responsive
                    let tx2       = tx.clone();
                    let focus     = app.current_focus();
                    let svc       = app.active_tab.clone();
                    let redis_cfg = app.redis_cfg.clone();
                    let storm_cfg = app.storm_cfg.clone();
                    let net_name  = app.network.name.clone();
                    tokio::spawn(dispatch_action(focus, svc, redis_cfg, storm_cfg, net_name, tx2));
                    None
                }
                _ => None,
            }
        }

        Screen::Done => None,
    }
}

/// Cycle a select field (max_memory, persistence, mode) by `delta` (+1 / -1).
fn cycle_field(app: &mut App, delta: i32) {
    let focus = app.current_focus();
    match app.active_tab {
        ServiceTab::Redis => {
            match focus {
                ConfigFocus::Field(1) => {
                    let n = MAX_MEMORY_OPTIONS.len();
                    app.redis_cfg.max_memory = ((app.redis_cfg.max_memory as i32 + delta).rem_euclid(n as i32)) as usize;
                }
                ConfigFocus::Field(2) => {
                    let n = PERSISTENCE_OPTIONS.len();
                    app.redis_cfg.persistence = ((app.redis_cfg.persistence as i32 + delta).rem_euclid(n as i32)) as usize;
                }
                ConfigFocus::Field(3) => {
                    let n = REDIS_MODE_OPTIONS.len();
                    app.redis_cfg.mode = ((app.redis_cfg.mode as i32 + delta).rem_euclid(n as i32)) as usize;
                }
                _ => {}
            }
        }
        ServiceTab::Storm => {
            match focus {
                ConfigFocus::Field(1) => {
                    let cur: i32 = app.storm_cfg.worker_count.parse().unwrap_or(2);
                    let next = (cur + delta).max(1).min(16);
                    app.storm_cfg.worker_count = next.to_string();
                }
                ConfigFocus::Field(2) => {
                    let cur: i32 = app.storm_cfg.slot_count.parse().unwrap_or(4);
                    let next = (cur + delta).max(1).min(32);
                    app.storm_cfg.slot_count = next.to_string();
                }
                _ => {}
            }
        }
        _ => {}
    }
}

// ── Background: action dispatcher ─────────────────────────────────────────────

pub async fn dispatch_action(
    focus:     ConfigFocus,
    svc:       ServiceTab,
    redis_cfg: app::RedisConfig,
    storm_cfg: app::StormConfig,
    net_name:  String,
    tx:        mpsc::Sender<AppEvent>,
) {
    let log = |msg: String| {
        let tx = tx.clone();
        async move { let _ = tx.send(AppEvent::CommandOutput(msg)).await; }
    };

    match svc {
        ServiceTab::Redis => {
            match focus {
                ConfigFocus::ActionInstall => {
                    log(format!("> podman pull {REDIS_IMAGE}")).await;
                    let (ok, out) = podman::pull_image(REDIS_IMAGE).await;
                    for line in out.lines() { log(line.to_string()).await; }
                    let state = probe_redis().await;
                    let _ = tx.send(AppEvent::ServiceStatus { service: "redis", state }).await;
                    if !ok { log("✗ Install failed".into()).await; }
                }
                ConfigFocus::ActionStart => {
                    log("> Starting Redis…".into()).await;
                    let mem  = MAX_MEMORY_OPTIONS[redis_cfg.max_memory];
                    let pers = match PERSISTENCE_OPTIONS[redis_cfg.persistence] {
                        "AOF" => "--appendonly yes",
                        "RDB" => "--save 60 1",
                        _     => "--save ''",
                    };
                    let (ok, out) = podman::start_redis(&redis_cfg.port, mem, pers, &net_name).await;
                    for line in out.lines() { log(line.to_string()).await; }
                    let state = probe_redis().await;
                    let _ = tx.send(AppEvent::ServiceStatus { service: "redis", state }).await;
                    if !ok { log("✗ Start failed".into()).await; }
                }
                ConfigFocus::ActionStop => {
                    log("> Stopping Redis…".into()).await;
                    let (_, out) = podman::stop_container(REDIS_CTR).await;
                    for line in out.lines() { log(line.to_string()).await; }
                    let state = probe_redis().await;
                    let _ = tx.send(AppEvent::ServiceStatus { service: "redis", state }).await;
                }
                ConfigFocus::ActionUpdate => {
                    log(format!("> Updating {REDIS_IMAGE}…")).await;
                    let (_, out) = podman::update_image(REDIS_IMAGE, REDIS_CTR).await;
                    for line in out.lines() { log(line.to_string()).await; }
                    let state = probe_redis().await;
                    let _ = tx.send(AppEvent::ServiceStatus { service: "redis", state }).await;
                }
                _ => {}
            }
        }

        ServiceTab::Storm => {
            match focus {
                ConfigFocus::ActionInstall => {
                    for img in [ZK_IMAGE, NIMBUS_IMAGE] {
                        log(format!("> podman pull {img}")).await;
                        let (_, out) = podman::pull_image(img).await;
                        for line in out.lines() { log(line.to_string()).await; }
                    }
                    let state = probe_storm().await;
                    let _ = tx.send(AppEvent::ServiceStatus { service: "storm", state }).await;
                }
                ConfigFocus::ActionStart => {
                    log("> Starting ZooKeeper + Nimbus + Supervisor…".into()).await;
                    let (ok, out) = podman::start_storm(&storm_cfg.nimbus_url, &net_name).await;
                    for line in out.lines() { log(line.to_string()).await; }
                    let state = probe_storm().await;
                    let _ = tx.send(AppEvent::ServiceStatus { service: "storm", state }).await;
                    if !ok { log("✗ Storm start failed".into()).await; }
                }
                ConfigFocus::ActionStop => {
                    log("> Stopping Storm…".into()).await;
                    for ctr in [podman::SUPER_CTR, NIMBUS_CTR, ZK_CTR] {
                        let (_, out) = podman::stop_container(ctr).await;
                        for line in out.lines() { log(line.to_string()).await; }
                    }
                    let state = probe_storm().await;
                    let _ = tx.send(AppEvent::ServiceStatus { service: "storm", state }).await;
                }
                ConfigFocus::ActionUpdate => {
                    for (img, ctr) in [(ZK_IMAGE, ZK_CTR), (NIMBUS_IMAGE, NIMBUS_CTR)] {
                        log(format!("> Updating {img}…")).await;
                        let (_, out) = podman::update_image(img, ctr).await;
                        for line in out.lines() { log(line.to_string()).await; }
                    }
                    let state = probe_storm().await;
                    let _ = tx.send(AppEvent::ServiceStatus { service: "storm", state }).await;
                }
                _ => {}
            }
        }
        _ => {}
    }
}

// ── Probes ────────────────────────────────────────────────────────────────────

async fn probe_redis() -> ServiceState {
    let image  = podman::image_exists(REDIS_IMAGE).await;
    let status = match podman::container_status(REDIS_CTR).await {
        CtrStatus::Running => ContainerStatus::Running,
        CtrStatus::Exited  => ContainerStatus::Stopped,
        CtrStatus::NotFound => if image { ContainerStatus::Stopped } else { ContainerStatus::NotInstalled },
        CtrStatus::Other(_) => ContainerStatus::Stopped,
    };
    let version = if image { podman::image_version(REDIS_IMAGE).await } else { String::new() };
    ServiceState { status, version, image }
}

async fn probe_storm() -> ServiceState {
    let image  = podman::image_exists(NIMBUS_IMAGE).await;
    let status = match podman::container_status(NIMBUS_CTR).await {
        CtrStatus::Running => ContainerStatus::Running,
        CtrStatus::Exited  => ContainerStatus::Stopped,
        CtrStatus::NotFound => if image { ContainerStatus::Stopped } else { ContainerStatus::NotInstalled },
        CtrStatus::Other(_) => ContainerStatus::Stopped,
    };
    let version = if image { podman::image_version(NIMBUS_IMAGE).await } else { String::new() };
    ServiceState { status, version, image }
}

// ── Background loading sequence ───────────────────────────────────────────────

async fn loading_sequence(tx: mpsc::Sender<AppEvent>) {
    macro_rules! step {
        ($idx:expr, $fut:expr, $ok_msg:expr, $err_msg:expr) => {{
            let _ = tx.send(AppEvent::StepUpdate {
                index: $idx, status: StepStatus::Running,
            }).await;
            tokio::time::sleep(Duration::from_millis(200)).await;
            let (ok, detail) = $fut.await;
            let status = if ok {
                StepStatus::Done
            } else {
                StepStatus::Failed(detail.lines().next().unwrap_or($err_msg).to_string())
            };
            let _ = tx.send(AppEvent::StepUpdate { index: $idx, status }).await;
            (ok, detail)
        }};
    }

    // Step 0 — check Podman
    let (pod_ok, _pod_ver) = step!(0,
        async { podman::check_installed().await },
        "ok", "not found"
    );
    if !pod_ok {
        // Mark remaining as failed
        for i in 1..6 {
            let _ = tx.send(AppEvent::StepUpdate {
                index: i,
                status: StepStatus::Failed("podman not installed".into()),
            }).await;
        }
        return;
    }

    // Step 1 — rootless
    let _ = tx.send(AppEvent::StepUpdate { index: 1, status: StepStatus::Running }).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let rootless = podman::check_rootless().await;
    let _ = tx.send(AppEvent::StepUpdate {
        index: 1,
        status: if rootless { StepStatus::Done } else { StepStatus::Failed("running as root".into()) },
    }).await;

    // Step 2 — inspect network
    let _ = tx.send(AppEvent::StepUpdate { index: 2, status: StepStatus::Running }).await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    let net = podman::inspect_network("pcw.network").await;
    let _ = tx.send(AppEvent::StepUpdate { index: 2, status: StepStatus::Done }).await;

    // Step 3 — create network if needed
    let (subnet, gateway) = ("10.89.0.0/24", "10.89.0.1");
    let (final_subnet, final_gw) = if net.exists {
        let _ = tx.send(AppEvent::StepUpdate { index: 3, status: StepStatus::Done }).await;
        (net.subnet.clone(), net.gateway.clone())
    } else {
        let _ = tx.send(AppEvent::StepUpdate { index: 3, status: StepStatus::Running }).await;
        let (ok, _) = podman::create_network("pcw.network", subnet, gateway).await;
        let _ = tx.send(AppEvent::StepUpdate {
            index: 3,
            status: if ok { StepStatus::Done } else { StepStatus::Failed("create failed".into()) },
        }).await;
        (subnet.to_string(), gateway.to_string())
    };

    // Emit network info → triggers NetworkPopup
    let _ = tx.send(AppEvent::NetworkReady(NetworkInfo {
        name:    "pcw.network".into(),
        driver:  if net.exists { net.driver } else { "bridge".into() },
        subnet:  final_subnet,
        gateway: final_gw,
    })).await;

    // Step 4 — Redis image
    let _ = tx.send(AppEvent::StepUpdate { index: 4, status: StepStatus::Running }).await;
    let redis_state = probe_redis().await;
    let _ = tx.send(AppEvent::StepUpdate { index: 4, status: StepStatus::Done }).await;
    let _ = tx.send(AppEvent::ServiceStatus { service: "redis", state: redis_state }).await;

    // Step 5 — Storm images
    let _ = tx.send(AppEvent::StepUpdate { index: 5, status: StepStatus::Running }).await;
    let storm_state = probe_storm().await;
    let _ = tx.send(AppEvent::StepUpdate { index: 5, status: StepStatus::Done }).await;
    let _ = tx.send(AppEvent::ServiceStatus { service: "storm", state: storm_state }).await;
}
