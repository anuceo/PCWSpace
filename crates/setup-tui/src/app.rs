use std::collections::VecDeque;

// ── Step / screen state ───────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum StepStatus {
    Pending,
    Running,
    Done,
    Failed(String),
}

#[derive(Debug, Clone)]
pub struct LoadingStep {
    pub label:  &'static str,
    pub status: StepStatus,
}

#[derive(Debug, Clone, Default)]
pub struct NetworkInfo {
    pub name:    String,
    pub driver:  String,
    pub subnet:  String,
    pub gateway: String,
}

#[derive(Debug, Clone, PartialEq)]
pub enum ServiceTab { Redis, Storm, Finish }

// ── Service state (one per service) ──────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum ContainerStatus { Unknown, NotInstalled, Stopped, Running, Updating }

impl std::fmt::Display for ContainerStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ContainerStatus::Unknown      => write!(f, "unknown"),
            ContainerStatus::NotInstalled => write!(f, "not installed"),
            ContainerStatus::Stopped      => write!(f, "stopped"),
            ContainerStatus::Running      => write!(f, "running"),
            ContainerStatus::Updating     => write!(f, "updating…"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ServiceState {
    pub status:  ContainerStatus,
    pub version: String,
    pub image:   bool,     // true = image is present locally
}

impl Default for ServiceState {
    fn default() -> Self {
        Self { status: ContainerStatus::Unknown, version: String::new(), image: false }
    }
}

// ── Redis config ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct RedisConfig {
    pub port:        String,
    pub max_memory:  usize,   // index into MAX_MEMORY_OPTIONS
    pub persistence: usize,   // index into PERSISTENCE_OPTIONS
    pub mode:        usize,   // index into MODE_OPTIONS
}

pub const MAX_MEMORY_OPTIONS:  &[&str] = &["256mb", "512mb", "1gb", "2gb", "4gb"];
pub const PERSISTENCE_OPTIONS: &[&str] = &["AOF", "RDB", "None"];
pub const REDIS_MODE_OPTIONS:  &[&str] = &["Standalone", "Sentinel"];

impl Default for RedisConfig {
    fn default() -> Self {
        Self { port: "6379".into(), max_memory: 1, persistence: 0, mode: 0 }
    }
}

// ── Storm config ──────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct StormConfig {
    pub nimbus_url:      String,
    pub worker_count:    String,
    pub slot_count:      String,
    pub heap_mb:         String,
}

impl Default for StormConfig {
    fn default() -> Self {
        Self {
            nimbus_url:   "http://storm-nimbus:8080".into(),
            worker_count: "2".into(),
            slot_count:   "4".into(),
            heap_mb:      "256".into(),
        }
    }
}

// ── Focus tracking inside a config panel ─────────────────────────────────────

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ConfigFocus { Field(usize), ActionInstall, ActionStart, ActionStop, ActionUpdate }

// ── Screen ────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq)]
pub enum Screen {
    Loading,
    NetworkPopup,
    ServiceConfig,
    Done,
}

// ── Async events passed from worker tasks back to the TUI loop ───────────────

#[derive(Debug)]
pub enum AppEvent {
    Tick,
    StepUpdate { index: usize, status: StepStatus },
    NetworkReady(NetworkInfo),
    ServiceStatus { service: &'static str, state: ServiceState },
    CommandOutput(String),
    Quit,
}

// ── Root application state ────────────────────────────────────────────────────

pub struct App {
    pub screen:       Screen,
    pub tick:         u64,

    // Loading screen
    pub steps:        Vec<LoadingStep>,
    pub loading_done: bool,

    // Network popup
    pub network:      NetworkInfo,

    // Service config
    pub active_tab:   ServiceTab,
    pub redis_state:  ServiceState,
    pub storm_state:  ServiceState,
    pub redis_cfg:    RedisConfig,
    pub storm_cfg:    StormConfig,
    pub redis_focus:  ConfigFocus,
    pub storm_focus:  ConfigFocus,

    // Shared output log
    pub log:          VecDeque<String>,
    pub log_scroll:   usize,
}

impl App {
    pub fn new() -> Self {
        Self {
            screen:       Screen::Loading,
            tick:         0,
            steps:        vec![
                LoadingStep { label: "Checking Podman installation",  status: StepStatus::Pending },
                LoadingStep { label: "Verifying rootless user mode",   status: StepStatus::Pending },
                LoadingStep { label: "Inspecting pcw.network",         status: StepStatus::Pending },
                LoadingStep { label: "Establishing pcw.network",       status: StepStatus::Pending },
                LoadingStep { label: "Checking Redis image",           status: StepStatus::Pending },
                LoadingStep { label: "Checking Storm images",          status: StepStatus::Pending },
            ],
            loading_done: false,
            network:      NetworkInfo::default(),
            active_tab:   ServiceTab::Redis,
            redis_state:  ServiceState::default(),
            storm_state:  ServiceState::default(),
            redis_cfg:    RedisConfig::default(),
            storm_cfg:    StormConfig::default(),
            redis_focus:  ConfigFocus::ActionInstall,
            storm_focus:  ConfigFocus::ActionInstall,
            log:          VecDeque::with_capacity(200),
            log_scroll:   0,
        }
    }

    pub fn push_log(&mut self, line: impl Into<String>) {
        let s = line.into();
        if self.log.len() >= 200 { self.log.pop_front(); }
        self.log.push_back(s);
        self.log_scroll = self.log.len().saturating_sub(1);
    }

    /// Loading progress 0.0 – 1.0
    pub fn load_progress(&self) -> f64 {
        let done = self.steps.iter().filter(|s| {
            matches!(s.status, StepStatus::Done | StepStatus::Failed(_))
        }).count();
        done as f64 / self.steps.len() as f64
    }

    /// Navigate tabs with left/right
    pub fn next_tab(&mut self) {
        self.active_tab = match self.active_tab {
            ServiceTab::Redis  => ServiceTab::Storm,
            ServiceTab::Storm  => ServiceTab::Finish,
            ServiceTab::Finish => ServiceTab::Redis,
        };
    }
    pub fn prev_tab(&mut self) {
        self.active_tab = match self.active_tab {
            ServiceTab::Redis  => ServiceTab::Finish,
            ServiceTab::Storm  => ServiceTab::Redis,
            ServiceTab::Finish => ServiceTab::Storm,
        };
    }

    /// Focus next action button within the current service tab
    pub fn focus_next(&mut self) {
        let focus = match self.active_tab {
            ServiceTab::Redis  => &mut self.redis_focus,
            ServiceTab::Storm  => &mut self.storm_focus,
            ServiceTab::Finish => return,
        };
        *focus = match *focus {
            ConfigFocus::Field(n)     => ConfigFocus::Field(n + 1),   // caller clamps
            ConfigFocus::ActionInstall => ConfigFocus::ActionStart,
            ConfigFocus::ActionStart   => ConfigFocus::ActionStop,
            ConfigFocus::ActionStop    => ConfigFocus::ActionUpdate,
            ConfigFocus::ActionUpdate  => ConfigFocus::ActionInstall,
        };
    }
    pub fn focus_prev(&mut self) {
        let focus = match self.active_tab {
            ServiceTab::Redis  => &mut self.redis_focus,
            ServiceTab::Storm  => &mut self.storm_focus,
            ServiceTab::Finish => return,
        };
        *focus = match *focus {
            ConfigFocus::Field(0)      => ConfigFocus::ActionInstall,
            ConfigFocus::Field(n)      => ConfigFocus::Field(n - 1),
            ConfigFocus::ActionInstall => ConfigFocus::ActionUpdate,
            ConfigFocus::ActionStart   => ConfigFocus::ActionInstall,
            ConfigFocus::ActionStop    => ConfigFocus::ActionStart,
            ConfigFocus::ActionUpdate  => ConfigFocus::ActionStop,
        };
    }

    pub fn current_focus(&self) -> ConfigFocus {
        match self.active_tab {
            ServiceTab::Redis  => self.redis_focus,
            ServiceTab::Storm  => self.storm_focus,
            ServiceTab::Finish => ConfigFocus::ActionInstall,
        }
    }

    /// Write the final Redis + Storm settings to `.env`.
    pub fn write_env(&self) {
        let redis_persist = match PERSISTENCE_OPTIONS[self.redis_cfg.persistence] {
            "AOF"  => "--appendonly yes",
            "RDB"  => "--save 60 1",
            _      => "--save ''",
        };
        let lines = format!(
            "# Redis\nREDIS_URL=redis://127.0.0.1:{port}\nREDIS_PORT={port}\nREDIS_MAX_MEMORY={mem}\nREDIS_PERSIST_FLAGS={persist}\n\
             # Storm\nSTORM_NIMBUS_URL={nimbus}\nSTORM_WORKERS={workers}\nSTORM_SLOTS={slots}\n",
            port    = self.redis_cfg.port,
            mem     = MAX_MEMORY_OPTIONS[self.redis_cfg.max_memory],
            persist = redis_persist,
            nimbus  = self.storm_cfg.nimbus_url,
            workers = self.storm_cfg.worker_count,
            slots   = self.storm_cfg.slot_count,
        );
        let _ = std::fs::write(".env.podman", lines);
    }
}
