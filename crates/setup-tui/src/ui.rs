use ratatui::{
    Frame,
    layout::{Alignment, Constraint, Direction, Layout, Margin, Rect},
    style::{Color, Modifier, Style},
    text::{Line, Span},
    widgets::{
        Block, Borders, Clear, Gauge, List, ListItem, Paragraph, Tabs, Wrap,
    },
};
use crate::app::{
    App, ConfigFocus, ContainerStatus, Screen, ServiceTab, StepStatus,
    MAX_MEMORY_OPTIONS, PERSISTENCE_OPTIONS, REDIS_MODE_OPTIONS,
};

const CYAN:   Color = Color::Cyan;
const GREEN:  Color = Color::Green;
const RED:    Color = Color::Red;
const YELLOW: Color = Color::Yellow;
const GRAY:   Color = Color::DarkGray;
const WHITE:  Color = Color::White;

fn accent(s: &str) -> Span<'static> {
    Span::styled(s.to_owned(), Style::default().fg(CYAN).add_modifier(Modifier::BOLD))
}
fn ok(s: &str) -> Span<'static> {
    Span::styled(s.to_owned(), Style::default().fg(GREEN))
}
fn err(s: &str) -> Span<'static> {
    Span::styled(s.to_owned(), Style::default().fg(RED))
}
fn muted(s: &str) -> Span<'static> {
    Span::styled(s.to_owned(), Style::default().fg(GRAY))
}
fn hi(s: &str) -> Span<'static> {
    Span::styled(s.to_owned(), Style::default().fg(YELLOW).add_modifier(Modifier::BOLD))
}

// ── Root dispatcher ───────────────────────────────────────────────────────────

pub fn draw(f: &mut Frame, app: &App) {
    match app.screen {
        Screen::Loading       => draw_loading(f, app),
        Screen::NetworkPopup  => { draw_loading(f, app); draw_network_popup(f, app); }
        Screen::ServiceConfig => draw_service_config(f, app),
        Screen::Done          => draw_done(f, app),
    }
}

// ── Loading screen ────────────────────────────────────────────────────────────

fn draw_loading(f: &mut Frame, app: &App) {
    let area = f.size();
    let outer = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CYAN))
        .title(Line::from(vec![
            Span::raw(" "),
            accent("PCW"),
            muted("  —  Podman Setup"),
            Span::raw(" "),
        ]).alignment(Alignment::Center));
    f.render_widget(outer, area);

    let inner = area.inner(&Margin { horizontal: 2, vertical: 1 });
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(2),  // title
            Constraint::Length(1),  // spacer
            Constraint::Length(3),  // gauge
            Constraint::Length(1),  // spacer
            Constraint::Min(1),     // step list
        ])
        .split(inner);

    // Title
    let title = Paragraph::new(Line::from(vec![
        accent("Initializing Podman"),
        muted("  ·  one-man business OS"),
    ])).alignment(Alignment::Center);
    f.render_widget(title, rows[0]);

    // Progress gauge
    let pct = (app.load_progress() * 100.0) as u16;
    let gauge = Gauge::default()
        .block(Block::default().borders(Borders::ALL).border_style(Style::default().fg(GRAY)))
        .gauge_style(Style::default().fg(CYAN).bg(Color::Black))
        .percent(pct)
        .label(format!("{pct}%"));
    f.render_widget(gauge, rows[2]);

    // Step list
    let spinner = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"];
    let spin = spinner[(app.tick / 2 % 10) as usize];
    let items: Vec<ListItem> = app.steps.iter().map(|step| {
        let (icon, style) = match &step.status {
            StepStatus::Pending       => (muted("○"), Style::default().fg(GRAY)),
            StepStatus::Running       => (hi(spin),    Style::default().fg(YELLOW)),
            StepStatus::Done          => (ok("✓"),      Style::default().fg(GREEN)),
            StepStatus::Failed(_)     => (err("✗"),      Style::default().fg(RED)),
        };
        let label = match &step.status {
            StepStatus::Failed(msg) => format!("{}  {}", step.label, msg),
            _                       => step.label.to_string(),
        };
        ListItem::new(Line::from(vec![
            icon,
            Span::raw("  "),
            Span::styled(label, style),
        ]))
    }).collect();

    let list = List::new(items)
        .block(Block::default().borders(Borders::NONE));
    f.render_widget(list, rows[4]);

    // Footer hint
    let footer_area = Rect {
        x: area.x + 2,
        y: area.y + area.height.saturating_sub(2),
        width: area.width.saturating_sub(4),
        height: 1,
    };
    let hint = Paragraph::new(Line::from(muted("  [q] Quit")));
    f.render_widget(hint, footer_area);
}

// ── Network popup ─────────────────────────────────────────────────────────────

fn draw_network_popup(f: &mut Frame, app: &App) {
    let area  = f.size();
    let popup = centered_rect(50, 13, area);

    f.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GREEN))
        .title(Line::from(vec![
            Span::raw(" "),
            ok("✓  Network Established"),
            Span::raw(" "),
        ]).alignment(Alignment::Center));
    f.render_widget(block, popup);

    let inner = popup.inner(&Margin { horizontal: 2, vertical: 1 });
    let net   = &app.network;

    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Min(1), Constraint::Length(1), Constraint::Length(1)])
        .split(inner);

    let info = Paragraph::new(vec![
        Line::from(vec![muted("  Name:    "), Span::styled(net.name.clone(),    Style::default().fg(WHITE))]),
        Line::from(vec![muted("  Driver:  "), Span::styled(net.driver.clone(),  Style::default().fg(WHITE))]),
        Line::from(vec![muted("  Subnet:  "), Span::styled(net.subnet.clone(),  Style::default().fg(WHITE))]),
        Line::from(vec![muted("  Gateway: "), Span::styled(net.gateway.clone(), Style::default().fg(WHITE))]),
        Line::from(vec![Span::raw("")]),
        Line::from(vec![
            Span::raw("  "),
            Span::styled("Press Enter to configure services →", Style::default().fg(CYAN).add_modifier(Modifier::BOLD)),
        ]),
    ]);
    f.render_widget(info, rows[0]);
}

// ── Service config ────────────────────────────────────────────────────────────

fn draw_service_config(f: &mut Frame, app: &App) {
    let area = f.size();
    let outer = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(CYAN))
        .title(Line::from(vec![
            Span::raw(" "),
            accent("PCW  —  Service Configuration"),
            Span::raw(" "),
        ]).alignment(Alignment::Center));
    f.render_widget(outer, area);

    let inner = area.inner(&Margin { horizontal: 1, vertical: 1 });
    let rows = Layout::default()
        .direction(Direction::Vertical)
        .constraints([
            Constraint::Length(3),  // tab bar
            Constraint::Min(1),     // body
            Constraint::Length(1),  // footer
        ])
        .split(inner);

    // Tab bar
    let tab_titles = ["  Redis  ", "  Storm  ", "  Finish  "];
    let selected = match app.active_tab {
        ServiceTab::Redis  => 0,
        ServiceTab::Storm  => 1,
        ServiceTab::Finish => 2,
    };
    let tabs = Tabs::new(tab_titles.iter().cloned().collect::<Vec<_>>())
        .block(Block::default().borders(Borders::BOTTOM).border_style(Style::default().fg(GRAY)))
        .select(selected)
        .highlight_style(Style::default().fg(CYAN).add_modifier(Modifier::BOLD))
        .style(Style::default().fg(GRAY));
    f.render_widget(tabs, rows[0]);

    // Body
    match app.active_tab {
        ServiceTab::Redis  => draw_redis_tab(f, rows[1], app),
        ServiceTab::Storm  => draw_storm_tab(f, rows[1], app),
        ServiceTab::Finish => draw_finish_tab(f, rows[1], app),
    }

    // Footer
    let footer = Paragraph::new(Line::from(vec![
        muted(" Tab/[→][←] switch tab  "),
        muted(" [↑][↓] navigate  "),
        muted(" Enter action  "),
        muted(" [q] quit"),
    ]));
    f.render_widget(footer, rows[2]);
}

// ── Redis tab ─────────────────────────────────────────────────────────────────

fn draw_redis_tab(f: &mut Frame, area: Rect, app: &App) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);

    // Left: status + actions
    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(8), Constraint::Length(7), Constraint::Min(1)])
        .split(cols[0]);

    draw_service_status(f, left[0], "Redis", &app.redis_state);
    draw_action_buttons(f, left[1], app.redis_focus, "redis");
    draw_log(f, left[2], app);

    // Right: config form
    draw_redis_config(f, cols[1], app);
}

fn draw_service_status(f: &mut Frame, area: Rect, name: &str, state: &crate::app::ServiceState) {
    let (status_icon, status_color) = match &state.status {
        ContainerStatus::Running      => ("● running",      GREEN),
        ContainerStatus::Stopped      => ("○ stopped",      YELLOW),
        ContainerStatus::NotInstalled => ("✗ not installed", RED),
        ContainerStatus::Updating     => ("⟳ updating…",    CYAN),
        ContainerStatus::Unknown      => ("? unknown",       GRAY),
    };
    let image_line = if state.image {
        Line::from(vec![ok("✓"), muted("  image cached")])
    } else {
        Line::from(vec![err("✗"), muted("  image not pulled")])
    };
    let ver = if state.version.is_empty() { "—".to_string() } else { state.version.clone() };

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GRAY))
        .title(Span::styled(format!(" {name} Status "), Style::default().fg(WHITE)));
    f.render_widget(block, area);

    let inner = area.inner(&Margin { horizontal: 1, vertical: 1 });
    let para = Paragraph::new(vec![
        Line::from(vec![
            Span::styled(status_icon.to_owned(), Style::default().fg(status_color)),
        ]),
        image_line,
        Line::from(vec![muted("version  "), Span::styled(ver, Style::default().fg(WHITE))]),
    ]);
    f.render_widget(para, inner);
}

fn draw_action_buttons(f: &mut Frame, area: Rect, focus: ConfigFocus, _svc: &str) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GRAY))
        .title(Span::styled(" Actions ", Style::default().fg(WHITE)));
    f.render_widget(block, area);

    let inner = area.inner(&Margin { horizontal: 1, vertical: 1 });
    let btns = [
        (ConfigFocus::ActionInstall, "Install"),
        (ConfigFocus::ActionStart,   "Start  "),
        (ConfigFocus::ActionStop,    "Stop   "),
        (ConfigFocus::ActionUpdate,  "Update "),
    ];
    let lines: Vec<Line> = btns.iter().map(|(f_, label)| {
        if *f_ == focus {
            Line::from(vec![
                Span::styled("▶ ", Style::default().fg(CYAN)),
                Span::styled(format!("[{label}]"), Style::default().fg(CYAN).add_modifier(Modifier::BOLD)),
            ])
        } else {
            Line::from(vec![
                Span::raw("  "),
                Span::styled(format!("[{label}]"), Style::default().fg(GRAY)),
            ])
        }
    }).collect();
    f.render_widget(Paragraph::new(lines), inner);
}

fn draw_redis_config(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GRAY))
        .title(Span::styled(" Redis Configuration ", Style::default().fg(WHITE)));
    f.render_widget(block, area);

    let inner = area.inner(&Margin { horizontal: 2, vertical: 1 });
    let cfg = &app.redis_cfg;

    let field_style = |idx: usize| {
        if app.redis_focus == ConfigFocus::Field(idx) {
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(WHITE)
        }
    };

    let mem   = MAX_MEMORY_OPTIONS[cfg.max_memory];
    let pers  = PERSISTENCE_OPTIONS[cfg.persistence];
    let mode  = REDIS_MODE_OPTIONS[cfg.mode];

    let lines = vec![
        Line::from(vec![muted("Port:         "),
            Span::styled(&cfg.port, field_style(0))]),
        Line::from(vec![muted("Max Memory:   "),
            Span::styled(format!("◀ {mem} ▶"), field_style(1))]),
        Line::from(vec![muted("Persistence:  "),
            Span::styled(format!("◀ {pers} ▶"), field_style(2))]),
        Line::from(vec![muted("Mode:         "),
            Span::styled(format!("◀ {mode} ▶"), field_style(3))]),
        Line::from(Span::raw("")),
        Line::from(vec![muted("REDIS_URL:    "),
            Span::styled(format!("redis://127.0.0.1:{}", cfg.port),
                Style::default().fg(GRAY))]),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

// ── Storm tab ─────────────────────────────────────────────────────────────────

fn draw_storm_tab(f: &mut Frame, area: Rect, app: &App) {
    let cols = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(35), Constraint::Percentage(65)])
        .split(area);

    let left = Layout::default()
        .direction(Direction::Vertical)
        .constraints([Constraint::Length(11), Constraint::Length(7), Constraint::Min(1)])
        .split(cols[0]);

    draw_storm_status(f, left[0], app);
    draw_action_buttons(f, left[1], app.storm_focus, "storm");
    draw_log(f, left[2], app);
    draw_storm_config(f, cols[1], app);
}

fn draw_storm_status(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GRAY))
        .title(Span::styled(" Storm Status ", Style::default().fg(WHITE)));
    f.render_widget(block, area);

    let inner = area.inner(&Margin { horizontal: 1, vertical: 1 });
    let s = &app.storm_state;
    let (icon, color) = match &s.status {
        ContainerStatus::Running      => ("● running",      GREEN),
        ContainerStatus::Stopped      => ("○ stopped",      YELLOW),
        ContainerStatus::NotInstalled => ("✗ not installed", RED),
        ContainerStatus::Updating     => ("⟳ updating…",    CYAN),
        ContainerStatus::Unknown      => ("? unknown",       GRAY),
    };
    let para = Paragraph::new(vec![
        Line::from(Span::styled(format!("Nimbus:     {icon}"), Style::default().fg(color))),
        Line::from(Span::styled("Supervisor: —",     Style::default().fg(GRAY))),
        Line::from(Span::styled("ZooKeeper:  —",     Style::default().fg(GRAY))),
        Line::from(Span::raw("")),
        Line::from(if s.image {
            vec![ok("✓"), muted("  images cached")]
        } else {
            vec![err("✗"), muted("  images not pulled")]
        }),
    ]);
    f.render_widget(para, inner);
}

fn draw_storm_config(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GRAY))
        .title(Span::styled(" Storm Configuration ", Style::default().fg(WHITE)));
    f.render_widget(block, area);

    let inner = area.inner(&Margin { horizontal: 2, vertical: 1 });
    let cfg = &app.storm_cfg;

    let field_style = |idx: usize| {
        if app.storm_focus == ConfigFocus::Field(idx) {
            Style::default().fg(CYAN).add_modifier(Modifier::BOLD)
        } else {
            Style::default().fg(WHITE)
        }
    };

    let lines = vec![
        Line::from(vec![muted("Nimbus URL:     "),
            Span::styled(&cfg.nimbus_url, field_style(0))]),
        Line::from(vec![muted("Worker nodes:   "),
            Span::styled(format!("◀ {} ▶", cfg.worker_count), field_style(1))]),
        Line::from(vec![muted("Slots/worker:   "),
            Span::styled(format!("◀ {} ▶", cfg.slot_count), field_style(2))]),
        Line::from(vec![muted("Worker heap MB: "),
            Span::styled(&cfg.heap_mb, field_style(3))]),
        Line::from(Span::raw("")),
        Line::from(vec![muted("Topology:  "),
            Span::styled("WorkflowSpout → WorkflowBolt × 4  +  DeltaShotBolt × 2",
                Style::default().fg(GRAY))]),
    ];
    f.render_widget(Paragraph::new(lines).wrap(Wrap { trim: false }), inner);
}

// ── Finish tab ────────────────────────────────────────────────────────────────

fn draw_finish_tab(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GREEN))
        .title(Span::styled(" Setup Summary ", Style::default().fg(GREEN)));
    f.render_widget(block, area);

    let inner = area.inner(&Margin { horizontal: 3, vertical: 2 });
    let redis_ok = app.redis_state.status == ContainerStatus::Running;
    let storm_ok = app.storm_state.status == ContainerStatus::Running;

    let lines = vec![
        Line::from(vec![
            if redis_ok { ok("✓") } else { err("✗") },
            Span::raw("  Redis   "),
            muted(&format!("redis://127.0.0.1:{}", app.redis_cfg.port)),
        ]),
        Line::from(vec![
            if storm_ok { ok("✓") } else { err("✗") },
            Span::raw("  Storm   "),
            muted(&app.storm_cfg.nimbus_url),
        ]),
        Line::from(Span::raw("")),
        Line::from(Span::styled(
            "Settings written to .env.podman",
            Style::default().fg(GRAY),
        )),
        Line::from(Span::raw("")),
        Line::from(vec![
            Span::styled("Press ", Style::default().fg(GRAY)),
            Span::styled("Enter", Style::default().fg(CYAN).add_modifier(Modifier::BOLD)),
            Span::styled(" to exit setup.", Style::default().fg(GRAY)),
        ]),
    ];
    f.render_widget(Paragraph::new(lines), inner);
}

// ── Shared output log ─────────────────────────────────────────────────────────

fn draw_log(f: &mut Frame, area: Rect, app: &App) {
    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GRAY))
        .title(Span::styled(" Output ", Style::default().fg(WHITE)));
    f.render_widget(block, area);

    let inner = area.inner(&Margin { horizontal: 1, vertical: 1 });
    let h = inner.height as usize;
    let start = app.log_scroll.saturating_sub(h.saturating_sub(1));
    let lines: Vec<Line> = app.log.iter().skip(start).take(h)
        .map(|s| Line::from(Span::styled(s.clone(), Style::default().fg(GRAY))))
        .collect();
    f.render_widget(Paragraph::new(lines), inner);
}

// ── Done screen ───────────────────────────────────────────────────────────────

fn draw_done(f: &mut Frame, _app: &App) {
    let area = f.size();
    let popup = centered_rect(50, 9, area);
    f.render_widget(Clear, popup);

    let block = Block::default()
        .borders(Borders::ALL)
        .border_style(Style::default().fg(GREEN))
        .title(Line::from(ok("  Setup Complete  ")).alignment(Alignment::Center));
    f.render_widget(block, popup);

    let inner = popup.inner(&Margin { horizontal: 2, vertical: 1 });
    let para = Paragraph::new(vec![
        Line::from(Span::raw("")),
        Line::from(vec![ok("✓ "), Span::raw(".env.podman written")]),
        Line::from(Span::raw("")),
        Line::from(vec![
            Span::raw("Run  "),
            Span::styled("./deploy/podman-up.sh", Style::default().fg(CYAN)),
            Span::raw("  to start the stack."),
        ]),
    ]).alignment(Alignment::Center);
    f.render_widget(para, inner);
}

// ── Helpers ───────────────────────────────────────────────────────────────────

fn centered_rect(percent_x: u16, height: u16, r: Rect) -> Rect {
    let w = r.width * percent_x / 100;
    Rect {
        x: r.x + (r.width.saturating_sub(w)) / 2,
        y: r.y + (r.height.saturating_sub(height)) / 2,
        width: w,
        height: height.min(r.height),
    }
}
