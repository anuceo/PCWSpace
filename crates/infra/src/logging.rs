use tracing_subscriber::{fmt, prelude::*, EnvFilter};

/// Initialise the global tracing subscriber.
///
/// The log level is read from `Settings::pcw_log_level` (populated via the
/// `PCW_LOG_LEVEL` environment variable, defaulting to `"info"`).  Output is
/// emitted as structured JSON to stdout.
///
/// Calling this more than once is safe: the underlying
/// `tracing_subscriber::registry` silently ignores subsequent registrations.
pub fn init_logging() {
    let level = pcw_core::config::get_settings().pcw_log_level.as_str();
    let filter = EnvFilter::try_new(level).unwrap_or_else(|_| EnvFilter::new("info"));

    // `try_init` returns an error if a subscriber is already registered;
    // we intentionally ignore that so callers do not need to guard against
    // double initialisation (e.g., in tests).
    let _ = tracing_subscriber::registry()
        .with(filter)
        .with(fmt::layer().json())
        .try_init();
}
