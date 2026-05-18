# AGENTS.md

## Cursor Cloud specific instructions

### Services

| Service | Purpose | How to start |
|---------|---------|-------------|
| **Redis** | Sole data store (event sourcing, sessions, keys, streams) | `redis-server --daemonize yes` |
| **PCW API** | Single Rust binary, Axum HTTP server on port 8000 | `cargo run` (from workspace root) |

### Key Gotchas

- **Rust 1.85+ required**: A transitive dependency (`getrandom`) uses `edition2024`, which requires Rust 1.85+. Run `rustup update stable && rustup default stable` if the build fails with `feature edition2024 is required`.
- **Interactive startup wizard**: The binary has a startup wizard (`crates/api/src/startup.rs`) that checks network and prompts for missing API keys via stdin. To run non-interactively, pre-populate **all** required keys in `.env` (or use `__SKIPPED__` sentinel for optional ones). Required keys: `REDIS_URL`, `ANTHROPIC_API_KEY` (can be `__SKIPPED__` for local dev), `PCW_API_KEY`.
- **No Cargo.lock committed**: `Cargo.lock` is gitignored, so `cargo build`/`cargo fetch` regenerates it each time. This means builds may pull newer patch versions of dependencies.
- **Degraded mode**: The server starts even when LLM API keys are missing/invalid. Redis is the only hard requirement at startup; `/health` returns `{"status":"degraded"}` if Redis is down.
- **API authentication**: All `/api/v1/*` endpoints require an `x-api-key` header matching the `PCW_API_KEY` env var (default: `dev-insecure`).

### Common Commands

See `README.md` for full details. Quick reference:
- **Build**: `cargo build`
- **Test**: `cargo test --verbose`
- **Lint**: `cargo clippy`
- **Run**: `cargo run` (starts on `$PCW_HOST:$PCW_PORT`, default `0.0.0.0:8000`)
- **Docs**: `cargo doc --open`

### .env Setup

Copy `.env.example` to `.env`. For local development without LLM keys:
```
REDIS_URL=redis://127.0.0.1:6379
ANTHROPIC_API_KEY=__SKIPPED__
DEEPSEEK_API_KEY=__SKIPPED__
NOTION_TOKEN=__SKIPPED__
PCW_API_KEY=dev-insecure
PCW_HOST=0.0.0.0
PCW_PORT=8000
PCW_LOG_LEVEL=info
SESSION_KEY_TTL_SECS=86400
```
