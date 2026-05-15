# Deployment Profiles

This guide describes production-oriented deployment profiles for the current Delta Agent runtime.

## 1) Single-node profile (dev / small staging)

Use when you want the simplest setup:

- one server process (`cargo run -p server`)
- one worker process (`cargo run -p worker`)
- optional Redis (in-memory fallback works for basic runs)

Recommended sizing:

- CPU: 2 vCPU
- Memory: 4 GB
- Disk: 20 GB SSD

Required configuration:

- `DELTA_AGENT_CONFIG` pointing to a valid TOML (default `configs/default.toml`)
- API auth keys if auth is enabled (`DELTA_AGENT_AUTH_*`)

Optional:

- `DELTA_AGENT_REDIS_URL` + `DELTA_AGENT_VDDAB_ROOT` for durable queue/lock/idempotency behavior
- `DELTA_AGENT_USE_REAL_AGENTS=true` and provider API keys

## 2) Distributed profile (production)

Use when you need durability and horizontal scale:

- 2+ server instances behind a load balancer
- 1+ worker instances polling workflow/notion execute-next endpoints
- managed Redis for distributed locking/idempotency/durable queue operations
- shared persistent disk or durable volume for VDDAB root path

Recommended sizing (starting point):

- Server: 4 vCPU / 8 GB RAM each
- Worker: 2 vCPU / 4 GB RAM each
- Redis: managed, same VPC/region as app instances
- Disk: 80+ GB SSD for VDDAB growth

Required:

- `DELTA_AGENT_REDIS_URL` must be set
- `DELTA_AGENT_VDDAB_ROOT` must be writable on each instance
- API auth should be enabled with writer/admin keys

## 3) Configuration model

`configs/default.toml` is now structured to map directly to runtime behavior:

- `[server]` host/port bind
- `[runtime]` namespace environment (maps to `DELTA_AGENT_ENV`)
- `[persistence]` Redis + VDDAB
- `[auth]` auth middleware toggles and key lists
- `[agents]` real-agent gate + provider keys/models
- `[notion]` notion sync integration settings
- `[worker]` polling, timeout, and notion polling toggle

Runtime environment variables still take precedence over TOML values when already set in process environment.

## 4) Notion sync behavior

Notion sync runs through background jobs processed by:

- `POST /api/v1/workflows/notion/execute-next`

When enabled (`NOTION_SYNC_ENABLED=true`) and fully configured, the worker posts queue outputs to Notion pages API and retries failed jobs until `NOTION_SYNC_MAX_ATTEMPTS` is reached.

Supported parent targets:

- `NOTION_DATABASE_ID` (uses `NOTION_DATABASE_TITLE_PROPERTY`, default `Name`)
- `NOTION_PARENT_PAGE_ID` (uses `title` property for child page creation)

If Notion integration is disabled or not fully configured, jobs are consumed and logged without outbound sync.

## 5) Operational checks

Before promotion to production:

1. `cargo fmt --all`
2. `cargo check --workspace`
3. `cargo clippy --workspace -- -D warnings`
4. `cargo test --workspace`
5. `python3 scripts/check_openapi_sync.py`

Runtime smoke checks:

- `GET /api/v1/health`
- create session + send message
- execute workflow queue drain endpoint
- execute notion queue drain endpoint
- run debug branch audit endpoint (admin key + persistence enabled)
