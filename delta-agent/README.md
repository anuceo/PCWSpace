# Delta Agent Workspace

A production-oriented Rust workspace focused on DeltaShot execution, Redis/VDDAB durability, workflow processing, and API/worker runtimes.

## Environment setup

```bash
cd delta-agent
./scripts/setup.sh
```

This prepares the environment by:

- Rust toolchain presence (uses `rust-toolchain.toml`)
- dependency fetch (`cargo fetch --locked || cargo fetch`)
- optional prewarm (`cargo check --workspace` + `cargo test --workspace --no-run`)

## Structure

- `crates/*`: reusable domain and infrastructure crates
- `apps/server`: API-facing server binary
- `apps/worker`: background execution worker
- `configs/default.toml`: baseline runtime configuration
- `scripts/dev.sh`: local development helper

### Active workspace scope

The active build graph is intentionally runtime-focused:

- core execution: `core`, `deltashot`, `replay`, `vddab`, `compression`, `hashing`
- API/runtime: `api`, `apps/server`, `apps/worker`
- supporting domain crate: `penpal`

To reduce maintenance surface, the lightly wired crates are currently trimmed from workspace builds:

- `crates/streaming`
- `crates/trace`
- `crates/cache`
- `crates/snapshot`
- `crates/scheduler`

## Quick start

```bash
cd delta-agent
cargo check --workspace
```

## Run server

```bash
cd delta-agent
./scripts/run-server.sh
```

Overrides:

- `DELTA_AGENT_CONFIG` (default: `configs/default.toml`)
- `RUST_LOG` (default: `server=info`)

Health route examples:

- `GET /api/v1/health`
- `GET /api/v1/sessions/{id}`
- `GET /api/v1/sessions/{id}/trace`
- `POST /api/v1/sessions/{id}/messages`

Deterministic request lifecycle for `POST /api/v1/sessions/{id}/messages`:

1. hydrate session state + recent messages
2. acquire session lock
3. append user message
4. route + run agent
5. apply state mutation + compute diff
6. write DeltaShot + hash chain
7. upsert artifact metadata (if produced)
8. append agent response
9. persist updated state
10. enqueue workflow continuation
11. enqueue async Notion sync job
12. release lock and respond

## Run worker

```bash
cd delta-agent
./scripts/run-worker.sh
```

Overrides:

- `DELTA_AGENT_CONFIG` (default: `configs/default.toml`)
- `RUST_LOG` (default: `worker=info`)
- `DELTA_AGENT_REDIS_URL` + `DELTA_AGENT_VDDAB_ROOT` enable durable background queues

Worker background execution endpoints:

- `POST /api/v1/workflows/execute-next`
- `POST /api/v1/workflows/notion/execute-next`

## API v1 Contract

Base path:

`/api/v1`

Formal spec artifacts:

- `docs/api-contract-v1.md`
- `docs/openapi.v1.json`
- `python3 scripts/check_openapi_sync.py` (runtime route/spec alignment check)

### Core endpoints

- `POST /api/v1/workspaces`
- `GET /api/v1/workspaces/{workspaceId}`
- `GET /api/v1/workspaces/{workspaceId}/sessions?limit=20&cursor=<timestamp>`
- `POST /api/v1/sessions`
- `POST /api/v1/sessions/{sessionId}/messages`
- `GET /api/v1/sessions/{sessionId}/state`
- `GET /api/v1/sessions/{sessionId}/messages?limit=50&cursor=<message_id>`
- `GET /api/v1/sessions/{sessionId}/deltashots`
- `GET /api/v1/deltashots/{deltashotId}`
- `POST /api/v1/sessions/{sessionId}/rollback`
- `POST /api/v1/artifacts`
- `GET /api/v1/artifacts/{artifactId}`
- `GET /api/v1/artifacts/{artifactId}/versions`
- `GET /api/v1/artifacts/{artifactId}/versions/{version}`
- `POST /api/v1/workflows/start`
- `POST /api/v1/workflows/execute-next`
- `POST /api/v1/workflows/notion/execute-next`
- `GET /api/v1/workflows/{workflowId}/state`
- `POST /api/v1/workflows/{workflowId}/step`
- `POST /api/v1/sessions/{sessionId}/agent`
- `GET /api/v1/sessions/{sessionId}/agents/logs`
- `GET /api/v1/sessions/{sessionId}/trace`
- `POST /api/v1/debug/sessions/{sessionId}/branches/{branchId}/audit`
- `GET /api/v1/health`

### Authentication / authorization middleware

`/api/v1/*` is protected by API-key middleware when auth is enabled. Credentials can be sent via:

- `Authorization: Bearer <api_key>`
- `x-api-key: <api_key>`

Role mapping:

- **reader**: GET/HEAD API routes
- **writer**: reader permissions + mutation routes (POST)
- **admin**: writer permissions + `/api/v1/debug/*`

Configuration environment variables:

- `DELTA_AGENT_AUTH_REQUIRED` (default `false`)
- `DELTA_AGENT_AUTH_DISABLED` (default `false`)
- `DELTA_AGENT_API_KEY` / `DELTA_AGENT_API_KEYS` (writer keys)
- `DELTA_AGENT_READONLY_API_KEY` / `DELTA_AGENT_READONLY_API_KEYS` (reader keys)
- `DELTA_AGENT_ADMIN_API_KEY` / `DELTA_AGENT_ADMIN_API_KEYS` (admin keys)
- `PCW_API_KEY` is also accepted as a writer key for compatibility

### Response metadata

All successful responses include:

```json
{
  "...": "...",
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 12
  }
}
```

Errors follow:

```json
{
  "error": {
    "code": "SESSION_LOCKED",
    "message": "session lock unavailable",
    "retryable": true
  },
  "meta": {
    "request_id": "req_123",
    "timestamp": 1775453669005,
    "latency_ms": 1
  }
}
```

### Deterministic message entrypoint example

```bash
curl -X POST "http://127.0.0.1:8080/api/v1/sessions/sess_2/messages" \
  -H "content-type: application/json" \
  -d '{
    "content": "Create a landing page for my product",
    "mode": "workflow",
    "metadata": {"priority":"normal"}
  }'
```

## Cloud agent environment

The repository root contains `.cursor/environment.json` to speed up and
stabilize cloud-agent runs for this workspace:

- uses stable Rust with `rustfmt` and `clippy`
- prewarms `cargo check --workspace` + `cargo test --workspace --no-run`
- persists Cargo/Rust caches and a shared Cargo target directory
