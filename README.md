# PCWSpace

**Persistent Cognitive Workspaces**

A self-hosted, single-binary Rust server that acts as an "operating system" for AI-assisted work. Every interaction with an LLM is permanently recorded, cryptographically verified, replayable, and branchable. Think of it as **Git + a multi-agent AI runtime + a task orchestration engine**, all backed by immutable event sourcing and tamper-proof delta chains.

## What is PCW?

Persistent Cognitive Workspace is the foundation of intelligent task automation. Every interaction with an LLM is:

- **Permanently recorded** — Event-sourced in Redis as append-only streams
- **Cryptographically verified** — SHA-256 hash chains with HMAC integrity
- **Replayable** — Reconstruct any historical state on demand
- **Branchable** — Fork sessions to explore parallel reasoning paths
- **Tamper-evident** — Any modification to history is immediately detected on replay

## Core Architecture

### 1. **Rediagents** — Multi-LLM Router

Abstracts two AI providers behind a common interface:

#### `claude.rs`
- Posts to `https://api.anthropic.com/v1/messages`
- Headers: `x-api-key`, `anthropic-version: 2023-06-01`
- Extracts text content blocks
- Returns `AgentResult` with token counts

#### `deepseek.rs`
- OpenAI-compatible format
- Posts to `{DEEPSEEK_BASE_URL}/v1/chat/completions` with `Authorization: Bearer`
- System prompt prepended as system role message

#### `router.rs`
- `AgentRouter::call()` dispatches to the right agent
- `select_agent(task_hint)` uses keyword heuristics:
  - **code/debug/refactor/algorithm** → DeepSeek
  - **everything else** → Claude

### 2. **Intelligence** — Task Analysis

Sits between the user's message and the agent router.

#### `analyzer.rs`
- `analyze_task(prompt)` categorizes requests into:
  - CodeGeneration, Debugging, DataAnalysis, Documentation, Research, Planning, General
- Returns suggested agent type
- Used by `Orchestrator::call_agent()` before every LLM call

#### `scoring.rs`
- `QualityScore` with dimensions:
  - **clarity** — sentence length ideality
  - **completeness** — text length bands
  - **actionability** — action verb count
  - **overall** — average across dimensions
- Characterizes input quality

### 3. **Artifacts** — Versioned Document Store with Baton

Manages every piece of content produced or stored in the workspace.

#### `service.rs`
- `ArtifactService::create()` stores v1, sets latest pointer, adds root to session artifact set
- `get()` resolves artifact by ID
- `list_for_session()` returns all artifacts for a session

#### `versions.rs`
- `new_version()` takes root ID, reads true current version number, increments, stores new artifact
- Appends to version list
- Advances latest pointer

#### `baton.rs` — Session Ownership Enforcement *(planned — not yet implemented)*
- `init()` — Binds session ID to root artifact at creation
- `verify()` — Checks deny set first (permanent), then compares stored session to caller
- `drop_baton()` — Adds root to `pcw:baton:dropped` (no TTL, permanent)
  - Sets 1-hour ephemeral TTL on baton key, latest pointer, versions list
  - Once dropped: every write and read returns `403 BatonDropped`, even from original owner
- **7-day expiry** on superseded snapshots (root and new latest exempt) *(planned)*

### 4. **DeltaShots** — Event Sourcing Engine

The most security-critical layer. Every state change flows through here.

#### `diff.rs`
- Pure JSON diff algorithm: `compute_diff(before, after)` → `{"added":{}, "removed":{}, "changed":{}}`
- `apply_diff(base, diff)` reconstructs state

#### `encryption.rs`
- AES-256-GCM (pure Rust, no OpenSSL)
- Format on disk: `[IV(12 bytes) || ciphertext+tag]`
- IV generated fresh per encrypt via `OsRng`

#### `hash.rs`
- `SHA-256(prev_hash || encrypted_payload)` for chain linking
- `HMAC-SHA256(content_hash, signing_key)` for integrity
- Constant-time comparison on verify

#### `store.rs`
- `XADD` to `pcw:deltashots:{session_id}` (append-only, never delete)
- `XRANGE/XREVRANGE` for reads
- Binary fields base64-encoded for stream storage

#### `engine.rs`
- Orchestrates full append pipeline: diff → encrypt → sequence → hash chain → sign → `XADD`
- `get_decrypted_diff()` for replay with TTL refresh on every key read

#### `replay.rs`
- Reconstructs full session state from stream
- Verifies every shot: prev_hash chain, content_hash, HMAC signature
- Any tamper raises `TamperDetected`
- `list_rollback_points()` for UI

**Key Invariant:** Encryption and signing keys live in Redis with sliding TTL (default 24h). Every read refreshes TTL, so active sessions never lose their chain.

### 5. **Workflows** — Task Orchestration Engine

Multi-step AI workflows with retry, timeout, and dead-letter handling.

#### `definitions.rs`
Two built-in templates:
- **client_outreach** — 4 steps: research → draft → review → finalize
- **content_creation** — 5 steps: research → outline → write → edit → save

Each step has agent type, prompt template, timeout, max retries.

#### `engine.rs`
- State machine: `start()`, `advance()`, `fail()`
- `start()` persists definition, enqueues first step
- `advance()` stores result, looks up next via state_transitions, marks `Completed` if no next
- `fail()` increments retry or marks `DeadLettered`

#### `executor.rs`
- `StepExecutor::execute_current_step()` runs one step
- Non-agent steps (finalize/save) return immediately
- Agent steps call `AgentRouter` with previous results as context
- Retry loop with exponential backoff: 500ms → 1s → 2s
- Per-step `tokio::time::timeout`

#### `worker.rs`
- Redis Streams consumer via `XREADGROUP` on `pcw:workflow:steps` consumer group `pcw-workers`
- On success: advance state, `XACK`
- On failure after retries: dead-letter stream + `XACK`

### 6. **Runtime** — Session Lifecycle Coordinator

Highest-level orchestration layer.

#### `orchestrator.rs`
- `Orchestrator::call_agent()` — Full session call pipeline:
  1. Load session
  2. Check not closed
  3. Capture before-state
  4. Analyze task
  5. Call agent
  6. Capture after-state
  7. Append DeltaShot
  8. Save session
  9. Return result with shot_id
- Also: `create_session()`, `close_session()`, `load_session()`, `save_session()`

#### `scheduler.rs`
- `run_workflow_worker_loop()` — Infinite loop calling `workflows::worker::process_next()` on configurable interval
- Spawned as background `tokio::spawn` task at server startup
- Logs warnings on error but never exits

### 7. **Audit** — Immutable Audit Log

Thin layer over Redis Streams for compliance and observability.

#### `log.rs`
- `record(event_type, session_id, actor, detail)` → `XADD pcw:audit:log`
- Convenience functions: `record_session_created()`, `record_agent_called()`

#### `report.rs`
- `list_recent(count)` — `XREVRANGE` newest-first
- `list_for_session(session_id)` — filtered scan

### 8. **Timeline** — Session Branching

Enables parallel exploration from any point in history.

#### `branch.rs`
- `fork_session(source_session_id, fork_at_sequence)`:
  1. Replay state to sequence
  2. Create new session in same workspace with `forked_from`/`fork_at_sequence` metadata
  3. Write initial DeltaShot with `BRANCH_FORK` action
  4. Start independent chain

#### `graph.rs`
- `get_timeline(session_id)` returns `TimelineNode` with shot count and fork metadata
- `list_rollback_points()` delegates to deltashots replay module

### 9. **API** — HTTP Server and Startup Wizard

The only binary in the workspace.

#### `startup.rs` — Interactive Terminal Wizard

Runs before server starts:

**Phase 1: Connectivity**
- TCP-probe internet (1.1.1.1:80, 8.8.8.8:53)
- If offline, show WiFi networks via `nmcli`, guide connection

**Phase 2: Configuration**
- Display current key values (masked if secret)
- Prompt for missing required keys
- Mark skipped optional keys with `__SKIPPED__` sentinel

**Phase 3: Live Verification**
- Redis PING
- Anthropic/DeepSeek/Notion API calls
- Saves `.env` preserving comments and order

#### `middleware.rs`
- `require_api_key` Axum middleware
- Compares `x-api-key` header against `PCW_API_KEY`
- Returns 401 if missing or wrong

#### `routes.rs`
17 routes under `/api/v1/` (all authenticated), plus:
- `GET /health` (unauthenticated) — Redis ping + version
- `.fallback(handler_404)` → JSON `{"ok":false,"error":"not found"}`

#### `handlers/mod.rs`
- `map_err()` translates `PcwError` to HTTP status:
  - 404 for not-found variants (session, artifact, workflow, workspace)
  - 400 for invalid input / invalid transition
  - 403 for `BatonDropped` *(planned — requires baton implementation)*
  - 500 for everything else

## API Reference

| Method | Path | What it does |
|--------|------|-------------|
| GET | `/health` | Redis ping + version |
| POST | `/api/v1/workspaces` | Create workspace |
| POST | `/api/v1/sessions` | Create session |
| GET | `/api/v1/sessions/:id` | Get session |
| POST | `/api/v1/sessions/:id/close` | Close session |
| POST | `/api/v1/sessions/:id/agent` | Call agent (full lifecycle) |
| POST | `/api/v1/artifacts` | Create artifact |
| GET | `/api/v1/artifacts/:id` | Get artifact |
| POST | `/api/v1/artifacts/:id/versions` | Create new version |
| GET | `/api/v1/artifacts/:id/versions` | List version IDs |
| GET | `/api/v1/sessions/:id/artifacts` | List session artifacts (current versions) |
| POST | `/api/v1/workflows` | Start workflow |
| GET | `/api/v1/workflows/:id` | Get workflow state |
| GET | `/api/v1/workflow-definitions` | List built-in templates |
| GET | `/api/v1/sessions/:id/deltashots/count` | Shot count |
| POST | `/api/v1/sessions/:id/replay` | Replay state to sequence N |
| GET | `/api/v1/sessions/:id/rollback-points` | All shots as rollback points |
| POST | `/api/v1/sessions/:id/fork` | Fork session at sequence N |

## The Crates

1. **pcw_core** — Foundation (config, errors, models)
2. **infra** — Shared infrastructure (Redis, logging, metrics, Notion)
3. **deltashots** — Event sourcing engine (diff, encryption, hash, store, engine, replay)
4. **workflows** — Task orchestration (definitions, engine, executor, worker)
5. **runtime** — Session coordinator (orchestrator, scheduler)
6. **audit** — Immutable audit log (log, report)
7. **timeline** — Session branching (branch, graph)
8. **api** — HTTP server (startup, middleware, routes, handlers) — binary: `pcw-server`
9. **agents** — Multi-LLM router (claude, deepseek, router)
10. **intelligence** — Task analysis (analyzer, scoring)
11. **artifacts** — Document store (service, versions)
12. **pcw-cli** — Command-line interface — binary: `pcw`

## Redis Layout

For one artifact chain with 3 versions:

```
pcw:artifact:{root_id}           → v1 JSON  (no TTL, permanent anchor)
pcw:artifact:{v2_id}             → v2 JSON  (604800s TTL — superseded)
pcw:artifact:{v3_id}             → v3 JSON  (no TTL — current latest)
pcw:artifact:{root_id}:latest    → v3_id
pcw:artifact:{root_id}:versions  → [root_id, v2_id, v3_id]
pcw:artifact:{root_id}:baton     → {session_id}
pcw:session:{sid}:artifacts      → {root_id}  (one entry, never grows)
```

## Architectural Patterns

### Event Sourcing
No session state is mutated in place. Every change is a DeltaShot: an encrypted, signed, hash-chained diff stored in a Redis Stream. State at any point is reconstructed by replaying from the beginning.

### Cryptographic Chain Integrity
`content_hash = SHA-256(prev_hash || encrypted_diff)`. Each shot's hash includes the previous, forming a tamper-evident chain. Any modification to any historical shot is detectable on replay.

### Sliding TTL Keys
Encryption keys for active sessions never expire mid-use. Every read (append or replay) calls `EXPIRE` to slide the TTL window forward.

### Root-Anchored Artifact Versioning
The session set holds only root IDs. A latest pointer advances on every version. Superseded snapshots auto-expire in 7 days. The root itself never expires — it's the permanent metadata anchor.

### Baton Pass Ownership
An artifact chain is exclusively bound to the session that created it. A foreign session attempting a write triggers an immediate, permanent, irrevocable seal. Even the original owner is locked out after a drop. Chain-navigation keys get 1-hour ephemeral TTLs; the deny set membership is permanent.

### Workflow State Machine with Dead-Letter
Steps are queued to a Redis Stream consumer group. The background worker processes them with exponential-backoff retry. Exhausted steps are moved to a dead-letter stream, never silently lost.

## Getting Started

### Prerequisites

- Rust 1.85 or later (edition 2024 required by transitive dependencies)
- Cargo
- Redis (for event store and session management)
- API keys for Claude and/or DeepSeek (optional — server starts in degraded mode without them)

### Installation

Clone the repository:

```bash
git clone https://github.com/anuceo/PCWSpace.git
cd PCWSpace
```

Build the project:

```bash
cargo build --release
```

### Running

Run the server (interactive startup wizard):

```bash
cargo run -p api --release
```

Or use the CLI:

```bash
cargo run -p pcw-cli -- health
```

The startup wizard will:
1. Check internet connectivity
2. Validate/collect required API keys
3. Verify Redis and LLM connections
4. Start the server on configured host/port

### Configuration

All configuration via environment variables (see `pcw_core::config`):

| Variable | Description | Default |
|----------|-------------|---------|
| `REDIS_URL` | Redis connection string | `redis://127.0.0.1:6379` |
| `ANTHROPIC_API_KEY` | Claude API key | (empty) |
| `CLAUDE_MODEL` | Claude model to use | `claude-sonnet-4-6` |
| `DEEPSEEK_API_KEY` | DeepSeek API key (optional) | (empty) |
| `DEEPSEEK_BASE_URL` | DeepSeek API endpoint | `https://api.deepseek.com` |
| `DEEPSEEK_MODEL` | DeepSeek model to use | `deepseek-chat` |
| `NOTION_TOKEN` | Notion integration token (optional) | (empty) |
| `NOTION_DATABASE_ID` | Notion database for sessions (optional) | (empty) |
| `NOTION_ARTIFACTS_DATABASE_ID` | Notion database for artifacts (optional) | (empty) |
| `PCW_API_KEY` | API key for authenticating requests | `dev-insecure` |
| `PCW_HOST` | Server bind host | `0.0.0.0` |
| `PCW_PORT` | Server bind port | `8000` |
| `PCW_LOG_LEVEL` | Log level (trace/debug/info/warn/error) | `info` |
| `SESSION_KEY_TTL_SECS` | Encryption key TTL in seconds | `86400` |

## Development

### Building Documentation

```bash
cargo doc --open
```

### Running Tests

```bash
cargo test --verbose
```

### Linting

```bash
cargo clippy --workspace --all-targets
```

### Key Files to Know

- `crates/core/src/config.rs` — Configuration singleton
- `crates/core/src/errors.rs` — Error types
- `crates/deltashots/src/engine.rs` — DeltaShot append pipeline
- `crates/deltashots/src/replay.rs` — State reconstruction
- `crates/api/src/handlers/` — Request handlers
- `crates/runtime/src/orchestrator.rs` — Full agent call lifecycle

### Web Dashboard

```bash
cd web && npm install && npm run dev
```

Opens on `http://localhost:3000` with proxy to the API server on port 8000.

### CLI

```bash
cargo build -p pcw-cli
./target/debug/pcw --help
```

## Contributing

Contributions welcome! Areas of interest:

- Additional LLM integrations
- Baton ownership enforcement (session-bound artifact access control)
- Workflow template library
- Audit report generators
- Performance optimizations
- Test coverage expansion

Please submit issues and pull requests on GitHub.

## License

MIT

## Author

**anuceo** — [GitHub Profile](https://github.com/anuceo)

## Support

For issues, questions, or suggestions, open an issue on the [GitHub repository](https://github.com/anuceo/PCWSpace/issues).
