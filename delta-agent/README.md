# Delta Agent Workspace

A production-oriented Rust workspace layout for DeltaShot, VDDAB, Penpal/Lattice, streaming, trace viewing, priority scheduling, compression, and hashing.

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
- `apps/worker`: background scheduler/replay worker
- `configs/default.toml`: baseline runtime configuration
- `scripts/dev.sh`: local development helper

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

- `GET /sessions/{id}`
- `GET /branches/{id}`
- `GET /streaming/ping`

## Run worker

```bash
cd delta-agent
./scripts/run-worker.sh
```

Overrides:

- `DELTA_AGENT_CONFIG` (default: `configs/default.toml`)
- `RUST_LOG` (default: `worker=info`)

## Cloud agent environment

The repository root contains `.cursor/environment.json` to speed up and
stabilize cloud-agent runs for this workspace:

- uses stable Rust with `rustfmt` and `clippy`
- prewarms `cargo check --workspace` + `cargo test --workspace --no-run`
- persists Cargo/Rust caches and a shared Cargo target directory
