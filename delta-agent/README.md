# Delta Agent Workspace

A production-oriented Rust workspace layout for DeltaShot, VDDAB, Penpal/Lattice, streaming, trace viewing, priority scheduling, compression, and hashing.

## Environment setup

```bash
cd delta-agent
./scripts/setup.sh
```

This validates:

- Rust toolchain presence (uses `rust-toolchain.toml`)
- formatting (`cargo fmt --all --check`)
- compile health (`cargo check --workspace`)
- tests (`cargo test --workspace`)

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

- `GET /sessions/:id`
- `GET /branches/:id`
- `GET /streaming/ping`

## Run worker

```bash
cd delta-agent
./scripts/run-worker.sh
```

Overrides:

- `DELTA_AGENT_CONFIG` (default: `configs/default.toml`)
- `RUST_LOG` (default: `worker=info`)
