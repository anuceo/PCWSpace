# Delta Agent Workspace

A production-oriented Rust workspace layout for DeltaShot, VDDAB, Penpal/Lattice, streaming, trace viewing, priority scheduling, compression, and hashing.

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
