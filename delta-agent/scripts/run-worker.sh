#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

CONFIG_PATH="${DELTA_AGENT_CONFIG:-${ROOT_DIR}/configs/default.toml}"
export DELTA_AGENT_CONFIG="${CONFIG_PATH}"
export RUST_LOG="${RUST_LOG:-worker=info}"

cd "${ROOT_DIR}"
cargo run -p worker
