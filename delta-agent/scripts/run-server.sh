#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

export DELTA_AGENT_CONFIG="${DELTA_AGENT_CONFIG:-${ROOT_DIR}/configs/default.toml}"

cd "${ROOT_DIR}"
cargo run -p server
