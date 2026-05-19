#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

cd "${ROOT_DIR}"

echo "Installing stable Rust toolchain (if needed)..."
rustup toolchain install stable
rustup default stable
rustup component add rustfmt clippy

echo "Fetching workspace dependencies..."
cargo fetch --locked || cargo fetch

if [[ "${DELTA_AGENT_PREWARM:-1}" == "1" ]]; then
  echo "Prewarming build artifacts (check + test --no-run)..."
  cargo check --workspace
  cargo test --workspace --no-run
fi

echo "Environment setup complete."
