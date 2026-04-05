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

echo "Environment setup complete."
