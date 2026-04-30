#!/usr/bin/env bash
# Install Quadlet unit files for rootless Podman + systemd production deployment.
# Run as the target user (not root).
set -euo pipefail

QUADLET_DIR="${HOME}/.config/containers/systemd"
QUADLET_SRC="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/.quadlets"
CONFIG_DIR="${HOME}/.config/pcw"
BIN_DIR="${HOME}/.local/bin"

echo "Installing PCW Quadlet units to ${QUADLET_DIR}"
mkdir -p "${QUADLET_DIR}" "${CONFIG_DIR}" "${BIN_DIR}"

# Copy all quadlet unit files
cp -v "${QUADLET_SRC}"/*.container "${QUADLET_DIR}/"
cp -v "${QUADLET_SRC}"/*.network   "${QUADLET_DIR}/"
cp -v "${QUADLET_SRC}"/*.volume    "${QUADLET_DIR}/"

# Copy Storm config
cp -v "$(dirname "${BASH_SOURCE[0]}")/storm.yaml" "${CONFIG_DIR}/"

# Install the storm worker binary
BINARY="$(dirname "${BASH_SOURCE[0]}")/../target/release/pcw-storm-worker"
if [[ -f "${BINARY}" ]]; then
    cp -v "${BINARY}" "${BIN_DIR}/pcw-storm-worker"
    chmod +x "${BIN_DIR}/pcw-storm-worker"
else
    echo "WARNING: ${BINARY} not found — run 'cargo build --release' first"
fi

echo ""
echo "Reloading systemd user daemon..."
systemctl --user daemon-reload

echo ""
echo "Installed units:"
systemctl --user list-unit-files 'pcw-*' 2>/dev/null || true

echo ""
echo "Enable and start with:"
echo "  systemctl --user enable --now pcw-redis pcw-zookeeper pcw-storm-nimbus pcw-storm-supervisor pcw-api"
