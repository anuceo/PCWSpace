#!/usr/bin/env bash
# Start the full PCW stack with podman-compose.
# Builds the image first if it doesn't exist.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "${SCRIPT_DIR}")"

cd "${ROOT_DIR}"

# Load .env if present (secrets, API keys)
if [[ -f ".env" ]]; then
    set -o allexport
    source .env
    set +o allexport
fi

echo "Starting PCW stack..."
podman-compose -f podman-compose.yml up -d --build

echo ""
echo "Stack status:"
podman-compose -f podman-compose.yml ps

echo ""
echo "API health:"
sleep 5
curl -sf http://localhost:8000/health | python3 -m json.tool || echo "(API not yet ready)"
