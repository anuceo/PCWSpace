#!/usr/bin/env bash
# Build the PCW container image using Podman.
# Usage: ./deploy/podman-build.sh [tag]
set -euo pipefail

TAG="${1:-latest}"
IMAGE="localhost/pcw:${TAG}"

echo "Building PCW image: ${IMAGE}"
podman build \
    --file Containerfile \
    --tag "${IMAGE}" \
    --layers \
    .

echo "Image built: ${IMAGE}"
podman image inspect "${IMAGE}" --format "Size: {{.Size}} bytes"
