#!/usr/bin/env python3
"""Verify api_v1 runtime routes are represented in OpenAPI paths."""

from __future__ import annotations

import json
import re
import sys
from pathlib import Path


HTTP_METHODS = {"get", "post", "put", "patch", "delete", "head", "options"}


def normalize_runtime_path(path: str) -> str:
    normalized = path.strip()
    if not normalized.startswith("/"):
        normalized = f"/{normalized}"
    if normalized != "/" and normalized.endswith("/"):
        normalized = normalized[:-1]
    if not normalized.startswith("/api/v1"):
        normalized = f"/api/v1{normalized}"
    return normalized


def load_runtime_routes(routes_file: Path) -> set[tuple[str, str]]:
    content = routes_file.read_text(encoding="utf-8")
    pattern = re.compile(r'\.route\(\s*"([^"]+)"\s*,\s*([a-z_]+)\s*\(', re.MULTILINE)
    routes: set[tuple[str, str]] = set()
    for raw_path, raw_method in pattern.findall(content):
        method = raw_method.lower()
        if method not in HTTP_METHODS:
            continue
        routes.add((method, normalize_runtime_path(raw_path)))
    return routes


def load_openapi_routes(spec_file: Path) -> set[tuple[str, str]]:
    spec = json.loads(spec_file.read_text(encoding="utf-8"))
    routes: set[tuple[str, str]] = set()
    for raw_path, path_item in spec.get("paths", {}).items():
        if not isinstance(path_item, dict):
            continue
        path = normalize_runtime_path(raw_path)
        for method in HTTP_METHODS:
            if method in path_item:
                routes.add((method, path))
    return routes


def main() -> int:
    repo_root = Path(__file__).resolve().parent.parent
    routes_file = repo_root / "crates" / "api" / "src" / "routes" / "api_v1.rs"
    spec_file = repo_root / "docs" / "openapi.v1.json"

    runtime_routes = load_runtime_routes(routes_file)
    openapi_routes = load_openapi_routes(spec_file)

    missing_in_spec = sorted(runtime_routes - openapi_routes)
    extra_in_spec = sorted(openapi_routes - runtime_routes)

    if not missing_in_spec and not extra_in_spec:
        print("OpenAPI sync check passed: runtime routes and spec paths are aligned.")
        return 0

    print("OpenAPI sync check failed.")
    if missing_in_spec:
        print("\nRoutes present at runtime but missing from OpenAPI:")
        for method, path in missing_in_spec:
            print(f"  - {method.upper()} {path}")
    if extra_in_spec:
        print("\nRoutes present in OpenAPI but missing from runtime router:")
        for method, path in extra_in_spec:
            print(f"  - {method.upper()} {path}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
