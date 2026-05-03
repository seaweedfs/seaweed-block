#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

exec "$ROOT/scripts/run-g15d-k8s-dynamic.sh" "$ROOT"
