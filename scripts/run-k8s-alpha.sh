#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"

exec bash "$ROOT/scripts/run-alpha-k8s-dynamic.sh" "$ROOT"
