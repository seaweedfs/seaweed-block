#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
VALUES_FILE="${SW_BLOCK_HELM_VALUES_FILE:-}"
OUT_DIR="${SW_BLOCK_ARTIFACT_DIR:-"$ROOT/tmp/helm-release-hygiene"}"
RELEASE_NAME="${SW_BLOCK_HELM_RELEASE:-sw-block}"
NAMESPACE="${SW_BLOCK_HELM_NAMESPACE:-kube-system}"
CHART_DIR="$ROOT/charts/seaweed-block"

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

chart_field() {
  local field="$1"
  awk -F: -v key="$field" '$1 == key {gsub(/^[ \t"]+|[ \t"]+$/, "", $2); print $2; exit}' "$CHART_DIR/Chart.yaml"
}

require_cmd helm
require_cmd awk
require_cmd grep

mkdir -p "$OUT_DIR"

if [[ ! -f "$CHART_DIR/Chart.yaml" ]]; then
  echo "helm_hygiene_status=failed"
  echo "failed_phase=chart_missing"
  exit 1
fi

chart_name="$(chart_field name)"
chart_version="$(chart_field version)"
chart_app_version="$(chart_field appVersion)"

if [[ -z "$chart_name" || -z "$chart_version" || -z "$chart_app_version" ]]; then
  {
    echo "helm_hygiene_status=failed"
    echo "failed_phase=chart_metadata"
    echo "chart_name=$chart_name"
    echo "chart_version=$chart_version"
    echo "chart_app_version=$chart_app_version"
  } | tee "$OUT_DIR/helm-release-hygiene-summary.txt"
  exit 1
fi

helm_args=()
if [[ -n "$VALUES_FILE" ]]; then
  if [[ ! -f "$VALUES_FILE" ]]; then
    {
      echo "helm_hygiene_status=failed"
      echo "failed_phase=values_missing"
      echo "values_file=$VALUES_FILE"
    } | tee "$OUT_DIR/helm-release-hygiene-summary.txt"
    exit 1
  fi
  helm_args=(-f "$VALUES_FILE")
fi

helm lint "$CHART_DIR" "${helm_args[@]}" >"$OUT_DIR/helm-lint.txt"
helm template "$RELEASE_NAME" "$CHART_DIR" \
  --namespace "$NAMESPACE" \
  "${helm_args[@]}" >"$OUT_DIR/helm-template.yaml"
helm package "$CHART_DIR" --destination "$OUT_DIR" >"$OUT_DIR/helm-package.txt"

package_file="$OUT_DIR/$chart_name-$chart_version.tgz"
if [[ ! -s "$package_file" ]]; then
  {
    echo "helm_hygiene_status=failed"
    echo "failed_phase=package_missing"
    echo "package_file=$package_file"
  } | tee "$OUT_DIR/helm-release-hygiene-summary.txt"
  exit 1
fi

rendered_storageclass_count="$(grep -c '^kind: StorageClass$' "$OUT_DIR/helm-template.yaml" || true)"
rendered_csidriver_count="$(grep -c '^kind: CSIDriver$' "$OUT_DIR/helm-template.yaml" || true)"
rendered_master_count="$(grep -c 'name: sw-blockmaster' "$OUT_DIR/helm-template.yaml" || true)"

{
  echo "helm_hygiene_status=ok"
  echo "chart_name=$chart_name"
  echo "chart_version=$chart_version"
  echo "chart_app_version=$chart_app_version"
  echo "values_file=${VALUES_FILE:-default}"
  echo "lint=helm-lint.txt"
  echo "template=helm-template.yaml"
  echo "package=$(basename "$package_file")"
  echo "rendered_storageclass_count=$rendered_storageclass_count"
  echo "rendered_csidriver_count=$rendered_csidriver_count"
  echo "rendered_master_count=$rendered_master_count"
} | tee "$OUT_DIR/helm-release-hygiene-summary.txt"
