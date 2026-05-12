#!/usr/bin/env bash
set -uo pipefail

MODE="local-k3s"
if [[ "${1:-}" == "--ghcr" ]]; then
  MODE="ghcr"
elif [[ "${1:-}" == "--local-k3s" || -z "${1:-}" ]]; then
  MODE="local-k3s"
else
  echo "usage: bash scripts/preflight-k8s-alpha.sh [--local-k3s|--ghcr]" >&2
  exit 2
fi

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

CHECKED=0
FAILED=0
UNCHECKED=0

pass() {
  CHECKED=$((CHECKED + 1))
  printf '[preflight] checked name=%s status=PASS detail="%s"\n' "$1" "$2"
}

fail() {
  CHECKED=$((CHECKED + 1))
  FAILED=$((FAILED + 1))
  printf '[preflight] checked name=%s status=FAIL remediation="%s"\n' "$1" "$2"
}

unchecked() {
  UNCHECKED=$((UNCHECKED + 1))
  printf '[preflight] unchecked name=%s reason="%s"\n' "$1" "$2"
}

check_cmd() {
  local name="$1"
  local remediation="$2"
  if command -v "$name" >/dev/null 2>&1; then
    pass "$name" "$(command -v "$name")"
  else
    fail "$name" "$remediation"
  fi
}

check_cmd bash "Install bash and re-run the quickstart."
check_cmd kubectl "Install kubectl and configure it for your test cluster."
check_cmd iscsiadm "Install open-iscsi/iscsiadm on the Kubernetes node."

if kubectl version --client=true >/tmp/sw-block-preflight-kubectl-version.txt 2>&1; then
  pass kubectl_client "$(head -n 1 /tmp/sw-block-preflight-kubectl-version.txt)"
else
  fail kubectl_client "kubectl client command failed; reinstall kubectl."
fi

if kubectl get nodes -o wide >/tmp/sw-block-preflight-nodes.txt 2>&1; then
  pass kubernetes_nodes "$(grep -m1 -E ' Ready |STATUS' /tmp/sw-block-preflight-nodes.txt | tr -s ' ')"
else
  fail kubernetes_nodes "kubectl cannot list nodes; check KUBECONFIG and cluster access."
fi

if [[ "$MODE" == "local-k3s" ]]; then
  check_cmd docker "Install Docker or use --ghcr with published images."
  check_cmd sudo "Passwordless sudo is required for k3s image import in the local path."
  if command -v sudo >/dev/null 2>&1 && sudo -n k3s ctr images ls >/tmp/sw-block-preflight-k3s-images.txt 2>&1; then
    pass k3s_ctr_images "k3s containerd image list accessible"
  else
    fail k3s_ctr_images "Enable passwordless sudo for k3s ctr image import, or use a registry/GHCR image path."
  fi
  unchecked ghcr_pull "local-k3s path selected"
else
  unchecked docker "GHCR path selected"
  unchecked k3s_ctr_images "GHCR path selected"
fi

if [[ "$FAILED" -eq 0 ]]; then
  printf '[preflight] summary status=PASS checked=%d failed=%d unchecked=%d mode=%s\n' "$CHECKED" "$FAILED" "$UNCHECKED" "$MODE"
  exit 0
fi

printf '[preflight] summary status=FAIL checked=%d failed=%d unchecked=%d mode=%s\n' "$CHECKED" "$FAILED" "$UNCHECKED" "$MODE"
exit 2
