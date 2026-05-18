#!/usr/bin/env bash
set -euo pipefail

MIN_K8S_NODES=3
ENV_OUT=""
PLACEMENT_OUT=""
TOPOLOGY_OUT=""
TOPOLOGY_JSON_OUT=""

usage() {
  cat >&2 <<'EOF'
usage: bash scripts/preflight-node-loss-lab.sh [options]

Checks whether the current Kubernetes context is eligible for the Node-Loss
Survival MVP D3/D4 gates.

Exit codes:
  0  eligible; env/placement outputs were written when requested
  2  invalid arguments or missing required tools
  3  lab is reachable but not eligible for the requested node-loss gate

Options:
  --min-k8s-nodes N       Required Ready schedulable Kubernetes nodes (default: 3)
  --env-out PATH          Write NODE_SPECS / APP_NODE shell env file
  --placement-out PATH    Write human-readable placement evidence
  --topology-out PATH     Write `kubectl get nodes -o wide`
  --topology-json-out PATH Write `kubectl get nodes -o json`
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --min-k8s-nodes)
      MIN_K8S_NODES="${2:-}"
      shift 2
      ;;
    --env-out)
      ENV_OUT="${2:-}"
      shift 2
      ;;
    --placement-out)
      PLACEMENT_OUT="${2:-}"
      shift 2
      ;;
    --topology-out)
      TOPOLOGY_OUT="${2:-}"
      shift 2
      ;;
    --topology-json-out)
      TOPOLOGY_JSON_OUT="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      usage
      exit 2
      ;;
  esac
done

case "$MIN_K8S_NODES" in
  ''|*[!0-9]*|0)
    echo "min-k8s-nodes must be a positive integer, got: $MIN_K8S_NODES" >&2
    exit 2
    ;;
esac

if [[ -z "${KUBECONFIG:-}" && -f /etc/rancher/k3s/k3s.yaml ]]; then
  export KUBECONFIG=/etc/rancher/k3s/k3s.yaml
fi

require_cmd() {
  if ! command -v "$1" >/dev/null 2>&1; then
    echo "missing required command: $1" >&2
    exit 2
  fi
}

require_cmd kubectl
require_cmd python3

tmp_dir="$(mktemp -d /tmp/sw-block-node-loss-preflight.XXXXXX)"
cleanup() {
  rm -rf "$tmp_dir"
}
trap cleanup EXIT

wide_path="${TOPOLOGY_OUT:-$tmp_dir/nodes.wide.txt}"
json_path="${TOPOLOGY_JSON_OUT:-$tmp_dir/nodes.json}"
mkdir -p "$(dirname "$wide_path")" "$(dirname "$json_path")"

kubectl get nodes -o wide >"$wide_path"
kubectl get nodes -o json >"$json_path"

python3 - "$json_path" "$MIN_K8S_NODES" "${ENV_OUT:-}" "${PLACEMENT_OUT:-}" <<'PY'
import ipaddress
import json
import pathlib
import shlex
import sys

json_path = pathlib.Path(sys.argv[1])
minimum = int(sys.argv[2])
env_out = sys.argv[3]
placement_out = sys.argv[4]

data = json.loads(json_path.read_text())
selected = []
seen_nodes = set()

for item in data.get("items", []):
    name = item.get("metadata", {}).get("name", "")
    labels = item.get("metadata", {}).get("labels", {})
    spec = item.get("spec", {})
    if not name or spec.get("unschedulable"):
        continue
    ready = any(
        c.get("type") == "Ready" and c.get("status") == "True"
        for c in item.get("status", {}).get("conditions", [])
    )
    if not ready:
        continue
    addresses = item.get("status", {}).get("addresses", [])
    internal = next((a.get("address", "") for a in addresses if a.get("type") == "InternalIP"), "")
    try:
        ip = ipaddress.ip_address(internal)
    except ValueError:
        continue
    if ip.is_loopback or ip.is_unspecified:
        continue
    physical = (
        labels.get("sw-block.seaweedfs.com/physical-host")
        or labels.get("topology.kubernetes.io/zone")
        or labels.get("kubernetes.io/hostname")
        or name
    )
    if name in seen_nodes:
        continue
    seen_nodes.add(name)
    selected.append((name, internal, physical))

if len(selected) < minimum:
    ineligible_text = (
        "node_loss_topology_eligible=false\n"
        "node_loss_lab_eligible=false\n"
        f"reason=requires_{minimum}_ready_schedulable_nodes_with_non_loopback_internal_ip\n"
        f"found={len(selected)}\n"
        "kubernetes_node_loss_claimed=false\n"
        "physical_host_loss_claimed=false\n"
    )
    if placement_out:
        pathlib.Path(placement_out).parent.mkdir(parents=True, exist_ok=True)
        pathlib.Path(placement_out).write_text(ineligible_text)
    if env_out:
        try:
            pathlib.Path(env_out).unlink()
        except FileNotFoundError:
            pass
    print(ineligible_text, end="")
    print(
        f"node-loss lab preflight failed: requires {minimum} Ready schedulable nodes with non-loopback InternalIP; found {len(selected)}",
        file=sys.stderr,
    )
    raise SystemExit(3)

selected = selected[:minimum]
physical_domains = sorted({physical for _, _, physical in selected})
physical_shape = "full-physical-host" if len(physical_domains) >= minimum else "shared-physical-host"
specs = []
placement = []
import_nodes = []
for idx, (name, ip, physical) in enumerate(selected, 1):
    replica = f"r{idx}"
    server = f"node-loss-{replica}"
    pool = f"node-loss-pool-{idx}"
    specs.append(f"{server}|{name}|{ip}|{pool}")
    import_nodes.append(ip)
    placement.append(f"replica={replica} server={server} node={name} host={ip} physical_host={physical} pool={pool}")

env_text = (
    "NODE_SPECS=" + shlex.quote(";".join(specs)) + "\n"
    "K3S_IMPORT_NODES=" + shlex.quote(",".join(import_nodes)) + "\n"
    "APP_NODE=" + shlex.quote(selected[0][0]) + "\n"
    "SURVIVOR_APP_NODE=" + shlex.quote(selected[1][0] if len(selected) > 1 else selected[0][0]) + "\n"
    "PHYSICAL_DOMAIN_COUNT=" + shlex.quote(str(len(physical_domains))) + "\n"
    "PHYSICAL_DOMAIN_SHAPE=" + shlex.quote(physical_shape) + "\n"
    "PHYSICAL_HOST_LOSS_CLAIMED=false\n"
)
placement_text = (
    "node_loss_topology_eligible=true\n"
    "node_loss_lab_eligible=true\n"
    f"selected_node_count={len(selected)}\n"
    f"app_node={selected[0][0]}\n"
    f"survivor_app_node={selected[1][0] if len(selected) > 1 else selected[0][0]}\n"
    f"k3s_import_nodes={','.join(import_nodes)}\n"
    f"physical_domain_count={len(physical_domains)}\n"
    f"physical_domain_shape={physical_shape}\n"
    "kubernetes_node_loss_claimed=true\n"
    "physical_host_loss_claimed=false\n"
    + "\n".join(placement)
    + "\n"
)

if env_out:
    pathlib.Path(env_out).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(env_out).write_text(env_text)
if placement_out:
    pathlib.Path(placement_out).parent.mkdir(parents=True, exist_ok=True)
    pathlib.Path(placement_out).write_text(placement_text)

print(placement_text, end="")
PY
