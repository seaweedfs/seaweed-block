#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase103-nvme-multihost-roce-preflight-gate}"
SUMMARY="${ARTIFACT_DIR}/phase103-nvme-multihost-roce-preflight-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

bool() {
  if "$@" >/dev/null 2>&1; then
    echo "true"
  else
    echo "false"
  fi
}

module_loaded() {
	local mod="$1"
	local sys_mod="${mod//-/_}"
	[[ -d "/sys/module/${sys_mod}" ]] || grep -q "^${sys_mod} " /proc/modules 2>/dev/null
}

module_available() {
	local mod="$1"
	local sys_mod="${mod//-/_}"
	if command -v modinfo >/dev/null 2>&1 && (modinfo "$mod" >/dev/null 2>&1 || modinfo "$sys_mod" >/dev/null 2>&1); then
		return 0
	fi
	local kernel
	kernel="$(uname -r 2>/dev/null || true)"
	[[ -n "$kernel" ]] && find "/lib/modules/${kernel}" \( -name "${mod}.ko*" -o -name "${sys_mod}.ko*" \) -print -quit 2>/dev/null | grep -q .
}

count_rdma_devices() {
	if [[ ! -d /sys/class/infiniband ]]; then
		echo 0
		return
	fi
	shopt -s nullglob
	local devices=(/sys/class/infiniband/*)
	echo "${#devices[@]}"
}

write_summary "phase103_nvme_multihost_roce_preflight_status=running"
write_summary "phase103_scope=nvme_tcp_multihost_and_roce_preflight"
write_summary "read_only=true"

nvme_present="$(bool command -v nvme)"
write_summary "nvme_cli_present=${nvme_present}"
if [[ "${nvme_present}" != "true" ]]; then
  write_summary "phase103_nvme_multihost_roce_preflight_status=blocked_missing_nvme_cli"
  exit 2
fi

if nvme list-subsys -o json >"${ARTIFACT_DIR}/nvme-list-subsys.json" 2>"${ARTIFACT_DIR}/nvme-list-subsys.err"; then
  write_summary "nvme_list_subsys_readable=true"
else
  write_summary "nvme_list_subsys_readable=false"
fi

for mod in nvme-fabrics nvme-tcp nvme-rdma; do
  shell_name="${mod//-/_}"
  if module_loaded "${mod}"; then
    write_summary "module_${shell_name}_loaded=true"
  else
    write_summary "module_${shell_name}_loaded=false"
  fi
  if module_available "${mod}"; then
    write_summary "module_${shell_name}_available=true"
  else
    write_summary "module_${shell_name}_available=false"
  fi
done

rdma_count="$(count_rdma_devices)"
write_summary "rdma_device_count=${rdma_count}"
if [[ "${rdma_count}" -gt 0 ]]; then
	( cd /sys/class/infiniband && ls -1 ) >"${ARTIFACT_DIR}/rdma-devices.txt" 2>/dev/null || true
else
	: >"${ARTIFACT_DIR}/rdma-devices.txt"
fi

nvme_tcp_ready=false
if module_loaded nvme-tcp || module_available nvme-tcp; then
  nvme_tcp_ready=true
fi
write_summary "nvme_tcp_preflight_ready=${nvme_tcp_ready}"

roce_preflight_candidate=false
roce_preflight_status="blocked_no_rdma_device"
if [[ "${rdma_count}" -gt 0 ]]; then
  if module_loaded nvme-rdma || module_available nvme-rdma; then
    roce_preflight_status="candidate_requires_live_roce_gate"
    roce_preflight_candidate=true
  else
    roce_preflight_status="blocked_missing_nvme_rdma_module"
  fi
fi
write_summary "roce_preflight_status=${roce_preflight_status}"
write_summary "roce_preflight_candidate=${roce_preflight_candidate}"
write_summary "roce_claim_allowed=false"
write_summary "roce_live_gate_required=true"
write_summary "roce_live_io_claim=false"
write_summary "performance_claim_allowed=false"

if [[ "${nvme_tcp_ready}" != "true" ]]; then
  write_summary "phase103_nvme_multihost_roce_preflight_status=blocked_missing_nvme_tcp_capability"
  exit 2
fi

write_summary "phase103_nvme_multihost_roce_preflight_status=ok"
