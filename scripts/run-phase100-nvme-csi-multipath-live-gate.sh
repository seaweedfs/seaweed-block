#!/usr/bin/env bash
set -euo pipefail

PRODUCT_ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${PRODUCT_ROOT}/results/phase100-nvme-csi-multipath-live-gate}"
SUMMARY="${ARTIFACT_DIR}/phase100-nvme-csi-multipath-live-summary.txt"
LIVE_DIR="${ARTIFACT_DIR}/live"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

count_pattern() {
  local path="$1"
  local pattern="$2"
  grep -Ec -- "$pattern" "$path" 2>/dev/null || true
}

write_summary "phase100_nvme_csi_multipath_live_status=running"
write_summary "phase100_scope=live_k8s_dynamic_pvc_nvme_multipath_attach"
write_summary "replication_factor=2"
write_summary "same_node_logical_replicas=2"

if ! (
  cd "${PRODUCT_ROOT}"
  SW_BLOCK_FRONTEND_PROTOCOL=nvme \
  SW_BLOCK_STAGE2_MULTIPATH=1 \
  SW_BLOCK_DYNAMIC_REPLICATION_FACTOR=2 \
  SW_BLOCK_SAME_NODE_LOGICAL_REPLICAS=2 \
  SW_BLOCK_LAUNCHER_PVC_OWNER_REF=1 \
  SW_BLOCK_RUN_LABEL=phase100-nvme-multipath \
  SW_BLOCK_ARTIFACT_DIR="${LIVE_DIR}" \
  bash scripts/run-alpha-k8s-dynamic.sh "${PRODUCT_ROOT}"
); then
  write_summary "phase100_nvme_csi_multipath_live_status=failed"
  exit 1
fi

write_summary "dynamic_pvc_writer_reader=pass"

GENERATED="${LIVE_DIR}/generated-blockvolume.yaml"
CSI_NODE_LOG="${LIVE_DIR}/blockcsi-node.log"
RUN_LOG="${LIVE_DIR}/run.log"
NVME_AFTER="${LIVE_DIR}/nvme-list-subsys.after-delete.json"

nvme_listen_count="$(count_pattern "${GENERATED}" "--nvme-listen=")"
nqn_unique_count="$(grep -Eo -- '--nvme-subsysnqn=[^", ]+' "${GENERATED}" 2>/dev/null | sort -u | wc -l | tr -d ' ')"
nsid_unique_count="$(grep -Eo -- '--nvme-ns=[0-9]+' "${GENERATED}" 2>/dev/null | sort -u | wc -l | tr -d ' ')"
iscsi_arg_count="$(count_pattern "${GENERATED}" "--iscsi-listen=")"
node_stage_multipath_count="$(count_pattern "${CSI_NODE_LOG}" "transport=nvme.*multipath=true|multipath=true.*transport=nvme")"
node_stage_two_portals_count="$(count_pattern "${CSI_NODE_LOG}" "portals=[^[:space:]]*,[^[:space:]]*")"
run_pass_count="$(count_pattern "${RUN_LOG}" "PASS: dynamic PVC create/delete completed checksum write/read and cleanup")"
nvme_residue_count="$(count_pattern "${NVME_AFTER}" "nqn\\.2026-05\\.io\\.seaweedfs")"

write_summary "generated_nvme_listen_count=${nvme_listen_count}"
write_summary "generated_nqn_unique_count=${nqn_unique_count}"
write_summary "generated_nsid_unique_count=${nsid_unique_count}"
write_summary "generated_iscsi_arg_count=${iscsi_arg_count}"
write_summary "node_stage_nvme_multipath_count=${node_stage_multipath_count}"
write_summary "node_stage_two_portals_count=${node_stage_two_portals_count}"
write_summary "run_pass_count=${run_pass_count}"
write_summary "nvme_residue_count=${nvme_residue_count}"

if [[ "${nvme_listen_count}" -lt 2 ]]; then
  echo "expected at least two generated NVMe listen paths, got ${nvme_listen_count}" >&2
  exit 1
fi
if [[ "${nqn_unique_count}" != "1" || "${nsid_unique_count}" != "1" ]]; then
  echo "expected one shared NQN/NSID across generated NVMe paths, got nqn=${nqn_unique_count} nsid=${nsid_unique_count}" >&2
  exit 1
fi
if [[ "${iscsi_arg_count}" != "0" ]]; then
  echo "NVMe gate rendered iSCSI args" >&2
  exit 1
fi
if [[ "${node_stage_multipath_count}" -lt 1 || "${node_stage_two_portals_count}" -lt 1 ]]; then
  echo "CSI NodeStage did not report NVMe multipath with two portals" >&2
  exit 1
fi
if [[ "${run_pass_count}" -lt 1 ]]; then
  echo "dynamic PVC run did not report PASS" >&2
  exit 1
fi
if [[ "${nvme_residue_count}" != "0" ]]; then
  echo "dangling Seaweed Block NVMe subsystem after delete" >&2
  exit 1
fi

write_summary "phase100_nvme_csi_multipath_live_status=ok"
