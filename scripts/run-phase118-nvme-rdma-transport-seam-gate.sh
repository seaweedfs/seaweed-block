#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase118-nvme-rdma-transport-seam-gate}"
SUMMARY="${ARTIFACT_DIR}/phase118-nvme-rdma-transport-seam-summary.txt"

mkdir -p "${ARTIFACT_DIR}"/{go,cli}
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

write_summary "phase118_nvme_rdma_transport_seam_status=running"

cd "${ROOT}"

go test ./core/frontend/nvme ./cmd/blockvolume \
  >"${ARTIFACT_DIR}/go/test.stdout.txt" 2>"${ARTIFACT_DIR}/go/test.stderr.txt"
write_summary "go_test_nvme_blockvolume=ok"

if go run ./cmd/blockvolume \
  --master 127.0.0.1:1 \
  --server-id phase118-rdma-refusal \
  --volume-id phase118-v1 \
  --replica-id r1 \
  --data-addr 127.0.0.1:0 \
  --ctrl-addr 127.0.0.1:0 \
  --nvme-listen 127.0.0.1:0 \
  --nvme-subsysnqn nqn.2026-07.io.seaweedfs.phase118:rdma-refusal \
  --nvme-transport rdma \
  >"${ARTIFACT_DIR}/cli/blockvolume-rdma.stdout.txt" \
  2>"${ARTIFACT_DIR}/cli/blockvolume-rdma.stderr.txt"; then
  write_summary "blockvolume_rdma_public_refusal=false"
  write_summary "phase118_nvme_rdma_transport_seam_status=failed"
  exit 1
fi

grep -q -- '--nvme-transport="rdma" unsupported; only "tcp" is implemented' \
  "${ARTIFACT_DIR}/cli/blockvolume-rdma.stderr.txt"

write_summary "target_transport_seam_present=true"
write_summary "rdma_target_error_typed=true"
write_summary "blockvolume_rdma_public_refusal=true"
write_summary "rdma_listener_implemented=false"
write_summary "roce_claim_allowed=false"
write_summary "phase118_nvme_rdma_transport_seam_status=ok"

echo "[phase118] PASS"
