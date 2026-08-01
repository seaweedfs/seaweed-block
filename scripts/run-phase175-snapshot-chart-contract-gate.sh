#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase175-snapshot-chart-contract-gate}"
SUMMARY="${ARTIFACT_DIR}/phase175-snapshot-chart-contract-summary.txt"
CHART="${ROOT}/charts/seaweed-block"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

require_rendered() {
  local pattern="$1"
  if ! grep -Fq -- "${pattern}" "${ARTIFACT_DIR}/helm-snapshot.yaml"; then
    echo "snapshot render missing: ${pattern}" >&2
    exit 1
  fi
}

write_summary "phase175_snapshot_chart_contract_status=running"

helm lint "${CHART}" >"${ARTIFACT_DIR}/helm-lint.log"
helm template sw-block "${CHART}" --namespace kube-system >"${ARTIFACT_DIR}/helm-default.yaml"
if grep -Eq 'csi-snapshotter|kind: VolumeSnapshotClass|--snapshot-api=' "${ARTIFACT_DIR}/helm-default.yaml"; then
  echo "default render exposed snapshot resources" >&2
  exit 1
fi
write_summary "snapshot_default_disabled=true"

if helm template sw-block "${CHART}" --namespace kube-system \
  --set snapshot.enabled=true >"${ARTIFACT_DIR}/helm-invalid.yaml" 2>"${ARTIFACT_DIR}/helm-invalid.err"; then
  echo "snapshot render accepted missing durable/identity prerequisites" >&2
  exit 1
fi
write_summary "snapshot_incomplete_config_rejected=true"

if helm template sw-block "${CHART}" --namespace kube-system \
  --set snapshot.backup.enabled=true >"${ARTIFACT_DIR}/helm-invalid-backup.yaml" 2>"${ARTIFACT_DIR}/helm-invalid-backup.err"; then
  echo "snapshot backup render accepted a disabled snapshot runtime" >&2
  exit 1
fi
write_summary "snapshot_backup_without_runtime_rejected=true"

if helm template sw-block "${CHART}" --namespace kube-system \
  --set snapshot.enabled=true \
  --set snapshot.runtimeSecretName=sw-block-snapshot \
  --set blockmaster.stateHostPath=/var/lib/sw-block \
  --set 'blockmaster.nodeSelector.kubernetes\.io/hostname=m02' \
  >"${ARTIFACT_DIR}/helm-loopback.yaml" 2>"${ARTIFACT_DIR}/helm-loopback.err"; then
  echo "snapshot render accepted a loopback runtime address" >&2
  exit 1
fi
if ! grep -Fq 'requires blockNodes[m02] to have a non-loopback frontendIP or internalIP' "${ARTIFACT_DIR}/helm-loopback.err"; then
  echo "snapshot loopback rejection did not explain the invalid node address" >&2
  exit 1
fi
write_summary "snapshot_loopback_config_rejected=true"

helm template sw-block "${CHART}" --namespace kube-system \
  --set snapshot.enabled=true \
  --set snapshot.runtimeSecretName=sw-block-snapshot \
  --set snapshot.backup.enabled=true \
  --set blockmaster.stateHostPath=/var/lib/sw-block \
  --set 'blockmaster.nodeSelector.kubernetes\.io/hostname=m02' \
  --set blockNodes[0].name=m02 \
  --set blockNodes[0].internalIP=192.168.1.184 \
  >"${ARTIFACT_DIR}/helm-snapshot.yaml"

require_rendered 'registry.k8s.io/sig-storage/csi-snapshotter:v8.5.0'
require_rendered '--snapshot-api=blockmaster.kube-system.svc.cluster.local:9444'
require_rendered 'api-server-ca.crt'
require_rendered 'api-client.crt'
require_rendered 'api-client.key'
require_rendered 'volumesnapshotclasses'
require_rendered 'volumesnapshotcontents/status'
require_rendered 'kind: VolumeSnapshotClass'
require_rendered 'name: sw-block-snapshot'
require_rendered 'driver: block.csi.seaweedfs.com'
require_rendered 'deletionPolicy: Delete'
require_rendered '--snapshot-backup-root=/var/lib/sw-block/backups'
require_rendered '--snapshot-backup-api-token-file=/var/run/sw-block/snapshot-runtime/backup-api-token'
require_rendered 'key: backup-api-token'
if [[ "$(grep -Fc -- 'key: backup-api-token' "${ARTIFACT_DIR}/helm-snapshot.yaml")" -ne 1 ]]; then
  echo "backup API token must be projected exactly once to blockmaster and never to CSI" >&2
  exit 1
fi

write_summary "snapshot_sidecar_rendered=true"
write_summary "snapshot_mtls_identity_projected=true"
write_summary "snapshot_rbac_rendered=true"
write_summary "volume_snapshot_class_rendered=true"
write_summary "snapshot_controller_crds_chart_owned=false"
write_summary "snapshot_backup_fixed_root_rendered=true"
write_summary "snapshot_backup_token_not_projected_to_csi=true"
write_summary "cleanup_status=ok"
write_summary "phase175_snapshot_chart_contract_status=ok"
