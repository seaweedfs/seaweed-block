#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase155-mounted-durable-status-head-lsn-confirmation-gate}"
SUMMARY="${ARTIFACT_DIR}/phase155-mounted-durable-status-head-lsn-confirmation-summary.txt"
PHASE152_DIR="${ARTIFACT_DIR}/phase152-recovery"
CANDIDATE_MAX_H2C_BYTES="${SW_BLOCK_PHASE155_CANDIDATE_MAX_H2C_BYTES:-65536}"
SEQ_MIB="${SW_BLOCK_PHASE155_SEQ_MIB:-4}"
RESTART_VERIFY_MIB="${SW_BLOCK_PHASE155_RESTART_VERIFY_MIB:-4}"
STATE_HOSTPATH="${SW_BLOCK_PHASE155_STATE_HOSTPATH:-/tmp/sw-block-phase155-recovery}"

mkdir -p "${ARTIFACT_DIR}" "${PHASE152_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

summary_value() {
  local file="$1"
  local key="$2"
  awk -F= -v key="$key" '$1 == key {value = substr($0, length(key) + 2)} END {if (value != "") print value}' "$file"
}

require_summary_value() {
  local file="$1"
  local key="$2"
  local value
  value="$(summary_value "$file" "$key")"
  if [[ -z "${value}" ]]; then
    echo "missing summary key ${key} in ${file}" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

assert_value() {
  local key="$1"
  local actual="$2"
  local want="$3"
  if [[ "${actual}" != "${want}" ]]; then
    echo "${key}=${actual}, want ${want}" >&2
    exit 1
  fi
}

assert_int_ge() {
  local actual="$1"
  local want="$2"
  local label="$3"
  python3 - "$actual" "$want" "$label" <<'PY'
import sys
actual = int(sys.argv[1])
want = int(sys.argv[2])
label = sys.argv[3]
if actual < want:
    raise SystemExit(f"{label}={actual}, want >= {want}")
PY
}

write_summary "phase155_mounted_durable_status_head_lsn_confirmation_status=running"
write_summary "phase152_followup=head_lsn_diagnostic_cleanup"
write_summary "frontend_transport=tcp"
write_summary "roce_claim_allowed=false"
write_summary "nvme_rdma_claim_allowed=false"
write_summary "performance_slo_claim_allowed=false"
write_summary "default_wal_format_unchanged=true"
write_summary "feature_gate_default=false"
write_summary "runtime_opt_in_name=durable-wal-multiblock-records"
write_summary "runtime_opt_in_enabled=true"
write_summary "recovery_test_disable_flusher_enabled=true"
write_summary "restart_persistence_mode=hostpath"
write_summary "state_hostpath=${STATE_HOSTPATH}"

SW_BLOCK_ARTIFACT_DIR="${PHASE152_DIR}" \
SW_BLOCK_PHASE152_CANDIDATE_MAX_H2C_BYTES="${CANDIDATE_MAX_H2C_BYTES}" \
SW_BLOCK_PHASE152_SEQ_MIB="${SEQ_MIB}" \
SW_BLOCK_PHASE152_RESTART_VERIFY_MIB="${RESTART_VERIFY_MIB}" \
SW_BLOCK_PHASE152_STATE_HOSTPATH="${STATE_HOSTPATH}" \
SW_BLOCK_PHASE125_FRONTEND_IP_MAP="${SW_BLOCK_PHASE125_FRONTEND_IP_MAP:-}" \
SW_BLOCK_PHASE125_EXPECTED_ROUTE_DEV="${SW_BLOCK_PHASE125_EXPECTED_ROUTE_DEV:-}" \
SW_BLOCK_IMPORT_K3S_SSH_KEY="${SW_BLOCK_IMPORT_K3S_SSH_KEY:-}" \
  bash "${ROOT}/scripts/run-phase152-wal-multiblock-recovery-compatibility-gate.sh" "${ROOT}" \
  >"${ARTIFACT_DIR}/phase152-recovery.stdout.txt" \
  2>"${ARTIFACT_DIR}/phase152-recovery.stderr.txt"

PHASE152_SUMMARY="${PHASE152_DIR}/phase152-wal-multiblock-recovery-compatibility-summary.txt"
PHASE120_DIR="${PHASE152_DIR}/phase126-profile/phase125-profile/block-profile"
PHASE120_SUMMARY="${PHASE120_DIR}/phase120-nvme-tcp-performance-baseline-summary.txt"
DURABLE_STATUS_JSON="${PHASE120_DIR}/status/status-durable-after-blockvolume-restart.json"
WRITE_PROFILE_SUMMARY="${PHASE120_DIR}/status/write-profile-summary.txt"
DURABLE_CHECK_SUMMARY="${ARTIFACT_DIR}/durable-status-head-lsn-summary.txt"

assert_value "phase152_wal_multiblock_recovery_compatibility_status" \
  "$(require_summary_value "${PHASE152_SUMMARY}" phase152_wal_multiblock_recovery_compatibility_status)" "ok"
assert_value "phase120_nvme_tcp_performance_baseline_status" \
  "$(require_summary_value "${PHASE120_SUMMARY}" phase120_nvme_tcp_performance_baseline_status)" "ok"

RUNTIME_ENABLED="$(require_summary_value "${PHASE152_SUMMARY}" runtime_opt_in_enabled)"
RECOVERY_TEST_DISABLE_FLUSHER="$(require_summary_value "${PHASE152_SUMMARY}" recovery_test_disable_flusher_enabled)"
RESTART_MODE="$(require_summary_value "${PHASE152_SUMMARY}" blockvolume_restart_mode)"
RECOVERY_COMPLETED="$(require_summary_value "${PHASE152_SUMMARY}" recovery_completed)"
RECOVERED_LSN="$(require_summary_value "${PHASE152_SUMMARY}" recovered_lsn_after_restart)"
WAL_FAULT="$(require_summary_value "${PHASE152_SUMMARY}" wal_integrity_fault_observed)"
READER_AFTER="$(require_summary_value "${PHASE152_SUMMARY}" reader_verified_after_restart)"
READY_AFTER="$(require_summary_value "${PHASE152_SUMMARY}" ready_after_restart)"
CLEANUP_STATUS="$(require_summary_value "${PHASE152_SUMMARY}" cleanup_status)"
VOLUME_ID="$(require_summary_value "${WRITE_PROFILE_SUMMARY}" write_profile_volume_id)"

assert_value "runtime_opt_in_enabled" "${RUNTIME_ENABLED}" "true"
assert_value "recovery_test_disable_flusher_enabled" "${RECOVERY_TEST_DISABLE_FLUSHER}" "true"
assert_value "blockvolume_restart_mode" "${RESTART_MODE}" "force_delete_pod"
assert_value "recovery_completed" "${RECOVERY_COMPLETED}" "true"
assert_int_ge "${RECOVERED_LSN}" 1 "recovered_lsn_after_restart"
assert_value "wal_integrity_fault_observed" "${WAL_FAULT}" "false"
assert_value "reader_verified_after_restart" "${READER_AFTER}" "true"
assert_value "ready_after_restart" "${READY_AFTER}" "true"
assert_value "cleanup_status" "${CLEANUP_STATUS}" "ok"

python3 - \
  "${DURABLE_STATUS_JSON}" \
  "${VOLUME_ID}" \
  "${RECOVERED_LSN}" \
  "${DURABLE_CHECK_SUMMARY}" <<'PY'
import json
import sys

status_path, volume_id, recovered_lsn_raw, out_path = sys.argv[1:5]
recovered_lsn = int(recovered_lsn_raw)
with open(status_path, "r", encoding="utf-8") as fh:
    body = json.load(fh)

volumes = body.get("Volumes") or body.get("volumes") or []
matched = None
for vol in volumes:
    got = vol.get("VolumeID") or vol.get("volumeID") or vol.get("volume_id")
    if got == volume_id:
        matched = vol
        break
if matched is None:
    if len(volumes) == 1:
        matched = volumes[0]
    else:
        raise SystemExit(f"durable status volume {volume_id!r} not found in {status_path}")

def pick_int(obj, *names):
    for name in names:
        if name in obj and obj[name] is not None:
            return int(obj[name])
    return None

def pick_bool(obj, *names):
    for name in names:
        if name in obj and obj[name] is not None:
            return bool(obj[name])
    return None

durable_lsn = pick_int(matched, "DurableLSN", "durableLSN", "durable_lsn")
head_lsn = pick_int(matched, "HeadLSN", "headLSN", "head_lsn")
epoch = pick_int(matched, "Epoch", "epoch")
latched = pick_bool(matched, "Latched", "latched")
operational = pick_bool(matched, "Operational", "operational")
evidence = matched.get("Evidence") or matched.get("evidence") or ""

if durable_lsn != recovered_lsn:
    raise SystemExit(f"durable_lsn={durable_lsn}, want recovered_lsn={recovered_lsn}")
if head_lsn != recovered_lsn:
    raise SystemExit(f"head_lsn={head_lsn}, want recovered_lsn={recovered_lsn}")
if latched is not True:
    raise SystemExit(f"latched={latched}, want true")
if operational is not True:
    raise SystemExit(f"operational={operational}, want true")
if epoch is None or epoch < 1:
    raise SystemExit(f"epoch={epoch}, want >= 1")
if f"recovered LSN={recovered_lsn}" not in evidence:
    raise SystemExit(f"evidence={evidence!r}, want recovered LSN={recovered_lsn}")

resolved_volume_id = matched.get("VolumeID") or matched.get("volumeID") or matched.get("volume_id")
with open(out_path, "w", encoding="utf-8") as out:
    out.write(f"durable_status_volume_id={resolved_volume_id}\n")
    out.write(f"durable_status_durable_lsn_after_restart={durable_lsn}\n")
    out.write(f"durable_status_head_lsn_after_restart={head_lsn}\n")
    out.write("durable_status_head_lsn_equals_recovered_lsn=true\n")
    out.write("durable_status_evidence_matches_recovered_lsn=true\n")
    out.write("durable_status_latched_after_restart=true\n")
    out.write("durable_status_operational_after_restart=true\n")
    out.write(f"durable_status_epoch_after_restart={epoch}\n")
PY

DURABLE_STATUS_VOLUME_ID="$(require_summary_value "${DURABLE_CHECK_SUMMARY}" durable_status_volume_id)"
DURABLE_LSN_AFTER="$(require_summary_value "${DURABLE_CHECK_SUMMARY}" durable_status_durable_lsn_after_restart)"
HEAD_LSN_AFTER="$(require_summary_value "${DURABLE_CHECK_SUMMARY}" durable_status_head_lsn_after_restart)"
HEAD_EQUALS_RECOVERED="$(require_summary_value "${DURABLE_CHECK_SUMMARY}" durable_status_head_lsn_equals_recovered_lsn)"
EVIDENCE_MATCHES_RECOVERED="$(require_summary_value "${DURABLE_CHECK_SUMMARY}" durable_status_evidence_matches_recovered_lsn)"
LATCHED_AFTER="$(require_summary_value "${DURABLE_CHECK_SUMMARY}" durable_status_latched_after_restart)"
OPERATIONAL_AFTER="$(require_summary_value "${DURABLE_CHECK_SUMMARY}" durable_status_operational_after_restart)"
EPOCH_AFTER="$(require_summary_value "${DURABLE_CHECK_SUMMARY}" durable_status_epoch_after_restart)"

write_summary "candidate_max_h2c_bytes=${CANDIDATE_MAX_H2C_BYTES}"
write_summary "seq_size_mib=${SEQ_MIB}"
write_summary "restart_verify_mib=${RESTART_VERIFY_MIB}"
write_summary "blockvolume_restart_mode=${RESTART_MODE}"
write_summary "recovery_completed=${RECOVERY_COMPLETED}"
write_summary "recovered_lsn_after_restart=${RECOVERED_LSN}"
write_summary "recovered_lsn_remains_correct=true"
write_summary "wal_integrity_fault_observed=${WAL_FAULT}"
write_summary "durable_status_volume_id=${DURABLE_STATUS_VOLUME_ID}"
write_summary "durable_status_durable_lsn_after_restart=${DURABLE_LSN_AFTER}"
write_summary "durable_status_head_lsn_after_restart=${HEAD_LSN_AFTER}"
write_summary "durable_status_head_lsn_equals_recovered_lsn=${HEAD_EQUALS_RECOVERED}"
write_summary "durable_status_evidence_matches_recovered_lsn=${EVIDENCE_MATCHES_RECOVERED}"
write_summary "durable_status_latched_after_restart=${LATCHED_AFTER}"
write_summary "durable_status_operational_after_restart=${OPERATIONAL_AFTER}"
write_summary "durable_status_epoch_after_restart=${EPOCH_AFTER}"
write_summary "reader_verified_after_restart=${READER_AFTER}"
write_summary "ready_after_restart=${READY_AFTER}"
write_summary "cleanup_status=${CLEANUP_STATUS}"
write_summary "phase155_decision=mounted_confirmed"
write_summary "next_recommendation=phase156_wal_multiblock_published_image_release_smoke_decision"
write_summary "phase155_mounted_durable_status_head_lsn_confirmation_status=ok"
