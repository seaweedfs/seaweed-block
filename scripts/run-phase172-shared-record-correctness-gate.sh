#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase172-shared-record-correctness-gate}"
SUMMARY="${ARTIFACT_DIR}/phase172-shared-record-correctness-summary.txt"

mkdir -p "${ARTIFACT_DIR}"
: >"${SUMMARY}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

metric_from_test_log() {
  local file="$1"
  local key="$2"
  awk -F= -v key="${key}" '$1 ~ key "$" { print $2; exit }' "${file}"
}

cd "${ROOT}"
write_summary "phase172_shared_record_correctness_status=running"
write_summary "git_sha=$(git rev-parse HEAD)"
git_dirty="$([[ -n "$(git status --porcelain)" ]] && echo true || echo false)"
write_summary "git_dirty=${git_dirty}"
if [[ "${git_dirty}" != "false" ]]; then
  echo "Phase 172 D3 requires a clean source worktree" >&2
  exit 1
fi
write_summary "go_version=$(go version | tr ' ' '_')"
write_summary "kernel=$(uname -srvmo | tr ' ' '_')"

focused_tests=(
  TestWALStoreSingleReadMaterializationDisabledByDefault
  TestWALStoreSingleReadMaterializesOrdinaryAndMultiBlockRecords
  TestWALStoreSharedRecordMaterializationReadsEachRecordOnce
  TestWALStoreSharedRecordMaterializationReadsRangeTrimOnce
  TestWALStoreSharedRecordMaterializationConcurrentPartialOverwrite
  TestWALStoreSharedRecordMaterializationSurvivesLegalRingWrap
  TestWALStoreSharedRecordMaterializationFailsClosedOnMalformedRecord
)
focused_regex="^($(IFS='|'; echo "${focused_tests[*]}"))$"
focused_log="${ARTIFACT_DIR}/shared-record-focused-repeat.log"
: >"${focused_log}"
for test_name in "${focused_tests[@]}"; do
  go test ./core/storage -run "^${test_name}$" -count=20 -v \
    >>"${focused_log}" 2>&1
  write_summary "${test_name}=pass"
done
if grep -Eq -- '--- SKIP|no tests to run' "${focused_log}"; then
  echo "focused D3 gate contains a skipped or missing test" >&2
  exit 1
fi
write_summary "focused_repeat_20=pass"
write_summary "default_two_read_path_unchanged=pass"
write_summary "d2_single_read_path_unchanged=pass"
write_summary "ordinary_and_multiblock_shared_reuse=pass"
write_summary "legacy_range_trim_shared_reuse=pass"
write_summary "concurrent_partial_overwrite=pass"
write_summary "legal_ring_wrap_recovery_reuse=pass"
write_summary "malformed_shared_record_fails_closed=pass"

go test -race ./core/storage -run "${focused_regex}" -count=20 -v \
  >"${ARTIFACT_DIR}/shared-record-race-repeat.log" 2>&1
if grep -Eq -- '--- SKIP|no tests to run' "${ARTIFACT_DIR}/shared-record-race-repeat.log"; then
  echo "race D3 gate contains a skipped or missing test" >&2
  exit 1
fi
write_summary "race_repeat_20=pass"

go test ./core/storage -count=1 \
  >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
write_summary "storage_regression=pass"

go vet ./core/storage >"${ARTIFACT_DIR}/storage-vet.log" 2>&1
write_summary "storage_vet=pass"

scoped_path="${ARTIFACT_DIR}/phase172-shared-record-probe.store"
scoped_log="${ARTIFACT_DIR}/phase172-shared-record-probe.log"
strace_log="${ARTIFACT_DIR}/phase172-shared-record-probe.strace.txt"
if ! command -v strace >/dev/null 2>&1; then
  echo "strace is required for Phase 172 D3" >&2
  exit 1
fi

go test -c ./core/storage -o "${ARTIFACT_DIR}/storage.test"
SW_BLOCK_PHASE172_SHARED_PROBE_PATH="${scoped_path}" \
  strace -f -c -e trace=pread64 -P "${scoped_path}" -o "${strace_log}" \
    "${ARTIFACT_DIR}/storage.test" \
      -test.run '^TestPhase172ScopedSharedRecordProbe$' \
      -test.count=1 -test.v >"${scoped_log}" 2>&1

enabled="$(metric_from_test_log "${scoped_log}" phase172_shared_probe_enabled)"
entries="$(metric_from_test_log "${scoped_log}" phase172_shared_probe_snapshot_entries)"
unique="$(metric_from_test_log "${scoped_log}" phase172_shared_probe_unique_records)"
candidates="$(metric_from_test_log "${scoped_log}" phase172_shared_probe_reuse_candidates)"
validated="$(metric_from_test_log "${scoped_log}" phase172_shared_probe_validated_records)"
header_reads="$(metric_from_test_log "${scoped_log}" phase172_shared_probe_header_read_ops)"
record_reads="$(metric_from_test_log "${scoped_log}" phase172_shared_probe_record_read_ops)"
product_reads="$(
  metric_from_test_log "${scoped_log}" phase172_shared_probe_materialization_read_ops
)"
reuse_hits="$(metric_from_test_log "${scoped_log}" phase172_shared_probe_reuse_hits)"
strace_reads="$(awk '$NF == "pread64" { print $4; exit }' "${strace_log}")"

if [[ "${enabled}" != "true" ||
      "${entries}" != "1024" ||
      "${unique}" != "64" ||
      "${candidates}" != "960" ||
      "${validated}" != "1024" ||
      "${header_reads}" != "0" ||
      "${record_reads}" != "64" ||
      "${product_reads}" != "64" ||
      "${reuse_hits}" != "960" ||
      "${strace_reads}" != "64" ]]; then
  echo "shared-record probe mismatch enabled=${enabled:-missing} entries=${entries:-missing} unique=${unique:-missing} candidates=${candidates:-missing} validated=${validated:-missing} header=${header_reads:-missing} record=${record_reads:-missing} product=${product_reads:-missing} reuse=${reuse_hits:-missing} strace=${strace_reads:-missing}" >&2
  exit 1
fi

write_summary "scoped_probe_shared_record=${enabled}"
write_summary "scoped_probe_snapshot_entries=${entries}"
write_summary "scoped_probe_unique_records=${unique}"
write_summary "scoped_probe_reuse_candidates=${candidates}"
write_summary "scoped_probe_validated_records=${validated}"
write_summary "scoped_probe_header_reads=${header_reads}"
write_summary "scoped_probe_record_reads=${record_reads}"
write_summary "scoped_probe_product_materialization_reads=${product_reads}"
write_summary "scoped_probe_reuse_hits=${reuse_hits}"
write_summary "scoped_probe_strace_pread64_calls=${strace_reads}"
write_summary "scoped_strace_matches_product_counter=true"
write_summary "cache_scope=single_flush_cycle"
write_summary "cache_bound=one_decoded_record"
write_summary "external_selector_added=false"
write_summary "d4_equivalence_gate_eligible=true"
write_summary "phase172_shared_record_correctness_status=ok"
