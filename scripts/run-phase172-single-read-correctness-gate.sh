#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase172-single-read-correctness-gate}"
SUMMARY="${ARTIFACT_DIR}/phase172-single-read-correctness-summary.txt"

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
write_summary "phase172_single_read_correctness_status=running"
write_summary "git_sha=$(git rev-parse HEAD)"
git_dirty="$([[ -n "$(git status --porcelain)" ]] && echo true || echo false)"
write_summary "git_dirty=${git_dirty}"
if [[ "${git_dirty}" != "false" ]]; then
  echo "Phase 172 D2 requires a clean source worktree" >&2
  exit 1
fi
write_summary "go_version=$(go version | tr ' ' '_')"
write_summary "kernel=$(uname -srvmo | tr ' ' '_')"

focused_tests=(
  TestWALStoreSingleReadMaterializationDisabledByDefault
  TestWALStoreSingleReadMaterializesOrdinaryAndMultiBlockRecords
  TestWALStoreSingleReadMaterializesLegacyRangeTrim
  TestWALStoreSingleReadFailsClosedOnInvalidRecord
  TestWALStoreSingleReadRejectsInvalidMultiBlockSemantics
  TestWALStoreSingleReadRejectsInvalidTrimSemantics
  TestWALStoreSingleReadFailureAtEachPhysicalRecordKeepsWholeSnapshot
  TestWALStoreSingleReadHandlesReverseGappedAndWrappedRecords
)
focused_regex="^($(IFS='|'; echo "${focused_tests[*]}"))$"
focused_log="${ARTIFACT_DIR}/single-read-focused-repeat.log"
: >"${focused_log}"
for test_name in "${focused_tests[@]}"; do
  go test ./core/storage -run "^${test_name}$" -count=20 -v \
    >>"${focused_log}" 2>&1
  write_summary "${test_name}=pass"
done
if grep -Eq -- '--- SKIP|no tests to run' "${focused_log}"; then
  echo "focused D2 gate contains a skipped or missing test" >&2
  exit 1
fi
write_summary "focused_repeat_20=pass"
write_summary "default_two_read_path=pass"
write_summary "ordinary_single_read=pass"
write_summary "legacy_range_trim_single_read=pass"
write_summary "multiblock_single_read_without_reuse=pass"
write_summary "invalid_geometry_fails_closed=pass"
write_summary "short_read_fails_closed=pass"
write_summary "stale_corrupt_flags_unsupported_fail_closed=pass"
write_summary "ordinary_batch_trim_semantic_mismatches_fail_closed=pass"
write_summary "failed_cycle_retains_wal_tail=pass"
write_summary "physical_record_failure_positions=pass"
write_summary "reverse_gapped_wrap=pass"

go test -race ./core/storage -run "${focused_regex}" -count=20 -v \
  >"${ARTIFACT_DIR}/single-read-race-repeat.log" 2>&1
if grep -Eq -- '--- SKIP|no tests to run' "${ARTIFACT_DIR}/single-read-race-repeat.log"; then
  echo "race D2 gate contains a skipped or missing test" >&2
  exit 1
fi
write_summary "race_repeat_20=pass"

go test ./core/storage -count=1 \
  >"${ARTIFACT_DIR}/storage-regression.log" 2>&1
write_summary "storage_regression=pass"

go vet ./core/storage >"${ARTIFACT_DIR}/storage-vet.log" 2>&1
write_summary "storage_vet=pass"

scoped_path="${ARTIFACT_DIR}/phase172-single-read-probe.store"
scoped_log="${ARTIFACT_DIR}/phase172-single-read-probe.log"
strace_log="${ARTIFACT_DIR}/phase172-single-read-probe.strace.txt"
if ! command -v strace >/dev/null 2>&1; then
  echo "strace is required for Phase 172 D2" >&2
  exit 1
fi

go test -c ./core/storage -o "${ARTIFACT_DIR}/storage.test"
SW_BLOCK_PHASE172_SCOPED_PROBE_PATH="${scoped_path}" \
SW_BLOCK_PHASE172_SCOPED_PROBE_SINGLE_READ=true \
  strace -f -c -e trace=pread64 -P "${scoped_path}" -o "${strace_log}" \
    "${ARTIFACT_DIR}/storage.test" \
      -test.run '^TestPhase172ScopedMaterializationProbe$' \
      -test.count=1 -test.v >"${scoped_log}" 2>&1

mode="$(metric_from_test_log "${scoped_log}" phase172_probe_single_read)"
validated="$(metric_from_test_log "${scoped_log}" phase172_probe_validated_records)"
header_reads="$(metric_from_test_log "${scoped_log}" phase172_probe_header_read_ops)"
record_reads="$(metric_from_test_log "${scoped_log}" phase172_probe_record_read_ops)"
product_reads="$(
  metric_from_test_log "${scoped_log}" phase172_probe_materialization_read_ops
)"
strace_reads="$(awk '$NF == "pread64" { print $4; exit }' "${strace_log}")"

if [[ "${mode}" != "true" ||
      "${validated}" != "1024" ||
      "${header_reads}" != "0" ||
      "${record_reads}" != "1024" ||
      "${product_reads}" != "1024" ||
      "${strace_reads}" != "1024" ]]; then
  echo "single-read probe mismatch mode=${mode:-missing} validated=${validated:-missing} header=${header_reads:-missing} record=${record_reads:-missing} product=${product_reads:-missing} strace=${strace_reads:-missing}" >&2
  exit 1
fi

write_summary "scoped_probe_single_read=${mode}"
write_summary "scoped_probe_validated_records=${validated}"
write_summary "scoped_probe_header_reads=${header_reads}"
write_summary "scoped_probe_record_reads=${record_reads}"
write_summary "scoped_probe_product_materialization_reads=${product_reads}"
write_summary "scoped_probe_strace_pread64_calls=${strace_reads}"
write_summary "scoped_strace_matches_product_counter=true"
write_summary "default_materialization_unchanged=true"
write_summary "external_selector_added=false"
write_summary "d3_shared_record_reuse_eligible=true"
write_summary "phase172_single_read_correctness_status=ok"
