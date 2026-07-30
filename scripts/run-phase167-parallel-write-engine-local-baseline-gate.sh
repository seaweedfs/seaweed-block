#!/usr/bin/env bash
set -euo pipefail

ROOT="${1:-$(pwd)}"
ARTIFACT_DIR="${SW_BLOCK_ARTIFACT_DIR:-${ROOT}/results/phase167-parallel-write-engine-local-baseline-gate}"
SUMMARY="${ARTIFACT_DIR}/phase167-parallel-write-engine-local-baseline-summary.txt"
WAL_BENCH="${ARTIFACT_DIR}/walstore-contention-benchmark.txt"
RF3_BENCH="${ARTIFACT_DIR}/rf3-sync-quorum-contention-benchmark.txt"
WAL_BENCHTIME="${SW_BLOCK_PHASE167_WAL_BENCHTIME:-1000x}"
RF3_BENCHTIME="${SW_BLOCK_PHASE167_RF3_BENCHTIME:-500x}"

mkdir -p "${ARTIFACT_DIR}"

write_summary() {
  echo "$*" | tee -a "${SUMMARY}" >/dev/null
}

benchmark_metric() {
  local file="$1"
  local benchmark="$2"
  local writers="$3"
  local metric="$4"
  awk -v prefix="${benchmark}/writers_${writers}-" -v metric="${metric}" '
    index($1, prefix) == 1 {
      for (i = 2; i <= NF; i++) {
        if ($(i + 1) == metric) {
          print $i
          exit
        }
      }
    }
  ' "${file}"
}

require_metric() {
  local file="$1"
  local benchmark="$2"
  local writers="$3"
  local metric="$4"
  local value
  value="$(benchmark_metric "${file}" "${benchmark}" "${writers}" "${metric}")"
  if [[ -z "${value}" ]]; then
    echo "missing ${benchmark}/writers_${writers} metric ${metric} in ${file}" >&2
    exit 1
  fi
  printf '%s' "${value}"
}

ratio() {
  awk -v numerator="$1" -v denominator="$2" 'BEGIN {
    if (numerator <= 0 || denominator <= 0) {
      exit 1
    }
    printf "%.3f", numerator / denominator
  }'
}

require_fixed_iterations() {
  local value="$1"
  local minimum="$2"
  local maximum="$3"
  local label="$4"
  if [[ ! "${value}" =~ ^[0-9]+x$ ]]; then
    echo "${label}=${value}, want a fixed iteration count such as ${minimum}x" >&2
    exit 1
  fi
  local iterations="${value%x}"
  if (( iterations < minimum )); then
    echo "${label}=${value}, want at least ${minimum}x" >&2
    exit 1
  fi
  if (( iterations > maximum )); then
    echo "${label}=${value}, want at most ${maximum}x for strict per-LBA data verification" >&2
    exit 1
  fi
}

require_fixed_iterations "${WAL_BENCHTIME}" 512 8192 "SW_BLOCK_PHASE167_WAL_BENCHTIME"
require_fixed_iterations "${RF3_BENCHTIME}" 256 8192 "SW_BLOCK_PHASE167_RF3_BENCHTIME"

: >"${SUMMARY}"
write_summary "phase167_parallel_write_engine_local_baseline_status=running"
write_summary "scope=local_engine_and_real_tcp_rf3"
write_summary "mounted_nvme_claim_allowed=false"
write_summary "parallel_engine_performance_claim_allowed=false"

cd "${ROOT}"
go test ./core/storage ./core/frontend/durable ./core/replication -count=1 \
  >"${ARTIFACT_DIR}/go-test.log" 2>&1
write_summary "unit_component_tests=pass"

if [[ "$(go env CGO_ENABLED)" == "1" ]]; then
  go test -race ./core/replication -count=1 \
    >"${ARTIFACT_DIR}/go-test-race.log" 2>&1
  write_summary "replication_race_test=pass"
else
  write_summary "replication_race_test=not_run_cgo_disabled"
fi

go test ./core/storage \
  -run '^$' \
  -bench '^BenchmarkPhase167WALStoreContention$' \
  -benchtime="${WAL_BENCHTIME}" \
  -count=1 \
  >"${WAL_BENCH}" 2>&1

go test ./core/replication \
  -run '^$' \
  -bench '^BenchmarkPhase167RF3SyncQuorumContention$' \
  -benchtime="${RF3_BENCHTIME}" \
  -count=1 \
  >"${RF3_BENCH}" 2>&1

for writers in 1 2 4 8; do
  wal_mibps="$(require_metric "${WAL_BENCH}" BenchmarkPhase167WALStoreContention "${writers}" MB/s)"
  wal_p99="$(require_metric "${WAL_BENCH}" BenchmarkPhase167WALStoreContention "${writers}" p99_ns)"
  wal_wait="$(require_metric "${WAL_BENCH}" BenchmarkPhase167WALStoreContention "${writers}" wal_lock_wait_ns/op)"
  commit_wait="$(require_metric "${WAL_BENCH}" BenchmarkPhase167WALStoreContention "${writers}" commit_lock_wait_ns/op)"
  rf3_mibps="$(require_metric "${RF3_BENCH}" BenchmarkPhase167RF3SyncQuorumContention "${writers}" MB/s)"
  rf3_p99="$(require_metric "${RF3_BENCH}" BenchmarkPhase167RF3SyncQuorumContention "${writers}" p99_ns)"
  rf3_fanout="$(require_metric "${RF3_BENCH}" BenchmarkPhase167RF3SyncQuorumContention "${writers}" repl_fanout_ns/op)"

  write_summary "wal_writers_${writers}_mibps=${wal_mibps}"
  write_summary "wal_writers_${writers}_p99_ns=${wal_p99}"
  write_summary "wal_writers_${writers}_lock_wait_ns_per_op=${wal_wait}"
  write_summary "wal_writers_${writers}_commit_lock_wait_ns_per_op=${commit_wait}"
  write_summary "rf3_writers_${writers}_mibps=${rf3_mibps}"
  write_summary "rf3_writers_${writers}_p99_ns=${rf3_p99}"
  write_summary "rf3_writers_${writers}_fanout_ns_per_op=${rf3_fanout}"
done

wal_1="$(require_metric "${WAL_BENCH}" BenchmarkPhase167WALStoreContention 1 MB/s)"
wal_4="$(require_metric "${WAL_BENCH}" BenchmarkPhase167WALStoreContention 4 MB/s)"
rf3_1="$(require_metric "${RF3_BENCH}" BenchmarkPhase167RF3SyncQuorumContention 1 MB/s)"
rf3_4="$(require_metric "${RF3_BENCH}" BenchmarkPhase167RF3SyncQuorumContention 4 MB/s)"
wal_scaling_ratio="$(ratio "${wal_4}" "${wal_1}")"
rf3_scaling_ratio="$(ratio "${rf3_4}" "${rf3_1}")"
write_summary "wal_four_writer_scaling_ratio=${wal_scaling_ratio}"
write_summary "rf3_four_writer_scaling_ratio=${rf3_scaling_ratio}"
write_summary "strict_replication_write_count=true"
write_summary "strict_replica_frontier_and_data=true"
write_summary "lsn_resequencing_tests=pass"
write_summary "next_recommendation=ordered_async_replication"
write_summary "phase167_parallel_write_engine_local_baseline_status=ok"
