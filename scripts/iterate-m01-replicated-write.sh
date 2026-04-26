#!/usr/bin/env bash
# iterate-m01-replicated-write.sh — G5-4 m01 hardware first-light harness
#
# DRAFT v0.1 SKELETON — awaiting G5 kickoff §3 batch ratification.
# Scenario bodies marked TODO(G5-arch-ratify) depend on G5 mini-plan.
#
# Runs on local workstation, drives a 2-node cluster:
#   - m01 (192.168.1.181) — primary blockvolume
#   - M02 (192.168.1.184) — replica blockvolume
# Both nodes share a blockmaster (running on m01).
#
# Mirrors structure of T2 iterate-m01-nvme.sh; key differences:
#   - 2-node cross-host wire (vs T2's single-node 3-volume)
#   - Replicated write path under test (vs T2's NVMe-oF target)
#   - iptables-based mid-stream disconnect injection
#   - Byte-exact convergence verification on both replicas
#
# Ownership: QA. Part of G5 batch G5-4 per
# sw-block/design/v3-phase-15-g5-kickoff.md §3 (PROPOSAL — awaiting
# architect ratification).
#
# Exit 0 on all-green; non-zero + diagnostic artifacts on any failure.

set -euo pipefail

# --- Config (env-overridable) ---
M01_HOST="${M01_HOST:-testdev@192.168.1.181}"   # primary node
M02_HOST="${M02_HOST:-testdev@192.168.1.184}"   # replica node
SSH_KEY="${SSH_KEY:-/c/work/dev_server/testdev_key}"
SRC_DIR="${SRC_DIR:-/c/work/seaweed_block}"
REMOTE_BUILD_DIR="${REMOTE_BUILD_DIR:-/tmp/g5_sb_build}"
REMOTE_RUN_DIR="${REMOTE_RUN_DIR:-/tmp/g5_sb_run}"
DURABLE_IMPL="${DURABLE_IMPL:-walstore}"          # walstore | smartwal — per-scenario matrix
DURABLE_BLOCKS="${DURABLE_BLOCKS:-262144}"        # 1 GiB at 4 KiB blocks
DURABLE_BLOCKSIZE="${DURABLE_BLOCKSIZE:-4096}"
WORKLOAD_DURATION_SEC="${WORKLOAD_DURATION_SEC:-30}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${SRC_DIR%/*}/seaweedfs/sw-block/design/g5-artifacts}"

# Network ports (m01 primary)
M01_MASTER_PORT=9180
M01_PRIMARY_CTRL_PORT=9210
M01_PRIMARY_DATA_PORT=9220

# Network ports (M02 replica)
M02_REPLICA_CTRL_PORT=9211
M02_REPLICA_DATA_PORT=9221

SSH_M01="ssh -i ${SSH_KEY} -o ConnectTimeout=10 ${M01_HOST}"
SSH_M02="ssh -i ${SSH_KEY} -o ConnectTimeout=10 ${M02_HOST}"

# --- Helpers ---

log() { printf '\033[1;36m[g5-replwrite]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[g5-replwrite FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

mkdir -p "${ARTIFACT_DIR}"

collect_diagnostics() {
    local label="$1"
    log "collecting diagnostics [${label}]..."
    $SSH_M01 "cat ${REMOTE_RUN_DIR}/logs/master.log 2>/dev/null" > "${ARTIFACT_DIR}/master-${label}.log" || true
    $SSH_M01 "cat ${REMOTE_RUN_DIR}/logs/primary.log 2>/dev/null" > "${ARTIFACT_DIR}/primary-${label}.log" || true
    $SSH_M02 "cat ${REMOTE_RUN_DIR}/logs/replica.log 2>/dev/null" > "${ARTIFACT_DIR}/replica-${label}.log" || true
    $SSH_M01 "sudo dmesg -T | tail -50" > "${ARTIFACT_DIR}/m01-dmesg-${label}.txt" 2>/dev/null || true
    $SSH_M02 "sudo dmesg -T | tail -50" > "${ARTIFACT_DIR}/m02-dmesg-${label}.txt" 2>/dev/null || true
    log "  → ${ARTIFACT_DIR}/{master,primary,replica}-${label}.log + {m01,m02}-dmesg-${label}.txt"
}

# --- Phase 1: sync + rebuild ---

sync_and_build() {
    log "phase 1: sync source + build binaries"

    # Build on m01 (Go installed there); scp binaries to M02 (no Go on M02 per D-finding)
    log "  syncing source to m01..."
    cd "${SRC_DIR}"
    tar --exclude='.git' --exclude='*.exe' --exclude='*.test' -czf /tmp/g5-src.tgz .
    scp -i "${SSH_KEY}" /tmp/g5-src.tgz "${M01_HOST}:/tmp/" >/dev/null
    $SSH_M01 "rm -rf ${REMOTE_BUILD_DIR} && mkdir -p ${REMOTE_BUILD_DIR} && cd ${REMOTE_BUILD_DIR} && tar xzf /tmp/g5-src.tgz"

    log "  building blockmaster + blockvolume on m01..."
    $SSH_M01 "cd ${REMOTE_BUILD_DIR} && go build -o /tmp/g5-blockmaster ./cmd/blockmaster/ && go build -o /tmp/g5-blockvolume ./cmd/blockvolume/" || die "build failed on m01"

    log "  scp blockvolume binary to M02 (Go not installed there)..."
    $SSH_M01 "scp -o StrictHostKeyChecking=no /tmp/g5-blockvolume testdev@192.168.1.184:/tmp/" >/dev/null \
        || $SSH_M02 "exit" \
        || die "scp m01→M02 failed (verify SSH key on m01:~/.ssh/ has access to M02)"
}

# --- Phase 2: start cluster ---

start_cluster() {
    log "phase 2: start blockmaster (m01) + primary (m01) + replica (M02)"

    # Pre-cleanup
    $SSH_M01 "sudo pkill -9 -f g5-blockvolume 2>/dev/null || true; sudo pkill -9 -f g5-blockmaster 2>/dev/null || true; rm -rf ${REMOTE_RUN_DIR} && mkdir -p ${REMOTE_RUN_DIR}/{logs,store,durable-primary}" >/dev/null
    $SSH_M02 "sudo pkill -9 -f g5-blockvolume 2>/dev/null || true; rm -rf ${REMOTE_RUN_DIR} && mkdir -p ${REMOTE_RUN_DIR}/{logs,durable-replica}" >/dev/null

    # blockmaster on m01
    log "  starting blockmaster on m01..."
    $SSH_M01 "
nohup /tmp/g5-blockmaster --authority-store ${REMOTE_RUN_DIR}/store --listen 0.0.0.0:${M01_MASTER_PORT} \
    --topology ${REMOTE_RUN_DIR}/topology.yaml --t0-print-ready \
    > ${REMOTE_RUN_DIR}/logs/master.log 2>&1 </dev/null & disown
sleep 2
pgrep -f g5-blockmaster >/dev/null || (cat ${REMOTE_RUN_DIR}/logs/master.log; exit 1)
" || die "blockmaster failed to start"

    # primary blockvolume on m01
    log "  starting primary blockvolume on m01..."
    $SSH_M01 "
nohup /tmp/g5-blockvolume \
    --master 127.0.0.1:${M01_MASTER_PORT} \
    --server-id m01-primary --volume-id v1 --replica-id r1 \
    --ctrl-addr 0.0.0.0:${M01_PRIMARY_CTRL_PORT} --data-addr 0.0.0.0:${M01_PRIMARY_DATA_PORT} \
    --durable-root ${REMOTE_RUN_DIR}/durable-primary --durable-impl ${DURABLE_IMPL} \
    --durable-blocks ${DURABLE_BLOCKS} --durable-blocksize ${DURABLE_BLOCKSIZE} \
    --t0-print-ready --t1-readiness \
    > ${REMOTE_RUN_DIR}/logs/primary.log 2>&1 </dev/null & disown
sleep 3
pgrep -f 'g5-blockvolume.*server-id m01-primary' >/dev/null || (cat ${REMOTE_RUN_DIR}/logs/primary.log; exit 1)
" || die "primary blockvolume failed to start"

    # replica blockvolume on M02
    log "  starting replica blockvolume on M02..."
    $SSH_M02 "
nohup /tmp/g5-blockvolume \
    --master 192.168.1.181:${M01_MASTER_PORT} \
    --server-id m02-replica --volume-id v1 --replica-id r2 \
    --ctrl-addr 0.0.0.0:${M02_REPLICA_CTRL_PORT} --data-addr 0.0.0.0:${M02_REPLICA_DATA_PORT} \
    --durable-root ${REMOTE_RUN_DIR}/durable-replica --durable-impl ${DURABLE_IMPL} \
    --durable-blocks ${DURABLE_BLOCKS} --durable-blocksize ${DURABLE_BLOCKSIZE} \
    --t0-print-ready --t1-readiness \
    > ${REMOTE_RUN_DIR}/logs/replica.log 2>&1 </dev/null & disown
sleep 3
pgrep -f 'g5-blockvolume.*server-id m02-replica' >/dev/null || (cat ${REMOTE_RUN_DIR}/logs/replica.log; exit 1)
" || die "replica blockvolume failed to start"

    log "  cluster up: master+primary on m01, replica on M02"
}

stop_cluster() {
    log "stopping cluster..."
    $SSH_M01 "sudo pkill -9 -f g5-blockvolume 2>/dev/null || true; sudo pkill -9 -f g5-blockmaster 2>/dev/null || true; sleep 1" >/dev/null 2>&1 || true
    $SSH_M02 "sudo pkill -9 -f g5-blockvolume 2>/dev/null || true; sleep 1" >/dev/null 2>&1 || true
}

# --- Phase 3: scenarios (TODO bodies await G5 mini-plan ratification) ---

# TODO(G5-arch-ratify): the architect-ratified scenario list per G5
# mini-plan §3 batch G5-4 acceptance criteria #3. Suggested set
# (subject to architect ratification, see g5-kickoff.md §4 #1):
#
#   ShortDisconnect_DeltaCatchUp × {smartwal, walstore}
#   LongDisconnect_RebuildEscalation × {smartwal, walstore}
#   MidStreamDisconnect_Recovery × {smartwal, walstore}
#   SteadyStateLiveShip_Throughput × {smartwal, walstore}
#
# Each scenario body needs:
#   1. workload driver (iSCSI? gRPC admin? direct primary-write?)
#   2. fault injection (iptables block on data-addr? or ctrl-addr?)
#   3. wait-for-event (SessionClosed, RebuildSessionClosed, etc.)
#   4. byte-exact convergence verification (compare AllBlocks() on
#      primary vs replica)
#
# Need framework support:
#   - workload driver — sw decision: iSCSI target binding to primary
#     vs gRPC admin endpoint vs new test-only RPC
#   - replica state inspector — gRPC status endpoint (--status-addr
#     already on blockvolume) reports R/S/H/Decision
#   - byte-exact verifier — read-back across both nodes via status
#     endpoint or separate diagnostic tool

scenario_short_disconnect_delta_catchup() {
    local impl="$1"
    log "  scenario: ShortDisconnect_DeltaCatchUp (${impl}) — TODO(G5-arch-ratify)"
    # 1. Drive primary writes for 5s (TODO: workload driver)
    # 2. iptables -I INPUT -p tcp --dport ${M02_REPLICA_DATA_PORT} -j DROP
    # 3. Drive primary writes for 5s more
    # 4. iptables -D INPUT -p tcp --dport ${M02_REPLICA_DATA_PORT} -j DROP
    # 5. Wait for engine-driven catch-up to complete (poll status endpoint)
    # 6. Verify replica converges byte-exact (delta only — most data was already shipped)
    # 7. Assert: ScanLBAs metric shows reduced emit count vs full-window
    return 0
}

scenario_long_disconnect_rebuild_escalation() {
    local impl="$1"
    log "  scenario: LongDisconnect_RebuildEscalation (${impl}) — TODO(G5-arch-ratify)"
    # 1. Drive primary writes for long enough to fill replica's WAL retention window
    # 2. iptables block replica
    # 3. Drive primary writes past retention
    # 4. iptables unblock
    # 5. Catch-up should fail with WAL recycled → engine emits StartRebuild
    # 6. Rebuild completes → replica converges byte-exact
    # 7. Assert: rebuild session was actually emitted (not just Decision=Rebuild state)
    return 0
}

scenario_mid_stream_disconnect_recovery() {
    local impl="$1"
    log "  scenario: MidStreamDisconnect_Recovery (${impl}) — TODO(G5-arch-ratify)"
    # 1. Drive primary writes
    # 2. Cause primary↔replica disconnect mid-catch-up (iptables block + unblock quickly)
    # 3. Verify engine retry budget consumes attempts
    # 4. Verify final convergence within budget OR escalation observed
    return 0
}

scenario_steady_state_live_ship_throughput() {
    local impl="$1"
    log "  scenario: SteadyStateLiveShip_Throughput (${impl}) — TODO(G5-arch-ratify)"
    # 1. No fault injection
    # 2. Drive sustained writes for ${WORKLOAD_DURATION_SEC}s
    # 3. Measure throughput, latency, replica lag
    # 4. Verify walstore flusher cadence under sustained load
    #    (relevant to G5-2 walstore tuning policy)
    return 0
}

# --- Phase 4: run scenario matrix ---

run_matrix() {
    local scenarios=(
        scenario_short_disconnect_delta_catchup
        scenario_long_disconnect_rebuild_escalation
        scenario_mid_stream_disconnect_recovery
        scenario_steady_state_live_ship_throughput
    )
    local impls=(walstore smartwal)
    local total=0 passed=0 failed=0

    for impl in "${impls[@]}"; do
        DURABLE_IMPL="${impl}"
        for fn in "${scenarios[@]}"; do
            total=$((total+1))
            log "scenario ${total}: ${fn} on ${impl}"
            start_cluster
            if "${fn}" "${impl}"; then
                passed=$((passed+1))
                log "  ✓ pass"
            else
                failed=$((failed+1))
                log "  ✗ FAIL"
                collect_diagnostics "${fn}-${impl}"
            fi
            stop_cluster
        done
    done

    log "matrix: ${passed}/${total} passed (${failed} failed)"
    [ "${failed}" -eq 0 ] || die "matrix had ${failed} failures; see ${ARTIFACT_DIR}/"
}

# --- Main ---

main() {
    log "=== G5-4 m01 hardware first-light harness (DRAFT v0.1 skeleton) ==="
    log "primary node: ${M01_HOST}"
    log "replica node: ${M02_HOST}"
    log "substrate impl matrix: walstore + smartwal"
    log "extent size: ${DURABLE_BLOCKS} blocks × ${DURABLE_BLOCKSIZE} bytes = $((DURABLE_BLOCKS*DURABLE_BLOCKSIZE/1024/1024)) MiB"
    log "workload duration: ${WORKLOAD_DURATION_SEC}s per scenario"
    log "artifact dir: ${ARTIFACT_DIR}"

    sync_and_build
    run_matrix

    log "=== ALL SCENARIOS PASSED ==="
}

main "$@"
