#!/usr/bin/env bash
# iterate-m01-replicated-write.sh — G5-5 m01 hardware first-light orchestration
#
# Per v3-phase-15-g5-5-mini-plan.md v0.3 §2 acceptance criteria:
#   #1 verify_cluster_ready          — blockmaster + 2x blockvolume, role-appropriate ready
#   #2 verify_byte_equal             — kernel iSCSI write → m01verify storage-aware compare
#   #3 verify_network_catchup        — iptables disconnect + /status/recovery R/H polling
#   #4 verify_restart_catchup        — SIGTERM replica + restart same --durable-root
#   #5 verify_race_stress            — TestG54_BinaryWiring x10 -race
#   #6 verify_full_suite             — go test ./... clean
#
# Two-node cluster:
#   m01 (192.168.1.181) — primary blockvolume + iSCSI target + blockmaster
#   M02 (192.168.1.184) — replica blockvolume
#
# Run from local workstation (Windows / Git Bash); SSH drives both nodes.
# Exit 0 on all-green; non-zero + diagnostic artifacts on any failure.

set -euo pipefail

# --- Config (env-overridable) ---
M01_HOST="${M01_HOST:-testdev@192.168.1.181}"
M02_HOST="${M02_HOST:-testdev@192.168.1.184}"
SSH_KEY="${SSH_KEY:-/c/work/dev_server/testdev_key}"
SRC_DIR="${SRC_DIR:-/c/work/seaweed_block}"
REMOTE_BUILD_DIR="${REMOTE_BUILD_DIR:-/tmp/g5_sb_build}"
REMOTE_RUN_DIR="${REMOTE_RUN_DIR:-/tmp/g5_sb_run}"
DURABLE_IMPL="${DURABLE_IMPL:-walstore}"
DURABLE_BLOCKS="${DURABLE_BLOCKS:-256}"            # 1 MiB at 4 KiB blocks (small for fast tests)
DURABLE_BLOCKSIZE="${DURABLE_BLOCKSIZE:-4096}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${SRC_DIR%/*}/seaweedfs/sw-block/design/g5-artifacts}"

# Network ports (m01 primary side)
M01_MASTER_PORT=9180
M01_PRIMARY_CTRL_PORT=9210
M01_PRIMARY_DATA_PORT=9220
M01_PRIMARY_STATUS_PORT=9290
M01_ISCSI_PORT=3260

# Network ports (M02 replica side)
M02_REPLICA_CTRL_PORT=9211
M02_REPLICA_DATA_PORT=9221
M02_REPLICA_STATUS_PORT=9291

# iSCSI target identity (primary serves a single LUN)
ISCSI_IQN="iqn.2026-04.io.seaweed.block:v1"

# Catch-up deadline (s) for #3 + #4
CATCHUP_DEADLINE=30

SSH_M01="ssh -i ${SSH_KEY} -o ConnectTimeout=10 -o StrictHostKeyChecking=no ${M01_HOST}"
SSH_M02="ssh -i ${SSH_KEY} -o ConnectTimeout=10 -o StrictHostKeyChecking=no ${M02_HOST}"

# Tier 1 (LOCAL_MODE=1): run everything on the current Linux host
# (e.g. WSL). Same ports, loopback only. No SSH overhead. Faster
# iteration. Use Tier 2 (default, m01 cross-node) when deployment
# realism is needed.
if [ "${LOCAL_MODE:-0}" = "1" ]; then
    log "LOCAL_MODE=1 — running both 'm01' + 'M02' on this host (Tier 1)"
    SSH_M01="bash -c"
    SSH_M02="bash -c"
    # In LOCAL_MODE, the M02 'master' connects to the same loopback master
    # (no cross-node), and the durable-root must differ between primary
    # and replica to avoid lock contention.
fi
LOCAL_MODE="${LOCAL_MODE:-0}"

# --- Helpers ---

log() { printf '\033[1;36m[g5-5]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[g5-5 FAIL]\033[0m %s\n' "$*" >&2; collect_diagnostics fail; exit 1; }

mkdir -p "${ARTIFACT_DIR}"

collect_diagnostics() {
    local label="$1"
    log "collecting diagnostics [${label}]..."
    $SSH_M01 "cat ${REMOTE_RUN_DIR}/logs/master.log 2>/dev/null"  > "${ARTIFACT_DIR}/master-${label}.log"  || true
    $SSH_M01 "cat ${REMOTE_RUN_DIR}/logs/primary.log 2>/dev/null" > "${ARTIFACT_DIR}/primary-${label}.log" || true
    $SSH_M02 "cat ${REMOTE_RUN_DIR}/logs/replica.log 2>/dev/null" > "${ARTIFACT_DIR}/replica-${label}.log" || true
}

# poll_status — fetch /status/recovery from a given node + status port,
# extract a JSON field via a tiny python one-liner. Returns the field
# value on stdout or empty on failure.
poll_status_field() {
    local ssh_cmd="$1" status_port="$2" field="$3"
    $ssh_cmd "curl -s --max-time 2 'http://127.0.0.1:${status_port}/status/recovery?volume=v1' | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get(\"${field}\",\"\"))' 2>/dev/null" || true
}

# wait_until — polls a check command until it returns 0 or deadline.
wait_until() {
    local desc="$1" deadline_s="$2" check_cmd="$3"
    local end=$(( $(date +%s) + deadline_s ))
    while [ "$(date +%s)" -lt "${end}" ]; do
        if eval "${check_cmd}" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    log "wait_until(${desc}) TIMEOUT after ${deadline_s}s"
    return 1
}

# --- Phase 1: sync + build ---

sync_and_build() {
    log "phase 1: sync source + build binaries on m01"
    cd "${SRC_DIR}"
    tar --exclude='.git' --exclude='*.exe' --exclude='*.test' -czf /tmp/g5-src.tgz .
    scp -i "${SSH_KEY}" -o StrictHostKeyChecking=no /tmp/g5-src.tgz "${M01_HOST}:/tmp/" >/dev/null
    $SSH_M01 "rm -rf ${REMOTE_BUILD_DIR} && mkdir -p ${REMOTE_BUILD_DIR} && cd ${REMOTE_BUILD_DIR} && tar xzf /tmp/g5-src.tgz"

    $SSH_M01 "
cd ${REMOTE_BUILD_DIR} && \
go build -o /tmp/g5-blockmaster   ./cmd/blockmaster/  && \
go build -o /tmp/g5-blockvolume   ./cmd/blockvolume/  && \
go build -tags m01verify -o /tmp/g5-m01verify ./cmd/m01verify/
" || die "build failed on m01"

    log "  scp blockvolume + m01verify to M02..."
    $SSH_M01 "scp -o StrictHostKeyChecking=no /tmp/g5-blockvolume /tmp/g5-m01verify ${M02_HOST}:/tmp/" >/dev/null \
        || die "scp m01→M02 failed"
}

# --- Phase 2: cluster lifecycle ---

write_topology() {
    $SSH_M01 "cat > ${REMOTE_RUN_DIR}/topology.yaml <<EOF
volumes:
  - volume_id: v1
    slots:
      - replica_id: r1
        server_id: m01-primary
      - replica_id: r2
        server_id: m02-replica
EOF"
}

start_master() {
    log "  start blockmaster on m01..."
    $SSH_M01 "
nohup /tmp/g5-blockmaster \
    --authority-store ${REMOTE_RUN_DIR}/master-store \
    --listen 0.0.0.0:${M01_MASTER_PORT} \
    --topology ${REMOTE_RUN_DIR}/topology.yaml \
    --expected-slots-per-volume 2 \
    --t0-print-ready \
    > ${REMOTE_RUN_DIR}/logs/master.log 2>&1 </dev/null & disown
sleep 1"
}

start_primary() {
    log "  start primary blockvolume on m01..."
    $SSH_M01 "
nohup /tmp/g5-blockvolume \
    --master 127.0.0.1:${M01_MASTER_PORT} \
    --server-id m01-primary --volume-id v1 --replica-id r1 \
    --ctrl-addr 0.0.0.0:${M01_PRIMARY_CTRL_PORT} \
    --data-addr 0.0.0.0:${M01_PRIMARY_DATA_PORT} \
    --status-addr 127.0.0.1:${M01_PRIMARY_STATUS_PORT} \
    --status-recovery \
    --durable-root ${REMOTE_RUN_DIR}/primary-store \
    --durable-impl ${DURABLE_IMPL} \
    --durable-blocks ${DURABLE_BLOCKS} --durable-blocksize ${DURABLE_BLOCKSIZE} \
    --iscsi-listen 127.0.0.1:${M01_ISCSI_PORT} \
    --iscsi-iqn ${ISCSI_IQN} \
    --t1-readiness \
    > ${REMOTE_RUN_DIR}/logs/primary.log 2>&1 </dev/null & disown"
}

start_replica() {
    log "  start replica blockvolume on M02..."
    $SSH_M02 "mkdir -p ${REMOTE_RUN_DIR}/{logs,replica-store} && \
nohup /tmp/g5-blockvolume \
    --master 192.168.1.181:${M01_MASTER_PORT} \
    --server-id m02-replica --volume-id v1 --replica-id r2 \
    --ctrl-addr 0.0.0.0:${M02_REPLICA_CTRL_PORT} \
    --data-addr 0.0.0.0:${M02_REPLICA_DATA_PORT} \
    --status-addr 127.0.0.1:${M02_REPLICA_STATUS_PORT} \
    --status-recovery \
    --durable-root ${REMOTE_RUN_DIR}/replica-store \
    --durable-impl ${DURABLE_IMPL} \
    --durable-blocks ${DURABLE_BLOCKS} --durable-blocksize ${DURABLE_BLOCKSIZE} \
    --t1-readiness \
    > ${REMOTE_RUN_DIR}/logs/replica.log 2>&1 </dev/null & disown"
}

start_cluster() {
    log "phase 2: start cluster"
    $SSH_M01 "sudo pkill -9 -f '[g]5-blockvolume' 2>/dev/null || true; sudo pkill -9 -f '[g]5-blockmaster' 2>/dev/null || true; rm -rf ${REMOTE_RUN_DIR} && mkdir -p ${REMOTE_RUN_DIR}/{logs,master-store,primary-store}"
    $SSH_M02 "sudo pkill -9 -f '[g]5-blockvolume' 2>/dev/null || true; rm -rf ${REMOTE_RUN_DIR} && mkdir -p ${REMOTE_RUN_DIR}/{logs,replica-store}"
    write_topology
    start_master
    start_primary
    start_replica
}

stop_cluster() {
    log "stop cluster"
    $SSH_M01 "sudo pkill -9 -f '[g]5-blockvolume' 2>/dev/null || true; sudo pkill -9 -f '[g]5-blockmaster' 2>/dev/null || true" || true
    $SSH_M02 "sudo pkill -9 -f '[g]5-blockvolume' 2>/dev/null || true" || true
}

# --- Phase 3: verify steps (per mini-plan v0.3 §2) ---

# §2 #1 — cluster reaches role-appropriate ready state
verify_cluster_ready() {
    log "verify_cluster_ready: primary should reach Healthy=true; replica Healthy=false"
    wait_until "primary Healthy=true" 15 \
        "[ \"\$($SSH_M01 \"curl -s --max-time 2 'http://127.0.0.1:${M01_PRIMARY_STATUS_PORT}/status?volume=v1' | grep -o '\\\"Healthy\\\":true'\")\" = '\"Healthy\":true' ]" \
        || die "primary did not reach Healthy=true within 15s"
    # Replica must respond to /status with Healthy=false (NOT Healthy=true; role split).
    local rh
    rh=$($SSH_M02 "curl -s --max-time 2 'http://127.0.0.1:${M02_REPLICA_STATUS_PORT}/status?volume=v1' | grep -o '\"Healthy\":[a-z]*'")
    [ "${rh}" = '"Healthy":false' ] || die "replica reported ${rh}; expected Healthy=false (architect role-split binding)"
    log "  ✓ cluster ready"
}

# §2 #2 — kernel iSCSI write + m01verify storage-aware compare
# Architect findings round 52: remote SSH bash blocks use
# `set -euo pipefail`; DEV must be non-empty; primary.H must advance
# after the write (catches silent-write-failure masking).
verify_byte_equal() {
    log "verify_byte_equal: iSCSI write 1 LBA + m01verify SHA-256"
    $SSH_M01 "command -v iscsiadm >/dev/null" || die "iscsiadm not installed on m01 (apt install open-iscsi)"
    local prim_h_before prim_h_after
    prim_h_before=$(poll_status_field "$SSH_M01" "${M01_PRIMARY_STATUS_PORT}" H)
    [ -n "${prim_h_before}" ] || die "could not read primary.H before write"
    # Discover + login + write + assert + logout — all under set -euo pipefail
    $SSH_M01 'set -euo pipefail
sudo iscsiadm -m discovery -t st -p 127.0.0.1:'"${M01_ISCSI_PORT}"' >/dev/null
sudo iscsiadm -m node -T '"${ISCSI_IQN}"' -p 127.0.0.1:'"${M01_ISCSI_PORT}"' --login >/dev/null
sleep 2
DEV=$(ls -t /dev/disk/by-path/*'"${ISCSI_IQN}"'*-lun-0 2>/dev/null | head -1)
[ -n "$DEV" ] || { echo "ERROR: iSCSI device not visible after login"; exit 2; }
echo "DEV=$DEV"
# Fill 1 block with 0xab and write to LBA 0
sudo dd if=/dev/zero bs=4096 count=1 2>/dev/null | tr "\0" "\xab" | sudo dd of="$DEV" bs=4096 count=1 conv=fsync,nocreat
sync
sudo iscsiadm -m node -T '"${ISCSI_IQN}"' -p 127.0.0.1:'"${M01_ISCSI_PORT}"' --logout >/dev/null
echo "WROTE-LBA-0-PATTERN-AB"
' || die "iscsi write failed"
    prim_h_after=$(poll_status_field "$SSH_M01" "${M01_PRIMARY_STATUS_PORT}" H)
    [ -n "${prim_h_after}" ] || die "could not read primary.H after write"
    [ "${prim_h_after}" -gt "${prim_h_before}" ] \
        || die "primary.H did not advance: before=${prim_h_before} after=${prim_h_after} (silent write failure masked?)"
    log "  primary.H advanced ${prim_h_before} → ${prim_h_after}"
    sleep 1  # let replication fan-out complete
    # Logout iscsi (release the device handle so we can stop replica cleanly later)
    $SSH_M01 "sudo iscsiadm -m node -T ${ISCSI_IQN} -p 127.0.0.1:${M01_ISCSI_PORT} --logout >/dev/null" || true
    # Stop replica daemon so m01verify can OpenReadOnly
    $SSH_M02 "sudo pkill -TERM -f '[g]5-blockvolume.*server-id m02-replica' && sleep 2"
    # m01verify reads replica's walstore
    local out
    out=$($SSH_M02 "/tmp/g5-m01verify --walstore ${REMOTE_RUN_DIR}/replica-store/v1.walstore --lba-start 0 --lba-count 1 --block-size 4096 --expected-pattern ab 2>&1")
    log "  m01verify output: ${out}"
    echo "${out}" | grep -q '^OK ' || die "byte-equal verify failed: ${out}"
    # Restart replica for subsequent steps
    start_replica
    sleep 3
    log "  ✓ byte-equal"
}

# §2 #3 — network disconnect catch-up via /status/recovery R/H polling
# Architect findings (round 52): (a) remote SSH bash blocks now use
# `set -euo pipefail` to fail-closed; (b) DEV must be non-empty;
# (c) primary.H must advance after the write OR test fails (catches
# silent-write-failure masking).
verify_network_catchup() {
    log "verify_network_catchup: iptables drop primary→replica WAL port + observe catch-up"
    local prim_h_before prim_h_after
    prim_h_before=$(poll_status_field "$SSH_M01" "${M01_PRIMARY_STATUS_PORT}" H)
    [ -n "${prim_h_before}" ] || die "could not read primary.H before disconnect (status endpoint offline?)"
    log "  pre-disconnect: primary.H=${prim_h_before}"
    # Block primary→replica data port at M02
    $SSH_M02 "sudo iptables -I INPUT -p tcp --dport ${M02_REPLICA_DATA_PORT} -j DROP"
    # Trigger primary write while disconnected (login + write + logout)
    $SSH_M01 'set -euo pipefail
sudo iscsiadm -m node -T '"${ISCSI_IQN}"' -p 127.0.0.1:'"${M01_ISCSI_PORT}"' --login >/dev/null
sleep 2
DEV=$(ls -t /dev/disk/by-path/*'"${ISCSI_IQN}"'*-lun-0 2>/dev/null | head -1)
[ -n "$DEV" ] || { echo "ERROR: iSCSI device not visible after login"; exit 2; }
sudo dd if=/dev/urandom of="$DEV" bs=4096 count=2 conv=fsync,nocreat
sync
sudo iscsiadm -m node -T '"${ISCSI_IQN}"' -p 127.0.0.1:'"${M01_ISCSI_PORT}"' --logout >/dev/null
echo "WROTE-2-BLOCKS"
' || die "iscsi write during disconnect failed"
    prim_h_after=$(poll_status_field "$SSH_M01" "${M01_PRIMARY_STATUS_PORT}" H)
    [ -n "${prim_h_after}" ] || die "could not read primary.H after write"
    [ "${prim_h_after}" -gt "${prim_h_before}" ] \
        || die "primary.H did not advance: before=${prim_h_before} after=${prim_h_after} (silent write failure?)"
    local rep_r
    rep_r=$(poll_status_field "$SSH_M02" "${M02_REPLICA_STATUS_PORT}" R)
    log "  pre-restore: primary.H=${prim_h_after} (advanced from ${prim_h_before}), replica.R=${rep_r}"
    [ -n "${rep_r}" ] && [ "${rep_r}" -lt "${prim_h_after}" ] \
        || die "replica.R should be behind primary.H after disconnect; got R=${rep_r} H=${prim_h_after}"
    # Restore network
    $SSH_M02 "sudo iptables -D INPUT -p tcp --dport ${M02_REPLICA_DATA_PORT} -j DROP"
    # Poll until replica catches up
    wait_until "replica catches up to primary.H=${prim_h_after}" "${CATCHUP_DEADLINE}" \
        "[ \"\$(poll_status_field '$SSH_M02' '${M02_REPLICA_STATUS_PORT}' R)\" -ge \"${prim_h_after}\" ]" \
        || die "replica did not catch up to H=${prim_h_after} within ${CATCHUP_DEADLINE}s"
    # Assert RecoveryDecision was catch_up — empty no longer acceptable
    # per architect round 52: empty decision means we missed the transition,
    # which is suspect; require explicit catch_up evidence
    local decision
    decision=$(poll_status_field "$SSH_M02" "${M02_REPLICA_STATUS_PORT}" RecoveryDecision)
    case "${decision}" in
        catch_up)     log "  RecoveryDecision=catch_up (explicit transition observed)" ;;
        rebuild)      die "expected RecoveryDecision=catch_up; got rebuild (gap exceeded retention?)" ;;
        "")           die "RecoveryDecision empty after observed catch-up — script missed the transition; tighten polling or capture decision history" ;;
        *)            die "unexpected RecoveryDecision=${decision}" ;;
    esac
    log "  ✓ network catch-up converged (R behind by ${prim_h_after}-${rep_r}=$((prim_h_after - rep_r)) before restore)"
}

# §2 #4 — replica process stop/restart catch-up
# Architect findings round 52 same as #3: set -e + DEV assert + H advancement.
verify_restart_catchup() {
    log "verify_restart_catchup: SIGTERM replica + restart same --durable-root"
    local prim_h_before prim_h_after
    prim_h_before=$(poll_status_field "$SSH_M01" "${M01_PRIMARY_STATUS_PORT}" H)
    [ -n "${prim_h_before}" ] || die "could not read primary.H before stop"
    # Trigger primary write while replica is down
    $SSH_M02 "sudo pkill -TERM -f '[g]5-blockvolume.*server-id m02-replica'"
    sleep 2
    $SSH_M01 'set -euo pipefail
sudo iscsiadm -m node -T '"${ISCSI_IQN}"' -p 127.0.0.1:'"${M01_ISCSI_PORT}"' --login >/dev/null
sleep 2
DEV=$(ls -t /dev/disk/by-path/*'"${ISCSI_IQN}"'*-lun-0 2>/dev/null | head -1)
[ -n "$DEV" ] || { echo "ERROR: iSCSI device not visible after login"; exit 2; }
sudo dd if=/dev/urandom of="$DEV" bs=4096 count=2 seek=4 conv=fsync,nocreat
sync
sudo iscsiadm -m node -T '"${ISCSI_IQN}"' -p 127.0.0.1:'"${M01_ISCSI_PORT}"' --logout >/dev/null
echo "WROTE-2-BLOCKS-AT-OFFSET-4"
' || die "iscsi write during replica-down failed"
    prim_h_after=$(poll_status_field "$SSH_M01" "${M01_PRIMARY_STATUS_PORT}" H)
    [ -n "${prim_h_after}" ] || die "could not read primary.H after offline write"
    [ "${prim_h_after}" -gt "${prim_h_before}" ] \
        || die "primary.H did not advance during replica-down write: before=${prim_h_before} after=${prim_h_after}"
    log "  primary.H ${prim_h_before} → ${prim_h_after} during replica-down"
    # Restart replica (same binary, same --durable-root)
    start_replica
    sleep 3
    # Poll until replica catches up
    wait_until "replica catches up post-restart to H=${prim_h_after}" "${CATCHUP_DEADLINE}" \
        "[ \"\$(poll_status_field '$SSH_M02' '${M02_REPLICA_STATUS_PORT}' R)\" -ge \"${prim_h_after}\" ]" \
        || die "replica did not catch up post-restart within ${CATCHUP_DEADLINE}s"
    log "  ✓ restart catch-up converged (replica.R reached ${prim_h_after} after restart)"
}

# §2 #5 — race ×10 stress on G5-4 integration test
verify_race_stress() {
    log "verify_race_stress: TestG54_BinaryWiring x10 -race on m01"
    $SSH_M01 "command -v gcc >/dev/null" || die "gcc not installed on m01 (needed for CGO -race)"
    $SSH_M01 "cd ${REMOTE_BUILD_DIR} && CGO_ENABLED=1 go test -race -count=10 -run TestG54_BinaryWiring ./cmd/blockvolume/ 2>&1" > "${ARTIFACT_DIR}/race-stress.log" \
        || die "race stress failed; see ${ARTIFACT_DIR}/race-stress.log"
    log "  ✓ race ×10"
}

# §2 #6 — full V3 suite green from m01
verify_full_suite() {
    log "verify_full_suite: go test ./... clean from m01"
    $SSH_M01 "cd ${REMOTE_BUILD_DIR} && go test ./... -count=1 -timeout 600s 2>&1" > "${ARTIFACT_DIR}/full-suite.log" \
        || die "full suite has failures; see ${ARTIFACT_DIR}/full-suite.log"
    log "  ✓ full suite green"
}

# --- Main ---

main() {
    log "=== G5-5 m01 hardware first-light ==="
    log "primary: ${M01_HOST} ; replica: ${M02_HOST}"
    log "substrate: ${DURABLE_IMPL} ; ${DURABLE_BLOCKS} blocks × ${DURABLE_BLOCKSIZE} B"
    log "artifacts: ${ARTIFACT_DIR}"

    sync_and_build
    start_cluster
    sleep 3

    verify_cluster_ready
    verify_byte_equal
    verify_network_catchup
    verify_restart_catchup
    verify_race_stress
    verify_full_suite

    stop_cluster
    log "=== G5-5 ALL VERIFY STEPS PASS ==="
}

main "$@"
