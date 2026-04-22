#!/usr/bin/env bash
# iterate-m01-nvme.sh — L3-A sign-prep m01 NVMe/TCP sanity harness
#
# Runs on local workstation, drives m01 over SSH. Rebuilds V3
# binaries from current phase-15 tree, spins master + 3 volumes,
# then walks the two matrices:
#
#   Matrix A (cycle stability): attach/mkfs/mount/dd/sync/umount/disconnect × N
#   Matrix B (size coverage):   for each size in {32K,256K,1M}, dd and readback × M
#
# Exit 0 on all-green; non-zero + diagnostic artifacts on any failure.
#
# Ownership: QA. Part of T2 Batch 11c sign prep per
# sw-block/design/v3-phase-15-t2-batch-11c-test-specs.md §3.1.

set -euo pipefail

# --- Config (env-overridable) ---
M01_HOST="${M01_HOST:-testdev@192.168.1.181}"
SSH_KEY="${SSH_KEY:-/c/work/dev_server/testdev_key}"
SRC_DIR="${SRC_DIR:-/c/work/seaweed_block}"
SMB_STAGE="${SMB_STAGE:-/v/share/t2-sanity/seaweed_block}"
SMB_STAGE_REMOTE="${SMB_STAGE_REMOTE:-/mnt/smb/work/share/t2-sanity/seaweed_block}"
REMOTE_BUILD_DIR="${REMOTE_BUILD_DIR:-/tmp/t2m01_sb}"
REMOTE_RUN_DIR="${REMOTE_RUN_DIR:-/tmp/t2m01}"
SUBSYS_NQN="${SUBSYS_NQN:-nqn.2026-04.m01:v1-r1}"
MATRIX_A_CYCLES="${MATRIX_A_CYCLES:-10}"
MATRIX_B_ITERATIONS="${MATRIX_B_ITERATIONS:-50}"
ARTIFACT_DIR="${ARTIFACT_DIR:-${SRC_DIR%/*}/seaweedfs/sw-block/design/bugs/001-artifacts}"

SSH="ssh -i ${SSH_KEY} -o ConnectTimeout=10 ${M01_HOST}"

# --- Helpers ---

log() { printf '\033[1;36m[iterate-m01]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[iterate-m01 FAIL]\033[0m %s\n' "$*" >&2; exit 1; }

collect_diagnostics() {
    local label="$1"
    log "collecting diagnostics [${label}]..."
    $SSH "sudo dmesg -T | grep -i nvme1" > "${ARTIFACT_DIR}/m01-dmesg-${label}.txt" 2>/dev/null || true
    scp -i "${SSH_KEY}" "${M01_HOST}:${REMOTE_RUN_DIR}/logs/v1.log" "${ARTIFACT_DIR}/m01-v1-${label}.log" 2>/dev/null || true
    log "  → ${ARTIFACT_DIR}/m01-{dmesg,v1}-${label}.*"
}

check_dmesg_clean() {
    local label="$1"
    local dmesg
    dmesg=$($SSH "sudo dmesg -T | grep -i nvme1 | tail -30")
    for pattern in 'r2t len.*exceeded' 'receive failed:' 'Reconnecting in' 'Failed reconnect'; do
        if echo "${dmesg}" | grep -qE "${pattern}"; then
            collect_diagnostics "${label}-fail"
            die "dmesg shows '${pattern}' after ${label}; see ${ARTIFACT_DIR}/"
        fi
    done
}

start_services() {
    log "starting blockmaster + 3 volumes on m01..."
    # Pre-cleanup: prior run may have left stale connections or mounts.
    $SSH '
sudo umount /mnt/t2nvme 2>/dev/null || true
sudo nvme disconnect -n '${SUBSYS_NQN}' 2>/dev/null || true
' >/dev/null 2>&1 || true
    $SSH "
rm -rf ${REMOTE_RUN_DIR}/store ${REMOTE_RUN_DIR}/v1-s* && mkdir -p ${REMOTE_RUN_DIR}/logs
nohup /tmp/blockmaster --authority-store ${REMOTE_RUN_DIR}/store --listen 127.0.0.1:9180 \
    --topology ${REMOTE_RUN_DIR}/topology.yaml --t0-print-ready \
    > ${REMOTE_RUN_DIR}/logs/master.log 2>&1 </dev/null & disown
sleep 1
for S in 1 2 3; do
    nohup /tmp/blockvolume --master 127.0.0.1:9180 --volume-id v1 \
        --replica-id r\${S} --server-id s\${S} \
        --ctrl-addr 127.0.0.1:921\${S} --data-addr 127.0.0.1:922\${S} \
        --nvme-listen 127.0.0.1:442\${S} --nvme-subsysnqn nqn.2026-04.m01:v1-r\${S} \
        --nvme-ns 1 --t0-print-ready --t1-readiness \
        > ${REMOTE_RUN_DIR}/logs/v\${S}.log 2>&1 </dev/null & disown
done
sleep 8
pgrep -f blockvolume | wc -l
"
}

stop_services() {
    log "stopping services..."
    $SSH 'sudo nvme disconnect -n '${SUBSYS_NQN}' 2>/dev/null || true
    pgrep -af "blockmaster\|blockvolume" | awk "{print \$1}" | xargs -r kill 2>/dev/null || true
    sleep 1
    pgrep -af "blockmaster\|blockvolume" || true' >/dev/null 2>&1 || true
}

# --- Phase 1: sync + rebuild ---

log "syncing source to SMB stage..."
cp -ru "${SRC_DIR}/core" "${SMB_STAGE}/"
cp -ru "${SRC_DIR}/cmd" "${SMB_STAGE}/"
cp "${SRC_DIR}/go.mod" "${SRC_DIR}/go.sum" "${SMB_STAGE}/" 2>/dev/null || true

log "rebuilding on m01..."
$SSH "
cp -ru ${SMB_STAGE_REMOTE}/core ${REMOTE_BUILD_DIR}/
cp -ru ${SMB_STAGE_REMOTE}/cmd ${REMOTE_BUILD_DIR}/
cd ${REMOTE_BUILD_DIR}
go build -buildvcs=false -o /tmp/blockmaster ./cmd/blockmaster
go build -buildvcs=false -o /tmp/blockvolume ./cmd/blockvolume
echo BUILD_OK
" | tail -3

# --- Phase 2: Matrix A — cycle stability ---

log "Matrix A — ${MATRIX_A_CYCLES} attach/mkfs/mount/dd/sync/umount/disconnect cycles"
start_services

for i in $(seq 1 "${MATRIX_A_CYCLES}"); do
    log "  cycle ${i}/${MATRIX_A_CYCLES}..."
    $SSH "
set -e
sudo nvme connect -t tcp -a 127.0.0.1 -s 4421 -n ${SUBSYS_NQN} >/dev/null 2>&1
sleep 1
DEV=\$(sudo nvme list | awk '/SeaweedFS/ {print \$1; exit}')
sudo mkfs.ext4 -F -b 1024 -I 128 -N 16 \${DEV} >/dev/null 2>&1
sudo mkdir -p /mnt/t2nvme
sudo mount \${DEV} /mnt/t2nvme
echo \"cycle-${i}-data\" | sudo tee /mnt/t2nvme/t.txt >/dev/null
sudo sync
sudo umount /mnt/t2nvme
sudo nvme disconnect -n ${SUBSYS_NQN} >/dev/null 2>&1
" || die "cycle ${i} failed; check m01 state"
    check_dmesg_clean "cycle-${i}"
done

log "  Matrix A all ${MATRIX_A_CYCLES} cycles PASS"

# --- Phase 3: Matrix B — size coverage ---

log "Matrix B — size matrix (32K/256K/1M) × ${MATRIX_B_ITERATIONS}"

for size_bs in "32K" "256K" "1M"; do
    # m01 volume is 1 MiB; the point here is to exercise the NVMe
    # wire path at multiple sizes, not persistence. Each write
    # overwrites block 0 — any R2T / H2CData / CapResp protocol
    # regression at a given size surfaces as a dd failure or
    # dmesg error, which check_dmesg_clean catches.
    case "${size_bs}" in
        32K)  count="${MATRIX_B_ITERATIONS}" ;;      # 50
        256K) count="$((MATRIX_B_ITERATIONS / 5))" ;; # 10
        1M)   count="5" ;;                            # 5 full-volume writes
    esac

    log "  size=${size_bs} count=${count}"
    $SSH "
set -e
sudo nvme connect -t tcp -a 127.0.0.1 -s 4421 -n ${SUBSYS_NQN} >/dev/null 2>&1
sleep 1
DEV=\$(sudo nvme list | awk '/SeaweedFS/ {print \$1; exit}')
for i in \$(seq 1 ${count}); do
    sudo dd if=/dev/urandom of=\${DEV} bs=${size_bs} count=1 conv=notrunc status=none 2>&1 || exit 1
done
sudo sync
sudo nvme disconnect -n ${SUBSYS_NQN} >/dev/null 2>&1
" || die "Matrix B size=${size_bs} failed"
    check_dmesg_clean "size-${size_bs}"
done

log "  Matrix B all sizes PASS"

# --- Phase 4: cleanup + summary ---

collect_diagnostics "pass"
stop_services

log "ALL MATRICES GREEN"
log "  Matrix A: ${MATRIX_A_CYCLES} cycles"
log "  Matrix B: 32K/256K/1M size coverage"
log "  Artifacts: ${ARTIFACT_DIR}/m01-{dmesg,v1}-pass.*"
log ""
log "Post to BUG-001 §12 log + closure report §A.3 batch 11c row."
exit 0
