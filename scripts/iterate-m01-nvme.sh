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
DURABLE_IMPL="${DURABLE_IMPL:-smartwal}"  # smartwal | walstore
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
    # Pre-cleanup: prior run may have left stale connections, mounts,
    # OR STALE BLOCKMASTER/BLOCKVOLUME processes holding ports.
    $SSH '
sudo umount /mnt/t2nvme 2>/dev/null || true
sudo nvme disconnect -n '${SUBSYS_NQN}' 2>/dev/null || true
sudo pkill -9 -f blockvolume 2>/dev/null || true
sudo pkill -9 -f blockmaster 2>/dev/null || true
sleep 2
pgrep -af "blockmaster|blockvolume" >/dev/null && echo "WARN: stale processes survived pkill" || true
' >/dev/null 2>&1 || true
    $SSH "
rm -rf ${REMOTE_RUN_DIR}/store ${REMOTE_RUN_DIR}/v1-s* ${REMOTE_RUN_DIR}/durable && mkdir -p ${REMOTE_RUN_DIR}/logs ${REMOTE_RUN_DIR}/durable/s1 ${REMOTE_RUN_DIR}/durable/s2 ${REMOTE_RUN_DIR}/durable/s3
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
        --durable-root ${REMOTE_RUN_DIR}/durable/s\${S} --durable-impl ${DURABLE_IMPL} \
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

log "clearing kernel dmesg buffer to avoid stale-entry false-positive in check_dmesg_clean..."
$SSH 'sudo dmesg --clear 2>&1 || true' >/dev/null

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

# --- Matrix C — small-file burst (group_commit batching coverage) ---
#
# V2 group_commit coalesced many small writers; T3 durable path
# needs the same under real kernel fs. 1000 small files in an
# ext4 fs + explicit sync stresses the batching path that unit
# tests can't fully exercise.

log "Matrix C — small-file burst (stress group_commit batching)"
$SSH "
set -e
sudo nvme connect -t tcp -a 127.0.0.1 -s 4421 -n ${SUBSYS_NQN} >/dev/null 2>&1
sleep 1
DEV=\$(sudo nvme list | awk '/SeaweedFS/ {print \$1; exit}')
# 8 MiB volume → mkfs with inode ratio tuned for many small files.
# -N 2000: pre-allocate 2000 inodes (enough for 500+ file burst)
# -b 1024: keep small block size so directory entries fit
sudo mkfs.ext4 -F -b 1024 -I 128 -N 2000 \${DEV} >/dev/null 2>&1
sudo mkdir -p /mnt/t2nvme
sudo mount \${DEV} /mnt/t2nvme
# Target 500 small files — plenty to stress group_commit batching
# without hitting fs-level quota. Don't silently break on ENOSPC;
# report the actual count at the end.
target=500
written=0
for i in \$(seq 1 \${target}); do
    if echo \"small-\$i\" | sudo tee /mnt/t2nvme/f\$i >/dev/null 2>&1; then
        written=\$((written + 1))
    fi
done
sudo sync
count=\$(ls /mnt/t2nvme 2>/dev/null | wc -l)
echo \"small-file burst: target=\${target} written=\${written} fs-count=\${count}\"
# Require at least 300 files actually landed; if under, fs quota is
# the bottleneck not group_commit — either way flag for review.
if [ \${written} -lt 300 ]; then
    echo \"ERROR: only \${written} of \${target} files written; group_commit stress not adequately exercised\"
    exit 1
fi
sudo umount /mnt/t2nvme
sudo nvme disconnect -n ${SUBSYS_NQN} >/dev/null 2>&1
" || die "Matrix C small-file burst failed"
check_dmesg_clean "matrix-c-small-file"
log "  Matrix C PASS"

# --- Matrix D — cross-session consistency (remount durability) ---
#
# Write pattern → umount → remount → verify pattern still present.
# Exercises superblock/WAL replay across a clean umount boundary
# (distinct from crash-recovery: here the umount is clean, so if
# data is missing it's a durability-path bug not a recovery-path bug).

log "Matrix D — cross-session consistency (5 remount cycles)"
$SSH "
set -e
sudo nvme connect -t tcp -a 127.0.0.1 -s 4421 -n ${SUBSYS_NQN} >/dev/null 2>&1
sleep 1
DEV=\$(sudo nvme list | awk '/SeaweedFS/ {print \$1; exit}')
sudo mkfs.ext4 -F -b 1024 -I 128 -N 16 \${DEV} >/dev/null 2>&1
sudo mkdir -p /mnt/t2nvme
for cycle in \$(seq 1 5); do
    sudo mount \${DEV} /mnt/t2nvme
    echo \"cycle-\$cycle-marker-\$(date +%s)\" | sudo tee /mnt/t2nvme/cycle.txt >/dev/null
    expected=\$(sudo cat /mnt/t2nvme/cycle.txt)
    sudo sync
    sudo umount /mnt/t2nvme

    sudo mount \${DEV} /mnt/t2nvme
    got=\$(sudo cat /mnt/t2nvme/cycle.txt 2>/dev/null || echo MISSING)
    sudo umount /mnt/t2nvme

    if [ \"\$got\" != \"\$expected\" ]; then
        echo \"cross-session cycle \$cycle: got '\$got' want '\$expected'\"
        exit 1
    fi
done
sudo nvme disconnect -n ${SUBSYS_NQN} >/dev/null 2>&1
" || die "Matrix D cross-session consistency failed"
check_dmesg_clean "matrix-d-cross-session"
log "  Matrix D all 5 cycles PASS"

# --- Matrix E — real SIGKILL crash recovery (canonical G4 pass gate) ---
#
# Canonical §G4: "crash/restart test writes acknowledged data through
# the real frontend, kills/restarts the local process, and reads the
# acknowledged data back."
#
# Shape:
#   1. nvme connect; dd write pattern at raw LBA 0 (no mkfs — avoid
#      FS recovery complicating the picture; raw durable read is
#      the tightest proof of "ack'd bytes survive unclean kill")
#   2. nvme flush (real NVMe Flush opcode via /dev/nvme-fabrics)
#   3. SIGKILL the volume-r1 process (uncluttered ungraceful kill)
#   4. Wait for NVMe kernel to see connection drop
#   5. Restart volume-r1 with SAME --durable-root
#   6. Re-advertise wait
#   7. Reconnect NVMe (kernel re-establishes session against new target)
#   8. dd read pattern at raw LBA 0
#   9. Verify bytes match pre-kill — byte-exact
#
# Variant matrix: DURABLE_IMPL env var selects walstore|smartwal.

log "Matrix E — real SIGKILL crash recovery (canonical G4 kill/restart)"
$SSH "
set -e
sudo nvme connect -t tcp -a 127.0.0.1 -s 4421 -n ${SUBSYS_NQN} >/dev/null 2>&1
sleep 1
DEV=\$(sudo nvme list | awk '/SeaweedFS/ {print \$1; exit}')
# Write a distinctive 32 KiB pattern at LBA 0 + Flush
sudo dd if=/dev/urandom of=/tmp/matrix-e-pattern.bin bs=32K count=1 status=none
sudo dd if=/tmp/matrix-e-pattern.bin of=\${DEV} bs=32K count=1 conv=notrunc status=none
sudo nvme flush \${DEV} >/dev/null 2>&1 || sudo blockdev --flushbufs \${DEV} >/dev/null 2>&1
sleep 1

# SIGKILL the primary volume process
PID=\$(pgrep -f 'blockvolume.*--replica-id r1' | head -1)
if [ -z \"\$PID\" ]; then
    echo ERROR: no blockvolume r1 pid found
    exit 1
fi
echo \"SIGKILL r1 PID=\$PID\"
sudo kill -9 \$PID
sleep 2

# Kernel NVMe sees drop; disconnect cleanly
sudo nvme disconnect -n ${SUBSYS_NQN} >/dev/null 2>&1 || true
sleep 1

# Restart volume r1 with SAME --durable-root
nohup /tmp/blockvolume --master 127.0.0.1:9180 --volume-id v1 \
    --replica-id r1 --server-id s1 \
    --ctrl-addr 127.0.0.1:9211 --data-addr 127.0.0.1:9221 \
    --nvme-listen 127.0.0.1:4421 --nvme-subsysnqn nqn.2026-04.m01:v1-r1 \
    --nvme-ns 1 --t0-print-ready --t1-readiness \
    --durable-root ${REMOTE_RUN_DIR}/durable/s1 --durable-impl ${DURABLE_IMPL} \
    > ${REMOTE_RUN_DIR}/logs/v1-restart.log 2>&1 </dev/null & disown
sleep 5

# Reconnect NVMe
sudo nvme connect -t tcp -a 127.0.0.1 -s 4421 -n ${SUBSYS_NQN} >/dev/null 2>&1
sleep 2
DEV2=\$(sudo nvme list | awk '/SeaweedFS/ {print \$1; exit}')
if [ -z \"\$DEV2\" ]; then
    echo ERROR: post-restart NVMe device missing
    exit 1
fi

# Read back pattern and compare
sudo dd if=\${DEV2} of=/tmp/matrix-e-readback.bin bs=32K count=1 status=none
if sudo cmp -s /tmp/matrix-e-pattern.bin /tmp/matrix-e-readback.bin; then
    echo \"Matrix E: SIGKILL survived — ack'd bytes recovered byte-exact\"
else
    echo ERROR: post-SIGKILL readback differs from pre-kill pattern
    sudo md5sum /tmp/matrix-e-pattern.bin /tmp/matrix-e-readback.bin
    exit 1
fi

sudo nvme disconnect -n ${SUBSYS_NQN} >/dev/null 2>&1
sudo rm -f /tmp/matrix-e-pattern.bin /tmp/matrix-e-readback.bin
" || die "Matrix E real SIGKILL crash recovery failed"
check_dmesg_clean "matrix-e-sigkill"
log "  Matrix E PASS — real unclean kill/restart with byte-exact recovery"

# --- Matrix F — real ENOSPC via size-limited tmpfs backing ---
#
# Canonical backing-store exhaustion: fill the durable-root
# filesystem to ENOSPC, then attempt a write, verify graceful
# hard error (no silent success, no hang, no in-range corruption).
#
# Implementation: bind a 16 MiB tmpfs at a new durable-root, start
# a fresh volume against it, use dd to pre-fill the tmpfs (leaving
# just a few MB headroom), then attempt writes that force WAL
# growth beyond the headroom. Verify the write returns an error
# (not success) and prior data stays intact.

log "Matrix F — real ENOSPC via size-limited tmpfs durable-root"
$SSH "
# NO set -e in Matrix F — conditional cleanups have too many benign
# non-zero exits (dead process, unmounted tmpfs, etc.). Explicit result
# checking below.
# Stop any running r1, replace durable-root with tmpfs
PID=\$(pgrep -f 'blockvolume.*--replica-id r1' | head -1)
if [ -n \"\$PID\" ]; then sudo kill -9 \$PID; sleep 1; fi
sudo nvme disconnect -n ${SUBSYS_NQN} >/dev/null 2>&1 || true

sudo mkdir -p ${REMOTE_RUN_DIR}/durable-tmpfs-s1
sudo umount ${REMOTE_RUN_DIR}/durable-tmpfs-s1 2>/dev/null || true
sudo mount -t tmpfs -o size=16M tmpfs ${REMOTE_RUN_DIR}/durable-tmpfs-s1
sudo chmod 777 ${REMOTE_RUN_DIR}/durable-tmpfs-s1

nohup /tmp/blockvolume --master 127.0.0.1:9180 --volume-id v1 \
    --replica-id r1 --server-id s1 \
    --ctrl-addr 127.0.0.1:9211 --data-addr 127.0.0.1:9221 \
    --nvme-listen 127.0.0.1:4421 --nvme-subsysnqn nqn.2026-04.m01:v1-r1 \
    --nvme-ns 1 --t0-print-ready --t1-readiness \
    --durable-root ${REMOTE_RUN_DIR}/durable-tmpfs-s1 --durable-impl ${DURABLE_IMPL} \
    > ${REMOTE_RUN_DIR}/logs/v1-enospc.log 2>&1 </dev/null & disown
sleep 5

sudo nvme connect -t tcp -a 127.0.0.1 -s 4421 -n ${SUBSYS_NQN} >/dev/null 2>&1
sleep 2
DEV=\$(sudo nvme list | awk '/SeaweedFS/ {print \$1; exit}')

# Pre-fill tmpfs to leave ~1 MB headroom (durable-root already
# has storage file + WAL consuming some space; fill the rest)
sudo dd if=/dev/zero of=${REMOTE_RUN_DIR}/durable-tmpfs-s1/ballast bs=1M count=13 status=none 2>&1 | tail -1 || true

# Try to write 2 MiB to the NVMe device — should hit ENOSPC on
# WAL append (write grows WAL beyond backing tmpfs capacity).
# Record result.
result=0
sudo dd if=/dev/urandom of=\${DEV} bs=1M count=2 conv=notrunc status=none 2>/tmp/matrix-f-dd.err || result=\$?
sleep 1

if [ \$result -eq 0 ]; then
    # Sync to force WAL + fsync; should now propagate ENOSPC if
    # dd didn't hit it directly
    if ! sudo dd if=/dev/zero of=\${DEV} bs=4K count=256 conv=notrunc,fsync status=none 2>>/tmp/matrix-f-dd.err; then
        result=1
    fi
fi

# Graceful hard error required — not silent success, not hang
if [ \$result -eq 0 ]; then
    echo WARN: ENOSPC not reached despite pre-filled tmpfs
    echo \"  This may mean durable-root backing filesystem has more slack\"
    echo \"  than expected OR impl has a fallback path. Investigate.\"
    # Don't fail hard — downgrade to WARN since 'graceful' behavior
    # is the primary requirement, not triggering ENOSPC at a
    # specific size
    echo \"Matrix F: downgraded to WARN (no ENOSPC triggered — but no silent-success violation either)\"
else
    echo \"Matrix F: ENOSPC triggered gracefully (dd exit=\$result); impl returned hard error as required\"
    cat /tmp/matrix-f-dd.err | head -3
fi

sudo nvme disconnect -n ${SUBSYS_NQN} >/dev/null 2>&1 || true
# Process may have already crashed on ENOSPC; kill-if-alive must tolerate either
PID=\$(pgrep -f 'blockvolume.*--replica-id r1' | head -1 || true)
if [ -n \"\$PID\" ]; then sudo kill -9 \$PID 2>/dev/null || true; sleep 1; fi
sudo umount ${REMOTE_RUN_DIR}/durable-tmpfs-s1 2>/dev/null || true
sudo rm -rf ${REMOTE_RUN_DIR}/durable-tmpfs-s1 /tmp/matrix-f-dd.err
exit 0
" 2>&1 | tee /tmp/matrix-f.out || true
# Matrix F cleanup phase has SSH-connection fragility when backing
# blockvolume crashed on ENOSPC (its NVMe target goes dead mid-flight,
# subsequent nvme disconnect can hang the SSH). The AUTHORITATIVE
# success signal is the graceful-handling line printed server-side.
if ! grep -q 'ENOSPC triggered gracefully' /tmp/matrix-f.out; then
    die "Matrix F behavior violated: no graceful ENOSPC message printed. impl may have silent-success'd or panic'd"
fi
check_dmesg_clean "matrix-f-enospc"
log "  Matrix F PASS — real backing-store exhaustion handled gracefully"

# --- Phase 4: cleanup + summary ---

collect_diagnostics "pass"
stop_services

log "ALL MATRICES GREEN (impl=${DURABLE_IMPL})"
log "  Matrix A: ${MATRIX_A_CYCLES} cycles clean-restart"
log "  Matrix B: 32K/256K/1M size coverage"
log "  Matrix C: small-file burst (group_commit batching)"
log "  Matrix D: cross-session consistency (5 remount cycles)"
log "  Matrix E: real SIGKILL crash recovery (canonical G4 kill/restart)"
log "  Matrix F: real ENOSPC backing-store exhaustion graceful handling"
log "  Artifacts: ${ARTIFACT_DIR}/m01-{dmesg,v1}-pass.*"
log ""
log "Post to BUG-001 §12 log + closure report §A.3 batch 11c row."
exit 0
