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
# Routable IPs for cross-host advertise (--data-addr / --ctrl-addr).
# Extracted from M01_HOST/M02_HOST. Used as the BIND addr too in
# Tier 2: binding to a specific local-interface IP works on Linux,
# AND ensures the daemon ADVERTISES a routable IP to master via
# heartbeat (peer.DataAddr in AssignmentFact). Binding 0.0.0.0
# would advertise "0.0.0.0:port" and break primary→replica dial
# (0.0.0.0 is not a routable destination). QA round 54 finding.
M01_IP="${M01_HOST##*@}"
M02_IP="${M02_HOST##*@}"
SSH_KEY="${SSH_KEY:-/c/work/dev_server/testdev_key}"
SRC_DIR="${SRC_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}"
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

# Two SSH option sets — DIFFERENT requirements for two patterns:
#
# 1. SSH_OPTS  — for `ssh host "CMD"` (command-as-argument).
#    Includes `-n` to close ssh's stdin so the connection doesn't
#    wait on terminal input. Used by collect_diagnostics, status
#    polls, simple one-shot commands.
#
# 2. SSH_OPTS_LAUNCH — for `ssh host bash -s <<EOF ... EOF`
#    (script-via-stdin pattern, used by launch_m01/launch_m02).
#    MUST NOT include `-n` because `-n` redirects ssh stdin from
#    /dev/null, which means `bash -s` reads EOF immediately and
#    exits before running the heredoc body — the daemon never
#    starts. QA round 53 isolated this: dbg.sh on m01 confirmed
#    `bash -s` works without `-n` and silently does nothing with
#    `-n`. The PTY-hang protection comes from `-T` alone, which
#    both option sets keep.
#
# Both sets keep `-T` (no PTY allocation) and `BatchMode=yes`
# (refuse to prompt for password/key passphrase).
SSH_OPTS_BASE="-T -i ${SSH_KEY} -o ConnectTimeout=10 -o StrictHostKeyChecking=no -o BatchMode=yes"
SSH_OPTS="${SSH_OPTS_BASE} -n"
SSH_OPTS_LAUNCH="${SSH_OPTS_BASE}"
SSH_M01="ssh ${SSH_OPTS} ${M01_HOST}"
SSH_M02="ssh ${SSH_OPTS} ${M02_HOST}"

# SSH_LAUNCH wraps the *_HOST ssh command with a hard timeout for
# the launch-and-detach pattern (start_master / start_primary /
# start_replica). Even with -T -n + setsid + nohup + disown + exit 0,
# Windows OpenSSH occasionally fails to release the connection. The
# 15s wall-clock cap means we move on regardless — the remote process
# is already detached, so the only risk is a stale ssh client process
# on the local box, which is harmless.
#
# Falls back to a no-op wrapper if `timeout` isn't installed (Git
# Bash without GNU coreutils — install via `pacman -S coreutils` or
# upgrade Git for Windows).
if command -v timeout >/dev/null 2>&1; then
    LAUNCH_TIMEOUT_PREFIX="timeout --preserve-status 15"
else
    LAUNCH_TIMEOUT_PREFIX=""
fi

# --- Helpers ---

log() { printf '\033[1;36m[g5-5]\033[0m %s\n' "$*" >&2; }
die() { printf '\033[1;31m[g5-5 FAIL]\033[0m %s\n' "$*" >&2; collect_diagnostics fail; exit 1; }

# launch_m01 / launch_m02 — accept a script body on stdin and execute
# it via `bash -s` either locally (LOCAL_MODE=1) or remote ssh
# (LOCAL_MODE=0). Used by start_master / start_primary / start_replica
# to launch background daemons safely.
#
# Architect option (A) round 53: `bash -s <<EOF ... EOF` avoids the
# `\<newline>` line-continuation hazard that plagued the prior
# double-quoted-ssh-arg form. With `bash -s`, the heredoc body is
# piped to remote bash's stdin and executed line-by-line with literal
# newlines preserved — `&` and `disown` stay on separate lines no
# matter what the local shell does to the script source.
#
# Hard-capped at 15s by LAUNCH_TIMEOUT_PREFIX (if `timeout` available).
# If launch ssh hangs, we move on; remote process is already detached.
launch_m01() {
    if [ "${LOCAL_MODE}" = "1" ]; then
        ${LAUNCH_TIMEOUT_PREFIX} bash -s
    else
        # SSH_OPTS_LAUNCH (no -n) — bash -s reads from stdin; -n
        # would redirect stdin from /dev/null and bash would EOF.
        ${LAUNCH_TIMEOUT_PREFIX} ssh ${SSH_OPTS_LAUNCH} ${M01_HOST} bash -s
    fi
}

launch_m02() {
    if [ "${LOCAL_MODE}" = "1" ]; then
        ${LAUNCH_TIMEOUT_PREFIX} bash -s
    else
        ${LAUNCH_TIMEOUT_PREFIX} ssh ${SSH_OPTS_LAUNCH} ${M02_HOST} bash -s
    fi
}

mkdir -p "${ARTIFACT_DIR}"

# Two-tier test mode (set BEFORE first SSH-or-bash command):
#   Tier 1 (LOCAL_MODE=1): single Linux host (e.g. WSL Ubuntu)
#     - both "m01" + "M02" processes on localhost
#     - no SSH overhead, no cross-node sync
#     - iSCSI tests skip if iscsiadm not installed
#     - use for fast iteration during dev
#   Tier 2 (default, LOCAL_MODE=0): m01 (192.168.1.181) + M02 (192.168.1.184)
#     - real network + real iSCSI/NVMe kernel + real cross-host wire
#     - use for deployment-realism close evidence
LOCAL_MODE="${LOCAL_MODE:-0}"
if [ "${LOCAL_MODE}" = "1" ]; then
    log "LOCAL_MODE=1 → Tier 1: both processes on localhost (no SSH)"
    SSH_M01="bash -c"
    SSH_M02="bash -c"
    M02_MASTER_TARGET="127.0.0.1"
    # Tier 1: bind+advertise on loopback (single host).
    M01_PRIMARY_BIND_IP="127.0.0.1"
    M02_REPLICA_BIND_IP="127.0.0.1"
else
    log "Tier 2: m01 cross-node (override with LOCAL_MODE=1 for fast local iteration)"
    M02_MASTER_TARGET="${M01_IP}"
    # Tier 2 (architect round 54): bind+advertise on each host's
    # routable IP. The daemon advertises the literal --data-addr /
    # --ctrl-addr to master heartbeat, master mints AssignmentFact.peers
    # with the same string, primary dials it. 0.0.0.0 advertise breaks
    # cross-host (primary tries to dial 0.0.0.0:port → unroutable).
    M01_PRIMARY_BIND_IP="${M01_IP}"
    M02_REPLICA_BIND_IP="${M02_IP}"
fi
HAS_ISCSI=0
if [ "${LOCAL_MODE}" = "1" ]; then
    if command -v iscsiadm >/dev/null 2>&1; then HAS_ISCSI=1; fi
else
    HAS_ISCSI=1  # assume m01/M02 have iscsiadm; verified at first iSCSI test
fi
[ "${HAS_ISCSI}" = "0" ] && log "  (iscsiadm not present locally → criteria #2/#3/#4 will skip with explicit message)"

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

# wait_until_byte_equal — poll-cycles m01verify against the replica's
# walstore until the LBA's bytes match the expected pattern, or the
# deadline expires. Each cycle: stop replica daemon (so OpenReadOnly
# is honest about the no-concurrent-writer assumption), invoke
# m01verify, restart replica. The "replica running" gap between
# cycles is what gives the catch-up ship traffic time to land.
#
# Caller must ensure the replica daemon is RUNNING at entry (we kill
# + restart). Caller is also responsible for the post-call replica
# state (we leave it RUNNING on both success and timeout, so the
# next test step can use the cluster).
#
# Usage: wait_until_byte_equal <lba> <hex-byte-pattern> <deadline_s> <desc>
# Pattern is a single hex byte (e.g. "ab"), per m01verify --expected-pattern.
wait_until_byte_equal() {
    local lba="$1" pattern="$2" deadline_s="$3" desc="$4"
    local end=$(( $(date +%s) + deadline_s ))
    local last_out=""
    while [ "$(date +%s)" -lt "${end}" ]; do
        # Stop replica so m01verify's OpenReadOnly assumption holds.
        $SSH_M02 "sudo killall -TERM g5-blockvolume 2>/dev/null; sleep 1" >/dev/null 2>&1 || true
        local out
        out=$($SSH_M02 "/tmp/g5-m01verify --walstore ${REMOTE_RUN_DIR}/replica-store/v1.bin --lba-start ${lba} --lba-count 1 --block-size 4096 --expected-pattern ${pattern} 2>&1" || true)
        last_out="${out}"
        # Restart replica so ship traffic can resume + cluster stays up.
        start_replica >/dev/null 2>&1 || true
        if echo "${out}" | grep -q '^OK '; then
            log "  byte-equal ${desc} lba=${lba} pattern=${pattern} ✓ ($(($(date +%s) - (end - deadline_s)))s)"
            return 0
        fi
        sleep 3  # let new ships arrive between attempts
    done
    log "wait_until_byte_equal(${desc}) TIMEOUT lba=${lba} pattern=${pattern} after ${deadline_s}s; last=${last_out}"
    return 1
}

# iscsi_write_pattern — drive a kernel iSCSI write at the given LBA
# with N blocks of the supplied 0xHH hex pattern. Login + write +
# logout under set -euo pipefail with explicit failure modes.
# Usage: iscsi_write_pattern <lba> <count> <hex-pattern>
# Total bytes written = count * 4096.
iscsi_write_pattern() {
    local lba="$1" count="$2" pattern="$3"
    local bytes=$(( count * 4096 ))
    $SSH_M01 'set -euo pipefail
sudo iscsiadm -m discovery -t st -p 127.0.0.1:'"${M01_ISCSI_PORT}"' >/dev/null
sudo iscsiadm -m node -T '"${ISCSI_IQN}"' -p 127.0.0.1:'"${M01_ISCSI_PORT}"' --login >/dev/null
sleep 2
DEV=$(ls -t /dev/disk/by-path/*'"${ISCSI_IQN}"'*-lun-0 2>/dev/null | head -1)
[ -n "$DEV" ] || { echo "ERROR: iSCSI device not visible after login"; exit 2; }
python3 -c "import sys; sys.stdout.buffer.write(b\"\x'"${pattern}"'\" * '"${bytes}"')" | sudo dd of="$DEV" bs=4096 count='"${count}"' seek='"${lba}"' conv=fsync,nocreat
sync
sudo iscsiadm -m node -T '"${ISCSI_IQN}"' -p 127.0.0.1:'"${M01_ISCSI_PORT}"' --logout >/dev/null
echo "WROTE-LBA-'"${lba}"'-COUNT-'"${count}"'-PATTERN-'"${pattern}"'"
'
}

# --- Phase 1: sync + build ---

sync_and_build() {
    if [ "${LOCAL_MODE}" = "1" ]; then
        if [ -n "${PREBUILT_BIN_DIR:-}" ] && [ -x "${PREBUILT_BIN_DIR}/g5-blockmaster" ]; then
            log "phase 1: use pre-built binaries from PREBUILT_BIN_DIR=${PREBUILT_BIN_DIR}"
            cp "${PREBUILT_BIN_DIR}/g5-blockmaster" /tmp/g5-blockmaster
            cp "${PREBUILT_BIN_DIR}/g5-blockvolume" /tmp/g5-blockvolume
            cp "${PREBUILT_BIN_DIR}/g5-m01verify" /tmp/g5-m01verify
            chmod +x /tmp/g5-blockmaster /tmp/g5-blockvolume /tmp/g5-m01verify
            return
        fi
        log "phase 1: build binaries locally (LOCAL_MODE — no sync, no scp)"
        log "  hint: cross-compile on Windows + set PREBUILT_BIN_DIR if WSL Go is too old (project needs go 1.21+)"
        cd "${SRC_DIR}" && \
            go build -o /tmp/g5-blockmaster ./cmd/blockmaster/ && \
            go build -o /tmp/g5-blockvolume ./cmd/blockvolume/ && \
            go build -tags m01verify -o /tmp/g5-m01verify ./cmd/m01verify/ \
            || die "local build failed (try PREBUILT_BIN_DIR=/path/to/cross-compiled-bins)"
        return
    fi
    # Tier 2 PREBUILT_BIN_DIR fast-path (architect option (a) round 53):
    # If the operator has pre-built the 3 binaries (e.g. on a Windows
    # workstation cross-compiled GOOS=linux, or fetched from a CI
    # artifact), skip the source-tar + remote-build cycle entirely.
    # Just scp the binaries to m01:/tmp/ and onward to M02.
    if [ -n "${PREBUILT_BIN_DIR:-}" ]; then
        log "phase 1: use pre-built binaries from PREBUILT_BIN_DIR=${PREBUILT_BIN_DIR}"
        for bin in g5-blockmaster g5-blockvolume g5-m01verify; do
            [ -x "${PREBUILT_BIN_DIR}/${bin}" ] || \
                die "PREBUILT_BIN_DIR set but ${PREBUILT_BIN_DIR}/${bin} is missing or not executable"
        done
        scp -i "${SSH_KEY}" -o StrictHostKeyChecking=no \
            "${PREBUILT_BIN_DIR}/g5-blockmaster" \
            "${PREBUILT_BIN_DIR}/g5-blockvolume" \
            "${PREBUILT_BIN_DIR}/g5-m01verify" \
            "${M01_HOST}:/tmp/" >/dev/null \
            || die "scp pre-built binaries → m01 failed"
        log "  scp blockvolume + m01verify to M02..."
        $SSH_M01 "scp -o StrictHostKeyChecking=no /tmp/g5-blockvolume /tmp/g5-m01verify ${M02_HOST}:/tmp/" >/dev/null \
            || die "scp m01→M02 failed"
        return
    fi

    log "phase 1: sync source + build binaries on m01"
    # Safety guard (architect option (a) round 53 binding): refuse to tar
    # if SRC_DIR is empty / "/" / missing repo markers. Catches the case
    # where the script was copied away from the repo (e.g. SMB-driven
    # `bash /tmp/iterate.sh`) and SRC_DIR resolves to / — without this
    # guard, the next line would tar the entire root filesystem.
    case "${SRC_DIR}" in
        ""|"/")
            die "SRC_DIR refuses to be empty or '/' (would tar root filesystem); set PREBUILT_BIN_DIR or run from a real seaweed_block checkout"
            ;;
    esac
    [ -f "${SRC_DIR}/go.mod" ] && [ -d "${SRC_DIR}/cmd/blockvolume" ] || \
        die "SRC_DIR=${SRC_DIR} doesn't look like a seaweed_block checkout (missing go.mod or cmd/blockvolume/); set PREBUILT_BIN_DIR or fix SRC_DIR"

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
    # Architect option (A) round 53: pipe command via `bash -s`
    # heredoc instead of double-quoted ssh argument. Single-line
    # `setsid nohup ... &` (no `\<newline>` continuations) avoids
    # the local-shell line-collapse hazard that joined `&` into the
    # following `disown` statement, making remote bash wait on the
    # bg job and hang the SSH session.
    launch_m01 <<EOF
setsid nohup /tmp/g5-blockmaster --authority-store ${REMOTE_RUN_DIR}/master-store --listen 0.0.0.0:${M01_MASTER_PORT} --topology ${REMOTE_RUN_DIR}/topology.yaml --expected-slots-per-volume 2 --t0-print-ready > ${REMOTE_RUN_DIR}/logs/master.log 2>&1 </dev/null &
disown \$! 2>/dev/null || true
sleep 1
exit 0
EOF
}

start_primary() {
    log "  start primary blockvolume on m01..."
    launch_m01 <<EOF
setsid nohup /tmp/g5-blockvolume --master 127.0.0.1:${M01_MASTER_PORT} --server-id m01-primary --volume-id v1 --replica-id r1 --ctrl-addr ${M01_PRIMARY_BIND_IP}:${M01_PRIMARY_CTRL_PORT} --data-addr ${M01_PRIMARY_BIND_IP}:${M01_PRIMARY_DATA_PORT} --status-addr 127.0.0.1:${M01_PRIMARY_STATUS_PORT} --status-recovery --durable-root ${REMOTE_RUN_DIR}/primary-store --durable-impl ${DURABLE_IMPL} --durable-blocks ${DURABLE_BLOCKS} --durable-blocksize ${DURABLE_BLOCKSIZE} --iscsi-listen 127.0.0.1:${M01_ISCSI_PORT} --iscsi-iqn ${ISCSI_IQN} --t1-readiness --degraded-probe-interval=5s > ${REMOTE_RUN_DIR}/logs/primary.log 2>&1 </dev/null &
disown \$! 2>/dev/null || true
sleep 1
exit 0
EOF
}

start_replica() {
    log "  start replica blockvolume on M02..."
    launch_m02 <<EOF
mkdir -p ${REMOTE_RUN_DIR}/{logs,replica-store}
setsid nohup /tmp/g5-blockvolume --master ${M02_MASTER_TARGET}:${M01_MASTER_PORT} --server-id m02-replica --volume-id v1 --replica-id r2 --ctrl-addr ${M02_REPLICA_BIND_IP}:${M02_REPLICA_CTRL_PORT} --data-addr ${M02_REPLICA_BIND_IP}:${M02_REPLICA_DATA_PORT} --status-addr 127.0.0.1:${M02_REPLICA_STATUS_PORT} --status-recovery --durable-root ${REMOTE_RUN_DIR}/replica-store --durable-impl ${DURABLE_IMPL} --durable-blocks ${DURABLE_BLOCKS} --durable-blocksize ${DURABLE_BLOCKSIZE} --t1-readiness > ${REMOTE_RUN_DIR}/logs/replica.log 2>&1 </dev/null &
disown \$! 2>/dev/null || true
sleep 1
exit 0
EOF
}

start_cluster() {
    log "phase 2: start cluster"
    # Pre-flight: drop any stale iptables rules from prior aborted runs.
    # Both possible patterns (old INPUT-on-M02 and new OUTPUT-on-m01) are
    # cleared so repeated test runs don't poison each other. Use `|| true`
    # so missing rules don't fail set -e.
    $SSH_M01 "sudo iptables -D OUTPUT -d ${M02_IP} -p tcp --dport ${M02_REPLICA_DATA_PORT} -j DROP 2>/dev/null || true"
    $SSH_M02 "sudo iptables -D INPUT -p tcp --dport ${M02_REPLICA_DATA_PORT} -j DROP 2>/dev/null || true"
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
    if [ "${HAS_ISCSI}" = "0" ]; then
        log "verify_byte_equal: SKIP (no iscsiadm; install open-iscsi for Tier 1 iSCSI coverage, OR use Tier 2 m01)"
        return 0
    fi
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
# Fill 1 block with 0xab and write to LBA 0.
# NOTE: do NOT use `tr "\0" "\xab"` — tr does not interpret \xHH
# escapes; it would substitute with the literal char "x" (0x78).
# Use python3 binary write for byte-exact 0xab.
python3 -c "import sys; sys.stdout.buffer.write(b\"\xab\" * 4096)" | sudo dd of="$DEV" bs=4096 count=1 conv=fsync,nocreat
sync
sudo iscsiadm -m node -T '"${ISCSI_IQN}"' -p 127.0.0.1:'"${M01_ISCSI_PORT}"' --logout >/dev/null
echo "WROTE-LBA-0-PATTERN-AB"
' || die "iscsi write failed"
    # NOTE (round-12): H-advance preflight removed. Engine's
    # Recovery.H is updated by probe events (RecoveryFactsObserved),
    # not by local writes. At adoption time engine sets H=walHead
    # (nextLSN, already=1 for empty walstore); after a local write
    # at LSN=1 the next-allocated LSN becomes 2 but Recovery.H stays
    # 1 until a probe re-reads boundaries. Using H-advance here is a
    # false-positive trap. The actual proof of write+ship is the
    # m01verify byte-equal compare below; failures upstream of that
    # surface via the primary's log (set -euo pipefail in the dd
    # block + non-zero exit on iscsi failure).
    prim_h_after=$(poll_status_field "$SSH_M01" "${M01_PRIMARY_STATUS_PORT}" H)
    log "  primary.H pre/post: ${prim_h_before} → ${prim_h_after} (informational)"
    sleep 1  # let replication fan-out complete
    # Logout iscsi (release the device handle so we can stop replica cleanly later)
    $SSH_M01 "sudo iscsiadm -m node -T ${ISCSI_IQN} -p 127.0.0.1:${M01_ISCSI_PORT} --logout >/dev/null" || true
    # Stop replica daemon so m01verify can OpenReadOnly
    $SSH_M02 "sudo pkill -TERM -f '[g]5-blockvolume.*server-id m02-replica' && sleep 2"
    # m01verify reads replica's walstore
    local out
    out=$($SSH_M02 "/tmp/g5-m01verify --walstore ${REMOTE_RUN_DIR}/replica-store/v1.bin --lba-start 0 --lba-count 1 --block-size 4096 --expected-pattern ab 2>&1")
    log "  m01verify output: ${out}"
    echo "${out}" | grep -q '^OK ' || die "byte-equal verify failed: ${out}"
    # Restart replica for subsequent steps
    start_replica
    sleep 3
    log "  ✓ byte-equal"
}

# §2 #3 — network disconnect catch-up
#
# Pattern (architect round 13 ratification per QA's planning input):
# direct byte-equal as the pass condition, replacing the H/R polling
# proxy. /status/recovery snapshots stay as supporting diagnostic
# only.
#
#   1. baseline write LBA[0]=0xab; byte-equal verify (already done in
#      verify_byte_equal — primary now contains LBA0=0xab)
#   2. iptables OUTPUT DROP on m01 toward M02:M02_REPLICA_DATA_PORT
#      (architect ruling: m01 OUTPUT drop preferred over M02 INPUT
#      drop; least-broad rule that proves the scenario)
#   3. primary write LBA[1]=0xcd (new pattern, BestEffort: primary
#      WAL accepts; ship to r2 fails until heal)
#   4. heal: iptables -D
#   5. wait_until_byte_equal LBA[1]=0xcd CATCHUP_DEADLINE
#   6. (informational) snapshot /status/recovery for diagnostic
#
# trap ensures iptables -D runs even on script abort so a failed run
# does not poison the box.
verify_network_catchup() {
    if [ "${HAS_ISCSI}" = "0" ]; then
        log "verify_network_catchup: SKIP (needs iscsiadm to drive write; use Tier 2 m01)"
        return 0
    fi
    log "verify_network_catchup: m01 OUTPUT drop to ${M02_IP}:${M02_REPLICA_DATA_PORT} + write + heal + byte-equal"

    # Cleanup trap: best-effort iptables -D, runs on any function exit
    # path (success, die, set -e abort upstream).
    local cleanup_done=0
    _verify_network_catchup_cleanup() {
        [ "${cleanup_done}" = "1" ] && return
        cleanup_done=1
        $SSH_M01 "sudo iptables -D OUTPUT -d ${M02_IP} -p tcp --dport ${M02_REPLICA_DATA_PORT} -j DROP" >/dev/null 2>&1 || true
    }
    trap _verify_network_catchup_cleanup RETURN

    # Step 2: iptables OUTPUT drop on m01.
    $SSH_M01 "sudo iptables -A OUTPUT -d ${M02_IP} -p tcp --dport ${M02_REPLICA_DATA_PORT} -j DROP" \
        || die "iptables -A failed"
    log "  network partitioned (m01 OUTPUT → ${M02_IP}:${M02_REPLICA_DATA_PORT} DROP)"

    # Step 3: primary write LBA[1]=0xcd (different pattern from #2's
    # LBA[0]=0xab so we can't false-positive on stale data).
    iscsi_write_pattern 1 1 cd >/dev/null || die "iscsi write LBA[1]=0xcd during disconnect failed"
    log "  primary wrote LBA[1]=0xcd (replica still partitioned)"

    # Step 4: heal.
    _verify_network_catchup_cleanup
    log "  network healed (iptables -D applied)"

    # Step 5: poll until byte-equal at replica.
    wait_until_byte_equal 1 cd "${CATCHUP_DEADLINE}" "verify_network_catchup-LBA1" \
        || die "replica did not converge LBA[1]=0xcd within ${CATCHUP_DEADLINE}s after heal"

    # Step 6: diagnostic snapshot (not pass/fail oracle).
    local prim_h rep_r decision
    prim_h=$(poll_status_field "$SSH_M01" "${M01_PRIMARY_STATUS_PORT}" H)
    rep_r=$(poll_status_field "$SSH_M02" "${M02_REPLICA_STATUS_PORT}" R)
    decision=$(poll_status_field "$SSH_M02" "${M02_REPLICA_STATUS_PORT}" RecoveryDecision)
    log "  diagnostic /status/recovery: primary.H=${prim_h} replica.R=${rep_r} replica.RecoveryDecision=${decision:-<empty>}"
    log "  ✓ network catch-up byte-equal converged"

    trap - RETURN
}

# §2 #4 — replica process stop/restart catch-up
#
# Pattern (architect round 13 ratification):
#   1. baseline: replica running, byte-equal at LBA[0]=0xab from #2
#   2. kill replica: pkill -TERM g5-blockvolume on M02
#   3. primary write LBA[2]=0xef while replica is down (BestEffort:
#      primary's WAL accepts the write; ship attempts to r2 fail
#      since r2 is offline)
#   4. restart replica with same --durable-root
#   5. wait_until_byte_equal LBA[2]=0xef CATCHUP_DEADLINE
#
# Run #4 NAIVELY first (architect ruling 2 round 13). If catch-up
# does not converge — primary's WAL did not retain writes during the
# disconnect — that is a real recovery-path finding to surface, NOT
# a test flaw. Do not change durability mode; do not paper over.
#
# Note: m01verify must read replica's walstore only while the
# replica daemon is stopped or quiesced (architect caution round 13).
# wait_until_byte_equal handles this internally (kill→verify→restart
# per cycle).
verify_restart_catchup() {
    if [ "${HAS_ISCSI}" = "0" ]; then
        log "verify_restart_catchup: SKIP (needs iscsiadm to drive write; use Tier 2 m01)"
        return 0
    fi
    log "verify_restart_catchup: kill replica + write while down + restart + byte-equal"

    # Step 2: stop replica daemon.
    $SSH_M02 "sudo killall -TERM g5-blockvolume; sleep 2" >/dev/null 2>&1 || true
    log "  replica stopped"

    # Step 3: primary write LBA[2]=0xef while replica is down.
    iscsi_write_pattern 2 1 ef >/dev/null || die "iscsi write LBA[2]=0xef during replica-down failed"
    log "  primary wrote LBA[2]=0xef (replica down)"

    # Step 4: restart replica.
    start_replica >/dev/null 2>&1 || true
    log "  replica restarted"

    # Step 5: wait_until_byte_equal at LBA[2]=0xef.
    wait_until_byte_equal 2 ef "${CATCHUP_DEADLINE}" "verify_restart_catchup-LBA2" \
        || die "replica did not converge LBA[2]=0xef within ${CATCHUP_DEADLINE}s after restart (CHECK: primary WAL retention during replica-down — real recovery-path finding if writes were dropped)"

    # Diagnostic snapshot (not pass/fail oracle).
    local prim_h rep_r decision
    prim_h=$(poll_status_field "$SSH_M01" "${M01_PRIMARY_STATUS_PORT}" H)
    rep_r=$(poll_status_field "$SSH_M02" "${M02_REPLICA_STATUS_PORT}" R)
    decision=$(poll_status_field "$SSH_M02" "${M02_REPLICA_STATUS_PORT}" RecoveryDecision)
    log "  diagnostic /status/recovery: primary.H=${prim_h} replica.R=${rep_r} replica.RecoveryDecision=${decision:-<empty>}"
    log "  ✓ restart catch-up byte-equal converged"
}

# §2 #5 — race ×10 stress on G5-4 integration test
verify_race_stress() {
    if [ "${LOCAL_MODE}" = "1" ]; then
        # WSL Ubuntu 22 ships Go 1.18; project needs 1.21+. Skip in LOCAL_MODE
        # unless WSL has been upgraded.
        local go_minor; go_minor=$(go version 2>/dev/null | grep -oP 'go1\.\K[0-9]+' || echo 0)
        if [ "${go_minor}" -lt 21 ]; then
            log "verify_race_stress: SKIP (LOCAL_MODE Go ${go_minor} < 1.21; use Tier 2 m01 for -race)"
            return 0
        fi
        log "verify_race_stress: TestG54_BinaryWiring x10 -race in SRC_DIR=${SRC_DIR}"
        ( cd "${SRC_DIR}" && CGO_ENABLED=1 go test -race -count=10 -run TestG54_BinaryWiring ./cmd/blockvolume/ ) > "${ARTIFACT_DIR}/race-stress.log" 2>&1 \
            || die "race stress failed; see ${ARTIFACT_DIR}/race-stress.log"
        log "  ✓ race ×10"
        return
    fi
    log "verify_race_stress: TestG54_BinaryWiring x10 -race on m01"
    $SSH_M01 "command -v gcc >/dev/null" || die "gcc not installed on m01 (needed for CGO -race)"
    $SSH_M01 "cd ${REMOTE_BUILD_DIR} && CGO_ENABLED=1 go test -race -count=10 -run TestG54_BinaryWiring ./cmd/blockvolume/ 2>&1" > "${ARTIFACT_DIR}/race-stress.log" \
        || die "race stress failed; see ${ARTIFACT_DIR}/race-stress.log"
    log "  ✓ race ×10"
}

# §2 #6 — full V3 suite green from m01
verify_full_suite() {
    if [ "${LOCAL_MODE}" = "1" ]; then
        local go_minor; go_minor=$(go version 2>/dev/null | grep -oP 'go1\.\K[0-9]+' || echo 0)
        if [ "${go_minor}" -lt 21 ]; then
            log "verify_full_suite: SKIP (LOCAL_MODE Go ${go_minor} < 1.21; use Tier 2 m01 for full suite)"
            return 0
        fi
        log "verify_full_suite: go test ./... in SRC_DIR=${SRC_DIR}"
        ( cd "${SRC_DIR}" && go test ./... -count=1 -timeout 600s ) > "${ARTIFACT_DIR}/full-suite.log" 2>&1 \
            || die "full suite failed; see ${ARTIFACT_DIR}/full-suite.log"
        log "  ✓ full suite green"
        return
    fi
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
