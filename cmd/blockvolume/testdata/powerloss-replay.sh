#!/usr/bin/env bash
# Power-loss replay harness for TestPowerLossReplayAckedWritesSurvive and
# TestPowerLossReplayChunkVolume. Root only, on a disposable host with the
# dm_log_writes module and replay-log (upstream log-writes); it formats only
# loop devices over private sparse files under WORKDIR.
#
#   powerloss-replay.sh REPLAY_LOG WORKDIR -- TEST_COMMAND...
#
# The test runs with SWBLOCK_REPLAY_ROOT (an ext4 mount on dm-log-writes),
# SWBLOCK_REPLAY_LOGS and SWBLOCK_REPLAY_CUT, a helper with two steps:
#   mark    marks the current log 'acknowledged'
#   replay  unmounts, replays the current log only up to that mark onto a
#           fresh device, puts a new log-writes layer on it and mounts it at
#           the same path, so the next power cut is logged too
# Exit status is the test's. Everything this script created is removed.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "root required" >&2; exit 2; }
replayer=$(realpath "${1:?replay-log}")
work=$(realpath "${2:?existing empty work dir}")
[ "${3:-}" = -- ] || { echo "usage: $0 REPLAY_LOG WORKDIR -- TEST_COMMAND..." >&2; exit 2; }
shift 3
for c in losetup dmsetup mkfs.ext4 mount umount mountpoint blockdev truncate; do command -v "$c" >/dev/null; done
dmsetup targets | awk '$1=="log-writes"{f=1} END{exit !f}' || { echo "dm-log-writes unavailable" >&2; exit 2; }
mkdir "$work/mnt" "$work/logs"
name=swb-powerloss-$$

# layer K BACKING_IMG: a log-writes target over BACKING_IMG with log K. The
# log holds only writes made through this layer, so its replay starts from a
# copy of BACKING_IMG as it is now (base-K).
layer() {
  local k=$1 img=$2 dev log
  cp --sparse=always "$img" "$work/base-$k.img"
  truncate -s 2G "$work/log-$k.img"
  dev=$(losetup --find --show "$img"); echo "$dev" >> "$work/loops"
  log=$(losetup --find --show "$work/log-$k.img"); echo "$log" >> "$work/loops"
  dmsetup create "$name-$k" --table "0 $(blockdev --getsz "$dev") log-writes $dev $log"
  echo "$k $log" > "$work/current"
}
truncate -s 1G "$work/data-0.img"
layer 0 "$work/data-0.img"
mkfs.ext4 -q -F -E lazy_itable_init=0,lazy_journal_init=0 "/dev/mapper/$name-0"
mount "/dev/mapper/$name-0" "$work/mnt"

cat > "$work/cut" <<EOF
#!/usr/bin/env bash
set -euo pipefail
read -r k log < "$work/current"
case "\${1:-}" in
  mark)
    dmsetup message "$name-\$k" 0 mark acknowledged
    echo "marked layer \$k" ;;
  replay)
    umount "$work/mnt"
    dmsetup status "$name-\$k" > "$work/logs/dm-status-\$k.txt"
    dmsetup remove "$name-\$k"
    n=\$((k + 1))
    cp --sparse=always "$work/base-\$k.img" "$work/data-\$n.img"
    rep=\$(losetup --find --show "$work/data-\$n.img")
    "$replayer" --log "\$log" --replay "\$rep" --end-mark acknowledged > "$work/logs/replay-\$k.log" 2>&1
    losetup -d "\$rep"
    # Layer k's own devices stay attached until cleanup; its images are no longer read.
    $(declare -f layer)
    name="$name" work="$work"
    layer "\$n" "$work/data-\$n.img"
    mount "/dev/mapper/$name-\$n" "$work/mnt"
    echo "replayed to mark onto layer \$n" ;;
  *) echo "usage: cut mark|replay" >&2; exit 2 ;;
esac
EOF
chmod 700 "$work/cut"

cleanup() {
  set +e
  mountpoint -q "$work/mnt" && umount "$work/mnt"
  for d in $(dmsetup ls | awk -v p="$name-" 'index($1, p) == 1 {print $1}'); do dmsetup remove "$d"; done
  [ -f "$work/loops" ] && while read -r l; do losetup -d "$l" 2>/dev/null; done < "$work/loops"
  # Only the sparse images this script created under WORKDIR.
  rm -f -- "$work"/data-*.img "$work"/log-*.img "$work"/base-*.img
}
trap cleanup EXIT
set +e
SWBLOCK_REPLAY_ROOT=$work/mnt SWBLOCK_REPLAY_CUT=$work/cut SWBLOCK_REPLAY_LOGS=$work/logs "$@"
rc=$?
set -e
exit $rc
