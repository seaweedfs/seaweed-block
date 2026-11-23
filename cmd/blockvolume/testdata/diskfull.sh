#!/usr/bin/env bash
# Disk-full harness for TestProcedureLocalDiskFull. Root only; it formats only
# loop devices over private sparse files under WORKDIR and always tears them
# down, also when the test fails or is killed.
#
#   diskfull.sh WORKDIR -- TEST_COMMAND...
#
# The test runs with SWBLOCK_DISKFULL_ROOT (512 MiB ext4, for durable roots),
# SWBLOCK_DISKFULL_LIMIT (8 MiB ext4, for --host-limit-dir) and
# SWBLOCK_REPLAY_LOGS. No reserved blocks (-m 0): the test runs as root.
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "root required" >&2; exit 2; }
work=$(realpath "${1:?existing empty work dir}")
[ "${2:-}" = -- ] || { echo "usage: $0 WORKDIR -- TEST_COMMAND..." >&2; exit 2; }
shift 2
for c in losetup mkfs.ext4 mount umount mountpoint truncate; do command -v "$c" >/dev/null; done
mkdir "$work/root" "$work/limit" "$work/logs"
loops=()
cleanup() {
  local rc=$? failed=0
  set +e
  for m in "$work/root" "$work/limit"; do
    mountpoint -q "$m" || continue
    # A blockvolume that outlived a killed test would hold the mount busy.
    command -v fuser >/dev/null && fuser -km "$m" >/dev/null 2>&1
    umount "$m" 2>/dev/null || { sleep 1; umount "$m"; } || { umount -l "$m"; failed=1; }
  done
  for l in "${loops[@]}"; do losetup -d "$l" || failed=1; done
  # Only the sparse images this script created under WORKDIR.
  rm -f -- "$work/root.img" "$work/limit.img"
  if [ "$failed" = 1 ] || losetup -a | grep -qF "$work/"; then
    echo "diskfull.sh: teardown incomplete (busy mount or loop device left)" >&2
    exit 3
  fi
  exit $rc
}
trap cleanup EXIT
for fs in root:512M limit:8M; do
  name=${fs%%:*}
  truncate -s "${fs#*:}" "$work/$name.img"
  dev=$(losetup --find --show "$work/$name.img"); loops+=("$dev")
  mkfs.ext4 -q -F -m 0 "$dev"
  mount "$dev" "$work/$name"
done
set +e
SWBLOCK_DISKFULL_ROOT=$work/root SWBLOCK_DISKFULL_LIMIT=$work/limit SWBLOCK_REPLAY_LOGS=$work/logs "$@"
rc=$?
set -e
exit $rc
