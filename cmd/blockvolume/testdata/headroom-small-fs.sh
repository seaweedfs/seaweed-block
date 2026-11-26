#!/usr/bin/env bash
# Small-file-system harness for TestHeadroomRefusesRestoreTargetsThenServesThem.
# Root only, on a disposable host; it formats only a loop device over a private
# sparse file under WORKDIR.
#
#   headroom-small-fs.sh WORKDIR -- TEST_COMMAND...
#
# It makes a 768 MiB ext4 (no reserved blocks) on a loop device, mounts it at
# WORKDIR/mnt and fills 600 MiB of it with WORKDIR/mnt/filler, so less than a
# volume's size + WAL (256 MiB + 64 MiB) is free; the test removes the filler
# when it needs room. The test runs with CHUNKSTORE_SMALL_ROOT (a directory on
# that file system) and CHUNKSTORE_SMALL_FILLER (the filler). Each run gets a
# fresh file system. Exit status is the test's. Everything this script created
# is removed.
#
# Example (from the repository root, as root):
#   mkdir /tmp/headroom && cmd/blockvolume/testdata/headroom-small-fs.sh /tmp/headroom -- \
#     env CHUNKSTORE_WEED=/path/to/weed go test -count=1 -tags 'subprocess chunkstore_integration' \
#     -run '^TestHeadroomRefusesRestoreTargetsThenServesThem$' -timeout 15m ./cmd/blockvolume/
set -euo pipefail
[ "$(id -u)" = 0 ] || { echo "root required" >&2; exit 2; }
work=$(realpath "${1:?existing empty work dir}")
[ "${2:-}" = -- ] || { echo "usage: $0 WORKDIR -- TEST_COMMAND..." >&2; exit 2; }
shift 2
for c in losetup mkfs.ext4 mount umount mountpoint truncate dd; do command -v "$c" >/dev/null; done
loop=
cleanup() {
  if mountpoint -q "$work/mnt"; then umount "$work/mnt"; fi
  if [ -n "$loop" ]; then losetup -d "$loop" || true; fi
  rm -rf "$work/mnt" "$work/small.img"
}
trap cleanup EXIT
mkdir "$work/mnt"
truncate -s 768M "$work/small.img"
loop=$(losetup --find --show "$work/small.img")
mkfs.ext4 -q -F -m 0 "$loop"
mount "$loop" "$work/mnt"
dd if=/dev/zero of="$work/mnt/filler" bs=1M count=600 status=none
sync
mkdir "$work/mnt/roots"
status=0
CHUNKSTORE_SMALL_ROOT="$work/mnt/roots" CHUNKSTORE_SMALL_FILLER="$work/mnt/filler" "$@" || status=$?
exit "$status"
