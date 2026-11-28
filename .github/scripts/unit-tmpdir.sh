#!/usr/bin/env bash
# Prints the directory the unit suite's TMPDIR and GOTMPDIR go under.
#
# Usage: unit-tmpdir.sh NEED_BYTES DISK_DIR
#
# The suite's temp files live in memory (/dev/shm) when both of these hold,
# else on disk (DISK_DIR):
#  - /dev/shm has NEED_BYTES free;
#  - the job's memory limit (its cgroup's memory.max, nearest one set) leaves
#    at least 2 x NEED_BYTES above what the job uses now: tmpfs pages count
#    against that limit, and the tests need memory of their own.
# NEED_BYTES is a bound on the peak, not a cap: a tmpfs under /dev/shm cannot
# be sized without root, so the bound comes from the measured peak.
# One line on stderr says which was chosen and why.
set -euo pipefail
need=$1 disk=$2

avail=$(df -B1 --output=avail /dev/shm 2>/dev/null | tail -1 | tr -d ' ') || avail=0
[ -n "$avail" ] || avail=0

# The nearest memory.max that is a number, from the job's cgroup upward.
headroom=unlimited
cg=$(sed -n 's/^0:://p' /proc/self/cgroup 2>/dev/null || true)
d=/sys/fs/cgroup${cg}
while [ -n "$cg" ] && [ "$d" != /sys/fs/cgroup ] && [ "$d" != /sys/fs ]; do
  if [ -r "$d/memory.max" ] && [ "$(cat "$d/memory.max")" != max ]; then
    headroom=$(( $(cat "$d/memory.max") - $(cat "$d/memory.current") ))
    break
  fi
  d=$(dirname "$d")
done

if [ "$avail" -lt "$need" ]; then
  echo "unit temp: disk ($disk): /dev/shm has $avail B free, need $need" >&2
  echo "$disk"
elif [ "$headroom" != unlimited ] && [ "$headroom" -lt $((2 * need)) ]; then
  echo "unit temp: disk ($disk): memory headroom $headroom B under the job's limit, need $((2 * need))" >&2
  echo "$disk"
else
  echo "unit temp: memory (/dev/shm): $avail B free, memory headroom $headroom B" >&2
  echo /dev/shm
fi
