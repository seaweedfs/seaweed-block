#!/usr/bin/env bash
# Runs each test named in NAMES_FILE in its own process of TEST_BINARY, in
# sequence, so a panic or a hang in one cannot hide the others. Each process
# is bounded by TIMEOUT (the test binary's own timeout, then killed). Every
# process's exit status is kept: a test that reports PASS and then fails or
# hangs on its way out is not a pass (required-tests-check.py --status).
#
# Usage: run-named-tests.sh TEST_BINARY NAMES_FILE EVENTS_OUT STATUS_OUT TIMEOUT [DIR]
#   TEST_BINARY  absolute path of a binary built with go test -c
#   EVENTS_OUT   go test -json events of every process, appended in order
#   STATUS_OUT   one line per named test: NAME EXIT_STATUS
#   DIR          the directory each process runs in (default .)
# TEST_PKG names the package in the events (default the blockvolume command).
# Exits 0 once every named test has run; the verdict is the checker's.
set -euo pipefail
[ "$#" -ge 5 ] || { echo "usage: run-named-tests.sh TEST_BINARY NAMES_FILE EVENTS_OUT STATUS_OUT TIMEOUT [DIR]" >&2; exit 2; }
bin=$1 names=$2 out=$3 status=$4 to=$5 dir=${6:-.}
case "$bin" in /*) ;; *) echo "run-named-tests: TEST_BINARY must be absolute" >&2; exit 2 ;; esac
pkg=${TEST_PKG:-github.com/seaweedfs/seaweed-block/cmd/blockvolume}
: >"$out"
: >"$status"
while read -r name; do
	start=$(date +%s)
	rc=0
	(cd "$dir" && timeout -k 30 "$to" "$bin" -test.v=test2json -test.count=1 \
		-test.timeout "$to" -test.run "^${name}\$" 2>&1) |
		go tool test2json -t -p "$pkg" >>"$out" || rc=$?
	echo "$name $rc" >>"$status"
	echo "$name: exit $rc, $(($(date +%s) - start))s"
done < <(sed 's/#.*//' "$names" | tr -d ' \t' | grep .)
