#!/usr/bin/env bash
# Self-test of the spec-trace check: PR descriptions and change lists against
# the exit code they must give.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
check=$here/spec-trace-check.sh
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
fail=0

# expect WANT_EXIT NAME BODY [STATUS PATH]...
expect() {
	local want=$1 name=$2 body=$3 got=0
	shift 3
	printf '%s' "$body" >"$tmp/body"
	if (($#)); then printf '%s\0' "$@"; fi | bash "$check" "$tmp/body" >/dev/null || got=$?
	if [[ $got != "$want" ]]; then
		echo "FAIL $name: exit $got, want $want"
		fail=1
	fi
}

ok='Feature: O-SNAP-2
Machine/INV: IMPL M5.Quiesce>Copy / INV-MAINT-ONE-OWNER
Model: docs/spec/MaintenanceOwner.tla, exhausted
Red-first: 7f542dbc TestMaintenanceHeldPass failed on base'
simplest='Simplest: one owner per volume on the existing cut path; no new machine.'

expect 0 "all four lines, docs only" "$ok" M docs/a.md
expect 0 "FIX kind" "${ok/IMPL M5.Quiesce>Copy/FIX INV-GC-SKEW-001}"
expect 0 "CHANGE kind" "${ok/IMPL M5.Quiesce>Copy/CHANGE M5}"
expect 0 "core change with Simplest" "$ok
$simplest" M core/storage/gc.go
expect 0 "core test only needs no Simplest" "$ok" M core/storage/gc_test.go
expect 0 "core file deleted needs no Simplest" "$ok" D core/storage/old.go
expect 0 "CRLF body" "$(printf '%s\n' "$ok" | sed 's/$/\r/')"
expect 0 "Model NONE with a reason" "${ok/Model: docs\/spec\/MaintenanceOwner.tla, exhausted/Model: NONE: CI wiring only, no state}"

expect 1 "empty body" ""
expect 1 "missing Red-first" "$(printf '%s\n' "$ok" | grep -v Red-first)"
expect 1 "short NONE" "${ok/Model: docs\/spec\/MaintenanceOwner.tla, exhausted/Model: NONE: n/a}"
expect 1 "no kind" "${ok/IMPL M5.Quiesce>Copy/B,D,E}"
expect 1 "IMPL without transition" "${ok/IMPL M5.Quiesce>Copy/IMPL M5}"
expect 1 "NONE is not a kind" "${ok/IMPL M5.Quiesce>Copy \/ INV-MAINT-ONE-OWNER/NONE: tooling only, no machine}"
expect 1 "unfilled template" "$(cat "$here/../pull_request_template.md")"
expect 1 "core change without Simplest" "$ok" M core/storage/gc.go
expect 1 "cmd change, short Simplest" "$ok
Simplest: small" A cmd/sw-block/new.go

[[ $fail == 0 ]] && echo "spec-trace self-test: ok"
exit $fail
