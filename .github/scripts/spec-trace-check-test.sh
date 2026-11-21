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
expect 0 "CRLF body" "$(printf '%s\n' "$ok" | sed 's/$/\r/')"
expect 0 "CHANGE of two machines" "${ok/IMPL M5.Quiesce>Copy/CHANGE M4, M6}"
expect 0 "list markers and bold keys" "$(printf '%s\n' "$ok" | sed "s/^Feature:/- **Feature:**/; s/^Model:/* Model:/")"
expect 0 "Model NONE with a reason" "${ok/Model: docs\/spec\/MaintenanceOwner.tla, exhausted/Model: NONE: CI wiring only, no state}"

expect 1 "empty body" ""
expect 1 "missing Red-first" "$(printf '%s\n' "$ok" | grep -v Red-first)"
expect 1 "short NONE" "${ok/Model: docs\/spec\/MaintenanceOwner.tla, exhausted/Model: NONE: n/a}"
expect 1 "no kind" "${ok/IMPL M5.Quiesce>Copy/B,D,E}"
expect 1 "IMPL without transition" "${ok/IMPL M5.Quiesce>Copy/IMPL M5}"
expect 1 "NONE is not a kind" "${ok/IMPL M5.Quiesce>Copy \/ INV-MAINT-ONE-OWNER/NONE: tooling only, no machine}"
expect 1 "core file deleted without Simplest" "$ok" D core/storage/old.go
expect 1 "two kinds" "${ok/IMPL M5.Quiesce>Copy/IMPL M5.Quiesce>Copy FIX INV-MAINT-ONE-OWNER}"
expect 1 "second Machine/INV line with another kind" "$ok
Machine/INV: FIX INV-MAINT-ONE-OWNER"
expect 1 "second Machine/INV line in list and bold form" "$ok
- **Machine/INV:** CHANGE M5"
expect 1 "Simplest declared twice" "$ok
$simplest
$simplest" M core/storage/gc.go
expect 1 "kind token with a trailing junk" "${ok/IMPL M5.Quiesce>Copy/FIX INV-GC-SKEW-001x}"
expect 1 "transition with a stray character" "${ok/IMPL M5.Quiesce>Copy/IMPL M5.Quiesce>Copy;x}"
expect 1 "key inside another word" "${ok/Feature: O-SNAP-2/Not-Feature: O-SNAP-2}"
expect 1 "placeholder left in Feature" "${ok/Feature: O-SNAP-2/Feature: <roadmap item>}"
expect 1 "placeholder left in Red-first" "$(printf '%s\n' "$ok" | sed "s/^Red-first:.*/Red-first: <commit + test>/")"
expect 1 "placeholder left in Simplest" "$ok
Simplest: <the simplest version, and what this PR did not add>" M core/storage/gc.go
expect 1 "unfilled template" "$(cat "$here/../pull_request_template.md")"
expect 1 "core change without Simplest" "$ok" M core/storage/gc.go
expect 1 "cmd change, short Simplest" "$ok
Simplest: small" A cmd/sw-block/new.go

# expect_raw WANT_EXIT NAME BYTES: a change list given as printf format.
expect_raw() {
	local want=$1 name=$2 got=0
	printf "%s" "$ok" >"$tmp/body"
	printf "$3" | bash "$check" "$tmp/body" >/dev/null 2>&1 || got=$?
	if [[ $got != "$want" ]]; then
		echo "FAIL $name: exit $got, want $want"
		fail=1
	fi
}
expect_raw 2 "record cut off before its NUL" "M\0core/storage/gc.go"
expect_raw 2 "status without a path" "M\0"
expect_raw 2 "status cut off before its NUL" "M\0docs/a.md\0M"
expect_raw 2 "unknown status" "R100\0core/a.go\0"
expect_raw 0 "well-formed docs change" "M\0docs/a.md\0"

# Kinds for PRs that change no behaviour: each is checked against its paths,
# and DOCS also against the Go diff (only comments may change in non-test Go).
# expect_kind WANT_EXIT NAME KIND GO_DIFF_TEXT [STATUS PATH]...
expect_kind() {
	local want=$1 name=$2 k=$3 d=$4 got=0
	shift 4
	printf '%s' "${ok/IMPL M5.Quiesce>Copy/$k}
$simplest" >"$tmp/body"
	printf '%s' "$d" >"$tmp/go.diff"
	if (($#)); then printf '%s\0' "$@"; fi | bash "$check" "$tmp/body" "$tmp/go.diff" >/dev/null || got=$?
	if [[ $got != "$want" ]]; then
		echo "FAIL $name: exit $got, want $want"
		fail=1
	fi
}
comment_diff='--- a/core/x/s.go
+++ b/core/x/s.go
@@ -1,3 +1,4 @@
-	Name string `json:"name"`
+	Name string `json:"name"` // the volume name
+	// Count is how many.
 	Count int'
code_diff='--- a/core/x/s.go
+++ b/core/x/s.go
@@ -1,2 +1,2 @@
-	limit := 512 // chunks
+	limit := 256 // chunks'
url_diff='--- a/core/x/s.go
+++ b/core/x/s.go
@@ -1,1 +1,1 @@
-	u := "http://a"
+	u := "http://b"'
expect_kind 0 "DOCS: docs and comment-only Go" DOCS "$comment_diff" M docs/a.md M core/x/s.go A core/x/s_test.go
expect_kind 1 "DOCS: a code change in Go" DOCS "$code_diff" M core/x/s.go
expect_kind 1 "DOCS: a change inside a string that holds //" DOCS "$url_diff" M core/x/s.go
expect_kind 1 "DOCS: Go changed but no diff given" DOCS "" M core/x/s.go
expect_kind 1 "DOCS: a non-docs, non-Go file" DOCS "" M deploy/x.env
expect_kind 0 "TEST: tests and testdata" TEST "" M core/x/s_test.go A core/x/testdata/in.bin
expect_kind 1 "TEST: production Go" TEST "$comment_diff" M core/x/s.go
expect_kind 0 "CI: workflow and scripts" CI "" M .github/workflows/w.yml A scripts/x.sh
expect_kind 1 "CI: anything else" CI "" M docs/a.md
expect_kind 1 "DOCS next to another kind" "DOCS FIX INV-X-001" "" M docs/a.md

[[ $fail == 0 ]] && echo "spec-trace self-test: ok"
exit $fail
