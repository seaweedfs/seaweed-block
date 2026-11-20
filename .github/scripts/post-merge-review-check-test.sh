#!/usr/bin/env bash
# Self-test of the post-merge-review check: the matcher against the checked-in
# allow list, the real Git diff it consumes, and the workflow's triggers.
set -euo pipefail
shopt -s inherit_errexit
here=$(cd "$(dirname "$0")" && pwd)
check=$here/post-merge-review-check.sh
changed=$here/post-merge-review-changed.sh
allow=$here/../post-merge-review-allow.txt
workflow=$here/../workflows/post-merge-review.yml
fail=0

expect() { # expect WANT_EXIT NAME PATH...
	local want=$1 name=$2
	shift 2
	local got=0
	printf '%s\0' "$@" | bash "$check" "$allow" >/dev/null || got=$?
	if [[ $got != "$want" ]]; then
		echo "FAIL $name: exit $got, want $want"
		fail=1
	fi
}

# Allowed.
expect 0 "docs" docs/README.md README.md cmd/sw-block/NOTES.md
expect 0 "harness" internal/testops/run.go cmd/sw-testops/main.go
expect 0 "test outside denied dirs" core/ops/cleanup_projection_test.go cmd/blockmaster/topology_test.go
expect 0 "named status files" core/host/master/observation_service.go core/host/volume/status_server.go
expect 0 "release is not lease" cmd/sw-block/release_test.go
expect 0 "names with a space, tab or newline" "docs/a b.md" $'docs/a\tb.md' $'docs/a\nb.md'
expect 0 "no paths" ""

# Denied by default: not on the allow list.
expect 1 "new file anywhere" core/newpkg/decide.go
expect 1 "decision code by name" core/host/master/product_loop.go core/lifecycle/planner.go
expect 1 "go.mod" go.mod
expect 1 "go.sum" go.sum
expect 1 "status-like but not named" core/ops/operator_status_controller.go

# Denied explicitly, even where an allow line matches.
expect 1 "storage" core/storage/walstore.go
expect 1 "unicode name in storage" $'core/storage/\xc3\xa9_test.go'
expect 1 "durable test file" core/frontend/durable/provider_chunk_detach_abort_test.go
expect 1 "doc inside a denied dir" core/storage/NOTES.md
expect 1 "authority test" core/authority/lease_stats_test.go
expect 1 "master workspace test" core/host/master/workspace_binding_review_test.go
expect 1 "volume workspace runtime" core/host/volume/workspace_runtime.go
expect 1 "volume workspace runtime test" core/host/volume/workspace_runtime_test.go
expect 1 "proto" core/rpc/proto/control.proto
expect 1 "lease test by name" cmd/foo/lease_table_test.go
expect 1 "gc test by name" cmd/sw-block/chunk_gc_test.go
expect 1 "the check itself" .github/post-merge-review-allow.txt
expect 1 "a workflow" .github/workflows/test.yml
expect 1 "PR template under .github" .github/pull_request_template.md
expect 1 "mixed: allowed plus denied" docs/README.md core/storage/walstore.go
expect 1 "mixed: allowed plus unlisted" docs/README.md go.mod

# A missing or empty allow list fails closed.
empty=$(mktemp)
for bad in "$empty" "/nonexistent/post-merge-review-allow.txt"; do
	got=0
	printf 'docs/README.md\0' | bash "$check" "$bad" >/dev/null 2>&1 || got=$?
	[[ $got == 2 ]] || { echo "FAIL allow list $bad: exit $got, want 2"; fail=1; }
done
rm -f "$empty"

# Real Git: names that git quotes in its default output, a rename out of a
# denied directory, and a retarget onto a base that differs.
repo=$(mktemp -d)
trap 'rm -rf "$repo"' EXIT
g() { git -C "$repo" -c user.name=t -c user.email=t@t -c core.quotePath=true -c core.protectNTFS=false "$@"; }
blob=
commit() { # commit PARENT PATH... : PARENT's tree plus PATHs (-PATH removes)
	local parent=$1
	shift
	g read-tree "$parent"
	local p
	for p in "$@"; do
		if [[ $p == -* ]]; then
			g update-index --force-remove -- "${p#-}"
		else
			g update-index --add --cacheinfo "100644,$blob,$p"
		fi
	done
	g commit-tree "$(g write-tree)" -p "$parent" -m c
}
expect_diff() { # expect_diff WANT_EXIT NAME BASE HEAD
	local got=0
	(cd "$repo" && bash "$changed" "$3" "$4") | bash "$check" "$allow" >/dev/null || got=$?
	if [[ $got != "$1" ]]; then
		echo "FAIL $2: exit $got, want $1"
		fail=1
	fi
}
g init -q
blob=$(printf x | g hash-object -w --stdin)
g read-tree --empty
root=$(g commit-tree "$(g write-tree)" -m root)
main=$(commit "$root" core/storage/old.go docs/x.md)
unicode=$(commit "$main" $'core/storage/\xc3\xa9_test.go')
oddnames=$(commit "$main" $'docs/tab\there.md' $'docs/new\nline.md' $'docs/\xc3\xa9.md')
moved=$(commit "$main" -core/storage/old.go docs/moved.md)
parent=$(commit "$main" core/storage/parent.go)
stacked=$(commit "$parent" docs/child.md)
later=$(commit "$main" core/storage/later.go)
n=$( (cd "$repo" && bash "$changed" "$main" "$oddnames") | tr -cd '\000' | wc -c | tr -d ' ')
[[ $n == 3 ]] || { echo "FAIL fixture: $n odd names in the diff, want 3"; fail=1; }
if ! (cd "$repo" && git diff --name-only "$main...$unicode") | grep -q '^"'; then
	echo "FAIL fixture: expected git to quote the unicode name in its default output"
	fail=1
fi
expect_diff 1 "git: unicode test file in storage" "$main" "$unicode"
expect_diff 0 "git: tab, newline and unicode names in docs" "$main" "$oddnames"
expect_diff 1 "git: moved out of storage into docs" "$main" "$moved"
expect_diff 0 "git: stacked PR against its parent" "$parent" "$stacked"
expect_diff 1 "git: the same PR retargeted to main" "$main" "$stacked"
expect_diff 0 "git: the base moved on with denied changes; the PR did not" "$later" "$oddnames"

# The workflow reruns on every change that can change the verdict, including
# a retarget (edited).
types=$(sed -n 's/^ *types: *\[\(.*\)\].*/\1/p' "$workflow" | tr -d ' ')
for want in opened synchronize reopened labeled unlabeled edited; do
	[[ ",$types," == *",$want,"* ]] || { echo "FAIL workflow does not run on $want"; fail=1; }
done

if ((fail)); then exit 1; fi
echo "post-merge-review check: self-test ok"
