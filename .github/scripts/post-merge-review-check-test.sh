#!/usr/bin/env bash
# Self-test of post-merge-review-check.sh against the checked-in deny list.
set -euo pipefail
here=$(cd "$(dirname "$0")" && pwd)
check=$here/post-merge-review-check.sh
deny=$here/../post-merge-review-deny.txt
fail=0

expect() { # expect WANT_EXIT NAME PATH...
	local want=$1 name=$2
	shift 2
	local got=0
	printf '%s\n' "$@" | bash "$check" "$deny" >/dev/null || got=$?
	if [[ $got != "$want" ]]; then
		echo "FAIL $name: exit $got, want $want"
		fail=1
	fi
}

expect 0 "docs only" docs/README.md README.md
expect 0 "tests and harness outside the data path" core/ops/cleanup_projection_test.go testops/run.sh
expect 0 "status and metrics" core/host/master/observation_service.go core/host/volume/memory_status.go
expect 0 "release is not lease" cmd/sw-block/release.go
expect 0 "no paths" ""
expect 1 "storage" core/storage/walstore.go
expect 1 "durable test file" core/frontend/durable/provider_chunk_detach_abort_test.go
expect 1 "nvme" core/frontend/nvme/target.go
expect 1 "authority" core/authority/lease_stats.go
expect 1 "master workspace" core/host/master/workspace_binding.go
expect 1 "volume workspace lease" core/host/volume/workspace_lease.go
expect 1 "proto" core/rpc/proto/control.proto
expect 1 "generated proto" core/rpc/control/control.pb.go
expect 1 "gc by name" cmd/sw-block/chunk_gc.go
expect 1 "lease by name elsewhere" cmd/foo/lease_table.go
expect 1 "epoch by name" core/ops/epoch_view.go
expect 1 "the check itself" .github/post-merge-review-deny.txt
expect 1 "a workflow" .github/workflows/test.yml
expect 1 "mixed: allowed plus denied" docs/README.md core/storage/walstore.go
# An empty deny list is a usage error, never a pass.
empty=$(mktemp)
got=0
echo docs/README.md | bash "$check" "$empty" >/dev/null 2>&1 || got=$?
rm -f "$empty"
[[ $got == 2 ]] || { echo "FAIL empty deny list: exit $got, want 2"; fail=1; }

if ((fail)); then exit 1; fi
echo "post-merge-review check: self-test ok"
