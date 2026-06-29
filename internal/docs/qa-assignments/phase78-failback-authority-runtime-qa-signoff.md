# Phase 78 Failback Authority Runtime QA Sign-off

Verdict: PASS.

Date: 2026-06-26.

Scope: local contract gate. No live Kubernetes mutation gate was required for
this phase because the new behavior is an authority package seam plus CRD/CLI
contract validation. Default deployed behavior remains disabled/status-only.

## Result

```text
phase78_failback_authority_runtime_status=ok
core_authority_failback_runtime_tests=pass
core_ops_failback_authority_runtime_tests=pass
cmd_failback_authority_runtime_tests=pass
```

## Gate Evidence

```text
authority_failback_reassign_minted=true
stale_expected_current_rejected=true
terminal_preconditions_required=true
failback_target_endpoint_fields=true
failback_target_expected_current_fields=true
executable_failback_requires_authority_endpoint=true
http_runtime_contract_includes_authority_fields=true
swblockvolume_returned_replica_endpoint_schema=true
failback_target_schema_authority_fields=true
target_writer_serializes_authority_fields=true
cmd_default_executor_still_disabled=true
cmd_runtime_success_allows_authority_mutation=true
```

Terminal authority evidence:

```text
authority_epoch_advanced=true
single_primary_after_failback=true
publish_target_swapped_after_failback=true
explicit_runtime_authority_mutation_allowed=true
storage_mutation_allowed=false
frontend_publication_allowed=false
```

## Checks

| Check | Result |
| --- | --- |
| Authority runtime mints reassignment through `Publisher.apply(IntentReassign)` | PASS |
| Stale expected-current replica/epoch is rejected before mutation | PASS |
| Missing ACK/fencing/frontier/identity terminal evidence is rejected | PASS |
| Returned-replica endpoint facts are preserved into target spec | PASS |
| Executable target requires endpoint and expected-current fields | PASS |
| Default and dry-run executor remain non-mutating | PASS |
| Explicit runtime success reports authority mutation only | PASS |
| Storage mutation and frontend publication remain false | PASS |
| CRD schemas use camelCase fields | PASS |
| Runner scenario validates | PASS |

## Verification Commands

```text
go test ./core/authority -run "TestFailbackAuthorityRuntime" -count=1 -v
go test ./core/ops -run "TestFailbackExecutor|TestFailbackTargetOwner|TestHTTPFailbackRuntime|TestPhase46D2SwBlockVolumeReturnedReplicaSchema|TestPhase75SwBlockReplicaFailbackTargetSchema|TestKubernetesStatusClientCreatesSwBlockReplicaFailbackWithoutStatus" -count=1 -v
go test ./cmd/sw-block -run "TestOpsFailback" -count=1 -v
go test ./core/authority ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase78-failback-authority-runtime-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-authority-runtime-chain.yaml
git diff --check
```

## Non-Claims

This sign-off does not claim:

```text
automatic failback in the deployed controller loop
blockmaster HTTP/gRPC failback endpoint
blockvolume frontend switching
frontend publication after failback
storage rebuild/catch-up traffic
workload mutation
NVMe ANA behavior
```

Phase 78 proves the first authority-owned seam and keeps the user-visible
default disabled. The next QA gate should cover the real product call-site that
invokes this seam.
