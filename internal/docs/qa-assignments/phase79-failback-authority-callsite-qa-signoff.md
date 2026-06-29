# Phase 79 Failback Authority Call-site QA Sign-off

Verdict: PASS.

Date: 2026-06-26.

Scope: local call-site gate. This phase proves the failback executor can invoke
the authority-owned failback runtime adapter under explicit execution policy.
It does not enable automatic deployed failback.

## Result

```text
phase79_failback_authority_callsite_status=ok
core_ops_failback_authority_callsite_tests=pass
```

## Gate Evidence

```text
authority_runtime_adapter_invoked_by_executor=true
stale_expected_current_blocks_callsite=true
execution_policy_still_required=true
dry_run_no_status_write=true
publisher_authority_line_advanced=true
authority_epoch_advanced=true
single_primary_after_failback=true
publish_target_swapped_after_failback=true
failed_back_status_written=true
runtime_failure_no_false_failback=true
authority_mutation_allowed_only_with_execution_policy=true
frontend_publication_allowed=false
storage_mutation_allowed=false
```

## Checks

| Check | Result |
| --- | --- |
| Executor invokes `AuthorityFailbackRuntime` adapter | PASS |
| Adapter advances Publisher authority line from `r2@2` to `r1@3` | PASS |
| Executor writes `failed_back` only after terminal evidence | PASS |
| Stale expected-current evidence is blocked with no false failback | PASS |
| Execution policy remains required | PASS |
| Dry-run writes no status | PASS |
| Frontend publication remains false | PASS |
| Storage mutation remains false | PASS |
| Runner scenario validates | PASS |

## Verification Commands

```text
go test ./core/ops -run "TestFailbackExecutorUsesAuthorityRuntimeAdapter|TestFailbackAuthorityRuntimeAdapterRejectsStaleExpectedCurrent|TestFailbackExecutorExecutionPolicyBlocks|TestFailbackExecutorDryRunDoesNotWriteStatus" -count=1 -v
go test ./core/authority ./core/ops ./cmd/sw-block
helm lint charts/seaweed-block
"C:\Program Files\Git\bin\bash.exe" scripts/run-phase79-failback-authority-callsite-gate.sh .
C:\work\swblock.exe validate testops\scenarios\failback-authority-callsite-chain.yaml
git diff --check
```

## Non-Claims

This sign-off does not claim:

```text
automatic failback from the deployed controller loop
blockmaster HTTP/gRPC failback endpoint
blockvolume frontend switching
frontend publication after failback
storage rebuild/catch-up traffic
workload mutation
NVMe ANA behavior
```

The call-site is in-process and explicit-policy gated. A future phase must wire
it to the real component that owns the Publisher before it becomes a deployed
product path.
