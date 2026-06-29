# Phase 54 D5 QA Sign-off: Authority Executor Negative Matrix

Verdict: **PASS**.

Validated live on m02 k3s with the current `sw-block:local` image built from
the Phase 54 branch.

Runner:

```text
C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results C:\work\seaweed_block\testops\scenarios\authority-executor-negative-chain.yaml
```

Run:

```text
20260623-112339-a395
authority-executor-negative-chain PASS, 26/26 actions
```

## Evidence

Summary:

```text
phase54_authority_executor_negative_status=running
phase54_authority_executor_image=sw-block:local
phase54_authority_executor_node=m02
exec_patch_swblockreplicaeligibilities_status_allowed=yes
exec_patch_swblockreplicaeligibilities_main_denied=no
exec_patch_swblockvolumes_status_denied=no
exec_create_events_denied=no
exec_create_pods_denied=no
exec_patch_pvc_denied=no
exec_update_storageclass_denied=no
blocked-preflight_blocked_reason=1
blocked-preflight_mutation_attempts=0
blocked-preflight_target_reason_absent=
blocked-preflight_expected_hold=preflight_not_ready
stale-frontier_blocked_reason=1
stale-frontier_mutation_attempts=0
stale-frontier_target_reason_absent=
stale-frontier_expected_hold=durable_frontier_behind
unsafe-frontend_blocked_reason=1
unsafe-frontend_mutation_attempts=0
unsafe-frontend_target_reason_absent=
unsafe-frontend_expected_hold=frontend_no_longer_fenced
ambiguous_target_missing_count=1
ambiguous_target_mutation_attempts=0
ambiguous_target_a_reason_absent=
ambiguous_target_b_reason_absent=
identity_mismatch_target_missing_count=1
identity_mismatch_mutation_attempts=0
identity_mismatch_target_reason_absent=
partial_contracts=2
partial_mutation_attempts=1
partial_terminal_missing=1
partial_a_reason=ack_eligibility_recorded
partial_b_reason_absent=
partial_c_no_target=0
phase54_authority_executor_negative_status=ok
```

## Gate Results

| Case | Result |
| --- | --- |
| Blocked preflight | PASS: held with zero mutation |
| Stale/frontier-behind evidence | PASS: held with zero mutation |
| Unsafe frontend state | PASS: held with zero mutation |
| Ambiguous target | PASS: no target status written |
| Cross-volume identity mismatch | PASS: no target status written |
| Mixed reconcile | PASS: eligible A wrote `ack_eligibility_recorded`; blocked B held; no-contract C untouched |
| Boundary carry-forward | PASS: only target status patch allowed; SwBlockVolume status, Events, pods, PVCs, storageclasses denied |
| Cleanup | PASS: namespace/jobs/CRs/CRDs cleaned; no Phase 54 residue found |

## Verdict

D5 closes. The executor handles the negative/hold matrix without speculative
mutation and can perform a partial reconcile without cross-volume contamination.

D6 should turn the mixed case into a dedicated multi-volume isolation gate with
three or more volumes and explicit no-contamination assertions across identities
and target statuses.
