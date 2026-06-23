# Phase 54 D6 QA Sign-off: Authority Executor Multi-Volume Isolation

Verdict: **PASS**.

Validated live on m02 k3s with the current `sw-block:local` image built from
the Phase 54 branch.

Runner:

```text
C:\work\swblock.exe run -results-dir C:\work\seaweed_block\results C:\work\seaweed_block\testops\scenarios\authority-executor-multivolume-chain.yaml
```

Run:

```text
20260623-113753-d07f
authority-executor-multivolume-chain PASS, 32/32 actions
```

## Evidence

Summary:

```text
phase54_authority_executor_multivolume_status=running
phase54_authority_executor_image=sw-block:local
phase54_authority_executor_node=m02
exec_patch_swblockreplicaeligibilities_status_allowed=yes
exec_patch_swblockreplicaeligibilities_main_denied=no
exec_patch_swblockvolumes_status_denied=no
exec_create_events_denied=no
multivolume_contracts=4
multivolume_mutation_attempts=2
multivolume_ack_mutation_attempts=2
multivolume_terminal_missing=1
multivolume_target_missing=1
eligible_a_reason=ack_eligibility_recorded
eligible_a_ack=true
eligible_a_no_cross_volume=true
eligible_a_source_ack_still_false=false
eligible_b_reason=ack_eligibility_recorded
eligible_b_ack=true
eligible_b_no_cross_volume=true
eligible_b_source_ack_still_false=false
blocked_c_reason_absent=
mismatch_e_reason_absent=
no_contract_d_target_count=0
eligible_written_count=2
cross_contamination_count=0
phase54_authority_executor_multivolume_status=ok
```

## Gate Results

| Check | Result |
| --- | --- |
| Eligible A | PASS: target status wrote `ack_eligibility_recorded` |
| Eligible B | PASS: target status wrote `ack_eligibility_recorded` |
| Blocked C | PASS: target reason remains absent |
| No-contract D | PASS: no target object was created |
| Mismatch E | PASS: mismatched target reason remains absent |
| Write count | PASS: exactly 2 ACK eligibility target statuses written |
| Cross-volume contamination | PASS: `cross_contamination_count=0` |
| Source volume status | PASS: source ACK fields remain unchanged |
| Boundary carry-forward | PASS: only target status patch allowed; target main, SwBlockVolume status, and Events denied |
| Cleanup | PASS: namespace/jobs/CRs/CRDs cleaned; no Phase 54 residue found |

## First-run Harness Fix

The first D6 run failed before sign-off because the gate counted
`reasonCode` with a compact-JSON grep while `kubectl -o json` emitted pretty
JSON. The product behavior in that run already matched the expected target
statuses. The gate was fixed to count via jsonpath, then re-run live to PASS.

## Verdict

D6 closes. The executor can write ACK eligibility for multiple eligible
volumes in one reconcile while holding blocked, mismatched, and no-contract
volumes without cross-volume status contamination.

D7 remains the live returned-replica close gate.
