# Phase 51 Returned-Replica ACK Evidence Gate QA Sign-off

Verdict: PASS.

Live QA run: `20260621-003502-a3ce`, 18/18 actions PASS.

## Gates

| Gate | Result | Evidence |
|---|---|---|
| Local tests | PASS | `go test -count=1 ./core/ops ./cmd/sw-block` |
| Script syntax | PASS | `bash -n scripts/run-phase47-returned-replica-status-schema-rbac-gate.sh` |
| Scenario validation | PASS | `swblock validate testops/scenarios/returned-replica-status-schema-rbac-chain.yaml` |
| Live CRD schema/RBAC | PASS | `swblock run ...`, 18/18 actions |
| ACK-known projection | PASS | `executor_preflight_ack_eligibility_known_projected=true` |
| Status subresource boundary | PASS | status/events yes; main/finalizers/pods/pvc/storageclass no |
| Server dry-run non-mutation | PASS | `server_dry_run_status_mutated=false` |

## Live Evidence

```text
operator_status_patch_status_allowed=yes
operator_status_update_status_allowed=yes
operator_status_create_events_allowed=yes
operator_status_main_patch_allowed=no
operator_status_finalizers_patch_allowed=no
operator_status_pods_patch_allowed=no
operator_status_pvc_patch_allowed=no
operator_status_storageclass_update_allowed=no
valid_returned_replica_status_server_dry_run=true
valid_executor_preflight_status_server_dry_run=true
executor_preflight_ack_eligibility_known_projected=true
executor_preflight_forbidden_mutation_class_projected=true
snake_case_action_rejected=true
unsupported_action_mode_rejected=true
main_object_patch_rejected=true
server_dry_run_status_mutated=false
phase47_returned_replica_status_schema_rbac_status=ok
```

## Boundary

No executor ran. No RBAC expanded. No CRD spec/finalizer/storage/workload
mutation was added.

The product now distinguishes:

```text
ACK eligibility unknown -> executor preflight hold
ACK eligibility known false -> executor preflight may be ready
```

This closes the main semantic risk before a future mutating returned-replica
executor is designed.
