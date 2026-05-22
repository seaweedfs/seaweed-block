# Invariant Ledger

This ledger tracks current Seaweed Block protocol claims that must remain
testable. It follows the V3 rule:

```text
An invariant without a test is a wish, not an invariant.
```

Rows are append-only. If a row is retired, archive it with a reason instead of
deleting it.

## Status

- `ACTIVE`: test exists and has been run for the current claim.
- `STUB`: row exists but a test/gate must still be added before release.
- `ARCHIVED`: retired or superseded.

## Current Rows

| ID | Statement | Owner Layer | Test / Evidence | Status |
|---|---|---|---|---|
| `INV-AUTH-ONE-PRIMARY-001` | A volume has at most one current primary after promotion; promotion evidence must report `post_failure_primary_count=1`. | authority / master | Stage 1 RF3 recovery, Stage 2 D4, Node-Loss D4 bundles | ACTIVE |
| `INV-FENCE-STALE-PRIMARY-001` | A stale primary cannot ACK post-failure writes after authority moves or target is stopped. | frontend + authority | Stage 2 D4 `old_primary_stale_io_success_count=0`; node-loss D4 stale-primary evidence | ACTIVE |
| `INV-PROMOTE-FRONTIER-001` | A replica is promotion-eligible only when it covers the required durable frontier for the active ACK profile. | recovery + durable | RF3 sync-quorum promotion-ready and recovery gates | ACTIVE |
| `INV-CSI-CONSUMES-AUTHORITY-001` | CSI consumes the current publish target from master; CSI must not mint authority or choose a primary. | CSI + master API | Control-plane observation D5 `csi_reattach_observed` event with master-minted event ID | ACTIVE |
| `INV-K8S-NONLOOPBACK-001` | Cross-node Kubernetes attach must not publish loopback targets; multi-node values must enable external iSCSI/status. | K8s adaptor / Helm | Node-loss D3/D4, Helm multi-node first-volume, `generate-helm-values` CLI tests | ACTIVE |
| `INV-OBS-PRODUCT-EVENTS-001` | Product-owned event stream must include placement, promotion candidate evaluation, authority publication, and CSI reattach evidence for node-loss recovery. | observation / master | Control-plane observation close gate, `cluster-evidence.json` | ACTIVE |
| `INV-HELM-RF-NODECOUNT-001` | Generated Helm values fail closed when requested replication factor exceeds selected Ready schedulable node count. | Helm values generator | `TestOpsGenerateHelmValuesRejectsRFAboveSelectedNodes` | ACTIVE |
| `INV-HOSTPATH-TRANSPARENT-001` | Transparent failover claim requires same pod UID, host path switched to promoted target, stale primary fenced, one primary, and post-failure checksum. | host path + authority + workload | Stage 2 D4 mounted-failover close report | ACTIVE |
| `INV-K8S-REATTACH-001` | Kubernetes node-loss recovery claim through pod recreate requires CSI re-stage to promoted frontend and reader checksum verification. | K8s adaptor + CSI + authority | Node-Loss Survival D4 run `20260516-160306-1e54` | ACTIVE |
| `INV-MANAGED-VOLUME-READMODEL-001` | User-facing PVC/volume explanations must be derived from the ManagedVolume read model, not independently recomposed by CLI, dashboard, TestOps, CSI logs, or shell grep. | operations model | `core/ops/managed_volume_model_test.go`, `core/ops/managed_volume_evidence_test.go`, `core/ops/observation_report_test.go` | ACTIVE |
| `INV-CONTROL-CONTEXT-001` | Cross-layer explanations must be derived from facts/events, not reconstructed from TestOps-only grep order. Superseded in scope by `INV-MANAGED-VOLUME-READMODEL-001` but retained for continuity. | observation | Product event stream D5; Phase 22 bundle artifact replay tests in `core/ops/managed_volume_artifact_test.go` and `core/ops/observation_bundle_test.go` | ACTIVE |
| `INV-K8S-ADAPTOR-FACTS-001` | K8s adaptor must express PVC/PV/Pod/Node/CSI facts as typed facts with source, time, generation, and confidence before deriving recovery status. | K8s adaptor / future operator | PVC pending, mount-failure, image-pull, loopback, and node-loss reattach tests in `core/ops/managed_volume_model_test.go` and `core/ops/managed_volume_artifact_test.go` | ACTIVE |
| `INV-HOSTPATH-FACTS-001` | Host path adaptor must distinguish iSCSI session reachability, multipath path state, ALUA state, and workload data check before claiming transparent failover. | host path adaptor | Stage 2 transparent host-path projection and non-claim tests in `core/ops/managed_volume_model_test.go` plus artifact replay in `core/ops/managed_volume_artifact_test.go` | ACTIVE |

## Add Row Checklist

When adding a row:

1. Give it a stable ID.
2. State one concrete behavior.
3. Name the owner layer.
4. Add the test or gate that proves it.
5. If no test exists yet, mark `STUB` and block release claims that depend on
   it.

## Release Discipline

For each alpha/beta release candidate:

```text
total rows:
active rows:
stub rows:
gates passed:
known deferrals:
```

No release note should claim behavior backed only by `STUB` rows.
