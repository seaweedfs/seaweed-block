# Current Plan: Phase 35 - Kubernetes-Native Read-Only Operator Foundation

Status: active, 50% complete. Started on 2026-06-02.

Branch: `phase33-testops-failure-hardening`

Previous phase: Phase 34 is closed in
`internal/docs/finished-plans/phase34_finishedplan_test_realism_dirty_failure_hardening.md`.

## Product Goal

Make Seaweed Block observable through normal Kubernetes surfaces before adding
more protocol or day-2 lifecycle features.

User-facing target:

```text
kubectl get swblockvolumes
kubectl describe swblockvolume <name>
kubectl get events

These should explain the same Ready/Blocked/Recovered/EvidenceStale facts that
sw-block ops report/dashboard/explain already expose.
```

This phase is the operator foundation, not the full operator lifecycle.

## Scope Contract

| In | Out |
|---|---|
| `SwBlockCluster` CRD | mutating admin actions |
| `SwBlockVolume` CRD | promote/repair/rebuild/failback |
| status-only controller loop | automatic cleanup |
| Kubernetes Conditions projection | finalizers/delete safety |
| Kubernetes Events projection | upgrade execution |
| read-only RBAC boundary tests | NVMe ANA parity |
| first-volume/blocked/stale integration gates | backup/snapshot/restore |

Allowed implementation rule:

```text
The controller may read Kubernetes and Seaweed Block observation APIs.
The controller may write CRD .status and Kubernetes Events.
The controller must not mutate storage, workloads, PVCs, PVs, Secrets,
StorageClasses, Helm releases, iSCSI sessions, multipath maps, or hostPath data.
```

## D1: CRD And RBAC Contract

Goal: define the Kubernetes API shape without starting mutating lifecycle.

Status: PASS.

Deliverables:

- `SwBlockCluster` CRD manifest:
  `charts/seaweed-block/crds/swblockclusters.block.seaweedfs.com.yaml`
- `SwBlockVolume` CRD manifest:
  `charts/seaweed-block/crds/swblockvolumes.block.seaweedfs.com.yaml`
- read-only/status-only RBAC manifest:
  `charts/seaweed-block/templates/operator-status-rbac.yaml`
- schema docs that map existing ManagedVolume fields to CRD status fields:
  `internal/docs/protocol/operator-readiness-contract.md`
- QA signoff:
  `internal/docs/qa-assignments/phase35-d1-operator-crd-qa-signoff.md`

Acceptance:

```text
[done] CRDs apply cleanly on k3s server-side dry-run
[done] status subresource is enabled
[done] RBAC permits get/list/watch and status/event writes only
[done] RBAC does not permit storage mutation verbs
[done] unit/schema tests cover required condition vocabulary
```

## D2: Status-Only Controller Skeleton

Goal: create the smallest controller that can reconcile observation evidence
into CRD `.status`.

Status: PASS.

Deliverables:

- status-only reconciler interfaces:
  `core/ops/operator_status_controller.go`
- tests proving reconcile writes only cluster/volume status and Events:
  `core/ops/operator_status_controller_test.go`
- dry-run CLI entrypoint:
  `sw-block ops operator-status --dry-run`
- disabled-by-default dry-run Helm Deployment:
  `charts/seaweed-block/templates/operator-status.yaml`
- QA signoff:
  `internal/docs/qa-assignments/phase35-d2-operator-status-qa-signoff.md`
- uses existing `core/ops` ManagedVolume projection as the status source.
- writes `.status`-shaped payloads only.

Acceptance:

```text
[done] controller starts under Helm as a dry-run Deployment
[done] controller writes cluster/volume status-shaped payloads
[done] controller writes no spec, PVC, PV, workload, Secret, StorageClass, or host data
[done] scoped tests prove mutation clients are not called
[done] chart refuses operatorStatus.dryRun=false until real status writes are wired
```

Non-blocking follow-up:

```text
[open] first dry-run iterations may log blockmaster connection refused before
       blockmaster gRPC is ready; polish logs to "waiting for blockmaster"
       before D3/D8 release close.
```

## D3: Happy-Path Conditions Gate

Goal: prove a normal first PVC becomes Kubernetes-native Ready status.

Status: PASS.

Boundary:

```text
D3 patches existing SwBlockCluster / SwBlockVolume .status subresources.
It does not create CR objects. Automatic SwBlockVolume object ownership is a
separate product contract and must not be added silently.
```

Scenario:

```text
helm install
run first-volume writer/reader
create SwBlockCluster/SwBlockVolume stubs
operatorStatus.dryRun=false patches CRD .status
read SwBlockVolume status
```

Acceptance:

```text
SwBlockVolume.status.phase=Ready
Condition Ready=True reason=first_volume_verified severity=info
ready_volume_count increments on SwBlockCluster
sw-block ops report/operator-snapshot agrees with CRD status
kubectl describe shows useful status without reading bundle files
cleanup_status=ok
```

Non-blocking follow-ups:

```text
[open] add server-side-dry-run/schema validation for CRD status payloads, so
       future required-field or enum drift is caught before live QA.
[open] write-mode retry log still says "dry-run iteration failed"; relabel
       to "operator-status iteration failed" before D8 close.
```

## D4: Blocked Conditions Gate

Goal: prove known blocked states become Kubernetes-native blocked status, not
false Ready.

Status: live QA blocked on duplicate Event 409 abort; idempotency fix landed
locally, live rerun pending QA.

Scenario:

```text
use existing CSI image-pull blocked bundle/live gate
project into SwBlockVolume status
```

Acceptance:

```text
Condition Ready=False
Condition Blocked=True
reason=csi_node_image_pull_failed
safe next action is dry_run/read_only only
no Ready=True appears in CRD, report, dashboard, or operator-snapshot
Kubernetes Warning Event is emitted
```

## D5: Unknown / EvidenceStale Conditions Gate

Goal: preserve the negative-first rule for unreachable or incomplete evidence.

Scenarios:

- live status endpoint unreachable.
- SmartWAL corruption projection from Phase 34.

Acceptance:

```text
pure unreachable status does not become Blocked=True
insufficient or stale evidence becomes Ready=Unknown
EvidenceStale=True when freshness is the reason
SmartWAL corruption never becomes Ready=True
preferred follow-up: reason=wal_integrity_fault when that reason is surfaced
```

## D6: Kubernetes Events Gate

Goal: make important transitions visible through standard Kubernetes Events.

Required Events:

- `VolumeReady` as Normal.
- `CsiNodeImagePullFailed` as Warning.
- `AuthorityPromoted` as Normal.
- `EvidenceStale` as Warning.
- `CleanupRequired` as Warning when residue is detected by evidence.

Acceptance:

```text
events include involved object, reason, type, message, volume id, and evidence ref
events are deduplicated enough to avoid timeline spam
events match ManagedVolume/operator-snapshot reason codes
```

## D7: Read-Only Boundary Gate

Goal: prove the new operator foundation cannot mutate storage.

Acceptance:

```text
RBAC audit shows no create/update/patch/delete on PVC/PV/workloads/storage data
controller tests fail if mutating clients are called
HTTP/dashboard/ops surfaces remain read-only
negative scenarios produce dry-run advice only
```

## D8: Close And Release Claim Alignment

Goal: close Phase 35 as a Kubernetes-native status foundation, not a full
operator lifecycle.

Required inputs:

- D1-D7 sign-offs.
- QA rerun of first-volume, blocked, stale/unknown, and event gates.
- README/quickstart wording that explains CRD status as read-only.
- Release note with explicit non-claims.

Acceptance:

```text
kubectl-visible CRD status agrees with sw-block ops surfaces
Kubernetes Events agree with reason codes
read-only boundary is proven
no mutating operator/admin claim is made
finished plan moved under internal/docs/finished-plans/
```

## Current Progress

- 0%: Phase 35 plan opened.
- 12%: D1 CRD/RBAC contract manifests landed locally. `go test ./core/ops
  ./cmd/sw-block`, `helm lint charts/seaweed-block`, and `helm template
  --include-crds --set operatorStatus.create=true` pass.
- 18%: D1 live k3s QA signoff passed. Server-side dry-run accepted all rendered
  CRDs/RBAC; status subresources, condition vocabulary, evidence refs, and
  read-only RBAC boundary were verified.
- 22%: D2 local status-only reconciler skeleton landed. It consumes
  `ClusterEvidence`, builds the existing operator snapshot, writes
  `SwBlockCluster`/`SwBlockVolume` status-shaped payloads, emits Events, and
  has no storage mutation interface.
- 28%: D2 dry-run packaging landed. `sw-block ops operator-status --dry-run`
  projects status from bundle/master-api evidence, Helm can render a disabled
  by default dry-run Deployment, and the chart fails if real CRD writes are
  requested before the writer exists.
- 34%: D2 live k3s QA signoff passed. The dry-run controller starts under
  Helm, reads blockmaster evidence, reports `mutation_allowed=false`, writes
  zero CRD objects, is disabled by default, and rejects `dryRun=false`.
- 42%: D3 local status writer landed. `sw-block ops operator-status` can run
  without `--dry-run`, using an in-cluster REST client that PATCHes only
  `swblockclusters/status` and `swblockvolumes/status`. Component tests verify
  method/path/body/auth and reject any `spec` patch. Helm can now render
  `operatorStatus.dryRun=false`, but live k3s publication is still pending.
- 45%: D3 live QA found the volume status payload used the snake_case
  operator-snapshot action contract where the CRD requires camelCase
  `allowedActions[].mutationAllowed`. The fix maps actions into a dedicated
  CRD-status DTO and preserves the snapshot JSON contract. Live rerun pending.
- 50%: D3 re-validation passed on `e3cf010`. `SwBlockVolume.status` is
  populated with camelCase `allowedActions[].mutationAllowed=false`, status is
  `ready`, reason is `first_volume_verified`, cluster ready count is 1, spec
  remains untouched, and the service account still has no storage/workload
  mutation power.
- 58%: D4 local event publication landed. In write mode the same
  status-only Kubernetes client now also creates core/v1 Events through the
  already-scoped `events/create` RBAC. Tests verify `csi_node_image_pull_failed`
  produces a Warning Event involved with the `SwBlockVolume`, while the status
  writer still patches only `/status`.
- 60%: D4 live QA found blocked volumes emit two same-reason Warning Events
  (`Ready=False` + `Blocked=True`), causing duplicate event names and HTTP 409.
  The local fix treats 409 AlreadyExists as idempotent success and makes event
  emission best-effort so telemetry cannot abort later status writes.

## Next Step

Ask QA to rerun the D4 blocked-condition gate with
`operatorStatus.dryRun=false`: create blocked evidence/stubs, verify
`SwBlockVolume.status` is blocked, verify a Kubernetes Warning Event appears,
and verify no Ready=True appears in CRD/report/dashboard/operator-snapshot.

```text
Condition Ready=False
Condition Blocked=True
reason=csi_node_image_pull_failed
event Warning CsiNodeImagePullFailed
```
