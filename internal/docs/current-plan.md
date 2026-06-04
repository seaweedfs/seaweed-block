# Current Plan: Phase 35 - Kubernetes-Native Read-Only Operator Foundation

Status: complete, 100%. Started on 2026-06-02. Closed on 2026-06-04.

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

Status: PASS.

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

Non-blocking follow-ups:

```text
[open] make Event names stable per object+reason so persistent blocked
       conditions dedupe across reconcile iterations instead of growing one
       Event every interval.
[open] include condition type or equivalent disambiguator if Ready=False and
       Blocked=True same-reason events should coexist as separate Events.
```

## D5: Unknown / EvidenceStale Conditions Gate

Goal: preserve the negative-first rule for unreachable or incomplete evidence.

Status: PASS.

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

Local verification:

```text
[done] operator-status projects evidence_stale as Ready=Unknown +
       EvidenceStale=True and emits a Warning Event.
[done] operator-status projects status_endpoint_unreachable as Ready=Unknown +
       EvidenceStale=True, not Blocked=True, and emits a Warning Event.
[done] operator-status projects wal_integrity_fault as non-Ready
       (Blocked/Ready=False) with reason=wal_integrity_fault.
[done] go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
[done] helm lint charts/seaweed-block
[done] helm template with operatorStatus.create=true,dryRun=false renders
       status/events-only RBAC and no --dry-run arg.
```

QA signoff:

```text
internal/docs/qa-assignments/phase35-d5-stale-status-projections-qa-signoff.md
```

Result:

```text
[done] status_endpoint_unreachable -> status=unknown, Ready=Unknown,
       EvidenceStale=True, no Blocked condition, Warning Event.
[done] wal_integrity_fault -> status=blocked, Ready=False, Blocked=True,
       Warning Event, no Ready=True.
[done] one reconcile wrote both volumes and emitted Events with exit=0.
[done] SA boundary unchanged: Events + status yes; spec/pods/PVC mutation no.
```

## D6: Kubernetes Events Gate

Goal: make important transitions visible through standard Kubernetes Events.

Status: PASS.

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

Local verification:

```text
[done] Kubernetes Event names are stable per SwBlockVolume object, Event type,
       and reason code. The name no longer embeds observedAt.UnixNano().
[done] Persistent same-reason Events become idempotent 409 success across
       reconcile iterations instead of creating a new Event each time.
[done] Normal and Warning Events for the same object/reason get different
       names.
[done] go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
[done] helm lint charts/seaweed-block
```

QA signoff:

```text
internal/docs/qa-assignments/phase35-d6-stable-event-identity-qa-signoff.md
```

Result:

```text
[done] three repeated write-mode reconciles against a persistent
       wal_integrity_fault volume all exited 0.
[done] distinct Event object count stayed at 1 across all three reconciles.
[done] Event name is stable per object/type/reason:
       pvc-walfault-warning-wal-integrity-fault.
[done] status stayed blocked/Ready=False with no Ready=True.
[done] SA boundary stayed status/events only.
```

Non-blocking follow-ups:

```text
[open] 409 dedupe does not patch count++/lastTimestamp; CRD status carries
       freshness, so this is optional telemetry polish.
[open] same type+reason conditions still collapse to one Event; CRD conditions
       carry the detailed Ready/Blocked split, so this is acceptable for D6.
[open] CleanupRequired Event remains pending until cleanup residue evidence is
       projected as a ManagedVolume condition.
```

## D7: Read-Only Boundary Gate

Goal: prove the new operator foundation cannot mutate storage.

Status: PASS.

Acceptance:

```text
RBAC audit shows no create/update/patch/delete on PVC/PV/workloads/storage data
controller tests fail if mutating clients are called
HTTP/dashboard/ops surfaces remain read-only
negative scenarios produce dry-run advice only
```

Local verification:

```text
[done] operator-status Helm RBAC grants only get/list/watch on CRDs,
       get/update/patch on CRD status subresources, and create Events.
[done] operator-status Deployment uses its dedicated ServiceAccount.
[done] KubernetesStatusClient only PATCHes CRD /status and POSTs Events.
[done] no operator-status code path exposes PVC/PV/workload/Secret/
       StorageClass/host mutation methods.
[done] go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
[done] helm lint charts/seaweed-block
```

QA assignment:

```text
internal/docs/qa-assignments/phase35-d7-read-only-boundary-qa-assignment.md
```

QA signoff:

```text
internal/docs/qa-assignments/phase35-d7-read-only-boundary-qa-signoff.md
```

Result:

```text
[done] live ClusterRole has exactly three rules:
       CRD get/list/watch, CRD status get/update/patch, Events create.
[done] 7 allowed status/event/read checks returned yes.
[done] 21 forbidden spec/storage/workload/config mutation checks returned no.
[done] write-mode reconcile changed only .status and Events.
[done] SwBlockCluster.spec and SwBlockVolume.spec remained unchanged.
[done] no PVC/PV/workload was created by operator-status.
```

Non-blocking doc fix:

```text
[done] assignment status-subresource can-i commands now use
       --subresource=status to avoid false negatives on kubectl v1.34.
```

## D8: Close And Release Claim Alignment

Goal: close Phase 35 as a Kubernetes-native status foundation, not a full
operator lifecycle.

Status: PASS.

Required inputs:

- D1-D7 sign-offs.
- QA rerun of first-volume, blocked, stale/unknown, and event gates.
- README/quickstart wording that explains CRD status as read-only.
- Release note with explicit non-claims.

Acceptance:

```text
[done] kubectl-visible CRD status agrees with sw-block ops surfaces
[done] Kubernetes Events agree with reason codes
[done] read-only boundary is proven
[done] no mutating operator/admin claim is made
[done] finished plan moved under internal/docs/finished-plans/
```

Close artifacts:

```text
internal/docs/finished-plans/phase35_finishedplan_kubernetes_native_read_only_operator_foundation.md
docs/releases/v0.4-beta-candidate.md
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
- 66%: D4 re-validation passed on `a2714c8`. Blocked one-shot reconciles exit
  0 even with duplicate/already-existing Events, Warning Event exists,
  `SwBlockVolume.status` is blocked with Ready=False and Blocked=True, no
  Ready=True appears on blocked surfaces, and RBAC remains status/events only.
- 72%: D5 local contract coverage landed. Operator-status tests now lock
  `evidence_stale`, `status_endpoint_unreachable`, and `wal_integrity_fault`
  projections through the CRD-status/Event layer. Scoped Go tests, Helm lint,
  and write-mode Helm render pass. Live QA is pending.
- 78%: D5 live QA passed on `6859be1`. In one write-mode reconcile,
  `status_endpoint_unreachable` projected to Unknown/EvidenceStale without
  Blocked or Ready=True, while `wal_integrity_fault` projected to
  Blocked/Ready=False. Warning Events landed and the status/events-only RBAC
  boundary held.
- 82%: D6 event identity hygiene landed locally. Kubernetes Event names are
  stable per object/type/reason, so repeated persistent conditions dedupe via
  idempotent 409 handling instead of producing timestamp-suffixed Event spam.
  Scoped Go tests and Helm lint pass. Live QA and full reason/type coverage are
  pending.
- 88%: D6 live QA passed on `1b22ccc`. Three repeated write-mode reconciles
  against a persistent `wal_integrity_fault` blocked volume all exited 0 and
  left exactly one distinct Kubernetes Event named
  `pvc-walfault-warning-wal-integrity-fault`. Status remained
  blocked/Ready=False and RBAC remained status/events only.
- 92%: D7 local read-only boundary review passed. The operator-status
  ServiceAccount, writer interface, Kubernetes client, and chart render are
  limited to CRD reads, CRD status writes, and core Event creation. Live
  `kubectl auth can-i` QA remains pending.
- 96%: D7 live QA passed on `a9f43e1`. The operator-status ClusterRole has
  exactly three rules, 7 allowed checks returned yes, 21 forbidden
  spec/storage/workload mutation checks returned no, and write-mode reconcile
  changed only CRD status and Events with `mutation_allowed=false`.
- 100%: D8 close completed. README, quickstart, release index, release note,
  roadmap, and finished plan now describe the narrow claim: Kubernetes-native
  read-only CRD status and Events foundation, not a mutating operator lifecycle.

## Next Step

Phase 35 is closed. Next phase should choose a new product loop explicitly:
mutating operator lifecycle, cleanup/finalizer safety, model hardening, NVMe ANA
parity, rebuild/failback, or backup/snapshot/restore. Do not mix them into this
closed read-only operator foundation.
