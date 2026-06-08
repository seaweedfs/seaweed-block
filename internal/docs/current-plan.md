# Current Plan: Phase 37 - Live Node Evidence Hardening

Status: active, 60% complete. Started on 2026-06-06.

Branch: `phase33-testops-failure-hardening`

Previous phase: Phase 36 is closed in
`internal/docs/finished-plans/phase36_finishedplan_productized_operations_actionability.md`.

## Product Goal

Make node readiness blockers real, not replay-only, without expanding into a
general node-operations phase.

Phase 36 proved that read-only status surfaces can agree. Phase 37 makes the
node facts behind that status trustworthy enough to support future lifecycle
actions.

The hard exit statement:

```text
kubectl, CRD status, report, dashboard, and ops explain must agree on node
readiness and node blockers from live evidence, not replay-only bundles or
helper summaries.
```

## Scope Contract

| In | Out |
|---|---|
| Kubernetes Node Ready / SchedulingDisabled evidence | mutating operator lifecycle |
| CSI node pod readiness evidence | finalizers/delete safety |
| CSIDriver and per-node CSI plugin registration evidence | automatic cleanup |
| required image presence or image-pull status | repair/rebuild/failback |
| iSCSI and multipath prerequisite evidence | backup/snapshot/restore |
| loopback publish-target cross-node blocker | NVMe ANA parity |
| CRD/report/dashboard/explain/Event agreement | broad node-health monitoring |
| TestOps live negative-node gates | performance/SLO claims |

Allowed implementation rule:

```text
Phase 37 may read Kubernetes, blockmaster observation, node/pod/image facts,
and host prerequisite evidence.

Phase 37 may write CRD .status and Kubernetes Events through the existing
status-only controller.

Phase 37 must not mutate workloads, PVCs, PVs, storage, iSCSI sessions,
multipath maps, hostPath data, Helm releases, image state, or CRD spec.
```

## D1: Live Node Evidence Contract

Goal: define the exact live node facts and stable reason codes before code
changes.

Status: complete.

Required facts:

- Kubernetes node Ready / SchedulingDisabled.
- CSI node pod Ready and image-pull state.
- CSIDriver exists.
- per-node CSI plugin registration exists in `CSINode`.
- required `sw-block` and `sw-block-csi` image readiness or image-pull blocker.
- iSCSI prerequisite evidence.
- multipath prerequisite evidence.
- loopback publish-target cross-node risk.

Acceptance:

```text
[x] each fact has one truth source and one projection path
[x] reason codes are named for node_not_ready, node_scheduling_disabled,
      csi_node_pod_not_ready, csi_driver_not_registered,
      image_missing_on_node, iscsi_prereq_missing,
      multipath_prereq_missing, publish_target_loopback_cross_node
[x] contract states which fields are stable/provisional/test-only
[x] contract preserves read-only boundary
[x] focused tests fail first for any missing projection field
```

Verification:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
internal review against control-structure-effectiveness-review.md
```

## D2: Kubernetes Node And CSI Registration Evidence

Goal: populate live node readiness from Kubernetes API facts, not replay-only
fixtures.

Status: complete.

Acceptance:

```text
[x] Ready node projects node_ready
[x] SchedulingDisabled node projects node_scheduling_disabled
[x] NotReady node projects node_not_ready
[x] missing CSIDriver projects csi_driver_not_registered
[x] missing per-node CSINode driver registration projects csi_driver_not_registered
[x] CSI node pod image-pull or not-ready state projects csi_node_pod_not_ready
[x] CRD status, report, dashboard, and operator-snapshot agree in live TestOps gate
[x] no workload/storage mutation verbs are added
```

Verification:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
TestOps live node-readiness gate
TestOps live CSI-registration blocker gate
```

## D3: Image Presence / Image Pull Evidence

Goal: make local-image and published-image node blockers visible from live
evidence.

Status: complete.

Acceptance:

```text
[x] missing required image on a selected node projects image_missing_on_node
[x] CSI image-pull failure on csi-node pod projects image_missing_on_node or
      csi_node_pod_not_ready with stable evidence
[x] build-host local k3s import evidence is checked before local-image gates
      claim node image readiness
[x] report/dashboard/explain name the same node blocker
[x] no image import or cleanup is executed by the operator
```

Verification:

```text
go test ./scripts ./core/ops ./cmd/sw-block
TestOps live missing-image node gate
QA rerun against local-image path that previously masked missing CSI image
```

## D4: Host Prerequisite Evidence

Goal: project iSCSI and multipath prerequisites into node readiness without
performing host changes.

Status: pending.

Acceptance:

```text
[ ] healthy iSCSI prerequisite projects ready evidence
[ ] missing iSCSI prerequisite projects iscsi_prereq_missing
[ ] healthy multipath prerequisite projects ready evidence
[ ] missing multipath prerequisite projects multipath_prereq_missing
[ ] status points to safe scripted verification, not automatic repair
[ ] CRD/report/dashboard/explain agree
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
TestOps host-prereq replay gate
TestOps live host-prereq smoke if lab-safe
```

## D5: Loopback Publish Target Cross-Node Blocker

Goal: make the default loopback target boundary visible when a consumer pod
would run on a different node.

Status: pending.

Acceptance:

```text
[ ] single-node / same-node loopback target remains allowed
[ ] multi-node consumer placement with loopback publish target projects
      publish_target_loopback_cross_node
[ ] status is blocked or unknown as appropriate, never false Ready=True
[ ] docs name loopback as single-node/local-consumer only
[ ] CRD/report/dashboard/explain agree
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
TestOps loopback-cross-node blocker gate
```

## D6: Surface Agreement And Close

Goal: prove live node evidence is consistent across user surfaces and close
Phase 37 without widening the scope.

Status: pending.

Acceptance:

```text
[ ] live healthy node path agrees across kubectl, CRD status, report,
      dashboard, operator-snapshot, and explain
[ ] live NotReady/SchedulingDisabled path agrees
[ ] live CSI registration blocker agrees
[ ] live image blocker agrees
[ ] host prereq blocker agrees or is explicitly replay-only with reason
[ ] loopback cross-node blocker agrees
[ ] no negative node path shows false Ready=True
[ ] no mutating operator verbs are added
[ ] finished plan records follow-ups and non-claims
```

Verification:

```text
go test ./scripts
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --include-crds \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false
git diff --check
QA strict rerun from clean lab
```

## Current Progress

- 0%: Phase 37 opened. Scope is live node evidence hardening only:
  Kubernetes node readiness, CSI registration, image readiness, iSCSI/multipath
  prerequisites, loopback cross-node blocker, and cross-surface agreement.
- 16%: D1 live node evidence contract complete. `core/ops` now pins the
  passive read-only node fact contract and reason vocabulary for Kubernetes
  Ready/SchedulingDisabled, CSI pod/driver registration, image readiness,
  iSCSI/multipath prerequisites, and loopback cross-node blockers.
- 28%: D2 dev-complete. `sw-block ops operator-status` enriches live master
  evidence from in-cluster read-only Kubernetes facts for Nodes, CSI node pods,
  CSIDriver, and CSINode driver registration. Chart RBAC adds only
  get/list/watch for those resources. Live QA remains pending.
- 30%: D2 QA blocker reworked. Live node enrichment now emits only CRD-enum-safe
  `Ready`/`Blocked` condition types with node reason codes, and enrichment moved
  into the shared live observation loader so report/dashboard/explain can see
  the same in-cluster Kubernetes node facts as operator-status.
- 36%: D2 QA PASS on `052b321`. Healthy, cordoned, NotReady, and missing CSI
  registration states now surface from live evidence as `node_ready`,
  `node_scheduling_disabled`, `node_not_ready`, and
  `csi_driver_not_registered` without false `node_ready`. CRD, report,
  dashboard, and operator-snapshot agree. Follow-ups: collapse duplicate Ready
  conditions per node and add a server-side-dry-run/envtest status payload
  regression.
- 48%: D3 dev-complete. Live Kubernetes CSI node pod specs/status now feed
  required image and image-pull evidence into node readiness. CSI pod
  `ImagePullBackOff`/`ErrImagePull` projects `image_missing_on_node` as the
  root cause instead of masking it behind CSI registration or pod-not-ready
  symptoms. Unit coverage proves the CRD-safe projection and read-only
  operator boundary. Live QA remains pending.
- 52%: D3 QA partial reworked. QA proved the authoritative CRD path reports
  `image_missing_on_node`, but found report/dashboard/explain defaulting node
  enrichment to the app namespace instead of the Helm/control-plane namespace.
  Live node enrichment now reads CSI pods from `SW_BLOCK_HELM_NAMESPACE`
  (default `kube-system`) while PVC/app inventory still uses `--namespace`.
  `ErrImageNeverPull` is also treated as image-pull evidence for local-image
  paths. QA rerun remains pending.
- 60%: D3 QA PASS on `43d7786`. Missing CSI image now surfaces live as
  `image_missing_on_node` across CRD, report, dashboard, and explain using the
  default app namespace. Both pull modes are covered:
  `ImagePullBackOff`/`ErrImagePull` and `ErrImageNeverPull`. This closes the
  local-image masking gap that previously appeared as CSI registration or pod
  readiness symptoms.

## Next Step

Start D4 host prerequisite evidence. Project iSCSI and multipath prerequisite
facts into node readiness without performing host changes. Do not implement
finalizers, cleanup automation, rebuild/failback, backup/restore, or NVMe ANA
parity in this phase.
