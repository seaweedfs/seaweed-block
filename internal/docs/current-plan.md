# Current Plan: Phase 36 - Productized Operations Actionability

Status: active, 58% complete. Started on 2026-06-05.

Branch: `phase33-testops-failure-hardening`

Previous phase: Phase 35 is closed in
`internal/docs/finished-plans/phase35_finishedplan_kubernetes_native_read_only_operator_foundation.md`.

## Product Goal

Turn the Phase 35 Kubernetes-native read-only status foundation into an
actionable operations surface.

The goal is not to make the operator mutate storage. The goal is to make the
central control-plane status model useful enough that a user can answer:

```text
Are my nodes ready to run Seaweed Block?
Why is this volume Ready, Blocked, Unknown, or CleanupRequired?
What evidence backs that answer?
What safe read-only or scripted next step should I take?
What support bundle should I collect or replay?
```

This phase is a read-only control-plane model consolidation slice. It continues
the pattern:

```text
truth owner publishes facts
status/orchestration entity aggregates judgment
executor remains non-mutating in this phase
evidence/timeline explains why the judgment is allowed
```

## Scope Contract

| In | Out |
|---|---|
| node readiness facts in `SwBlockCluster.status.nodes[]` | mutating operator lifecycle |
| support-bundle/evidence refs in CRD status | automatic support-bundle upload |
| cleanup visibility and `CleanupRequired` projection | automatic cleanup |
| safe next-step / dry-run action hints | promote/repair/rebuild/failback |
| status/report/dashboard/operator-snapshot agreement | finalizers/delete safety |
| negative-first TestOps gates | upgrade execution |
| docs and release-claim alignment | NVMe ANA parity |
| | backup/snapshot/restore |

Allowed implementation rule:

```text
The controller may read Kubernetes and Seaweed Block observation APIs.
The controller may write CRD .status and Kubernetes Events.
The controller may point to evidence and suggest safe commands.
The controller must not mutate storage, workloads, PVCs, PVs, Secrets,
StorageClasses, Helm releases, iSCSI sessions, multipath maps, hostPath data,
or CRD spec.
```

## D1: Operations Model Contract Review

Goal: define the exact Phase 36 status vocabulary before code changes.

Status: PASS.

Deliverables:

- update the operator/readiness contract with node readiness, support evidence,
  cleanup visibility, and safe next-step fields,
- define which facts are truth-owner inputs and which fields are aggregated
  judgments,
- mark every field stable/provisional/test-only,
- include explicit non-claims and mutation boundary.

Acceptance:

```text
[done] field contract names `SwBlockCluster.status.nodes[]`
[done] field contract names cleanup visibility and residue counters
[done] field contract names evidence refs and support-bundle pointers
[done] field contract names safe next-step / dry-run action hints
[done] internal review confirms no field implies mutating operator ownership
[done] scoped unit tests fail first for any new status projection contract
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
internal review against roadmap and Phase 35 non-claims
```

## D2: Node Readiness / Preflight Status

Goal: publish node-level readiness facts into `SwBlockCluster.status.nodes[]`.

Status: PASS.

Required facts:

- Kubernetes node name and observed IP,
- schedulable/Ready state,
- iSCSI capability and residue state,
- multipath capability and residue state,
- image readiness or image-pull blocker,
- hostPath persistence readiness when enabled,
- observed component/version/image where available.

Acceptance:

```text
[done] healthy node evidence projects Ready node status
[done] missing image projects blocked node reason
[done] blocked node reason is stable and visible in CRD status
[done] node readiness projects to operator-snapshot as well as CRD status
[done] no storage/workload mutation verbs are added
[done] live 1-node and 3-node status agreement
[done] replay-only missing-image blocker projects blocked/image_missing_on_node
```

Verification:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
TestOps healthy-node-readiness gate
TestOps negative node-preflight blocker gate
QA rerun on 1-node and 3-node lab if code reaches live status
```

## D3: Support Bundle Pointers And Evidence Refs

Goal: make blocked/unknown status self-explaining through evidence references.

Status: PASS.

Deliverables:

- CRD status includes report/support-bundle/evidence refs when available,
- `sw-block ops report` and dashboard render the same refs,
- `ops explain` names the safe collection command when evidence is insufficient,
- from-bundle replay preserves the same refs without live cluster access.

Acceptance:

```text
[done] blocked status includes evidence_ref
[done] unknown/stale status includes evidence_ref or missing-evidence reason
[done] support-bundle command is suggested as read-only/scripted next step
[done] report summary, HTML, operator-snapshot, and CRD status expose support refs
[done] from-bundle report/dashboard/operator-snapshot agree with live report
[done] cold-reader bundle can explain a blocker without SSH/log spelunking
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
TestOps support-bundle blocked-path gate
TestOps from-bundle replay agreement gate
QA cold-reader review of generated bundle
```

## D4: Cleanup Visibility, Not Automatic Cleanup

Goal: project cleanup residue into status without performing cleanup.

Status: pending.

Required status behavior:

- clean state: `CleanupRequired=False`,
- residue found: `CleanupRequired=True`,
- residue type/count fields identify Kubernetes/iSCSI/multipath/dmsetup/process/
  hostPath categories,
- safe next step points to existing cleanup/verifier scripts,
- status remains read-only and does not delete anything.

Acceptance:

```text
[ ] verifier summary maps to CRD cleanup fields
[ ] residue scenario projects CleanupRequired=True
[ ] clean scenario projects CleanupRequired=False
[ ] cleanup action hint is mode=read_only or mode=scripted, never mutation_allowed=true
[ ] support surfaces agree on residue counts
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
TestOps cleanup-residue visibility gate
TestOps clean cleanup visibility gate
QA verifies no operator cleanup mutation occurred
```

## D5: Surface Agreement And Negative-First Gates

Goal: prove CRD status, report, dashboard, operator-snapshot, explain, and
Events agree on the same operational truth.

Status: pending.

Scenarios:

- healthy first-volume,
- blocked node/preflight,
- stale/unreachable evidence,
- cleanup required,
- multi-volume sanity if D2-D4 touch volume aggregation.

Acceptance:

```text
[ ] no blocked/unknown/cleanup-required path shows false Ready=True
[ ] CRD status agrees with report summary
[ ] CRD status agrees with operator-snapshot.json
[ ] dashboard /operator-snapshot.json agrees with CRD status
[ ] explain names the same reason code and evidence refs
[ ] Kubernetes Events use stable bounded identity
```

Verification:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --include-crds \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false
TestOps D5 surface-agreement scenario set
QA strict rerun from clean lab
```

## D6: Release Claim Alignment And Close

Goal: close Phase 36 only if the product claim remains narrow and user-visible.

Status: pending.

Deliverables:

- finished plan under `internal/docs/finished-plans/`,
- README/quickstart/release note updates if claims changed,
- QA close report references,
- roadmap updated with next explicit product loop.

Acceptance:

```text
[ ] Phase 36 claim says actionable read-only operations, not mutating lifecycle
[ ] known non-claims remain visible
[ ] QA signoff covers healthy and negative paths
[ ] all scoped checks pass
[ ] current plan moved to finished-plans
```

Verification:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi
helm lint charts/seaweed-block
git diff --check
QA close report
PM wording review if README/release note changes
```

## Current Progress

- 0%: Phase 36 plan opened. Scope is read-only control-plane operations
  actionability: node readiness, support evidence refs, cleanup visibility,
  surface agreement, and release-claim alignment.
- 16%: D1 contract closed. The Go CRD contract, `SwBlockCluster` CRD schema,
  and operator readiness document now define node readiness, cleanup visibility,
  support-bundle refs, and safe next-step fields while preserving the
  status/events-only mutation boundary. Focused tests failed first on missing
  fields, then passed after the structural contract landed.
- 28%: D2 local node readiness projection landed. Existing `NodeEvidence`
  now maps into `SwBlockCluster.status.nodes[]` and `operator-snapshot.json`
  with stable node reasons such as `node_ready`, `node_not_ready`,
  `node_scheduling_disabled`, and `image_missing_on_node`. Local tests cover
  healthy and missing-image nodes; live QA remains pending.
- 34%: D2 QA passed. Live 1-node and 3-node labs project healthy nodes into
  `SwBlockCluster.status.nodes[]` and `operator-snapshot.json`; G3 missing
  image is replay-only PASS because live blockmaster node evidence currently
  hardcodes `Ready=true`, `Schedulable=true`, and omits `MissingImages`.
  Follow-up: populate live `NodeEvidence` from real Kubernetes node readiness
  and image-presence facts so negative node reasons are reachable live.
- 48%: D3 local support evidence projection landed. Cluster-level support refs
  and a read-only `observe.collect_bundle` safe next step now project into
  `SwBlockCluster.status.supportBundleRefs[]`,
  `SwBlockCluster.status.safeNextSteps[]`, `operator-snapshot.json`,
  `summary.txt`, and the report HTML. Live/from-bundle QA remains pending.
- 58%: D3 QA passed. Blocked CSI image-pull evidence projects support refs and
  read-only collect-bundle safe next steps across CRD status,
  `operator-snapshot.json`, `summary.txt`, report HTML, dashboard replay, and
  `ops explain`. The operator suggests `collect-helm-support-bundle.sh` but
  does not execute it and gains no mutation power.

## Next Step

Continue D4 cleanup visibility. Track the live negative node-evidence
follow-up; do not add mutating controller behavior.
