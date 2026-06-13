# Current Plan: Phase 40 - Operator Production Hardening

Status: active, 78% complete. Started on 2026-06-13.

Branch: `phase33-testops-failure-hardening`

Previous phase: Phase 39 is closed in
`internal/docs/finished-plans/phase39_finishedplan_delete_safety_status_boundary.md`.

## Product Goal

Make the operator-status foundation trustworthy enough to release and to build
future lifecycle mutation on.

Phases 35-39 produced a useful read-only/status-only operator surface:
`SwBlockCluster.status`, `SwBlockVolume.status`, Kubernetes Events, live node
evidence, support/cleanup evidence, safe next steps, action decisions, and
delete-safety status. The recurring weakness was not the model; it was that
several CRD schema/RBAC bugs were only caught by live QA after mock unit tests
and Helm rendering passed.

Phase 40 closes that gap as one larger hardening phase. It adds real
Kubernetes API conformance coverage, cleans up known status-polish issues, adds
upgrade/rollback drift status, and prepares a release candidate for the
operator-status foundation.

The hard exit statement:

```text
The operator-status foundation is release-ready as a status/events-only
control-plane surface. It is schema/RBAC-validated against a real Kubernetes API,
does not mutate storage or lifecycle objects, and reports install/status/drift
conditions with consistent CRD/report/dashboard/operator-snapshot evidence.
```

## Release Decision

Cut the next release after Phase 40 if QA passes.

Do not wait for Phase 41. Phase 41 is the next mutating lifecycle-owner slice
and should start after the status-only operator foundation has a clean release
boundary. Shipping after Phase 40 gives users a coherent product increment:
Helm + PVC + read-only operator status + Events + diagnostics + delete-safety
visibility, with explicit non-claims for finalizer ownership and automatic
cleanup.

## Scope Contract

| In | Out |
|---|---|
| real CRD/RBAC status-writer conformance tests | finalizer add/remove |
| stale status cleanup and condition/event polish | automatic cleanup execution |
| upgrade/rollback drift status | upgrade execution |
| operator-status release docs and claim alignment | repair/rebuild/failback |
| QA release close gate | backup/snapshot/restore |
| TestOps coverage for API/schema failures | NVMe ANA parity |

Allowed implementation rule:

```text
Phase 40 may change status projection, CRD status schema, Events, TestOps
coverage, docs, and release packaging.

Phase 40 must not add storage/workload/host mutation, automatic cleanup,
finalizer mutation, promotion/fencing/rebuild/failback, or upgrade execution.
```

## D1: Kubernetes API Conformance Harness

Goal: catch CRD schema and RBAC defects before live QA.

Status: dev-complete; QA/internal review pending.

Acceptance:

```text
[x] harness loads real SwBlockCluster/SwBlockVolume CRD schemas
[x] harness uses an operator-status-equivalent RBAC boundary
[x] cluster status patch succeeds with current status DTOs
[x] volume status patch succeeds with conditions, allowedActions, cleanup,
      deleteSafety, node-derived evidence, and scripted actions
[x] schema-negative cases fail in the harness, not only in live QA
[x] event create and duplicate-event behavior are covered
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
new envtest/live-apiserver status-writer test target
helm template with operatorStatus.create=true dryRun=false
```

## D2: Status Correctness Polish

Goal: remove known confusing status leftovers without changing product scope.

Status: dev-complete; QA/internal review pending.

Acceptance:

```text
[x] stale deleteSafety is cleared or marked not_requested when current delete
      evidence disappears
[x] non-healthy node status has one effective Ready condition per node surface
[x] Events stay bounded and remain stable per object/reason/type
[x] report, dashboard, operator-snapshot, and CRD agree after each polish
[x] no new mutating action or RBAC grant is introduced
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
from-bundle regression for stale deleteSafety clearing
status-surface regression for node condition shape
event identity regression
```

## D3: Upgrade / Rollback Drift Status

Goal: make install drift visible before implementing upgrade execution.

Status: dev-complete; QA/internal review pending.

Acceptance:

```text
[x] SwBlockCluster.status reports chart/app/operator image identity where
      evidence is available
[x] drift status distinguishes current, desired, missing, and mismatched images
[x] upgrade/rollback status is read-only and never runs helm/kubectl mutation
[x] report/dashboard/operator-snapshot show the same drift status
[x] non-claims explicitly state that upgrade execution is not implemented
```

Verification:

```text
go test ./core/ops ./cmd/sw-block
from-bundle drift scenarios: aligned, image mismatch, missing evidence
helm template/lint
```

## D4: TestOps API-Failure Regression Gate

Goal: turn the live-only failures from Phases 35-39 into repeatable gates.

Status: dev-complete; QA/internal review pending.

Acceptance:

```text
[x] casing drift in status payload fails before QA
[x] enum drift in status conditions/actions fails before QA
[x] wrong CRD endpoint usage fails before QA
[x] RBAC boundary drift fails before QA
[x] blocked/releasable/delete-safety status gates still pass
[x] QA can run the gate from a clean lab or local envtest path
```

Verification:

```text
testops scenario or scripted gate for CRD/RBAC conformance
go test target for conformance harness
QA assignment with explicit pass/fail evidence paths
```

## D5: Release Claim And Docs Alignment

Goal: prepare a coherent release boundary for the status-only operator
foundation.

Status: dev-complete; QA/PM review pending.

Acceptance:

```text
[x] README/quickstart/release notes describe supported status-only operator
      behavior
[x] non-claims are visible: no finalizer owner, no automatic cleanup, no
      upgrade execution, no repair/rebuild/failback, no backup/restore
[x] feature/status table reflects Helm, PVC, ops, CRD status, Events, node
      evidence, delete-safety status, and known limitations
[x] release note names immutable image digest or clearly states local build
      evidence if digest is pending
[x] PM/user wording distinguishes status visibility from lifecycle mutation
```

Verification:

```text
docs review: README, quickstart, release note, roadmap
QA minimal new-user walkthrough if release digest is available
```

## D6: Release Candidate Gate

Goal: prove the release candidate from a user and QA perspective.

Status: local gate passed; lab QA pending.

Acceptance:

```text
[ ] fresh Helm install from documented values
[ ] first PVC writer/reader passes
[ ] operator-status CRDs show healthy volume status and Events
[ ] negative status scenario shows no false Ready=True
[ ] cleanup leaves zero residue
[x] conformance harness passes
[ ] docs and claims match the tested image
```

Verification:

```text
go test ./core/ops ./cmd/sw-block ./cmd/blockcsi ./scripts
helm lint charts/seaweed-block
helm template sw-block charts/seaweed-block --namespace kube-system --include-crds \
  --set operatorStatus.create=true --set operatorStatus.dryRun=false
QA minimal new-user validation
QA negative-status validation
git diff --check
```

## D7: Close Gate

Goal: close Phase 40 only when the operator-status foundation is release-ready
and Phase 41 can safely start as a separate mutating lifecycle-owner track.

Acceptance:

```text
[ ] D1-D6 pass
[ ] operator-status remains status/events-only
[ ] known Phase 39 follow-ups are either fixed or explicitly carried forward
[ ] release/no-release decision is recorded
[ ] Phase 41 entry criteria are listed
```

Verification:

```text
finished plan
QA close report
roadmap update
release PR or explicit hold note
```

## Current Progress

- 0%: Phase 40 opened. Scope is consolidated into one larger
  operator-production-hardening phase instead of splitting envtest, status
  polish, drift status, and release alignment into separate tiny phases.
- 14%: D1 dev-complete. Added a Phase 40 conformance harness that loads the
  real `SwBlockCluster`/`SwBlockVolume` CRD status schemas and enforces an
  operator-status-equivalent API boundary. It validates full cluster and volume
  status patches, scripted actions, deleteSafety, Events, duplicate-event
  idempotency, and negative cases for snake_case payload drift, unsupported
  condition enums, main-resource patches, and finalizer endpoint usage.
- 28%: D2 dev-complete. Volume status now emits `deleteSafety:null` when no
  current delete evidence exists so Kubernetes merge-patch clears stale
  delete-safety state. Node readiness projection now replaces old `Ready`
  conditions with one computed authoritative `Ready` condition, while keeping
  non-Ready conditions. Existing stable event identity remains covered by D1
  and writer tests.
- 42%: D3 dev-complete. Added read-only install drift evidence from
  `install-drift-summary.txt`, projecting current/desired chart, app,
  blockvolume, CSI, and operator image identity into `SwBlockCluster.status`,
  operator-snapshot, report summary, dashboard HTML, and from-bundle replay.
  Drift can be `ok`, `mismatch`, or `unknown`; mismatch is negative-first
  `Blocked=True`, missing evidence is `EvidenceStale=True`, and the operator
  snapshot now explicitly carries `no_upgrade_execution`.
- 56%: D4 dev-complete. Added `run-phase40-status-api-conformance` scripts for
  PowerShell and bash plus the `operator-status-api-conformance-chain` TestOps
  scenario. The gate runs the CRD/RBAC conformance tests that catch casing
  drift, enum drift, wrong endpoint usage, RBAC broadening, and delete-safety
  status regressions, then emits a summary file for QA assertions.
- 70%: D5 dev-complete. README, user capabilities, roadmap, release index, and
  v0.4 beta-candidate notes now describe the status/events-only operator
  foundation, delete-safety visibility, install drift visibility, CRD/RBAC
  conformance gates, and explicit non-claims for finalizer ownership,
  automatic cleanup, upgrade execution, repair/rebuild/failback, backup/restore,
  and NVMe ANA parity.
- 78%: D6 local gate passed. Added `run-phase40-release-candidate-local.ps1`
  and the D6 QA assignment. Local checks pass for Go release-scope tests, Helm
  lint/template with operator-status write mode, Phase 40 status API
  conformance, and `git diff --check`. Release remains held until QA completes
  the lab gates for fresh Helm install, first PVC writer/reader, live
  operator-status CRD/Event publication, negative status, and zero-residue
  cleanup.

## Prerequisites / Risks

- `tp01` was reported `NotReady`/unreachable during recent QA. Restore before
  any live RF=3 or 3-node release gate.
- Do not fix conformance failures by broadening operator-status RBAC to main
  object mutation. The status-only safety boundary remains the product promise.
- Conformance coverage should use a real CRD schema and real or equivalent RBAC;
  mock-only tests are not sufficient for this phase.
- Drift status must not become upgrade execution.

## Next Step

Send `internal/docs/qa-assignments/phase40-d6-release-candidate-qa.md` to QA.
Do not cut the release until the required lab gates pass against either the
published release image digest or a clearly identified local build.
