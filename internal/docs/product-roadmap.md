# Product Roadmap

This is the short internal roadmap. Keep it current and readable.

## Product Goal

- Build a small Kubernetes block storage service that is easier to try and
  reason about than a large distributed-storage stack.
- Target early users running lab or small Kubernetes clusters.
- Keep alpha/beta claims narrow: dynamic PVC, iSCSI/NVMe protocol-gated paths,
  app write/read, clean teardown, and explicit non-claims.
- CHAP, iSCSI ALUA, NVMe ANA, CSI protocol selection, and mounted failover are
  release-gated in the lab. Do not claim production HA, broad distro
  compatibility, performance, upgrade safety, or multi-node readiness until
  separately tested.

## Product Phases

### Release Ladder

- `v0.1-alpha`: Kubernetes block foundation. CSI/PVC path, product-owned
  blockvolume lifecycle, inventory, recovery gates, iSCSI ALUA/dm-multipath,
  node-loss recovery, and read-only control-plane observation are proven in
  lab gates. This is an engineering alpha, not a polished install product.
- `v0.2-alpha`: Day-1 activation. A supported user can run the script-based
  activation path, create a first PVC, verify writer/reader data, generate a
  local read-only report, and clean up. This remains the script fallback
  boundary.
- `v0.3-alpha`: Helm activation. Package the same supported Day-1 path as a
  normal Kubernetes add-on: chart values, immutable images, CHAP/external
  iSCSI configuration, StorageClass, readiness output, first-volume smoke,
  local read-only report/dashboard, and uninstall hygiene. Phase 25 closed this
  release target on 2026-05-22.
- `v0.3.4-alpha`: Helm lifecycle, restart persistence, read-only operations
  surface, deterministic cleanup, and negative-first status vocabulary merged
  through PR #50 (`8102cf3`) on 2026-05-27.
- `v0.3.5-alpha` candidate: TestOps failure hardening. Expand release proof
  from happy-path gates to negative paths: blocked/stale/unreachable status
  surfaces, support-bundle replay under corrupt/partial evidence, failed-run
  cleanup, and multi-volume interference checks. Phase 33 closed this scope;
  release packaging remains a separate decision.
- `v0.3.6-alpha`: Phase 34 test realism and anti self-proving gates. Selected
  replay or summary-grep checks were upgraded into independent live/dirty
  evidence: live status endpoint unreachable, restart convergence, SmartWAL
  corruption refusal, and targeted cross-validation between helper summaries
  and product or Kubernetes facts. Phase 34 closed on 2026-06-02.
- `v0.4-beta-candidate`: Kubernetes-native read-only operator foundation. Phase
  35 closed this scope on 2026-06-04 with CRDs, Conditions, Events, a
  status-only controller, stable Event identity, stale/blocked status gates,
  and read-only RBAC proof. This is the first release boundary that can
  credibly feel like a normal Kubernetes product loop rather than an install
  script. Mutating workflows such as repair, rebuild, failback, delete safety,
  automatic cleanup, and CR object ownership come after the read-only status
  contract is stable.
- Closed on 2026-06-06: Phase 36 Productized Operations Actionability. It uses
  the Phase 35 CRD/status/Event foundation to publish node readiness, support
  evidence refs, cleanup visibility, safe next-step hints, and cross-surface
  agreement. This remains read-only and does not add mutating operator
  lifecycle.
- Active phase: Phase 37 Live Node Evidence Hardening. Keep it read-only and
  bounded to live node blockers; do not extend it into mutating lifecycle.
- Model hardening gate before the next large release: complete the
  ManagedVolume Operations Model under `internal/docs/protocol/` before
  expanding operator or broader HA claims. The goal is to prevent Kubernetes,
  CSI, authority, host-path, recovery, and future NVMe logic from becoming
  scattered scripts or unrelated small automata.

Do not skip from scripts directly to mutating operator lifecycle. Helm has
stabilized the installation contract, and Phase 35 added read-only CRD status,
Conditions, and Events. Any in-cluster controller ownership of upgrades, repair,
rebuild, delete safety, or cleanup must start as a separate gated phase.

### Alpha Preview

- Status: protocol-gated; moving toward beta hardening.
- Single-node k3s quick start works.
- Dynamic PVC create/delete works.
- App pod writes and reads through a PVC.
- Cleanup leaves no active iSCSI sessions and no visible Kubernetes residue.
- Default frontend: iSCSI.
- Optional frontend: NVMe-oF, behind explicit protocol selection and release
  gates.
- Default backend: `walstore`.

### Alpha Stabilization

- Make cleanup product-owned instead of script-owned.
- Make PVC owner-reference cleanup the default path.
- Stabilize iSCSI with real OS initiators and larger filesystem writes.
- Make repeated create/write/read/delete stable.
- Keep artifact bundles useful for QA and issue reports.
- Keep docs modest and accurate.

### Beta Candidate

- Multi-node Kubernetes attach.
- Durable volume state across `blockvolume` restart. (Closed for supported
  single-node RF=1 alpha path in phase 13.)
- Same-node multi-node-capable attach and placement visibility. (Closed for
  the supported RF=1 loopback alpha path in phase 14.)
- Basic failover with an attached workload.
- Returned replica lifecycle: observed -> candidate -> syncing/rebuilding ->
  ready.
- Explicit ACK profile: best-effort, sync-quorum, or sync-all
  (quorum/full-ack aliases may appear in older docs or discussions).
- TestOps can run named smoke scenarios and return result bundles.

### Production Candidate

- Soak and fault testing.
- Upgrade and uninstall story.
- Security and resource hardening.
- Operator-visible status and diagnostics.
- Reproducible release images.
- Documented operational limits.

### Iterative Release Rule

- Do not wait for the full enterprise block vision before publishing useful
  slices.
- Every hardening phase should have a user-visible preview with clear
  non-claims.
- TestOps evidence is release evidence, but user installs and issues are the
  feedback loop.
- Keep the basic block product and basic runner open enough to build trust;
  reserve advanced fleet automation, private scenario corpus, hosted
  validation, and enterprise operations as possible enterprise layers.

## Priority Tracks

### Track A: Kubernetes Install And Cleanup

- Closed: v0.2 alpha closes the script-based Day-1 loop for the supported
  Kubernetes path: activate, verify node readiness, create a first PVC, run
  writer/reader verification, inspect status/report evidence, and clean up.
  Product-owned generated `blockvolume` workload lifecycle is also closed for
  the supported alpha path.
- Closed on 2026-05-22: v0.3 Helm activation. Package the same Day-1 path as a chart while
  keeping preflight and host cleanup explicit:
  - charted blockmaster, CSI controller/node, RBAC, CSI driver, StorageClass,
    CHAP Secret, and cluster spec,
  - values for image tags/digests, ACK profile, external iSCSI/status,
    Stage 2 multipath, namespace, and StorageClass,
  - generated Day-1 values file for multi-node labs,
  - `helm install` + first PVC smoke + `sw-block ops report`,
  - local read-only `sw-block ops dashboard` over the same evidence,
  - `helm uninstall` plus explicit host cleanup verification.
- Next major milestone: Phase 35 read-only operator foundation. Introduce
  CRDs/Conditions/Events and status-only reconciliation after the Helm contract
  is stable. Keep mutating cleanup, delete finalizers, upgrade execution, and
  repair/rebuild outside this first operator slice.

### Track B: iSCSI Frontend Stability

- Current: Linux/open-iscsi and Windows 11 built-in Initiator correctness are
  validated for the current alpha claim. Evidence includes mkfs/NTFS format,
  checksum write/read, cleanup, and Linux dmesg-delta checks.
- Next: move remaining iSCSI work to component-first session/backend pressure
  tests instead of broad V2 porting.
- Later: larger compatibility coverage across host distros and initiator
  versions.

### Track C: Durable State

- Current: durable hostPath restart/reattach is closed for the supported
  single-node RF=1 alpha path. Users can configure a run-scoped durable path,
  restart the generated `blockvolume`, reattach through the PVC, and verify
  durable status/inventory evidence.
- Next: keep durable evidence wired into multi-node attach and later
  availability gates.
- Later: integrate returned-replica rebuild and storage-engine compaction.

### Track D: Availability And Recovery

- Current: Stage 1 mounted recovery and Stage 2 transparent host-path failover
  are closed for the documented Kubernetes alpha path. Stage 1 proved RF=3
  `sync-quorum` recovery through CSI/node reattach on pod recreate. Stage 2
  proved RF=3 `sync-quorum` iSCSI ALUA plus Linux dm-multipath failover with
  the same mounted writer pod, no pod recreate, no CSI re-stage, stale-primary
  fencing, and post-failure checksum verification.
- Next: Node-loss survival is the highest-impact availability gap. Move from
  same-node logical replica failure to real Kubernetes node failure semantics:
  distinct nodes, non-loopback frontends, node-aware publish target selection,
  authority promotion to a surviving replica, host-path recovery through the
  documented mechanism, and support-bundle proof of fencing and data integrity.
- Later: returned-replica rebuild/reintegration/failback, NVMe ANA Kubernetes
  multipath parity, stronger committed-frontier reporting, broad distro/host
  compatibility, and longer soak under failure. NVMe ANA parity should follow
  the Kubernetes-native status foundation so ANA facts, path states, and
  protocol-specific reasons project through the same CRD/Condition/Event model.

### Track E: Protocol / Backend Expansion

- Current: iSCSI and NVMe-oF are protocol-gated frontends. `walstore` remains
  the MVP backend.
- Next: storage-engine boundary tests, backend pressure behavior, and
  smartwal/delta experiments behind explicit gates.
- Protocol hardening now has a dedicated working area under
  `internal/docs/protocol/`. New protocol semantics should update the control
  model, invariant ledger, and anti-pattern checklist there before release
  claims are made.
- Later: RDMA/KV-backed data-plane experiments and semantic storage protocols
  only after the block core is mature.
- Guardrail: do not let backend/library extraction weaken the user-visible
  block contract. A simpler data-plane seam is useful only if it preserves the
  product semantics the block layer needs: bounded buffers/streaming for large
  objects, explicit acknowledgement profile, durable frontier, fencing, retry
  behavior, and supportable failure evidence. If an abstraction becomes easier
  to test but cannot carry real volume I/O semantics, keep it experimental and
  do not build a product claim on it.

### Track F: Operations Layer

- Current: cluster operations inventory is closed for the supported alpha path:
  it discovers live Seaweed Block volumes from Kubernetes, maps them to
  PVC/PV/generated workloads, attaches per-replica status bundles, and names
  stale/orphan residue without relying on TestOps artifacts. It also serves as
  the proof surface for product-owned lifecycle, durable restart evidence, and
  same-node placement/attach evidence. Phase 19 closed the shared observation
  core for users/support/AI, JSON automation, and future dashboard use.
- Current: observation is now part of the first-user loop. Users can inspect
  cluster status, volume detail, timeline, static report artifacts, and a local
  read-only dashboard without SSHing into every node.
- Closed model-hardening slice: Phase 22 ManagedVolume Operations Model
  (`internal/docs/protocol/phase22-control-context-plan.md`) made PVC-backed
  volumes a first-class internal read model that composes K8s, CSI, authority,
  recovery, host path, workload, and evidence facts while keeping local
  controllers small and testable.
- Closed on 2026-06-04: Phase 35 turned the existing read-only operations model
  into Kubernetes-native status:
  - `SwBlockCluster` and `SwBlockVolume` CRDs,
  - a status-only controller that writes `.status`,
  - ManagedVolume Conditions projected into Kubernetes Conditions,
  - Kubernetes Events for ready, blocked, stale-evidence, and WAL-integrity
    transitions,
  - tests proving the controller has no mutating storage authority.
- Later: metrics, read-only dashboard hardening, conservative admin controls,
  enterprise operations, hosted validation, fleet automation, and cloud-scale
  test lifecycle.

### Track F2: ManagedVolume Model And Protocol Hardening

- Closed for current scope: V3 protocol principles have been pulled into
  `internal/docs/protocol/`: truth-domain ownership, anti-patterns, invariant
  ledger, engine design guidelines, and the Phase 22 ManagedVolume plan. The scope review in
  `internal/docs/protocol/operations-state-dependency-review.md` defines Phase
  22 as a PVC-backed ManagedVolume read model plus read-only operations
  alignment.
- Seed landed: `core/ops` now has an initial ManagedVolume projection model,
  typed facts, status priority, read-only/dry-run action contracts,
  `VolumeEvidence` and bundle-artifact bridges, and table tests for healthy
  first-volume, blocked loopback cross-node attach, CSI image-pull and mount
  blockers, node-loss reattach recovery, Stage 2 transparent multipath
  recovery, non-claims, and dual-primary invalid priority.
- Execution discipline: every Phase 22 D-step must carry TDD, internal review
  against the engine guidelines, and a regression command before it can close.
- Later: use ManagedVolume as the semantic core for operator
  Conditions/Events, read-only dashboard, and any future safe mutating admin
  workflows.

### Track F3: Operations Surface / Dashboard / Operator-Readiness

- Current: Phase 23 is closed for scope. Seaweed Block now exposes
  ManagedVolume Conditions, evidence refs, report/explain alignment, and a
  future-operator status contract from the shared read model.
- Current: Phase 24 is closed for scope. Seaweed Block can serve a local
  read-only dashboard/API surface over the same `ClusterEvidence` and
  ManagedVolume model used by `sw-block ops report`.
- Closed on 2026-05-22: Phase 25 packages that operations surface into the
  v0.3 Helm first-volume release story and validates docs/gates against it.
- Closed on 2026-06-04: Phase 35 implemented the first Kubernetes-native
  operator foundation slice over this surface: CRDs, read-only `.status`,
  Conditions, Events, stable Event identity, and read-only RBAC. It did not add
  mutating admin workflows.
- Seed landed:
  - `NewObservationDashboardHandler` serves `index.html`,
    `cluster-evidence.json`, `timeline.jsonl`, `summary.txt`, and `healthz`,
  - `sw-block ops dashboard` serves bundle-backed and master-api-backed
    evidence on a loopback address by default,
  - unsafe HTTP methods return `405` with a read-only boundary message,
  - `ops explain` now emits ManagedVolume Conditions, dry-run action
    preconditions, invariant refs, evidence refs, and non-claims,
  - Conditions carry additive `evidence_refs`,
  - `ManagedVolumeOperatorContractFromProjection` defines how Conditions map to
    future operator status and Kubernetes Events while keeping
    `mutation_allowed=false`,
  - `internal/docs/protocol/operator-readiness-contract.md` documents the
    future operator boundary,
  - replay tests cover first-volume, blocked, and recovery bundles.
- Later: production dashboard hardening and operator reconciliation can depend
  on this surface, but should not bypass ManagedVolume or mint their own truth.

### Product Semantics Rule

- Roadmap slices must preserve a sharp product question. For block storage, the
  question is not "can we model this state?" but "can an operator and workload
  safely use this volume under the documented topology and failure mode?"
- Passing tests are not enough when the tested abstraction has weaker semantics
  than the user-facing product. Such cases must be documented as non-claims,
  safe refusals, or experiments.
- Mature-product direction means each operations and availability slice should
  improve one of: provision, attach, mounted I/O, durability, fencing, reattach,
  cleanup, status, or support-bundle actionability.

### Top Light-Use Product Blockers

These are the main gaps between the current functional block substrate and a
credible light-use product:

- Product-owned generated workload lifecycle: scripts/TestOps still own too
  much apply/cleanup and run-scoped state management.
- Install/upgrade/uninstall: v0.2 script activation works and v0.3 Helm is the
  current packaging boundary. Operator lifecycle comes after the Helm contract
  is stable.
- Observation beyond one volume: the read-only CLI/dashboard now covers cluster
  inventory and first-volume evidence; users will still need richer lifecycle
  status, metrics, and production UI hardening.
- Safe admin controls: repair/promote/cleanup actions must wait until the
  read-only observation model is stable and release-gated.

### Track H: Stage 1.5 Product Usability Hardening

- Current: core product loops are proven for light-use alpha, but day-2
  usability still depends too much on internal context.
- Next:
  - add `kubectl get`-readable readiness/degraded/recovering/blocked conditions
    with stable reason codes through Phase 35 CRD status,
  - provide one-command support-bundle capture with minimum evidence for attach,
    failover, and cleanup diagnosis,
  - set conservative default timeout/retry profiles for iSCSI/NVMe/CSI paths,
  - add upgrade/rollback smoke gates for PVC attach/read continuity,
  - enforce capacity/replication preflight guards with operator-readable errors,
  - harden delete/residue cleanup auditing for sessions/targets/artifacts,
  - run multi-volume concurrency baselines (create/attach/delete) as product
    gates, not ad-hoc checks,
  - keep fail-closed blocker reasons directly mapped to operator runbook steps.
- Later: fold proven Stage 1.5 hardening into beta entry criteria.

### Track I: Enterprise Feature Gap Closure (Backup, UI, DR, Security)

- Current: Seaweed Block is intentionally narrow and honest for alpha/beta.
  Compared with mature Kubernetes block platforms, major product gaps still
  include:
  - backup/snapshot lifecycle as a first-class product workflow,
  - disaster-recovery orchestration across clusters/regions,
  - operator-facing control UI and guided diagnostics,
  - policy/RBAC/audit guardrails for admin actions,
  - richer day-2 automation (scheduled policies, safe runbooks, rollback UX).
- Reference signals from mature products:
  - Longhorn exposes recurring snapshot/backup jobs and UI-first operations.
  - Ceph/Rook exposes RBD snapshots and mirroring for DR workflows.
  - OpenEBS Replicated PV Mayastor exposes replicated PV operations and CSI
    snapshot integration.
  - Portworx positions backup + DR + enterprise security as core platform
    pillars.
- Next (priority order):
  - P1: backup/snapshot policy + restore workflow with support-bundle evidence,
  - P1: control-plane read UI over the observation API described in
    `ref/control-plane-observation-api-mvp.md`,
  - P2: admin action protocol (promote/repair/cleanup) with strict fencing and
    audit trail,
  - P2: cross-cluster DR contract (RPO/RTO class + failover/failback runbook),
  - P3: security hardening track (RBAC, secret handling, action audit,
    tenancy boundaries).
- Later: turn these tracks into explicit beta/production entry gates and avoid
  feature-claim drift between docs and runnable gates.

### Track G: Test Management Control Plane

- Current: runner scenarios write result/status bundles, but M01/M02 shared lab
  ownership is still mostly implicit.
- Closed: Phase 33 TestOps failure hardening. The runner and helpers now
  exercise negative paths, collect useful failure snapshots, assert no false
  Ready states, and prove cleanup/replay behavior after failed runs.
- Closed: Phase 34 test realism. Reduced self-proving gates by adding
  independent cross-checks, live status-endpoint-unreachable injection,
  restart convergence assertions, and one dirty storage failure: SmartWAL
  corruption. The D4 gate is release-relevant because it checks whether dirty
  storage evidence can still leak through as false `Ready=True`; it now passes
  on the core contract.
- Later: shared-drive control data for active runs, scenario library indexing,
  queueing, remote agents on lab nodes, matrix scheduling, hosted validation,
  and discovery-agent ingestion.

## Productized Operations Gap Priority

These are the current operation gaps in priority order. Phase 35 closed the P0
read-only status foundation; the remaining groups should become separate gated
phases rather than being mixed into the closed foundation.

### P0: Become Kubernetes-Native For Read-Only Status

1. CRD + status-only operator:
   `SwBlockCluster`, `SwBlockVolume`, and `.status` writes only. Status:
   closed in Phase 35.
2. Conditions writer:
   project `Ready`, `Blocked`, `Recovering`, `Recovered`, `EvidenceStale`, and
   `CleanupRequired` from ManagedVolume facts. Status: core vocabulary closed;
   cleanup projection remains follow-up.
3. Kubernetes Events:
   emit normal/warning Events such as `VolumeReady`,
   `CsiNodeImagePullFailed`, `WalIntegrityFault`, and `EvidenceStale`. Status:
   closed for read-only status Events.

### P1: Make Operations Actionable

4. Node readiness/preflight status:
   iSCSI, multipath, image readiness, hostPath readiness, and observed version
   under `SwBlockCluster.status.nodes[]`. Status: Phase 36 validated positive
   read-only node readiness and replay-only missing-image blockers. Live
   negative node evidence remains Phase 37.
5. Support-bundle pointers:
   keep the CLI collection path, but expose evidence refs and suggested
   commands from status. Status: closed in Phase 36.
6. Cleanup visibility:
   `CleanupRequired=True`, residue type, and safe next step. Do not automate
   cleanup yet. Status: closed in Phase 36.
7. Surface agreement:
   CRD status, Events, report, dashboard, operator-snapshot, and explain must
   agree on healthy, blocked, stale, cleanup-required, and multi-volume paths.
   Status: closed in Phase 36. Live operational follow-ups remain for
   build-host CSI image-import evidence, loopback publish-target documentation,
   and force-delete iSCSI node DB residue visibility.

### P2: Enter Mutating Lifecycle Carefully

8. Finalizers and delete safety for PVC/CRD lifecycle.
9. Upgrade/rollback status and drift reporting before upgrade execution.

### P3: Advanced Day-2 Features

10. Rebuild, reintegration, and failback.
11. Backup, snapshot, and restore.

NVMe ANA parity is important, but it should not be the next plan. It should
reuse the Phase 35 status foundation so protocol-specific path state does not
become another isolated status model.

## Capability And Model Maturity Snapshot

This section is the product-state checkpoint after the Phase 35/36 operations
push. It exists to stop the roadmap from becoming an unbounded sequence of
small operations fixes.

### Current Block Capabilities

The product can already demonstrate a narrow but real Kubernetes block-storage
loop:

- Helm install on supported lab clusters.
- CSI dynamic PVC provisioning.
- App pod mount, write, read, and replacement-reader verification.
- iSCSI frontend with Linux initiator and dm-multipath/ALUA gates.
- RF=3 multi-volume lab path with independent volume identity, primary, and
  publish target.
- CSI reattach recovery with pod recreate.
- Mounted transparent failover on the gated Stage-2 iSCSI ALUA path.
- HostPath restart persistence for supported gates, including authority
  preservation after promotion.
- Dirty-evidence protection: stale/unreachable/corrupt evidence does not become
  false `Ready=True`.
- Strict cleanup verification across Kubernetes resources, iSCSI sessions,
  iSCSI node DB records, multipath, dmsetup, product processes, and hostPath
  residue.

### Current Kubernetes And Operations Capabilities

- Helm is the primary install path; scripts remain development/fallback tools.
- `sw-block ops generate-helm-values` can derive Day-1 Helm values from the
  target Kubernetes cluster.
- Read-only CLI/report/dashboard/support-bundle flows are established.
- `SwBlockCluster` and `SwBlockVolume` CRDs exist with status subresources.
- The optional operator-status controller writes `.status` and Kubernetes
  Events only.
- Positive/read-only node readiness, support evidence refs, cleanup visibility,
  safe next-step hints, and surface agreement are QA-validated under the
  read-only model. Live negative node blockers remain Phase 37.

### Model Convergence State

The control-plane model is materially better than the earlier script-centered
shape, but it is not finished. See
`internal/docs/protocol/control-structure-effectiveness-review.md` for the
review standard that separates real capability closure from semantic-only model
work.

Converged:

- `ManagedVolume` is the shared semantic object for volume status.
- CRD status, Events, report, dashboard, `operator-snapshot.json`, and
  `ops explain` share the same Condition/reason vocabulary.
- The design pattern is now explicit:
  truth owner publishes facts; orchestration/status entity aggregates judgment;
  executor remains bounded; evidence/timeline explains why the judgment is
  allowed.
- Negative-first is enforced across status surfaces.

Still incomplete:

- Live node evidence does not yet derive all negative facts from real Kubernetes
  node/image/CSI-driver state.
- Some volume-side fault reasons still need stronger end-to-end propagation to
  user-visible surfaces.
- Mutating lifecycle actions now have a non-mutating lifecycle-owner contract
  shape: action + precondition + invariant + executor + evidence.
- PVC lifecycle, blockvolume lifecycle, and Kubernetes attach lifecycle still
  need a real lifecycle-owner component and API/admission proof before anything
  mutates Kubernetes lifecycle objects.

### Product Priority From Here

The operations work can continue forever unless the next phases are bounded by
product risk. The recommended order is:

1. **Phase 37: Live node evidence hardening.** Closed.
   Kubernetes Ready/SchedulingDisabled, CSI node pod readiness,
   CSIDriver/node-plugin registration, image-pull status, host-prereq replay,
   and loopback target cross-node blockers are now projected through the shared
   status surfaces.
2. **Phase 38: Lifecycle action model executable contract.** Closed.
   Future actions now have facts, preconditions, invariants, allowed executor,
   policy gate, evidence, and allow/reject decisions. It includes both a dry-run
   action gate and a rejected-action gate.
3. **Phase 39: Delete-safety status boundary.** Closed.
   Live QA proved the original RBAC-only finalizer boundary is not viable for
   CRD finalizers: CRDs have no usable HTTP `/finalizers` endpoint, so
   `metadata.finalizers` changes require main `patch swblockvolumes`
   authorization. The chosen path is to keep operator-status status/events-only
   and defer finalizer add/remove to a future lifecycle owner.
4. **Phase 40: Operator production hardening.** Closed.
   The v0.4 beta foundation released Helm + PVC + status/events-only
   operator-status, install drift visibility, delete-safety visibility, and
   schema/RBAC conformance gates.
5. **Phase 41: Lifecycle owner foundation.** Active.
   Defines observer/lifecycle-owner/executor boundaries, keeps operator-status
   status/events-only, defers finalizer mutation, and exposes dry-run
   lifecycle-owner decisions for delete-safety. It does not ship deletion
   protection.
6. **Phase 42 candidate: real lifecycle-owner API/admission gate.**
   Before any finalizer add/remove, prove main-object patch confinement against
   a real Kubernetes API with admission/RBAC. Only then consider a first
   bounded mutation.
7. **Returned-replica rebuild/failback.**
   High product value, but it depends on the action model and status trust.
8. **Backup/snapshot/restore and NVMe ANA parity.**
   Important, but they should reuse the status/action model rather than create
   another isolated control plane.

### Effort Shape

Approximate engineering effort if scope remains tight:

- Live node evidence hardening: small/medium. Mostly observation ingestion,
  projection tests, and live TestOps gates. Low mutation risk.
- Lifecycle action model review: medium. Mostly design and tests, but it should
  block later mutating work from scattering logic across listeners/scripts.
- Delete-safety status: closed. Keeps delete safety visible without widening
  operator-status mutation rights.
- Lifecycle-owner finalizers: medium/high. Cleaner than operator-status main
  patch, but requires a separate lifecycle-owner component plus real
  API/admission proof that only finalizers can be patched.
- Rebuild/failback: high. Requires storage semantics, authority/fencing,
  returned-replica state, and long-running failure gates.
- Backup/snapshot/restore: high. Requires durable data semantics and user-facing
  restore guarantees.
- NVMe ANA parity: medium/high. Protocol-specific work, but cheaper if it uses
  the existing CRD/Condition/Event model.

## PR Cadence

- Prefer one coherent milestone PR, not one PR per tiny fix.
- Target one or two PRs per day at most.
- Keep minor doc/test cleanups batched unless they block current work.
- QA can push evidence-only PRs when assigned, but product code should stay
  milestone-based.

## Current Execution Pointer

- Active work should be tracked in `internal/docs/current-plan.md`.
- Phase 35 closed the Kubernetes-native read-only operator foundation:
  CRDs, status-only reconciliation, Conditions, Events, stable Event identity,
  and read-only boundary tests.
- Phase 36 Productized Operations Actionability is closed. Positive/read-only
  node readiness, support evidence pointers, cleanup visibility, and surface
  agreement are QA-validated under the read-only control-plane model. Live
  negative node evidence remains Phase 37.
- Phase 37 Live Node Evidence Hardening is closed.
- Phase 38 Lifecycle Action Model Executable Contract is closed.
- Phase 39 Delete-Safety Status Boundary is closed.
- Phase 40 Operator Production Hardening is closed and is the v0.4 beta release
  boundary.
- Active work is Phase 41 Lifecycle Owner Foundation. It is non-mutating:
  finalizer add/remove is deferred, while lifecycle-owner dry-run decisions and
  delete-safety preconditions are made visible.
- Do not start NVMe ANA parity, rebuild/failback, backup/restore, or mutating
  operator workflows by extending Phase 41. Pick those as separate gated phases
  after the lifecycle-owner API/admission boundary is proven.
- When the current plan closes, move it to `internal/docs/finished-plans/`
  with a phase/topic filename such as
  `phase1_finishedplan_frontend_protocol_readiness.md`.
- Keep deeper technical design in separate files only when it is needed for
  review or future maintenance.
- Keep long audits and historical references under `internal/docs/ref/`.
