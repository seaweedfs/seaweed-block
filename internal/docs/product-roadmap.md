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
  local read-only report, and clean up. This is the current shipped
  user-facing alpha packaging boundary.
- `v0.3-alpha`: Helm activation. Package the same supported Day-1 path as a
  normal Kubernetes add-on: chart values, immutable images, CHAP/external
  iSCSI configuration, StorageClass, readiness output, first-volume smoke, and
  uninstall hygiene. Helm is the next installer milestone.
- `v0.4-beta-candidate`: Operator lifecycle. Add a Kubernetes-native control
  plane with CRDs/Conditions/Events for install, node eligibility, volume
  lifecycle, recovery observation, safe cleanup, and eventually gated repair or
  rebuild workflows. This is the first release boundary that can credibly feel
  like a complete Kubernetes product loop rather than an install script.

Do not skip from scripts directly to an operator. Helm should stabilize the
installation contract before an in-cluster controller owns upgrades and day-2
lifecycle.

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
- Active: v0.3 Helm activation. Package the same Day-1 path as a chart while
  keeping preflight and host cleanup explicit:
  - charted blockmaster, CSI controller/node, RBAC, CSI driver, StorageClass,
    CHAP Secret, and cluster spec,
  - values for image tags/digests, ACK profile, external iSCSI/status,
    Stage 2 multipath, namespace, and StorageClass,
  - generated Day-1 values file for multi-node labs,
  - `helm install` + first PVC smoke + `sw-block ops report`,
  - `helm uninstall` plus explicit host cleanup verification.
- Later: v0.4 operator lifecycle. Introduce CRDs/Conditions/Events and scoped
  reconciliation only after the Helm contract is stable.

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
  compatibility, and longer soak under failure.

### Track E: Protocol / Backend Expansion

- Current: iSCSI and NVMe-oF are protocol-gated frontends. `walstore` remains
  the MVP backend.
- Next: storage-engine boundary tests, backend pressure behavior, and
  smartwal/delta experiments behind explicit gates.
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
- Next: make observation part of the first-user loop: cluster status, node
  readiness, volume list, volume detail, timeline, logs, and bundle pointers
  should be visible without SSHing into every node.
- Later: metrics, read-only dashboard hardening, conservative admin controls,
  enterprise operations, hosted validation, fleet automation, and cloud-scale
  test lifecycle.

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
- Install/upgrade/uninstall: v0.2 script activation works, but users need a
  normal Kubernetes add-on flow. Helm is the next packaging blocker; operator
  lifecycle comes after the Helm contract is stable.
- Observation beyond one volume: the read-only CLI now covers cluster inventory;
  users will still need lifecycle status, metrics, and eventually UI.
- Safe admin controls: repair/promote/cleanup actions must wait until the
  read-only observation model is stable and release-gated.

### Track H: Stage 1.5 Product Usability Hardening

- Current: core product loops are proven for light-use alpha, but day-2
  usability still depends too much on internal context.
- Next:
  - add `kubectl get`-readable readiness/degraded/recovering/blocked conditions
    with stable reason codes,
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
- Next: add simple shared-drive control data for active runs, history, locks,
  artifact pointers, commits, target nodes, and stale-run detection.
- Later: scenario library indexing, queueing, remote agents on lab nodes,
  matrix scheduling, hosted validation, and discovery-agent ingestion.

## PR Cadence

- Prefer one coherent milestone PR, not one PR per tiny fix.
- Target one or two PRs per day at most.
- Keep minor doc/test cleanups batched unless they block current work.
- QA can push evidence-only PRs when assigned, but product code should stay
  milestone-based.

## Current Execution Pointer

- Active work should be tracked in `internal/docs/current-plan.md`.
- When the current plan closes, move it to `internal/docs/finished-plans/`
  with a phase/topic filename such as
  `phase1_finishedplan_frontend_protocol_readiness.md`.
- Keep deeper technical design in separate files only when it is needed for
  review or future maintenance.
- Keep long audits and historical references under `internal/docs/ref/`.
