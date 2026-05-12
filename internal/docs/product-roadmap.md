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
- Durable volume state across `blockvolume` restart.
- Basic failover with an attached workload.
- Returned replica lifecycle: observed -> candidate -> syncing/rebuilding ->
  ready.
- Explicit ACK profile: best-effort, quorum, or full-ack.
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

- Current: the active plan is defining the light-use install/lifecycle loop:
  install product, create StorageClass/PVC, run an app pod, delete resources,
  and verify product-owned cleanup without TestOps acting as the operator.
- Next: make cleanup ownership explicit and avoid harness-only cleanup paths.
- Later: add a small controller/operator for generated `blockvolume`
  workloads.

### Track B: iSCSI Frontend Stability

- Current: Linux/open-iscsi and Windows 11 built-in Initiator correctness are
  validated for the current alpha claim. Evidence includes mkfs/NTFS format,
  checksum write/read, cleanup, and Linux dmesg-delta checks.
- Next: move remaining iSCSI work to component-first session/backend pressure
  tests instead of broad V2 porting.
- Later: larger compatibility coverage across host distros and initiator
  versions.

### Track C: Durable State

- Current: alpha generated workloads still use throwaway storage in several
  paths.
- Next: define durable root layout for generated `blockvolume` workloads and
  prove restart/reattach preserves data.
- Later: integrate returned-replica rebuild and storage-engine compaction.

### Track D: Availability And Recovery

- Current: iSCSI and NVMe mounted failover are release-gated in single-node lab
  scenarios.
- Next: returned-replica state machine, old-primary stale I/O fencing, and
  multi-node attach/reconnect path.
- Later: rebuild/reintegration and longer soak under failure.

### Track E: Protocol / Backend Expansion

- Current: iSCSI and NVMe-oF are protocol-gated frontends. `walstore` remains
  the MVP backend.
- Next: storage-engine boundary tests, backend pressure behavior, and
  smartwal/delta experiments behind explicit gates.
- Later: RDMA/KV-backed data-plane experiments and semantic storage protocols
  only after the block core is mature.

### Track F: Operations Layer

- Current: first read-only operator loop is closed: `sw-block ops status`
  collects one volume's master/frontend/durable/residue evidence and emits a
  self-describing JSON report, human summary, and support bundle.
- Next: wire that status bundle into the first-volume install/lifecycle path,
  then move to cluster-wide list/status and product-owned lifecycle visibility.
- Later: observation API/UI, metrics, conservative admin controls, enterprise
  operations, hosted validation, fleet automation, and cloud-scale test
  lifecycle.

### Top Light-Use Product Blockers

These are the main gaps between the current functional block substrate and a
credible light-use product:

- Product-owned generated workload lifecycle: scripts/TestOps still own too
  much cleanup and run-scoped state management.
- Install/upgrade/uninstall: alpha scripts work for tests, but users need a
  normal K8s add-on flow.
- Observation beyond one volume: the current CLI is one-volume read-only
  diagnosis; users will need cluster-wide list/status and eventually metrics/UI.
- Safe admin controls: repair/promote/cleanup actions must wait until the
  read-only observation model is stable and release-gated.

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
