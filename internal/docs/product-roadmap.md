# Product Roadmap

This is the short internal roadmap. Keep it current and readable.

## Product Goal

- Build a small Kubernetes block storage service that is easier to try and
  reason about than a large distributed-storage stack.
- Target early users running lab or small Kubernetes clusters.
- Keep alpha claims narrow: dynamic PVC, iSCSI path, app write/read, clean
  teardown.
- CHAP is implemented for target-side iSCSI and CSI node staging, with lab
  validation. Do not claim production HA, seamless mounted failover,
  performance, multipath, or NVMe-oF readiness until separately tested.

## Product Phases

### Alpha Preview

- Status: current.
- Single-node k3s quick start works.
- Dynamic PVC create/delete works.
- App pod writes and reads through a PVC.
- Cleanup leaves no active iSCSI sessions and no visible Kubernetes residue.
- Default frontend: iSCSI.
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

## Priority Tracks

### Track A: Kubernetes Install And Cleanup

- Current: owner-reference cleanup is being defaulted for alpha scripts.
- Next: avoid harness-only cleanup paths.
- Later: add a small controller/operator for generated `blockvolume`
  workloads.

### Track B: iSCSI Frontend Stability

- Current: OS initiator correctness, session stability, product-backed K8s
  fio/attach-detach, and CHAP are implemented and evidenced.
- Next: CSI lifecycle and restart behavior.
- Later: ALUA/MPIO and mounted failover.

### Track C: Durable State

- Current: alpha generated workloads still use throwaway storage in several
  paths.
- Next: define durable root layout for generated `blockvolume` workloads.
- Later: prove restart and reattach preserve data.

### Track D: Availability And Recovery

- Current: recovery/failover components are tested below the K8s surface.
- Next: multi-node attach and reconnect path.
- Later: failover while mounted, old-primary stale I/O fencing, returned
  replica reintegration.

### Track E: Protocol / Backend Expansion

- Current: iSCSI + `walstore` only for alpha.
- Next: protocol-neutral CSI dispatch tests.
- Later: NVMe-oF, `smartwal`, and multipath after the default path is stable.

## PR Cadence

- Prefer one coherent milestone PR, not one PR per tiny fix.
- Target one or two PRs per day at most.
- Keep minor doc/test cleanups batched unless they block current work.
- QA can push evidence-only PRs when assigned, but product code should stay
  milestone-based.

## Current Execution Pointer

- Active work should be tracked in `internal/docs/current-plan.md`.
- When the current plan closes, rename it to a finished plan such as
  `internal/docs/iscsi-p2-plan-finished.md`.
- Keep deeper technical design in separate files only when it is needed for
  review or future maintenance.
- Keep long audits and historical references under `internal/docs/ref/`.
