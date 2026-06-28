# Current Plan: Phase 101 NVMe Hardening And Soak

Status: complete. Local tests and m02 runner gates PASS on 2026-06-28.

## Why This Is Next

Phase 99 closed the NVMe baseline:

- ANA Identify/Get Log Page and provider-backed ANA state exist.
- Direct-host ANA/multipath gates exist.
- CSI can select and stage a single NVMe publish target.

Phase 100 closed the supported-lab Kubernetes CSI NVMe multipath attach path:

```text
dynamic PVC protocol=nvme, replicationFactor=2
  -> two NVMe frontend paths for one NQN/NSID
  -> master status groups them as one multipath publish target
  -> CSI publish context carries nvmeAddrs
  -> NodeStage connects every path
  -> app pod writer/reader passes
  -> cleanup proves no stale NVMe subsystem residue
```

The next risk is not "can NVMe attach once"; that is proven. The next risk is
whether the NVMe path behaves like a product feature when it is stressed,
observed, and partially failed.

This phase hardens the NVMe path without broadening the claim into production
HA, RoCE, performance, or broad host compatibility.

## Product Goal

Move NVMe from a supported-lab attach proof to a supported-lab hardening proof:

```text
NVMe multipath attach
  -> visible path identity and ANA/path health in status surfaces
  -> one-path failure does not produce false Ready
  -> path recovery converges
  -> repeated stage/unstage leaves no residue
  -> bounded soak keeps writer/reader and cleanup true
```

## D1: NVMe Status Surface Contract

Status: complete.

Expose enough NVMe facts for operators and gates to reason about the attached
path instead of grepping CSI logs.

Required facts:

- protocol=`nvme`;
- `nqn`;
- `nsid`;
- first `nvmeAddr`;
- full `nvmeAddrs[]` path list;
- path count;
- multipath requested/observed;
- ANA state when available;
- stale/missing path reason when evidence is incomplete.

Surfaces:

- CRD status;
- `operator-snapshot.json`;
- report summary;
- dashboard JSON;
- `ops explain`.

Success criteria:

- a healthy Phase 100-style volume shows two NVMe paths and no iSCSI frontend;
- a missing-path snapshot shows `Ready=Unknown` or `Blocked`, never false
  `Ready=True`;
- all surfaces agree.

## D2: NVMe Path Failure Gate

Status: complete. Runner `nvme-path-failure-status-chain` PASS
`20260628-013848-bdd2`.

Prove that the system does not lie when one NVMe path disappears.

Minimum acceptable gate:

- start from a mounted two-path NVMe volume;
- make one path unavailable without deleting the whole volume;
- keep workload data path semantics scoped and explicit;
- status must not claim a clean two-path Ready state while evidence shows one
  path missing;
- cleanup must remove all NVMe subsystem residue.

Success criteria:

- no false Ready;
- path-count/path-health reason is visible;
- writer/reader result is recorded honestly, whether pass or degraded;
- recovery or final cleanup converges.

## D3: Stage/Unstage Idempotency And Residue Gate

Status: complete. Runner `nvme-stage-unstage-residue-chain` PASS
`20260628-014526-dcd3`.

Exercise attach/detach repetition, because host initiator residue is a common
NVMe/iSCSI product failure mode.

Minimum acceptable gate:

- repeat NodeStage/NodeUnstage through Kubernetes pod churn or runner helper;
- record `nvme list-subsys` before/after each cycle;
- assert no stale subsystem/controller survives final cleanup.

Success criteria:

- repeated stage/unstage does not accumulate stale NVMe sessions;
- failed mid-cycle cleanup is surfaced as cleanup-required, not hidden;
- final verifier returns zero residue.

## D4: Bounded NVMe Soak Gate

Status: complete. Runner `nvme-bounded-soak-chain` PASS
`20260628-015211-562f`.

Run a small, deterministic soak rather than a performance benchmark.

Minimum acceptable gate:

- mounted writer/reader loop for a bounded duration or iteration count;
- optional path flap if D2 is stable;
- periodic status snapshots;
- final cleanup verifier.

Success criteria:

- no false Ready during the run;
- no path identity drift across snapshots;
- no final NVMe residue;
- result explicitly does not claim throughput, latency, or production SLO.

## D5: Close Gate And Release Wording

Status: complete.

Close the milestone by updating user-facing docs and finished-plan evidence.

Release wording must say:

- NVMe-oF is supported as a lab-gated frontend path;
- Kubernetes CSI NVMe multipath attach and basic hardening gates exist;
- iSCSI remains the default broad-compatibility path;
- no RoCE, production HA, broad distro/kernel compatibility, performance, or
  soak/SLO claim.

## Non-Claims

- no RoCE claim;
- no production HA claim;
- no performance/latency/throughput claim;
- no broad distro/kernel compatibility claim;
- no transparent Kubernetes node-loss failover claim;
- no backup/snapshot/restore claim.

## Release Relationship

Operation Layer v0.5 remains a large release candidate but still needs matching
published images and pinned-image smoke before being marked released.

Phase 101 is a storage/protocol hardening milestone after Phase 100. It should
not block the operation release, and the operation release should not claim
Phase 101 NVMe hardening unless matching release images are published and
smoked.

## Next Candidate

The next NVMe work should not broaden claims blindly. Reasonable next candidates
are:

- publish-image smoke for the Phase 101 NVMe gates if this is intended for a
  release claim;
- RoCE / multi-host NVMe path design and lab preflight, explicitly separated
  from the current TCP supported-lab claim;
- NVMe performance/R2T/in-capsule characterization, explicitly labelled as
  measurement rather than SLO.
