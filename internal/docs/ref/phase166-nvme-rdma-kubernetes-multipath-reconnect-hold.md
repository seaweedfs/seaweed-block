# Phase 166 NVMe/RDMA Kubernetes Multipath Reconnect

Status: implementation complete; live close gate blocked by lab topology.

Phase 165 proved the first opt-in Kubernetes NVMe/RDMA publish and attach path:
StorageClass intent, target publication, CSI attach, mounted I/O, status, and
cleanup all used RDMA without TCP fallback. Initial Phase 166 live runs also
proved an important boundary: tearing down the only target behind a mounted
single-path filesystem causes I/O errors even when the target returns with the
same controller identity and durable hostPath. Transparent outage recovery
therefore requires a surviving path; it cannot be claimed from reconnect logic
alone.

## Validation Status

- The transport-aware CSI reconnect contract and the Phase 166 TestOps close
  gate are implemented. Local CSI, launcher, ops, script, and scenario checks
  pass.
- Phase 165's published Kubernetes NVMe/RDMA path remains green: one live RDMA
  controller, mounted writer/reader I/O, no TCP fallback, and zero residue.
- The shared reconnect implementation passes the Phase 133 NVMe/TCP RF2
  endpoint-replacement gate: two initial paths, connect the new endpoint,
  preserve the surviving controller and mounted Pod, prune the old endpoint,
  cross-surface agreement, and zero residue.
- The Phase 166 RF2 RDMA close gate is not yet runnable honestly on the current
  lab. m01 and m02 are the only RoCE-capable nodes and also host the two RF2
  targets. Placing the initiator on either node makes one path a local RDMA
  self-connect, which the kernel rejects with `Invalid argument`. tp01 is
  offline and was not configured as a RoCE initiator.
- Unblock by adding a third Ready RoCE-capable Kubernetes initiator, or another
  validated topology where both target endpoints are remote to the initiator.
  Until that exists, Phase 166 remains open and must not be described as live
  RDMA multipath support.

## Goal

Close the mounted NVMe/RDMA reconnect loop with RF2 multipath. Keep a workload
mounted while one RDMA path is unavailable or replaced, reconnect the exact
desired RDMA endpoint, and prune only the stale controller after the new path is
live.

## Deliverables

### D1. Exact Live-Path Contract

- Reconcile each desired path by exact `(transport, NQN, address)`.
- Count a path as connected only when its controller state is `live`.
- Preserve explicit `rdma` publish evidence through reconnect calls.
- Reject transport changes on an existing staging mount; require restage rather
  than silently mixing TCP and RDMA.

### D2. RF2 RDMA Multipath Baseline

- Publish one RF2 volume from m01 and m02 over their RoCE frontend addresses.
- Attach two live RDMA controllers for one NQN to the Kubernetes workload node.
- Use hostPath persistence so a restarted target reopens the same durable data.
- Require Stage 2 multipath, mounted writer/reader I/O, and no TCP controller.

### D3. Surviving-Path Outage And Recovery

- Stop or restart one non-primary RDMA target while the other path remains live.
- Preserve the mounted Pod UID and complete I/O during the one-path interval.
- Restore the target and require CSI-node to reconnect the missing exact RDMA
  path without restaging the workload.
- Require two live RDMA paths and readable pre-outage data after recovery.

### D4. Desired Endpoint Replacement

- Change one replica's desired RDMA listen endpoint while the other path remains
  live.
- Connect the new endpoint before pruning the old controller.
- Assert the old endpoint disappears, the new endpoint is live, the second path
  is untouched, and mounted I/O continues with the same Pod UID.

### D5. Control, User, And Safety Surfaces

- Require publish context, host sysfs, `SwBlockVolume.status`, report, dashboard,
  and explain to agree on RDMA and the current two-path set.
- Never satisfy a desired RDMA path with a TCP controller at the same address.
- Do not report a reconnecting controller as live.
- Scope disconnect/prune/cleanup to the gate-owned NQN and endpoint; do not use
  `disconnect-all` in the product path or touch unrelated controllers.

### D6. TestOps Close Gate

- Reuse the proven Phase 111/132/133 RF2 multipath harness through a thin
  Phase 166 RDMA wrapper rather than duplicating a single-node gate.
- Run with fresh matching product and CSI images on the m01/m02 RoCE lab.
- Require source tests, two-path baseline, outage I/O, exact reconnect,
  connect-before-prune ordering, surface agreement, no TCP fallback, and
  independent zero-residue verification in one bundle.

## Exit Criteria

Phase 166 closes only when an already-mounted Kubernetes RF2 NVMe/RDMA PVC
retains I/O through loss of one target path, reconnects that path after target
recovery, replaces one desired RDMA endpoint without remounting the Pod, and
finishes with exactly two live desired RDMA paths and zero residue.

This phase does not claim transparent recovery after loss of the only NVMe path,
authority promotion or target-node failover, performance, broad hardware
compatibility, or an RTO/SLO. Those require separate evidence. The failed
single-path live runs are retained as evidence for this boundary rather than
being weakened into a passing test.
