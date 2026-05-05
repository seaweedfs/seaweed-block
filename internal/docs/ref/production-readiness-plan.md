# Production Readiness Plan

This document is the internal post-alpha plan for moving `seaweed-block` from
the current Kubernetes alpha MVP toward a small production-usable block service.

It keeps the global product view, current protocol tracks, and readiness bars in
one place. Public docs should expose the stable user-facing subset, not this
full internal execution ledger.

## Product Phases

### Phase 0: Alpha Preview

Status: current.

User-facing bar:

- README quick start passes on a fresh single-node k3s lab.
- Dynamic PVC create/delete works through CSI.
- A normal app pod writes and reads data through the mounted PVC.
- Cleanup leaves no active iSCSI sessions and no visible Kubernetes resources.
- Limitations are visible: single-node evidence, iSCSI only, `walstore` only,
  no production HA claim.

Current strongest evidence:

```text
tree: seaweed-block@59277f9
lab: M02 k3s single-node
mode: literal README Quick Start, KUBECONFIG unset, no manual workaround
result: [alpha] PASS: dynamic PVC create/delete completed checksum write/read and cleanup
artifacts: V:\share\sw-block-final-close\20260504T000127Z\
```

### Phase 1: Alpha Stabilization

Goal: make the alpha useful for early Kubernetes users without the test harness
acting as the operator.

Bar:

- generated `blockvolume` workloads are created and cleaned by product logic,
  not by smoke-test script sweeps,
- PVC owner-reference cleanup is the default alpha path,
- repeated create/write/read/delete is stable,
- larger-volume iSCSI filesystem operations work with real OS initiators,
- artifact collection is standard enough for QA and external bug reports.

### Phase 2: Beta Candidate

Goal: make the service credible for small lab clusters and early contributor
testing beyond a single-node demo.

Bar:

- multi-node Kubernetes attach works,
- data survives `blockvolume` pod restart,
- basic failover is tested under an attached workload,
- returned replica lifecycle is explicit,
- ACK profile behavior is named and test-pinned,
- TestOps can run the smoke suite by scenario name.

### Phase 3: Production Candidate

Goal: make operational limits, failure behavior, and upgrade/cleanup semantics
clear enough for real deployments.

Bar:

- soak and fault testing,
- security/resource hardening,
- upgrade/uninstall story,
- documented operational limits,
- reproducible release artifacts/images,
- observability sufficient for diagnosis without source-level debugging.

## Product Rule

Production readiness is not a single protocol milestone. It requires the whole
runtime loop to be understandable:

```text
user intent -> placement -> authority -> runtime execution -> progress facts
            -> recovery / failover / cleanup -> operator-visible status
```

The control plane must keep these facts distinct:

- placement intent is not authority,
- authority movement is not data continuity,
- frontend readiness is not replica readiness,
- heartbeat observation is not recovery completion,
- best-effort acknowledgement is not full-replica durability.

These boundaries protect the project from over-claiming availability before the
data path and operator story are actually proven.

## Current iSCSI Steps

The iSCSI track should stay aligned with V2 coverage at the behavior/test level,
not by copying V2 control-plane code.

### iSCSI-P1: OS Initiator Correctness

Status: implemented and evidenced.

Scope:

- Linux `iscsiadm` login,
- 256 MiB target,
- `mkfs.ext4`,
- mount,
- write/read checksum,
- logout,
- no dangling session,
- large WRITE R2T/Data-Out handling,
- large READ Data-In splitting.

Key implemented slices:

- pending SCSI command during Data-Out is queued, not fatal,
- V3-local DataOutCollector validates DataSN, BufferOffset, F-bit, overflow,
- bounded pending queue,
- Data-Out timeout,
- real Linux OS initiator smoke,
- DataInWriter large-read split.

Next validation before considering P1 fully stable:

- repeat OS smoke several times,
- run 256 MiB K8s PVC path,
- run 60s `fio` randrw,
- run Windows iSCSI Initiator format if we want a Windows demo claim.

### iSCSI-P2: Session Stability And Memory Pressure

Goal: close the high-value V2 RX/TX coverage gap.

Test coverage to add or port by behavior:

- many concurrent sessions,
- close while active I/O is in flight,
- repeated open/close,
- non-Data-Out PDUs during Data-Out,
- NOP-Out during Data-Out,
- StatSN monotonicity and error-response StatSN,
- response backpressure,
- target close while active sessions exist,
- new session after target close,
- 10 x 4 MiB writes without unbounded heap growth,
- slow backend does not accumulate unbounded buffers.

Close bar:

- local protocol tests green,
- real OS harness supports loop count,
- no session goroutine leak,
- no unbounded memory growth under large writes.

### iSCSI-P3: Product-Backed Stability

Goal: verify iSCSI through real `blockvolume`, WAL, CSI, and Kubernetes paths.

Scenarios:

- sustained filesystem write/read on `walstore`,
- `SYNCHRONIZE_CACHE` pressure,
- K8s 256 MiB PVC write/read,
- 60s `fio` randrw,
- app writer pod replaced by reader pod on same PVC,
- repeated attach/detach loop,
- reconnect after initiator logout/login.

Close bar:

- no iSCSI session errors,
- no dangling sessions after cleanup,
- no Kubernetes residue,
- data checksum passes through pod path.

### iSCSI-P4: Security And Access Control

Goal: match the useful V2 CHAP behavior without pulling V2 control-plane
semantics into V3.

Scope:

- CHAP config validation,
- auth success/failure tests,
- wrong-secret and missing-secret negative tests,
- Kubernetes Secret integration later,
- clear docs that CHAP is disabled by default in alpha.

Close bar:

- unauthenticated access fails when CHAP is required,
- authenticated access passes,
- auth failure does not leak sessions or partially open devices.

### iSCSI-P5: CSI Node Lifecycle Hardening

Goal: make iSCSI reliable under normal Kubernetes node-plugin behavior.

Scope:

- NodeStage idempotency,
- NodeUnstage cleanup,
- plugin restart cleanup,
- stale device/session detection,
- failed attach cleanup,
- repeated attach/detach,
- clear host requirements for `iscsi_tcp`, privileged mode, and mount tools.

Close bar:

- kubelet retries do not wedge the node plugin,
- failed attach leaves no device/session leak,
- repeated PVC create/delete works without manual host cleanup.

### iSCSI-P6: Multipath / Failover Frontend

Goal: support mounted-volume failover semantics instead of only reconnect-based
single-path behavior.

Scope:

- ALUA/MPIO for iSCSI,
- standby/active target state,
- REPORT TARGET PORT GROUPS behavior,
- path failure and recovery,
- failover while mounted,
- old primary cannot serve stale successful I/O.

Close bar:

- authority/failover facts are stable enough to drive frontend path state,
- failover while mounted has byte-equal proof,
- no stale-primary success,
- no claim before ALUA/MPIO behavior is tested with real initiators.

## Production Tracks

### 1. Install And Cleanup Surface

Goal: users can install and remove the alpha stack without the harness acting as
an operator.

Required work:

- package a stable Kubernetes install surface, preferably Helm or a single
  generated manifest bundle,
- replace `emptyDir` state for generated `blockvolume` workloads with a durable
  node-local path,
- add a small controller/operator to create and delete generated `blockvolume`
  workloads,
- make `DeleteVolume` cleanup fully controller-owned,
- keep the README quick start as a smoke test for every release candidate.

Pass bar:

- repeated create/write/read/delete leaves no Kubernetes resources and no iSCSI
  sessions,
- pod restart and CSI pod restart do not leak blockvolume workloads,
- users do not need to know internal generated manifest paths.

### 2. Multi-Node Kubernetes

Goal: prove the service works across real Kubernetes node boundaries.

Required work:

- run at least two Kubernetes nodes,
- place blockvolume and workload pod on distinct nodes,
- attach through a non-loopback frontend target,
- verify dynamic PVC write/read checksum,
- preserve complete artifacts for master, CSI, blockvolume, kubelet-facing
  events, and iSCSI state.

Pass bar:

- same user-facing quick-start shape works on multi-node lab,
- no hard-coded `127.0.0.1` assumptions leak into publish context,
- cleanup is still complete.

### 3. Durable State

Goal: data survives blockvolume pod restart and node-local process restart.

Required work:

- define the default durable root layout,
- make generated workloads mount the durable root explicitly,
- prove blockvolume restart recovers the volume,
- prove CSI reattach reads previously written bytes,
- document which failure cases are still unsupported.

Pass bar:

- write data, restart generated blockvolume pod, reattach, read same data,
- no manual state repair required.

### 4. Failover While Mounted

Goal: the availability claim becomes real, not just component-level.

Required work:

- run a pod that writes through a mounted PVC,
- kill or isolate the current primary,
- move authority through the publisher path,
- force the frontend reconnect path,
- verify acknowledged data is still readable,
- verify old primary cannot serve stale successful reads or writes.

Pass bar:

- byte-equal proof through the mounted workload path,
- no stale-primary success,
- old-primary return becomes candidate/syncing/rebuilding, not ready.

### 5. Replica Lifecycle And Recovery Policy

Goal: lagging or returning replicas become a normal managed state.

Required work:

- model returned replica states explicitly:

  ```text
  observed -> candidate -> syncing/rebuilding -> replica_ready
  ```

- keep frontend primary readiness separate from replica readiness,
- define catch-up vs rebuild policy based on durable progress and WAL
  retention,
- expose recovery progress to operators,
- prevent placement or ACK eligibility from using a replica before it is ready.

Pass bar:

- returned replica cannot become ready from heartbeat alone,
- recovery progress facts are visible,
- full rebuild and smaller catch-up paths are both tested.

### 6. ACK Profiles And Write Availability

Goal: users know what a successful write means.

MVP may support `best_effort`, but it must be named. In that mode, frontend
success does not wait for every replica to durably ACK. A lagging replica still
must be recovered; best-effort is not permission to ignore it.

Future production profiles:

- `best_effort`: primary success, recovery catches replicas up later,
- `quorum`: configured quorum must durably ACK,
- `full_ack`: every configured replica must durably ACK.

Required work:

- expose the configured ACK profile,
- make sync/full-ack unavailable states explicit,
- define whether writes block, fail, degrade, or become read-only when the only
  secondary is recovering,
- test RF=2 and RF=3 behavior separately.

Pass bar:

- system never returns "full sync" success while the required replica durability
  condition is unavailable.

### 7. Observability And Diagnostics

Goal: users diagnose state without reading internal debug logs.

Required work:

- add a status endpoint or CLI for frontend readiness, authority role,
  replication role, durable ack frontier, recovery phase, placement reason, and
  degraded reason,
- add concise structured logs for assignment, attach, recovery, and cleanup,
- keep TestOps artifact bundles stable.

Pass bar:

- a failed attach/recovery can be diagnosed from status output and artifact
  bundle, not only source-code knowledge.

### 8. Security And Resource Hardening

Goal: alpha does not expose obvious denial-of-service or privilege hazards.

Required work:

- keep iSCSI transfer sizes bounded by negotiated limits,
- keep SCSI/NVMe bounds checks overflow-safe,
- make privileged CSI node permissions explicit and minimal,
- document required host modules and capabilities,
- add resource requests/limits for Kubernetes workloads,
- gate noisy debug logs behind an explicit flag.

Pass bar:

- protocol fuzz/negative tests cover oversized transfer, stale session, and
  invalid LBA paths,
- manifests explain privileged requirements.

### 9. Protocol And Backend Expansion

Goal: expand only after the iSCSI/walstore path is stable.

Order:

1. Keep iSCSI as the default Kubernetes path.
2. Add protocol-neutral CSI target dispatch.
3. Add NVMe-oF connect path behind an explicit feature gate.
4. Introduce `smartwal` as a tested backend option, not a silent default
   switch.

Pass bar:

- each new protocol/backend runs the same create/write/read/delete and restart
  scenarios as the default path.

## Suggested Milestones

### Alpha Preview

Status: current.

Bar:

- README quick start passes,
- single-node k3s dynamic PVC smoke passes,
- limitations are visible.

### Alpha Stabilization

Bar:

- controller owns generated blockvolume workload cleanup,
- durable node-local state replaces default `emptyDir`,
- repeated create/delete smoke is stable,
- artifact collection is standard.

### Beta Candidate

Bar:

- multi-node K8s smoke passes,
- blockvolume restart preserves data,
- failover while mounted is tested,
- recovery progress is operator-visible,
- RF/ACK profile behavior is explicitly named.

### Production Candidate

Bar:

- soak and fault testing,
- security/resource hardening,
- upgrade/uninstall story,
- documented operational limits,
- release artifacts/images are reproducible.

## What Not To Merge Into Public Docs

Internal mini-plans remain useful for archaeology, but should not be copied into
public docs one by one.

Keep as internal reference:

- gate-by-gate implementation ledgers,
- old target-LSN recovery debates,
- hardware run transcripts,
- agent task ledgers,
- V2 port audit tables.

Merge into public docs only when the content becomes one of:

- a user-visible behavior,
- a contributor-facing architecture rule,
- a production-readiness checklist,
- a reproducible test instruction.
