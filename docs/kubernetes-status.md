# Kubernetes block volumes: status and claims

**Use this when:** you need the exact status of a Kubernetes feature, what the
gated alpha path lets you do today, what it does not claim, or which image tag
matches which release line.

**Source of truth:** the [release notes](releases/README.md) hold the run
evidence for every claim below. [User capabilities](user-capabilities.md)
describes current behavior and the read-only operations commands.

This page holds the detailed Kubernetes claim lists that used to be in the
top-level README. The text is unchanged.

The Kubernetes path is focused on one user-visible loop:

```text
install on Kubernetes
-> create a PVC-backed block volume
-> mount it in an app pod
-> write/read data
-> inspect status and support evidence
-> clean up
```

This is an **alpha** product path for supported lab clusters, not production.

## Feature and status

| Feature | Status | Feature | Status |
|---|---|---|---|
| Kubernetes PVC provisioning | Available | Helm install path | Available |
| First PVC writer/reader verification | Available | Read-only report/dashboard | Available |
| Support-bundle replay | Available | Negative-first status reasons | Available |
| Multi-volume RF=3 lab path | Gated | CSI reattach recovery | Gated |
| iSCSI ALUA/dm-multipath mounted failover | Gated | Restart persistence with hostPath | Gated |
| Actionable read-only CRD status + Events | Available | Bounded SwBlockVolume finalizer lifecycle | Beta candidate |
| Returned-replica ACK eligibility executor | Beta candidate | Returned-replica failback runtime | Source-gated |
| Returned-replica rebuild traffic | Planned | Frontend publication after failback | Planned |
| NVMe/TCP CSI multipath + backend write batching | Source-gated | NVMe/RDMA Kubernetes CSI publish/attach | Source-gated |
| WAL multi-block record opt-in | Source-gated | WAL format default change | Not claimed |
| Backup/snapshot/restore | Planned | NVMe/RDMA failover or performance SLO | Not claimed |
| Production SLO/performance claims | Not claimed | Hosted production UI | Not claimed |

## What you can do today

- Install Seaweed Block through Helm on a supported Kubernetes/k3s lab.
- Create and mount PVC-backed block volumes through normal Kubernetes PVCs.
- Verify writer/reader persistence with ordinary app pods.
- Run multiple RF=3 volumes in the gated lab path.
- Validate gated recovery paths:
  - CSI reattach with pod recreate,
  - iSCSI ALUA + dm-multipath transparent mounted failover on the proven
    Stage-2 path,
  - interleaved multi-volume failover isolation.
- Inspect cluster, volume, replica, primary, frontend, timeline, and reason
  evidence through read-only CLI/report/dashboard surfaces.
- Publish the same read-only status into Kubernetes-native `SwBlockCluster` /
  `SwBlockVolume` `.status` and Events on the gated operator-status path.
- Inspect node readiness, support evidence refs, cleanup-required status, and
  safe read-only/scripted next-step hints through that same status model.
- Use the beta-candidate lifecycle-owner path to protect `SwBlockVolume` CRs
  with a Seaweed Block finalizer and release it only after clean externally
  supplied cleanup evidence.
- Inspect install drift status for current versus desired chart/app/image
  identity. This is visibility only, not upgrade execution.
- In the gated returned-replica path, inspect a bounded authority-executor
  result that records ACK eligibility on `SwBlockReplicaEligibility.status`
  after live evidence proves the old primary remains fenced, the current primary
  is unchanged, and the required durable frontier is covered.
- From source, run the opt-in returned-replica failback runtime gates. This path
  can move authority through blockmaster only when a `SwBlockReplicaFailback`
  target, explicit execution policy, expected-current evidence, and terminal
  evidence are all present. It is not enabled by default and is not yet a
  published release claim.
- From source, run the supported-lab NVMe/TCP gates: ANA/direct-host baseline,
  CSI protocol selection, Kubernetes CSI multipath attach for one NQN/NSID with
  multiple NVMe frontend paths, one-path-loss status honesty, mounted pod
  write/read after one observed path loss, mounted pod write/read after the
  removed path is restored, multi-volume mounted path isolation, bounded
  multi-volume path churn, repeated stage/unstage residue checks, a bounded
  writer/reader soak, a baseline over the configured 100GbE TCP frontend
  address, a source-gated CSI-node reconnect owner contract, and a live
  host-path reconnect gate for scoped `nvme disconnect -d` path loss, plus a
  live desired path-set replacement gate where CSI-node connects the new
  desired NVMe path and prunes the stale old host path without remounting the
  pod, and a durable backend full-block write-batching gate with
  product-owned `/status/durable` counters. The source-gated WAL multi-block
  record opt-in has mounted profile and restart/recovery compatibility evidence,
  plus mounted durable-status `HeadLSN` confirmation, but remains default-off
  and requires a future matching-image release smoke before it becomes a public
  image claim. The opt-in NVMe/RDMA path now carries an explicit transport from
  StorageClass through blockmaster publication and CSI NodeStage, mounts a
  dynamic PVC on a second RoCE host, and verifies writer/reader data through a
  standard Linux `nvme connect -t rdma` controller. It remains source-gated.
  These are supported-lab source-gated claims, not broad NVMe compatibility,
  production HA, or performance/SLO claims.
- Replay support bundles offline.

These are narrow alpha claims tied to documented gates. See
[release notes](releases/README.md) for exact run evidence.

## What you should not expect yet

- Production readiness or production SLOs.
- A production-grade operator or broad mutating admin workflow. The current
  lifecycle-owner owns only the Seaweed Block `SwBlockVolume` protection
  finalizer.
- Automatic cleanup execution, host repair, or PVC/PV/workload deletion.
  Delete-safety uses externally supplied cleanup evidence.
- Backup, snapshot, or restore.
- Returned-replica rebuild traffic, frontend publication after failback, or
  automatic deployed failback. The failback runtime path is explicit, opt-in,
  and source-gated until a release smoke validates it on published images.
- Transparent Kubernetes node-loss failover without pod recreate.
- Broad NVMe/RoCE compatibility, performance/SLO, or transparent failover
  parity beyond the documented supported-lab gates. The CSI-node reconnect
  owner, host-path reconnect gate, desired path-set replacement gate, and
  stale path pruning gate exist only as source-gated NVMe/TCP evidence. The
  NVMe/RDMA path has standalone and Kubernetes single-path publish/attach
  evidence, but no RDMA multipath, reconnect/failover, performance, or SLO
  claim.
- Broad distro/kernel/initiator compatibility.
- Upgrade or rollback execution. The status layer can report install drift, but
  it does not run Helm or kubectl mutations.

## Release images


Current validated published quickstart image tag: `sha-dc2972d0059b`.
This tag covers the v0.4 read-only/status foundation path. The v0.5
bounded lifecycle-owner path requires matching `sw-block` and `sw-block-csi`
images published from the Phase 44 release commit; do not use the older
quickstart tag to validate lifecycle-owner behavior.
The v0.6 returned-replica ACK eligibility executor path likewise requires
matching images published from the Phase 54 release commit and must be validated
with the release smoke before it is marked shipped.
The returned-replica failback runtime added after v0.6 is source-gated and
requires a future release smoke before it becomes a public image claim. The
NVMe/TCP supported-lab path is also source-gated until matching `seaweed-block`
and `seaweed-block-csi` images are published and pass the NVMe release smoke.

Mutable `:alpha` is a smoke/demo tag only; it can drift from the source tree.
