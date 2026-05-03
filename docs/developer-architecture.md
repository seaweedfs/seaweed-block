# Seaweed Block Developer Architecture

This document is for developers who want to understand or contribute to
`seaweed-block`. It describes the current alpha architecture without relying on
internal planning labels.

The project is still alpha. The design direction is stable enough to review,
but several production surfaces are intentionally incomplete.

## Product Shape

`seaweed-block` is a small block-storage service for Kubernetes.

```text
Kubernetes PVC
  -> CSI controller / node plugin
  -> blockmaster control plane
  -> launcher-generated blockvolume replicas
  -> frontend protocol target
  -> WAL-backed local block storage
```

The current user-visible path is dynamic PVC provisioning through CSI and iSCSI.
The tested alpha path is single-node Kubernetes with two local replicas.

## Core Design Principles

### WAL + Extent

The local data path is based on a write-ahead log plus block extents.

```text
Write
  -> append WAL
  -> Sync / durable acknowledgement boundary
  -> later extent/checkpoint work
```

The WAL is the recovery and replication source of truth while the extent layer is
the durable block-image shape the system converges toward.

Important implications:

- A replica can be behind and still recover if the primary retains the needed
  WAL window.
- A recovery decision must not be reduced to "reached this old target number".
- WAL retention must eventually be governed by real replica progress facts, not
  by optimistic assumptions.

### Dual-Lane Recovery

Recovery has two kinds of data:

- baseline/base data: the replica's block image before live tail is caught up
- WAL/live-tail data: writes that happen during or after the base transfer

The system keeps these as separate lanes internally, but the ownership rule is
single:

```text
one peer, one WAL-feeding decision owner
```

That rule matters more than the number of goroutines or sockets. Multiple
helpers can exist, but they must not independently advance the same peer's WAL
truth.

### Protocol And Execution Separation

The control plane must not treat every useful fact as authority.

```text
node observed != replica ready
placement intent != assignment
authority moved != data continuity proven
frontend fact != storage readiness
```

Only the authority publisher mints assignment facts. Other modules produce
observations, placement intent, verification results, or runtime effects.

This separation is deliberate. It makes the product slower to build, but keeps
failover, recovery, placement, and frontend protocols reviewable.

## Main Modules

### `cmd/blockmaster`

`blockmaster` is the product control-plane daemon.

Responsibilities:

- load cluster spec and desired volume declarations
- accept node/replica observations
- compute placement intent
- verify placement against observations
- publish assignment facts through the authority layer
- generate per-replica runtime manifests for the current alpha Kubernetes path

Non-responsibilities:

- it should not write block data
- it should not impersonate a blockvolume
- it should not directly make a frontend healthy
- it should not bypass the authority publisher when publishing assignment state

### `core/lifecycle`

Lifecycle stores the slower product facts:

- desired volumes
- node inventory
- placement intent
- verified placement snapshots

Lifecycle is not authority. A verified placement may produce an assignment ask,
but the publisher remains the assignment minting point.

### `core/authority`

Authority owns assignment facts and their lineage.

Responsibilities:

- maintain assignment epoch/version identity
- publish assignment updates
- let blockvolume subscribers receive current assignment facts
- provide the boundary between "candidate/intent" and "assigned"

Design rule:

```text
No upstream module should fabricate authority fields.
```

If a module needs authority, it should produce an explicit request or ask, then
call into the authority publisher.

### `core/launcher`

Launcher is the current alpha bridge from product control state to runtime
processes.

In Kubernetes alpha, it renders one `blockvolume` Deployment per selected
replica.

This is intentionally simple. It is not yet a full operator.

### `cmd/blockvolume`

`blockvolume` is the data-plane daemon for one volume replica.

Responsibilities:

- serve frontend protocol targets
- own local WAL-backed storage
- subscribe to assignment facts
- act as primary or replica according to assignment
- participate in replication and recovery

Current alpha packaging runs one daemon per volume replica. A future multi-volume
daemon is possible, but it is not the lowest-risk path for the alpha.

### `core/host/volume`

The host layer connects authority facts to the local adapter/frontends.

Important detail: a locally healthy engine is not always frontend-healthy. If
authority moves to another replica, the old replica must fail closed even if its
local engine has not yet observed a local demotion.

The projection bridge exists for this exact reason:

```text
local engine projection + supersede probe -> frontend projection
```

Do not remove this seam unless a replacement proves the old-primary fail-closed
contract another way.

### `core/replication`

Replication owns peer-level write fan-out and state tracking.

Current design direction:

- normal writes should not be blocked by ad hoc recovery helpers unless the
  product mode explicitly requires full synchronous acknowledgement
- peer recovery and live WAL feeding should share a single decision owner
- slow or recovering replicas should be handled by policy, not by silent success
  claims

### `core/recovery`

Recovery owns base transfer, WAL catch-up, live-tail feeding, and close
witnessing.

Important rules:

- recovery close is not "walApplied >= target"
- close requires base completion plus a real cut/witness condition
- live writes during recovery must go through the recovery-aware WAL feeding path
- the receiver side should be idempotent for repeated base/WAL application

### `core/frontend/iscsi`

iSCSI is the current alpha frontend.

Responsibilities:

- expose a Linux-compatible block target
- translate SCSI read/write/sync operations into backend calls
- support CSI NodeStage/NodePublish through standard Linux tooling

This is the first product path because it works with common Kubernetes nodes and
standard `iscsiadm`.

### `core/frontend/nvme`

NVMe-oF is a future frontend target.

The architecture should allow NVMe-oF to plug into the same storage and
authority model, but the alpha Kubernetes path is iSCSI-first.

### `cmd/blockcsi`

The CSI driver connects Kubernetes to `blockmaster` and node-local attach logic.

Current alpha coverage:

- Identity
- CreateVolume / DeleteVolume
- ControllerPublish
- NodeStage / NodePublish
- iSCSI discovery, login, filesystem mount, bind mount

Non-claims:

- no production-grade operator lifecycle yet
- no snapshot/resize/clone support yet
- no topology-aware scheduling yet
- no failover-under-mounted-pod guarantee yet

## Current Kubernetes Alpha Flow

```text
kubectl apply PVC
  -> CSI CreateVolume
  -> blockmaster records desired volume
  -> product loop verifies placement
  -> authority publishes assignment
  -> launcher creates blockvolume Deployments
  -> selected primary exposes iSCSI target
  -> CSI ControllerPublish returns target info
  -> kubelet NodeStage logs in and mounts
  -> pod writes and reads the mounted filesystem
```

The current smoke tests verify a pod can write a 4 KiB payload and read it back
through this chain.

## Development Guardrails

Use these checks when reviewing changes:

- Does this change turn observation into authority?
- Does this change let two senders feed the same peer WAL independently?
- Does this change let a stale primary remain frontend-healthy?
- Does this change claim recovery close from a numeric target alone?
- Does this change put frontend protocol decisions into storage/recovery code?
- Does this change add Kubernetes behavior that only works because startup order
  is deterministic?

If the answer is yes, the change needs either a stronger design note or a test
that pins the intended boundary.

## Where To Start Reading

For a new contributor:

1. `README.md`
2. `deploy/k8s/alpha/README.md`
3. `docs/architecture.md`
4. this document
5. `docs/roadmap.md`

For implementation work:

1. `cmd/blockmaster`
2. `core/lifecycle`
3. `core/authority`
4. `cmd/blockvolume`
5. `cmd/blockcsi`
6. `core/recovery`
7. `core/frontend/iscsi`

