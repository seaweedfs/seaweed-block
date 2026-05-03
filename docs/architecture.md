# Seaweed Block Architecture

`seaweed-block` is a lightweight Kubernetes block-storage project. The current
alpha MVP is intentionally narrow: it proves the main control/data path before
expanding into a full storage operator.

## Product Shape

```text
Kubernetes PVC
  -> CSI controller / node plugin
  -> blockmaster lifecycle + authority control plane
  -> launcher-generated blockvolume daemon
  -> iSCSI frontend
  -> walstore-backed block data path
```

## Main Components

### blockmaster

`blockmaster` owns product control-plane state:

- desired volumes
- node inventory
- placement intent
- verified placement
- authority publication
- launcher manifest generation

Important boundary: lifecycle and placement facts are not authority by
themselves. Only the authority publisher mints assignment facts.

### blockvolume

`blockvolume` serves one volume replica. In the current Kubernetes MVP, the
launcher renders one Deployment per replica workload.

Responsibilities:

- serve the iSCSI frontend
- own local WAL-backed storage
- subscribe to assignment facts from blockmaster
- participate in recovery/replication machinery

Current MVP limitation: launcher-generated blockvolume state uses `emptyDir`.
That is fine for smoke tests, but not durable product storage.

### blockcsi

`blockcsi` is the Kubernetes integration layer:

- CSI Identity
- CSI ControllerPublish / NodeStage / NodePublish
- CSI CreateVolume / DeleteVolume for dynamic PVCs
- iSCSI discovery/login/mount on node side

The CSI layer should stay frontend-protocol-neutral. Today only iSCSI is the
MVP path; NVMe-oF should be added behind the same frontend-target abstraction
later.

## Current Data Path

```text
Pod write
  -> filesystem
  -> Linux block device
  -> iSCSI session
  -> blockvolume iSCSI target
  -> walstore Write + Sync
```

Current tested frontend: iSCSI.

Current tested durable backend: `walstore`.

## Current Dynamic Provisioning Flow

```text
PVC created
  -> csi-provisioner calls blockcsi CreateVolume
  -> blockcsi calls blockmaster LifecycleService.CreateVolume
  -> blockmaster records desired volume
  -> product loop computes placement intent
  -> launcher loop writes blockvolume Deployment YAML
  -> harness/operator applies generated Deployment
  -> blockvolume starts and heartbeats
  -> blockmaster publishes assignment
  -> blockcsi ControllerPublish returns iSCSI frontend fact
  -> kubelet NodeStage logs in and mounts
```

Current gap: the harness is acting as the operator for generated Deployment
apply/delete. A real controller/operator is a planned follow-up.

## Recovery And Replication

The current tree contains a deeper recovery stack than the Kubernetes MVP
currently exposes:

- dual-lane recovery data plane
- single WAL egress ownership direction
- target-band debt removed from recover close predicate
- failover data-continuity tests
- returned-replica lifecycle work

The K8s MVP currently runs RF=1 for the dynamic PVC smoke. Recovery/failover
under a mounted K8s pod remains follow-up scope.

## Design Rules

The project follows a few rules because previous ports showed that storage
systems become fragile when control-plane facts are conflated:

- Observation is not authority.
- Placement intent is not assignment.
- Authority movement is not data-continuity proof.
- Frontend fact exposure must match the current authority line.
- Recovery close requires data-plane evidence, not just a target number.
- WAL feeding should have one monotonic owner per peer.

These rules are enforced through unit tests, component tests, hardware tests,
and TestOps scenarios.
