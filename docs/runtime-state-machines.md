# Runtime State Machines

This is a lightweight map for engineers reading the runtime code for the first
time. It is descriptive, not a normative protocol specification.

The key idea: `seaweed-block` is not one giant state machine. It is a group of
smaller loops with explicit ownership boundaries.

## System Map

```mermaid
flowchart TB
  K8S[Kubernetes PVC / Pod]
  CSI[blockcsi]
  BM[blockmaster]
  LIFE[lifecycle stores]
  AUTH[authority publisher]
  LAUNCH[launcher]
  BV[blockvolume]
  ISCSI[iSCSI frontend]
  WAL[WAL-backed storage]
  REC[recovery / replication]

  K8S --> CSI
  CSI --> BM
  BM --> LIFE
  BM --> AUTH
  BM --> LAUNCH
  LAUNCH --> BV
  AUTH --> BV
  BV --> ISCSI
  ISCSI --> WAL
  BV --> REC
  REC --> WAL
```

Reading order:

1. Kubernetes asks for a volume.
2. CSI calls `blockmaster`.
3. `blockmaster` records desired state and publishes authority through the
   authority module.
4. launcher materializes `blockvolume` replicas.
5. `blockvolume` exposes iSCSI and owns the data path.
6. recovery/replication feed lagging peers without redefining authority.

## Control Plane Loop

```mermaid
stateDiagram-v2
  [*] --> DesiredVolume : CreateVolume
  DesiredVolume --> PlacementIntent : planner
  PlacementIntent --> VerifiedPlacement : observation matches intent
  VerifiedPlacement --> AssignmentAsk : bridge request
  AssignmentAsk --> AssignmentPublished : authority publisher
  AssignmentPublished --> RuntimeManifest : launcher
  RuntimeManifest --> ReplicaObserved : blockvolume heartbeat
```

Main owners:

| State | Owner |
|---|---|
| Desired volume | `core/lifecycle` |
| Placement intent | `core/lifecycle` planner/reconciler |
| Verified placement | `core/host/master` + lifecycle observation store |
| Assignment | `core/authority` |
| Runtime manifest | `core/launcher` |
| Replica observation | `cmd/blockvolume` heartbeat to `cmd/blockmaster` |

Important boundary:

```text
verified placement != assignment
assignment != data continuity proof
```

## Data Plane Loop

```mermaid
flowchart LR
  POD[Pod filesystem write]
  DEV[Linux block device]
  SCSI[iSCSI session]
  FRONT[blockvolume frontend]
  BACK[backend Write]
  SYNC[Sync / durable boundary]
  WAL[WAL]
  EXT[extent/checkpoint work]

  POD --> DEV --> SCSI --> FRONT --> BACK --> WAL
  FRONT --> SYNC --> WAL
  WAL --> EXT
```

The current alpha path is iSCSI-first. NVMe-oF should plug in later as another
frontend, not as a different storage truth model.

## Recovery / Replication Loop

```mermaid
stateDiagram-v2
  [*] --> PeerHealthy
  PeerHealthy --> PeerLagging : missed sync / ship failure / probe evidence
  PeerLagging --> CatchingUp : WAL window available
  PeerLagging --> Rebuilding : WAL window not enough
  CatchingUp --> PeerReady : base not needed and witness closes
  Rebuilding --> PeerReady : base complete and witness closes
  PeerReady --> PeerHealthy : authority / policy admits it
  CatchingUp --> PeerLagging : session fails
  Rebuilding --> PeerLagging : session fails
```

Data-plane rule:

```text
one peer, one WAL-feeding decision owner
```

Recovery may send base data and WAL data through different internal mechanisms,
but the decision about what WAL to feed next must be single-owner and monotonic
per peer.

## Frontend Eligibility Loop

```mermaid
flowchart TD
  ASSIGN[authority assignment]
  LOCAL[local engine projection]
  SUPER[supersede probe]
  VIEW[frontend projection]
  IO{allow IO?}

  ASSIGN --> SUPER
  LOCAL --> VIEW
  SUPER --> VIEW
  VIEW --> IO
  IO -->|healthy| OPEN[open/read/write]
  IO -->|stale or superseded| CLOSED[fail closed]
```

This is why `core/host/volume/projection_bridge.go` exists. A local engine can
still look healthy while another replica has become authoritative. The frontend
must fail closed in that case.

## CSI Node Flow

```mermaid
sequenceDiagram
  participant K as kubelet
  participant C as blockcsi
  participant M as blockmaster
  participant V as blockvolume
  participant OS as Linux iSCSI/mount

  K->>C: ControllerPublishVolume
  C->>M: lookup publish target
  M-->>C: iSCSI address + IQN
  K->>C: NodeStageVolume
  C->>OS: iscsiadm discovery/login
  C->>OS: mkfs/mount staging path
  K->>C: NodePublishVolume
  C->>OS: bind mount to pod target
  OS->>V: SCSI READ/WRITE/SYNC
```

Current alpha tests verify this path with a checksum-writing pod.

## What Is Still Not Automated

- durable Kubernetes storage roots beyond alpha smoke paths
- multi-node scheduling and topology constraints
- failover while a pod remains mounted
- RF=3 production policy and quorum behavior
- operator-grade runtime reconciliation
- NVMe-oF frontend integration

