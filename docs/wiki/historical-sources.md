# Historical Knowledge Sources

This wiki is built on two generations of design material:

1. current repo docs under `docs/` and `internal/docs/`;
2. older design/tutorial/methodology notes under
   `C:\work\seaweedfs\sw-block\design`.

The older material is important because it explains why the system is shaped the
way it is. Many current docs record what passed QA; the older docs often explain
the block-storage method behind the decisions.

## Tutorial Series

Source directory:

```text
C:\work\seaweedfs\sw-block\design\tutorial
```

Important files:

| File | Use |
|---|---|
| `01-project-map-and-terms.md` | terminology and mental model |
| `02-daemons-flags-and-process-layout.md` | daemon/process layout |
| `03-host-master-stream-and-assignment.md` | heartbeat, assignment, master stream |
| `04-adapter-single-route-and-async-callbacks.md` | adapter event route and callbacks |
| `05-engine-control-protocol.md` | deterministic engine Event/Command loop |
| `06-persistence-wal-logical-storage.md` | WAL and logical storage |
| `07-replication-volume-and-peers.md` | primary fan-out and peers |
| `08-transport-lineage-barrier.md` | lineage, barrier, transport facts |
| `09-recovery-walshipper-dual-lane.md` | recovery / rebuild / catch-up mechanics |
| `10-frontend-projection-iscsi-nvme.md` | frontend readiness projection |
| `11-master-authority-and-grpc.md` | blockmaster and authority |
| `12-inter-server-wire-protocols.md` | wire protocols and messages |
| `13-data-and-recovery-plane-messages.md` | data-plane vs recovery-plane messages |
| `14-core-code-groups-and-files.md` | core package/file map |

## Methodology Series

Source directory:

```text
C:\work\seaweedfs\sw-block\design\methdology
```

Important files:

| File | Use |
|---|---|
| `01-problem-shape-and-method.md` | observe -> constrain -> decide -> execute -> close method |
| `02-block-clump-initiator-target-session-lineage-barrier-recovery.md` | block-storage vocabulary and obligations |
| `03-csi-clump-lifecycle-compiler.md` | CSI lifecycle thinking |
| `04-iscsi-nvme-clump-wire-state-machines.md` | protocol wire-state concerns |
| `05-code-landings-engine-adapter-frontend-transport-recovery-storage.md` | implementation landing zones |
| `06-authority-placement-clump.md` | authority and placement |
| `07-replication-sync-clump.md` | replication/sync |
| `08-recovery-clump.md` | recovery mechanics |
| `09-storage-wal-clump.md` | WAL and local storage |
| `10-projection-readiness-clump.md` | projection vs readiness |
| `11-observability-clump.md` | observability method |

## Core Vocabulary To Carry Forward

| Term | Meaning |
|---|---|
| initiator | client side that sends block commands, e.g. host OS or kube node path |
| target/controller | server side accepting protocol commands |
| session | bounded protocol conversation with identity and lifecycle |
| lineage | generation identity; old generation must not override new generation |
| barrier/fence | explicit durability or ordering checkpoint |
| frontier | durable progress boundary that recovery can trust |
| projection | semantic publication of state |
| readiness | execution permission derived from semantic state |

## Method Rule

For any subsystem, answer four questions:

```text
what facts does it observe?
what constraints must those facts satisfy?
who is allowed to decide?
where does the decision become real?
```

This is the lens the deep-dive pages should use. If a page only lists code
files, it is not deep enough.

