# `core/` Overview

This directory contains the main product libraries. Most durable semantics,
control-plane boundaries, and data-plane behavior should live here rather than
inside `cmd/`.

## Packages

| Package | Purpose | Main Interfaces / Boundaries |
|---|---|---|
| `adapter/` | Bridges engine decisions to volume-facing operations. | engine projection, command executor, probe/session callbacks |
| `authority/` | Owns assignment facts and publication. | publisher, subscribers, assignment consumers |
| `calibration/` | Scenario calibration helpers. | harness/report/result types |
| `conformance/` | Contract-style conformance runner. | YAML cases and runner tests |
| `csi/` | CSI controller/node implementation. | Create/Delete, publish, stage/publish, Linux utility seam |
| `engine/` | Recovery and replica-state decision core. | commands, events, durable ack, flow-control facts |
| `frontend/` | Protocol-neutral frontend contract plus protocol packages below it. | backend interface, projection view, stale/healthy gating |
| `host/` | Composed master/volume hosts. | lifecycle + authority wiring, volume projection bridge, status surfaces |
| `launcher/` | Renders Kubernetes runtime manifests from product state. | manifest renderer/writer |
| `lifecycle/` | Desired volume, inventory, placement intent, and reconciliation. | stores, planner, reconciler |
| `ops/` | Operational HTTP/API surface experiments. | handlers, server, state |
| `recovery/` | Base transfer, WAL catch-up, live-tail feeding, close witnessing. | primary bridge, replica bridge, sender/receiver |
| `replication/` | Peer fan-out and replication coordination. | peer state, durability policy, replica set updates |
| `rpc/` | RPC support package. | shared RPC helpers |
| `runtime/` | Runtime runner helpers. | process/runtime abstractions |
| `schema/` | Test/scenario schema helpers. | case loading/conversion |
| `storage/` | WAL-backed storage primitives. | logical storage, flusher, group commit, recovery contract |
| `transport/` | Transport-level recovery and shipping execution. | block executor, barrier, catch-up/rebuild senders |

## Key Boundaries

- `lifecycle` may produce placement intent; it must not publish assignment.
- `authority` publishes assignment; it should not inspect data-plane bytes.
- `frontend` gates IO through projection; it should not mint authority.
- `recovery` feeds data to lagging replicas; it should not decide cluster
  placement.
- `replication` owns peer fan-out; it should not create a second independent WAL
  feeder for the same peer.
- `storage` owns local durability mechanics; it should not know Kubernetes.

## Reading Order

For control-plane work:

1. `lifecycle`
2. `host/master`
3. `authority`
4. `launcher`
5. `csi`

For data-plane work:

1. `storage`
2. `frontend`
3. `host/volume`
4. `replication`
5. `recovery`
6. `transport`

