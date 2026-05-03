# seaweed-block

Alpha-stage block storage for Kubernetes.

`seaweed-block` is an early block-storage project exploring a simpler path for
small Kubernetes clusters that need persistent volumes without adopting a large
storage platform on day one.

It is not production-ready. The current code can run a single-node Kubernetes
alpha smoke path: dynamic PVC creation, CSI attach/stage, iSCSI mount, pod
write/read checksum, and cleanup. The project still needs durable Kubernetes
packaging, multi-node validation, failover-under-mount testing, and operational
hardening before it should be used for real workloads.

## Why This Exists

Many teams want block storage that is easy to deploy and reason about:

- small companies running a few Kubernetes nodes
- developers who want a local or lab block service
- teams that find Ceph too large for their first storage step
- users who want CSI volumes without hiding recovery semantics behind a black box

The goal is not to replace mature storage systems today. The goal is to build a
small, inspectable block service that can grow carefully toward RF=2/RF=3,
failover, recovery, and Kubernetes-native operations.

## Technical Direction

The new block design is built around three ideas.

### WAL + Extent

Writes first go through a WAL-style path, then drain into extent storage. This
keeps the local data process explicit:

```text
write -> WAL -> flush/checkpoint -> extent
```

The current Kubernetes alpha uses `walstore`. A smarter WAL backend is planned,
but it is not the default alpha path yet.

### Dual-Lane Recovery

Recovery is designed as more than “copy until target LSN”. The newer path keeps
base transfer and WAL/live-tail feeding separate enough to avoid blocking normal
write flow, while still enforcing a single owner for WAL egress decisions.

The important rule is:

```text
one peer, one monotonic WAL feeding owner
```

This avoids the previous class of bugs where multiple senders tried to advance
the same recovery truth.

### Protocol And Execution Separation

Control-plane facts, assignment authority, frontend protocol, and runtime
execution are kept separate:

```text
observation != authority
placement intent != assignment
authority moved != data continuity proven
frontend fact != storage readiness
```

This makes the system slower to design, but easier to review. The aim is to keep
iSCSI, future NVMe-oF, recovery, and placement from redefining each other's
contracts.

## How It Compares

This is not a feature-complete comparison; it is the intended product position.

| System | Strength | Tradeoff |
|---|---|---|
| Ceph/Rook | mature, powerful, broad storage platform | operationally heavy for small clusters |
| OpenEBS-style local engines | Kubernetes-friendly, easier to start | behavior depends heavily on chosen engine/topology |
| seaweed-block | aims to be small, inspectable, CSI-first, recovery-contract-driven | alpha, incomplete, not production-ready |

The attraction of `seaweed-block`, if it succeeds, is a middle path:

- simpler than a full distributed storage platform
- more structured than ad hoc local disks
- CSI-first for Kubernetes
- designed for RF=2/RF=3 replication
- iSCSI first, NVMe-oF later
- recovery semantics documented and testable

## Current Progress

Currently demonstrated:

- CSI dynamic PVC `CreateVolume`
- blockmaster lifecycle and placement flow
- launcher-generated `blockvolume` Deployment
- iSCSI frontend attach/mount
- pod filesystem write/read checksum
- CSI `DeleteVolume` cleanup path
- no dangling iSCSI session after cleanup in the lab run
- TestOps registry and a minimal `cmd/sw-testops` CLI

Current alpha defaults:

- Kubernetes: single-node k3s lab
- frontend: iSCSI
- backend: `walstore`
- dynamic volume state: launcher-generated blockvolume uses `emptyDir`
- replication in the demo StorageClass: RF=1

Important non-claims:

- not production-ready
- not multi-node validated as a Kubernetes product
- not durable across blockvolume pod restart in the alpha manifest
- not yet a full operator
- no failover-under-mounted-PVC claim
- no NVMe-oF CSI claim yet
- no performance/soak claim

## Quick Start

Use a Linux Kubernetes node where privileged CSI pods are allowed and
`iscsi_tcp` is loadable.

Quick Start prerequisites:

- Docker
- `kubectl`
- a running Kubernetes cluster such as k3s
- `iscsi_tcp` loadable on the node
- `KUBECONFIG` set for your cluster

For a default k3s install:

```bash
export KUBECONFIG="${KUBECONFIG:-/etc/rancher/k3s/k3s.yaml}"
```

Build local images:

```bash
bash scripts/build-g15b-images.sh "$PWD"
```

For k3s, import them:

```bash
docker save sw-block:local | sudo k3s ctr images import -
docker save sw-block-csi:local | sudo k3s ctr images import -
```

Run the alpha smoke:

```bash
bash scripts/run-k8s-alpha.sh "$PWD"
```

Expected result:

```text
[g15d] PASS: dynamic PVC create/delete completed checksum write/read and cleanup
```

For the manual `kubectl apply` flow, see:

- [deploy/k8s/alpha/README.md](deploy/k8s/alpha/README.md)

## Simple User Manual

The current alpha flow is:

1. Build `sw-block:local` and `sw-block-csi:local`.
2. Deploy blockmaster, CSI controller, and CSI node manifests.
3. Create a PVC using the `sw-block-dynamic` StorageClass.
4. Apply the launcher-generated blockvolume Deployment.
5. Run a pod that mounts the PVC.
6. Delete the pod and PVC.
7. Confirm generated blockvolume workload and iSCSI sessions are gone.

The script below performs that whole smoke path:

```bash
bash scripts/run-k8s-alpha.sh "$PWD"
```

This is still a lab workflow. A real operator should eventually replace the
manual/harness step that applies generated blockvolume Deployments.

## Roadmap

Near-term work:

- replace `emptyDir` in the alpha manifest with a durable node-local path
- package a cleaner one-command install path
- add a small operator/controller for generated blockvolume workloads
- make TestOps remote K8s shell scenarios easier to run
- reduce noisy debug logs

Availability work:

- RF=2/RF=3 Kubernetes path
- multi-node attach
- failover while a pod remains mounted
- returned-replica reintegration
- WAL retention and flow-control behavior under pressure

Protocol/backend work:

- keep iSCSI as the MVP default
- add protocol-neutral CSI dispatch
- add NVMe-oF behind the same frontend-target model later
- introduce smart WAL backend behind an explicit test gate

More detail:

- [docs/architecture.md](docs/architecture.md)
- [docs/developer-architecture.md](docs/developer-architecture.md)
- [docs/runtime-state-machines.md](docs/runtime-state-machines.md)
- [docs/roadmap.md](docs/roadmap.md)

## Repository Layout

```text
cmd/
  blockmaster/    control plane daemon
  blockvolume/    per-replica data/frontend daemon
  blockcsi/       CSI controller/node plugin
  sw-testops/     minimal TestOps scenario runner

core/
  authority/      assignment publication and observation model
  csi/            CSI implementation
  host/           composed master/volume hosts
  lifecycle/      desired volume, node inventory, placement intent
  launcher/       Kubernetes manifest renderer
  recovery/       recovery execution components
  replication/    peer replication pieces

deploy/k8s/alpha/ alpha Kubernetes manifests and manual guide
docs/              architecture and roadmap notes
internal/          non-public support libraries and test scenario registry
scripts/           build and smoke-test helpers
```

## Development Tests

Development tests additionally need Go installed.

Useful smoke tests:

```bash
go test ./cmd/sw-testops ./internal/testops ./cmd/blockcsi ./core/launcher ./cmd/blockmaster ./core/host/master ./core/lifecycle -count=1
```

Kubernetes alpha smoke:

```bash
bash scripts/run-k8s-alpha.sh "$PWD"
```

## Honesty Note

This repository should currently be read as an alpha block-storage system with
a runnable Kubernetes smoke path and a serious recovery/control-plane design
under construction.

It should not be read as finished storage software.
