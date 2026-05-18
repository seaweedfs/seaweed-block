# Seaweed Block

Seaweed Block is an experimental Kubernetes block storage service built around
normal Kubernetes PVCs, a CSI driver, and SeaweedFS block components.

The current alpha is focused on one user-visible loop:

```text
install on Kubernetes
-> create a PVC-backed block volume
-> mount it in an app pod
-> write/read data
-> inspect status and support evidence
-> clean up
```

Start with the Kubernetes tutorial:

- [First volume on Kubernetes](docs/quickstart-kubernetes.md)

## Status

Alpha / early beta shape. Not production-ready.

What has been validated in the current alpha:

- Day-1 install-to-first-volume path: activate stack, create PVC, writer pod
  verifies data, reader pod verifies persisted data, status report generated,
  cleanup clean.
- dynamic PVC provisioning through CSI.
- product-owned generated `blockvolume` lifecycle.
- multiple PVCs / volumes visible in inventory.
- durable local volume restart/reattach coverage.
- RF=3 `sync-quorum` recovery through CSI/pod recreate.
- RF=3 Kubernetes node-loss recovery through CSI/pod recreate on a surviving
  node.
- RF=3 iSCSI ALUA + Linux dm-multipath transparent mounted failover on the
  proven alpha path.
- read-only operations evidence through `sw-block ops inventory`,
  `sw-block ops cluster --master-api ... -o json`, support bundles, and
  product-owned event timelines.

These are narrow alpha claims tied to documented gates and support artifacts.
Do not treat them as broad production HA or compatibility claims.

Known missing pieces:

- production-grade installer/operator lifecycle or Helm chart,
- hosted dashboard/UI (only a local static read-only report exists),
- backup, snapshot, and restore workflow,
- returned-replica rebuild, reintegration, and failback,
- transparent node-loss failover without pod recreate,
- NVMe ANA parity for the transparent failover path,
- broad distro/kernel/initiator compatibility matrix,
- upgrade/rollback safety,
- performance, RTO, or SLO claims,
- security/RBAC/audit hardening for mutating admin actions.

## Why this exists

Kubernetes storage usually comes down to two options:

- a full storage stack such as Ceph/Rook
- Local PVs with weaker failure handling

`seaweed-block` explores a smaller block storage design for cases where a full
storage platform is too heavy but local storage is not enough. The current
goal is to validate the control path, data path, and recovery model before
expanding the system.

## Architecture

The alpha path:

- Kubernetes PVC
- CSI driver
- block master / controller
- block volume process
- iSCSI target
- WAL-backed local write path

The local write path:

```text
write → WAL → flush/checkpoint → extent
```

The WAL-first design makes writes easier to inspect and recover during
development.

Recovery is split into base transfer and live WAL feeding so normal writes
keep flowing while a peer catches up. Only one peer is the source of truth
for a recovery stream at any given time.

## Quick Start

Use [First volume on Kubernetes](docs/quickstart-kubernetes.md). It runs
preflight, builds/imports local images, installs the stack, creates one PVC,
writes and reads data through an app pod replacement, and shows cleanup and
support-bundle evidence to inspect if anything fails.

Minimal lab path:

```bash
bash scripts/activate-k8s-alpha.sh "$PWD"
bash scripts/run-basic-app-example.sh "$PWD"
cat "$(ls -td /tmp/sw-block-basic-app-* | head -1)/first-volume-summary.txt"
```

Expected summary fields:

```text
first_volume_status=ok
writer_verified=true
reader_verified=true
inventory_status=ok
status_report=status/report/index.html
cleanup_status=ok
```

The activation script writes `/tmp/sw-block-activation-*/activation-summary.txt`
with the blockmaster, CSI controller, CSI node, StorageClass, protocol, ACK
profile, and next inspection commands.

For QA/PM user-path testing against published images, use the same activation
entry point with image mode set to `published`:

```bash
SW_BLOCK_ACTIVATION_IMAGE_MODE=published \
  bash scripts/activate-k8s-alpha.sh "$PWD"
```

That path uses `ghcr.io/seaweedfs/seaweed-block:alpha` and
`ghcr.io/seaweedfs/seaweed-block-csi:alpha` by default. Prefer immutable
release tags or `sha-<commit>` tags once a release candidate is cut.

## Operations

```bash
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333
sw-block ops cluster --master-api 127.0.0.1:9333 -o json \
  > /tmp/sw-block-cluster-evidence.json
sw-block ops report --master-api 127.0.0.1:9333 --out /tmp/sw-block-report
```

If `sw-block` is not in `PATH`, run the same commands from this repository as
`go run ./cmd/sw-block ops ...`.

`sw-block ops report` writes a local static read-only status page plus
machine-readable artifacts. It is not a hosted dashboard and it has no mutating
admin actions.

For replica-level support evidence:

```bash
sw-block ops inventory \
  --namespace default \
  --master 127.0.0.1:9333 \
  --out /tmp/sw-block-inventory
```

## What Users Can Do Today

- Install the alpha stack on a supported Kubernetes/k3s lab.
- Create PVC-backed block volumes through Kubernetes.
- Run app pods that mount the PVC and verify file data.
- Inspect cluster, volume, replica, primary, frontend, and event evidence.
- Generate a local read-only HTML status report from live master evidence or a
  saved support bundle.
- Collect inventory and product evidence bundles for support.
- Exercise documented recovery gates in TestOps/lab environments.

## What Users Should Not Expect Yet

- A production-grade installer/operator or Helm chart.
- A hosted dashboard.
- Backup, snapshot, or restore workflows.
- Mutating admin actions such as promote, repair, rebuild, failback, or cleanup.
- Upgrade/rollback safety.
- Performance, RTO, or SLO guarantees.
- Broad distro/kernel/initiator compatibility.
- Transparent Kubernetes node-loss failover without pod recreate.

## Repository layout

```text
cmd/        command entry points (blockmaster, blockvolume, blockcsi, sw-block)
core/       core storage logic (authority, csi, host, lifecycle, recovery, replication)
internal/   non-public packages and TestOps registry
deploy/     Kubernetes manifests
docs/       design and quickstart docs
examples/   Kubernetes examples
scripts/    build, install, and smoke-test helpers
```

## Development

Run tests:

```bash
go test ./...
```

Run the Kubernetes alpha smoke:

```bash
bash scripts/run-k8s-alpha.sh "$PWD"
```

More detail:

- [docs/architecture.md](docs/architecture.md)
- [docs/developer-architecture.md](docs/developer-architecture.md)
- [docs/runtime-state-machines.md](docs/runtime-state-machines.md)
- [docs/roadmap.md](docs/roadmap.md)

## License

Apache-2.0
