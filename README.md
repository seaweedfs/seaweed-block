# Seaweed Block

<p align="center">
  <img src="docs/assets/seaweed-block-hero.svg" alt="Seaweed Block alpha architecture: Kubernetes PVC to CSI, blockmaster, blockvolume, iSCSI, and WAL-backed recovery" width="100%">
</p>

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

Start here:

- [Kubernetes quickstart](docs/quickstart-kubernetes.md)
- [What users can do today](docs/user-capabilities.md)
- [Release notes and validated claims](docs/releases/README.md)

## Status

Alpha / early beta shape. Not production-ready.

Current QA-gated capabilities:

- Helm install to first PVC with writer/reader verification and clean uninstall.
- Standard Kubernetes PVC provisioning through CSI.
- Multiple PVC-backed volumes in the gated lab path.
- RF=3 recovery gates: CSI reattach, mounted ALUA/dm-multipath failover, and
  interleaved multi-volume isolation.
- Restart persistence on the hostPath-backed alpha path.
- Read-only operations evidence: report, dashboard, inventory, timeline,
  explain output, support-bundle replay, and `operator-snapshot.json`.

These are narrow alpha claims tied to documented gates. See
[release notes](docs/releases/README.md) for exact run evidence.

## Five-Minute Quick Start

Use Helm on a supported Kubernetes/k3s lab:

```bash
go build -o sw-block ./cmd/sw-block
export PATH="$PWD:$PATH"

sw-block ops generate-helm-values \
  --out values.day1.yaml \
  --image ghcr.io/seaweedfs/seaweed-block:sha-6260e46fd3be \
  --csi-image ghcr.io/seaweedfs/seaweed-block-csi:sha-6260e46fd3be

helm install sw-block charts/seaweed-block \
  --namespace kube-system \
  --create-namespace \
  -f values.day1.yaml \
  --wait \
  --timeout 10m

SW_BLOCK_INSTALL_MODE=helm \
SW_BLOCK_HELM_RELEASE=sw-block \
SW_BLOCK_HELM_NAMESPACE=kube-system \
SW_BLOCK_HELM_VALUES_FILE=values.day1.yaml \
  bash scripts/run-basic-app-example.sh "$PWD"
```

Expected summary shape:

```text
first_volume_status=ok
writer_verified=true
reader_verified=true
inventory_status=ok
status_report=status/report/index.html
cleanup_status=ok
```

Current validated alpha image tag: `sha-6260e46fd3be`.

Mutable `:alpha` is a smoke/demo tag only; it can drift from the source tree.

## Operations

Port-forward blockmaster when reading live cluster state:

```bash
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333
```

Common read-only commands:

| Command | Use |
|---|---|
| `sw-block ops cluster --master-api 127.0.0.1:9333 -o json` | Cluster snapshot. |
| `sw-block ops volumes --master-api 127.0.0.1:9333` | List volumes, status, primary, node, frontend. |
| `sw-block ops describe volume <id> --namespace default --master 127.0.0.1:9333` | Describe one volume. |
| `sw-block ops timeline volume --from-bundle <dir> <id> -o jsonl` | Read a saved event timeline. |
| `sw-block ops explain volume --from-bundle <dir> <id>` | Explain ready, blocked, stale, or recovering state. |
| `sw-block ops report --master-api 127.0.0.1:9333 --out <dir>` | Generate static report artifacts. |
| `sw-block ops dashboard --master-api 127.0.0.1:9333 --listen 127.0.0.1:9334` | Serve local read-only dashboard. |
| `sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out <dir>` | Collect replica-level inventory. |

Reports and dashboard expose `operator-snapshot.json`, a read-only status
projection for future operator work. There are no mutating admin actions.

Support-bundle replay:

```bash
bash scripts/collect-helm-support-bundle.sh "$PWD"
sw-block ops report --from-bundle <bundle-or-artifact-dir> --out /tmp/sw-block-report
sw-block ops explain volume <volume-id> --from-bundle <bundle-or-artifact-dir>
sw-block ops dashboard --from-bundle <bundle-or-artifact-dir> --listen 127.0.0.1:9334
```

The status surface is negative-first. If evidence is missing or stale, the
system reports `Ready=Unknown` / `EvidenceStale=True` rather than claiming a
false ready state.

## Cleanup

```bash
helm uninstall sw-block --namespace kube-system
bash scripts/uninstall-k8s-alpha.sh "$PWD"
bash scripts/verify-helm-cleanup.sh
```

The cleanup verifier checks Kubernetes resources, iSCSI sessions, iSCSI node
DB records, dm-multipath maps, `dmsetup` devices, product processes, and
hostPath residue.

## What You Can Do

- Install Seaweed Block through Helm on a supported Kubernetes/k3s lab.
- Create and mount PVC-backed block volumes through normal Kubernetes PVCs.
- Verify writer/reader persistence with ordinary app pods.
- Inspect cluster, volume, replica, primary, frontend, timeline, and reason
  evidence through read-only CLI/report/dashboard surfaces.
- Replay support bundles offline.
- Exercise gated multi-volume, restart, and recovery scenarios in lab/TestOps
  environments.

More detail: [docs/user-capabilities.md](docs/user-capabilities.md).

## What You Should Not Expect Yet

- Production readiness or production SLOs.
- A production-grade operator or mutating admin workflow.
- Backup, snapshot, or restore.
- Returned-replica rebuild, reintegration, or failback.
- Transparent Kubernetes node-loss failover without pod recreate.
- NVMe ANA parity for the transparent failover path.
- Broad distro/kernel/initiator compatibility.
- Broad upgrade/rollback safety beyond gated smoke paths.

## Documentation

- [Kubernetes quickstart](docs/quickstart-kubernetes.md) - first install and PVC.
- [User capabilities](docs/user-capabilities.md) - detailed current behavior.
- [Release notes](docs/releases/README.md) - exact validated claims and evidence.
- [Roadmap](docs/roadmap.md) - public planning summary.
- [Architecture](docs/architecture.md), [developer architecture](docs/developer-architecture.md),
  and [runtime state machines](docs/runtime-state-machines.md) - engineering
  references, not release-claim sources.

## Development

Run release-relevant scoped tests:

```bash
go test ./cmd/sw-block ./core/ops ./core/csi ./core/launcher ./core/host/master -count=1
```

`go test ./...` is the full repository sweep and may include unrelated
in-progress packages outside the current release gate.

Script activation remains available for development/local-image fallback:

```bash
bash scripts/activate-k8s-alpha.sh "$PWD"
bash scripts/run-basic-app-example.sh "$PWD"
```

## License

Apache-2.0
