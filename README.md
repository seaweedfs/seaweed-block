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

Start with the Kubernetes tutorial:

- [First volume on Kubernetes](docs/quickstart-kubernetes.md)
- [Release notes](docs/releases/README.md)

## Status

Alpha / early beta shape. Not production-ready.

The current alpha has QA-gated evidence for:

- Helm install to first PVC with writer/reader verification and clean uninstall.
- Standard Kubernetes PVC provisioning through CSI.
- Multiple PVC-backed volumes in the gated lab path.
- RF=3 recovery gates: CSI reattach, mounted ALUA/dm-multipath failover, and
  interleaved multi-volume isolation.
- Restart persistence on the hostPath-backed alpha path.
- Read-only operations evidence: report, dashboard, inventory, timeline,
  explain output, support-bundle replay, and `operator-snapshot.json`.

These are narrow alpha claims tied to documented gates and support artifacts.
See [release notes](docs/releases/README.md) for the exact run evidence. Do
not treat these as broad production HA or compatibility claims.

Known missing pieces:

- production-grade operator lifecycle,
- Helm is the supported alpha install path for supported labs; a narrow
  upgrade/rollback smoke is gated, but production-grade Helm lifecycle is not
  claimed,
- production hosted dashboard/UI; a local read-only dashboard/report exists,
- backup, snapshot, and restore workflow,
- returned-replica rebuild, reintegration, and failback,
- transparent node-loss failover without pod recreate,
- NVMe ANA parity for the transparent failover path,
- broad distro/kernel/initiator compatibility matrix,
- broad upgrade/rollback safety beyond the gated smoke path,
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

Use [First volume on Kubernetes](docs/quickstart-kubernetes.md). For the
current alpha, start with Helm on a supported Kubernetes/k3s lab.

Fast path:

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

`generate-helm-values` reads the current Kubernetes API through `kubectl`,
selects Ready schedulable nodes, and writes chart values. One selected node
uses loopback mode. Multiple selected nodes use external iSCSI/status addresses
and CHAP by default.

Expected summary fields:

```text
first_volume_status=ok
writer_verified=true
reader_verified=true
inventory_status=ok
status_report=status/report/index.html
cleanup_status=ok
```

For QA/PM user-path testing against published images, prefer immutable image
tags in Helm values or activation env:

```bash
ghcr.io/seaweedfs/seaweed-block:sha-<commit>
ghcr.io/seaweedfs/seaweed-block-csi:sha-<commit>
```

Current validated alpha image tag:
`sha-6260e46fd3be`.

Published image digests:

```text
seaweed-block
  index:       sha256:ef9c60f82c36f22360b10faafd32caf807f98ac0ea86c0365c0d0836e5f67110
  linux/amd64: sha256:36481cbc1fc98fafdfa386823e0e5906785cb6f35748ef698ff1cec39bb40464
seaweed-block-csi
  index:       sha256:b160ceee874dc6743074ef6b6735ccf05914c1de5951972922f6d3779bc73592
  linux/amd64: sha256:82e41b7ef92ad8db38b6927e334cc1d564b1012ad916e9bde2e882cece680be8
```

Mutable `:alpha` is a smoke/demo tag only; it can drift from the source tree.

Script activation is a development/local-image fallback, not the primary user
path:

```bash
bash scripts/activate-k8s-alpha.sh "$PWD"
bash scripts/run-basic-app-example.sh "$PWD"
cat "$(ls -td /tmp/sw-block-basic-app-* | head -1)/first-volume-summary.txt"
```

The `"$PWD"` argument is the repository root. The helper scripts use it to
locate chart, manifest, and example files.

## Operations

Port-forward blockmaster when reading live cluster state:

```bash
kubectl -n kube-system port-forward deploy/sw-blockmaster 9333:9333
```

Common read-only commands:

| Command | Use |
|---|---|
| `sw-block ops cluster --master-api 127.0.0.1:9333 -o json` | Cluster snapshot for automation or support. |
| `sw-block ops volumes --master-api 127.0.0.1:9333` | List volumes, status, RF, primary, node, and frontend. |
| `sw-block ops status --volume <id> --master <addr> --status-addr <addr> --out <dir>` | Collect a focused live status bundle for one volume. |
| `sw-block ops describe volume <id> --namespace default --master 127.0.0.1:9333` | Describe one volume through live Kubernetes/master evidence. |
| `sw-block ops timeline volume --from-bundle <dir> <id> -o jsonl` | Read the event timeline for one volume from a saved bundle. |
| `sw-block ops explain volume --from-bundle <dir> <id>` | Cold-read why a volume is ready, blocked, stale, or recovering. |
| `sw-block ops report --master-api 127.0.0.1:9333 --out <dir>` | Generate static HTML/JSON/text evidence. |
| `sw-block ops dashboard --master-api 127.0.0.1:9333 --listen 127.0.0.1:9334` | Serve the same evidence as a local read-only dashboard. |
| `sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out <dir>` | Collect replica-level support inventory. |

Minimal live report example:

```bash
sw-block ops cluster --master-api 127.0.0.1:9333 -o json \
  > /tmp/sw-block-cluster-evidence.json
sw-block ops report --master-api 127.0.0.1:9333 --out /tmp/sw-block-report
sw-block ops dashboard --master-api 127.0.0.1:9333 --listen 127.0.0.1:9334
```

If `sw-block` is not in `PATH`, run the same commands from this repository as
`go run ./cmd/sw-block ops ...`.

`sw-block ops report` writes static read-only artifacts. `sw-block ops
dashboard` serves the same evidence as a local read-only dashboard. Reports and
dashboard also expose `operator-snapshot.json`, a read-only status projection
for future operator work. Neither has mutating admin actions.

For support-bundle replay on another machine:

```bash
bash scripts/collect-helm-support-bundle.sh "$PWD"
sw-block ops report --from-bundle <bundle-or-artifact-dir> --out /tmp/sw-block-report
sw-block ops explain volume <volume-id> --from-bundle <bundle-or-artifact-dir>
sw-block ops dashboard --from-bundle <bundle-or-artifact-dir> --listen 127.0.0.1:9334
```

The status surface is negative-first. If evidence is missing or stale, the
system reports `Ready=Unknown` / `EvidenceStale=True` rather than claiming a
false ready state. A blocked example such as `csi_node_image_pull_failed`
appears consistently in `summary.txt`, `operator-snapshot.json`, dashboard
JSON, explain output, and support bundles.

Minimal blocked-state drill:

```bash
# Example: install with an intentionally bad CSI image tag, then collect bundle.
bash scripts/collect-helm-support-bundle.sh "$PWD"
sw-block ops explain volume <volume-id> --from-bundle <bundle-or-artifact-dir>
```

Expected shape: no `Ready=True`; instead `Blocked=True
reason=csi_node_image_pull_failed` and read-only/dry-run suggested actions.

Strict cleanup:

```bash
helm uninstall sw-block --namespace kube-system
bash scripts/uninstall-k8s-alpha.sh "$PWD"
bash scripts/verify-helm-cleanup.sh
```

The cleanup verifier checks Kubernetes resources, iSCSI sessions, iSCSI node
DB records, dm-multipath maps, `dmsetup` devices, product processes, and
hostPath residue.

## What Users Can Do Today

- Install Seaweed Block through Helm on a supported Kubernetes/k3s lab, create
  the first PVC, run a writer pod, run a replacement reader pod, inspect a
  read-only report/dashboard, and uninstall cleanly.
- Use normal Kubernetes PVC semantics: `kubectl apply` a PVC, let CSI
  dynamically provision the backing block volume, and mount it from app pods.
- Run multiple PVC-backed volumes in the gated lab path. The current QA gates
  validate three RF=3 volumes with independent primaries and publish targets.
- Validate restart persistence in the hostPath-backed alpha path. The gated
  restart tests verify data remains readable and promoted authority does not
  roll back after k3s restart.
- Exercise gated recovery paths: CSI reattach with pod recreate, transparent
  mounted failover through iSCSI ALUA/dm-multipath on the proven Stage-2 path,
  and interleaved multi-volume failover isolation.
- Collect support evidence and replay it offline through `sw-block ops report`,
  `sw-block ops explain`, and the local read-only dashboard.
- Inspect `operator-snapshot.json` as the read-only operator-facing status
  projection.

These are alpha, gate-backed capabilities for supported labs. They are not
broad production HA, broad platform compatibility, or SLO commitments.

## What Users Should Not Expect Yet

- A production-grade operator.
- Production-grade Helm lifecycle management; Helm is the supported alpha
  install path and one upgrade/rollback smoke is gated, but broad upgrade
  safety is not claimed.
- A production hosted dashboard; the current dashboard is local and read-only.
- Backup, snapshot, or restore workflows.
- Mutating admin actions such as promote, repair, rebuild, failback, or cleanup.
- Broad upgrade/rollback safety beyond the gated smoke path.
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

Run the release-relevant scoped tests:

```bash
go test ./cmd/sw-block ./core/ops ./core/csi ./core/launcher ./core/host/master -count=1
```

`go test ./...` is the full repository sweep and may include unrelated
in-progress packages outside the current release gate.

Run the Kubernetes alpha smoke:

```bash
bash scripts/run-k8s-alpha.sh "$PWD"
```

More detail:

- [docs/releases/README.md](docs/releases/README.md)
- [docs/architecture.md](docs/architecture.md)
- [docs/developer-architecture.md](docs/developer-architecture.md)
- [docs/runtime-state-machines.md](docs/runtime-state-machines.md)
- [docs/roadmap.md](docs/roadmap.md)

## License

Apache-2.0
