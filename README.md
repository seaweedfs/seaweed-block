# seaweed-block

`seaweed-block` is an experimental block storage service for Kubernetes.

It provides a CSI-based block volume path backed by SeaweedFS components. The
current implementation focuses on a small, understandable storage stack for
testing block volume creation, attach, mount, write/read, and cleanup flows.

## Status

Alpha.

The current code passes a single-node Kubernetes smoke test:

```text
dynamic PVC create
→ CSI attach
→ iSCSI mount
→ pod write/read checksum
→ cleanup
```

This is not production-ready.

Known missing pieces:

- multi-node failover
- failover while a volume is mounted
- durable production packaging (the alpha manifest still uses `emptyDir`)
- broader recovery testing
- RF=2/RF=3 replication on the Kubernetes path
- a real operator (a manifest launcher does the work today)

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

## Quick start

Start here:

- [First volume on Kubernetes](docs/quickstart-kubernetes.md)

That guide is the single supported alpha entry point for a new user. It uses a
single-node Kubernetes/k3s path, runs preflight, builds/imports local images,
creates one PVC, writes and reads data through an app pod replacement, and
shows the cleanup and support-bundle evidence to inspect if anything fails.

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

## Current limitations

This project is still early.

Do not use it for production workloads. The current alpha is for local
development, design validation, and Kubernetes smoke testing.

Before production use, the project needs stronger recovery behavior, multi-node
support, durable packaging, observability, failure testing, and operational
documentation.

## License

Apache-2.0
