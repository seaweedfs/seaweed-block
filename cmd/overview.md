# `cmd/` Overview

This directory contains executable entry points. Command packages should be thin:
they parse flags, wire modules, and run loops. Product semantics should live in
`core/`.

## Commands

| Directory | Purpose | Main Interfaces |
|---|---|---|
| `blockmaster/` | Control-plane daemon for desired volumes, node inventory, placement, authority publication, and launcher output. | lifecycle store, authority publisher, launcher renderer, master RPC/HTTP surfaces |
| `blockvolume/` | Data-plane daemon for one volume replica. Serves frontend protocols and subscribes to assignment facts. | storage backend, frontend target, authority subscription, replication/recovery |
| `blockcsi/` | Kubernetes CSI driver. Handles Create/Delete, ControllerPublish, NodeStage, NodePublish. | CSI gRPC, blockmaster client, Linux iSCSI/mount helpers |
| `sw-testops/` | Minimal scenario runner for registered TestOps scenarios. | `internal/testops` registry and drivers |
| `m01verify/` | Hardware/lab verification helper. | lab-only verification flow |
| `sparrow/` | Older/bootstrap experimental command surface retained for compatibility and tests. | bootstrap and calibration helpers |

## Design Rules

- Do not mint authority facts directly from command packages.
- Do not put storage/recovery policy in flag parsing code.
- Keep Kubernetes-specific runtime wiring in `blockmaster`/`blockcsi` edges, not
  in generic lifecycle or authority packages.
- New product behavior should be testable below `cmd/` before a subprocess test
  is added.

