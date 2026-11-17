# Seaweed Block Engineering Wiki

This wiki explains how the Kubernetes line of Seaweed Block works, for
developers changing it. It is not a release-claim source: the
[release notes](../releases/README.md) and
[Kubernetes status](../kubernetes-status.md) hold the claims, and the
design documents are the canonical description: the
[core design](../design/block-design.md) (core block service, verification,
known issues), the [agent workspace design](../design/agent-workspace-design.md)
and the [Kubernetes block design](../design/kubernetes-block-design.md).

## Pages

- [Code Map](code-map.md) - main commands, packages and responsibility
  boundaries.
- [State Machines](state-machines.md) - the product states developers must not
  violate.
- [QA and TestOps Map](testops-map.md) - which gates prove which claims.
- [Runtime State Machines](../runtime-state-machines.md) - control-plane, CSI
  and frontend loops.

## Current Product Loop

The current validated loop is:

```text
Helm install
-> CSI CreateVolume
-> SwBlockVolume identity CR
-> lifecycle-owner protection finalizer
-> operator-status writes CRD status and Events
-> delete request
-> delete-safety hold on unsafe evidence
-> release on clean cleanup evidence
-> uninstall zero residue
```

This is a bounded operation-layer capability. It is not automatic cleanup, not a
production operator, and not backup/NVMe expansion.

Returned-replica work adds a separately gated, opt-in failback runtime path.
See [Returned Replica Failback Runtime](deep-dives/returned-replica-failback.md)
for the authority owner, target CR, executor, gRPC service, terminal evidence,
and non-claims. That line can plan a frontend-publication target from terminal
failback evidence and invoke the publication runtime only under explicit
policy; workload-visible path switching remains a separate gate.

## Deep Dives

New pages follow the [deep-dive template](deep-dives/template.md). A page is
deep only if a developer could use it to make a correct code change, write the
right gate and diagnose a failed run.

- [CSI Lifecycle](deep-dives/csi-lifecycle.md)
- [Operation Layer v0.5](deep-dives/operation-layer-v05.md)
- [Status Writer Conformance](deep-dives/status-writer-conformance.md)
- [ManagedVolume Readiness](deep-dives/managed-volume-readiness.md)
- [SmartWAL Dirty Failure](deep-dives/smartwal-dirty-failure.md)
- [WAL Frontier Recovery](deep-dives/wal-frontier-recovery.md)
- [blockvolume Process Readiness](deep-dives/blockvolume-process-readiness.md)
- [Authority And Promotion](deep-dives/authority-promotion.md)
- [iSCSI ALUA Multipath](deep-dives/iscsi-alua-multipath.md)
- [Live Node Evidence](deep-dives/live-node-evidence.md)
- [Support Bundle Cold Reader](deep-dives/support-bundle-cold-reader.md)
- [Release Engineering Image Skew](deep-dives/release-engineering-image-skew.md)
- [Returned Replica Rebuild](deep-dives/returned-replica-rebuild.md)
- [Returned Replica Failback](deep-dives/returned-replica-failback.md)
- [Read-Write Control Plane](deep-dives/read-write-control-plane-roadmap.md)
- [TestOps Authoring](deep-dives/testops-authoring.md)

## Source Evidence

- `docs/` - public and user-facing docs.
- `internal/docs/` - phase plans, design references and QA sign-offs.
- `testops/` - scenario definitions and suite structure.

When a wiki page conflicts with the code, a release note or a QA sign-off, the
wiki page is wrong.

## Local Preview

```bash
pip install mkdocs-material
mkdocs serve        # http://127.0.0.1:8000
```

Use Mermaid for state machines and sequence diagrams. Link Markdown pages under
`docs/` with relative links, and source code with GitHub links, since MkDocs
serves only files under `docs/`.
