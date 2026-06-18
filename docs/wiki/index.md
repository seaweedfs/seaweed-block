# Seaweed Block Engineering Wiki

This wiki is the engineering map for Seaweed Block development. It is not a
replacement for finished plans, QA sign-offs, or release notes. It is the entry
point that explains how those documents fit together.

Seaweed Block has passed through more than forty phases. A single architecture
document cannot explain the accumulated product model. The useful structure is:

```text
problem background
-> industry pattern
-> Seaweed Block implementation
-> state machine / invariant
-> code entry points
-> QA gates and failure evidence
-> current limits and next work
```

## How To Read This Wiki

Start here:

- [Developer Guide](developer-guide.md) - what belongs in the wiki and what
  remains in source evidence.
- [Coverage Matrix](coverage-matrix.md) - whether the wiki covers the major
  months-long engineering domains and where deeper docs are still needed.
- [Topic Inventory](topic-inventory.md) - full topic backlog and whether each
  topic is implementation-grade, summary-level, mapped, missing, or future.
- [Historical Sources](historical-sources.md) - older tutorial and methodology
  material that explains the block-storage method behind the current design.
- [Code Map](code-map.md) - main packages, commands, and responsibility
  boundaries.
- [State Machines](state-machines.md) - the product states developers must not
  violate.
- [Protocol Catalog](protocol-catalog.md) - the current catalog of mini
  protocols and state machines that deserve Mermaid diagrams.
- [Phase Map](phase-map.md) - how Phase 1 through Phase 44 connect.
- [QA and TestOps Map](testops-map.md) - which gates prove which claims.

Related local docs:

- [Seaweed RDMA Engineering Wiki](http://127.0.0.1:8011/wiki/) - Rust RDMA
  data path, native RC/DC, UCX, pull-RDMA, and future GPU-style destinations.

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
production operator, and not rebuild/failback/backup/NVMe expansion.

## Source Evidence

The wiki links to these source areas:

- `docs/` - public and user-facing docs.
- `internal/docs/finished-plans/` - phase-by-phase completion records.
- `internal/docs/ref/` - design references and product contracts.
- `internal/docs/protocol/` - control-model and invariant documents.
- `internal/docs/qa-assignments/` - gate assignments and QA sign-offs.
- `testops/` - scenario definitions and suite structure.

When a wiki page conflicts with a finished plan or QA sign-off, the finished
plan or QA sign-off wins.

## Depth Standard

For storage/control-plane topics, a page is only considered deep if it can guide
a code change and a QA gate. It should include protocol or domain background,
product contract, ownership model, state machine, publish/API shape, code
entry points, evidence contract, failure taxonomy, implementation checklist,
QA history, and non-claims.

A correct summary is still useful, but it is not enough for feature work.
