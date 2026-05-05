# `docs/` Overview

This directory contains public and technical documentation.

## Public / Contributor Docs

| File | Purpose |
|---|---|
| `architecture.md` | Short product architecture overview. |
| `developer-architecture.md` | Deeper module-by-module architecture for contributors. |
| `runtime-state-machines.md` | Mermaid state-machine and loop overview. |
| `roadmap.md` | Alpha roadmap and non-claims. |
| `kubernetes-app-demo.md` | Presentation-friendly Kubernetes app + PVC demo. |

## Calibration Evidence

`calibration/` contains evidence documents still referenced by calibration code.
Keep them here until those references move to the shared design archive.

## Documentation Rule

If a document is meant for external users, avoid internal phase or gate labels.
Internal planning, readiness tracking, audits, and historical design notes
belong under `internal/docs/`, not in this public docs entry path.
