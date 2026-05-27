# `docs/` Overview

This directory contains public and technical documentation.

## Public / Contributor Docs

| File | Purpose |
|---|---|
| `architecture.md` | Short product architecture overview. |
| `developer-architecture.md` | Deeper module-by-module architecture for contributors. |
| `runtime-state-machines.md` | Mermaid state-machine and loop overview. |
| `roadmap.md` | Alpha roadmap and non-claims. |
| `quickstart-kubernetes.md` | First-volume Kubernetes quickstart. |
| `user-capabilities.md` | User-facing current capabilities, operations commands, status vocabulary, and non-claims. |
| `operations-v1.md` | Legacy V1 alpha operations manual; use quickstart and release notes for current claims. |
| `kubernetes-app-demo.md` | Presentation-friendly Kubernetes app + PVC demo. |

## Calibration Evidence

`calibration/` contains evidence documents still referenced by calibration code.
Keep them here until those references move to the shared design archive.

## Documentation Rule

If a document is meant for external users, avoid internal phase or gate labels.
Internal planning, readiness tracking, audits, and historical design notes
belong under `internal/docs/`, not in this public docs entry path.
