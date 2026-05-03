# `scripts/` Overview

This directory contains developer and QA helper scripts.

## Scripts

| Script | Purpose |
|---|---|
| `run-k8s-alpha.sh` | Public alpha smoke wrapper. Prefer this in README/user docs. |
| `build-alpha-images.sh` | Builds local Docker images used by the Kubernetes alpha harness. |
| `run-alpha-k8s-dynamic.sh` | Public dynamic PVC alpha smoke entry. |
| `build-g15b-images.sh` | Compatibility wrapper for older QA scripts. |
| `run-g15b-k8s-static.sh` | Historical static PV Kubernetes harness. |
| `run-g15d-k8s-dynamic.sh` | Historical dynamic PVC harness used by compatibility scenarios. |
| `iterate-m01-nvme.sh` | Lab iteration helper for NVMe work. |
| `iterate-m01-replicated-write.sh` | Lab iteration helper for replicated write tests. |
| `genproto.sh` | Protocol/code generation helper. |

## Design Rules

- User-facing scripts should avoid internal gate labels in output.
- Lab scripts may keep historical names for reproducibility, but wrapper scripts
  should provide stable product-facing names.
- Scripts should preserve artifacts on failure before cleanup.
