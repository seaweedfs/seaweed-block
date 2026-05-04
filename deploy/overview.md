# `deploy/` Overview

This directory contains deployment assets.

## Layout

| Directory | Purpose |
|---|---|
| `k8s/alpha/` | Stable alpha Kubernetes path for users and external testers. Prefer this path in public docs. |
| `k8s/g15b/`, `k8s/g15d/` | Historical test manifests kept for reproducibility of earlier lab scenarios. New user-facing docs should not point here unless debugging a specific old run. |

## Alpha Manifest Roles

| File | Purpose |
|---|---|
| `block-stack.yaml` | blockmaster and base blockvolume runtime objects |
| `csi-controller.yaml` | CSI controller Deployment and sidecars |
| `csi-node.yaml` | CSI node DaemonSet with iSCSI host integration |
| `csi-driver.yaml` | Kubernetes CSIDriver object |
| `rbac.yaml` | Service accounts and permissions |
| `demo-dynamic-pvc-pod.yaml` | Smoke PVC and checksum pod |

## Design Rules

- Public documentation should use `deploy/k8s/alpha/`.
- Test-specific historical paths may remain, but should not leak into quick-start
  instructions.
- Manifests should make host assumptions explicit, especially iSCSI and
  privileged node operations.

