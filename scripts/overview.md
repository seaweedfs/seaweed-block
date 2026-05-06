# `scripts/` Overview

This directory contains developer and QA helper scripts.

## Scripts

| Script | Purpose |
|---|---|
| `install-k8s-alpha.sh` | Installs the alpha blockmaster and CSI stack, leaving it running for user PVCs. |
| `apply-k8s-alpha-blockvolumes.sh` | Temporary operator substitute: applies blockvolume workloads generated after PVC creation. |
| `uninstall-k8s-alpha.sh` | Removes the alpha stack and generated blockvolume workloads. |
| `run-k8s-demo.sh` | Public app-demo wrapper. Shows writer pod -> same PVC -> reader pod verification. |
| `run-k8s-demo-ghcr.sh` | Public app-demo wrapper using published GHCR alpha images. |
| `run-k8s-alpha.sh` | Public alpha smoke wrapper. Prefer this in README/user docs. |
| `run-k8s-alpha-large.sh` | Optional 256 MiB Kubernetes alpha smoke for larger iSCSI write coverage. |
| `run-k8s-alpha-fio.sh` | Optional 256 MiB Kubernetes alpha smoke with a 60s fio randrw workload. |
| `run-k8s-attach-detach-loop.sh` | QA loop wrapper. Repeats the app PVC attach/write/read/delete flow and stores per-iteration artifacts. |
| `run-k8s-csi-node-restart.sh` | QA wrapper. Restarts the CSI node DaemonSet between writer and reader pods using the same PVC. |
| `build-alpha-images.sh` | Builds local Docker images used by the Kubernetes alpha harness. |
| `run-alpha-k8s-dynamic.sh` | Public dynamic PVC alpha smoke entry. |
| `run-alpha-app-demo.sh` | App-demo implementation used by `run-k8s-demo.sh`. |
| `run-iscsi-os-smoke.sh` | Privileged Linux OS-initiator smoke: `iscsiadm` -> `mkfs.ext4` -> mount -> checksum -> logout on a 256 MiB target. |
| `run-iscsi-backend-fio-matrix.sh` | Privileged Linux OS-initiator backend matrix: runs the same fio workload against `walstore` and `smartwal`, writing per-backend artifacts and an experimental summary. |
| `run-iscsi-alua-os-smoke.sh` | Privileged Linux OS-initiator smoke for ALUA reporting: `iscsiadm` -> `sg_inq`/`sg_rtpg` -> mount -> checksum -> logout. |
| `run-iscsi-alua-multipath-smoke.sh` | Privileged Linux OS-initiator smoke for two iSCSI paths: two portals -> ALUA active/standby evidence -> standby write reject -> `multipath -ll`. |
| `run-iscsi-alua-mounted-failover-smoke.sh` | Privileged Linux OS-initiator smoke for mounted multipath failover: mount `/dev/mapper/*`, kill active path, wait r2 primary, verify checksum read/write. |
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
