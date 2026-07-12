# Phase 120 Finished Plan: NVMe/TCP Management-LAN Baseline

Status: closed as a functional/default-network baseline.

Phase 120 added and QA-validated a supported-lab Kubernetes NVMe/TCP gate:

```text
scripts/run-phase120-nvme-tcp-performance-baseline-gate.sh
testops/scenarios/nvme-tcp-performance-baseline-chain.yaml
```

## Result

Final live QA passed on commit `fb19f58` and was recorded in:

```text
internal/docs/qa-assignments/phase120-nvme-tcp-performance-baseline-qa-signoff.md
```

Evidence:

```text
publish_target=192.168.1.181:4420
seq_write_mibps=82.37
seq_read_mibps=231.05
small_write_iops=771.08
cleanup_status=ok
```

## Correct Interpretation

This is a management-LAN/default Kubernetes InternalIP baseline:

```text
192.168.1.x -> Kubernetes/LAN TCP path
```

It proves the supported NVMe/TCP PVC path can build, install, attach, mount,
write, read, collect metrics, and clean up. It is not the right performance
baseline for the 100GbE/RoCE fabric.

## Follow-Up

Phase 121 should add explicit data-plane address capability before rerunning a
high-speed NVMe/TCP baseline:

```text
managementIP != frontend/data-plane IP
NVMe/TCP 100GbE baseline != RoCE/NVMe-RDMA claim
```

## Non-Claims

Phase 120 did not claim RoCE, NVMe/RDMA attach, NVMe/RDMA performance, a
performance SLO, production tuning, broad kernel/distro compatibility,
GPU/cuObject, NIXL production support, or published-image support.
