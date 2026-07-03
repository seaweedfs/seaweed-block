# Phase 123 Finished Plan: NVMe/TCP Performance Bottleneck Triage

Status: closed, QA PASS on 2026-07-03.

## Problem

Phase 122 proved that the NVMe/TCP target can publish on the configured
data-plane address (`10.0.0.1:4420`) and recorded a mounted PVC baseline. The
numbers were far below the network's theoretical capacity, but without an
independent comparator we could not say whether the bottleneck was the 10.0.0.x
fabric, the target implementation, the backend, Kubernetes attach/filesystem
overhead, or the test shape.

## What Changed

- Added `scripts/run-phase123-nvme-tcp-bottleneck-triage-gate.sh`.
- Added `testops/scenarios/nvme-tcp-bottleneck-triage-chain.yaml`.
- The gate now collects:

  ```text
  ip route get <frontend target>
  iperf3 m02 -> m01 over 10.0.0.x
  Phase122 mounted NVMe/TCP metrics
  runtime kubectl top / pod / log snapshots
  cleanup verifier
  ```

- The gate keeps explicit non-claims:

  ```text
  frontend_transport=tcp
  nvme_rdma_supported=false
  roce_claim_allowed=false
  performance_slo_claim_allowed=false
  ```

## Verification

Local checks:

```text
bash -n scripts/run-phase123-nvme-tcp-bottleneck-triage-gate.sh
swblock validate testops/scenarios/nvme-tcp-bottleneck-triage-chain.yaml
```

Runner gate:

```text
nvme-tcp-bottleneck-triage-chain PASS
20 actions: 20 passed, 0 failed
```

Key evidence:

```text
network_baseline_mibps=4106.55
publish_target=10.0.0.1:4420
route_dev=enp1s0np0
k8s_mounted_seq_write_mibps=127.74
k8s_mounted_seq_read_mibps=248.06
k8s_mounted_small_write_iops=755.16
top_bottleneck=unknown
next_recommendation=phase124_target_backend_shape_split
cleanup_status=ok
```

QA sign-off:

```text
internal/docs/qa-assignments/phase123-nvme-tcp-bottleneck-triage-qa-signoff.md
```

## Product Meaning

The data-plane network is not the first suspect: it moved about 4 GiB/s over
`10.0.0.x`, while the mounted Block NVMe/TCP read path was about 248 MiB/s and
write was about 128 MiB/s. The evidence is strong enough to defer NVMe/RDMA
implementation and first split the current target/backend/Kubernetes/test-shape
path.

## Next Step

Phase 124 should compare Block NVMe/TCP against a same-shape local-path PVC and
vary the current write/read shape enough to separate:

```text
test shape / fsync overhead
Kubernetes mounted filesystem overhead
blockvolume target path
durable backend path
```

Do not start a real NVMe/RDMA target until this split identifies a transport
bottleneck.

## Non-Claims

Phase 123 does not implement or validate NVMe/RDMA, RoCE, GPU Direct,
cuFile/cuObject, NIXL, production HA, broad host compatibility, or a
performance SLO.
