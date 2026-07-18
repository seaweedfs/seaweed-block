# Current Plan: Phase 164 NVMe/RDMA Standalone Productization And Hardening

Status: planning.

Phase 163 proved the first real Seaweed Block NVMe/RDMA data path:

```text
Linux nvme-rdma initiator
-> kernel nvmet-rdma target
-> product-owned NBD bridge
-> Seaweed Block frontend.Backend
```

The supported-lab gate connected from m01 to the m02 RoCE address, verified
write/read/flush against the Seaweed backend, and left no target, host, or NBD
residue. Kubernetes publication, failover, and performance remain non-claims.

## Goal

Turn the Phase 163 implementation spike into one hardened standalone product
slice before any Kubernetes integration. This phase owns correctness, restart,
isolation, refusal, observability, and cleanup as one close gate rather than as
separate small phases.

## Deliverables

### D1. Lifecycle And Rollback

- Allocate and release NBD devices without cross-run contamination.
- Roll back NBD/configfs state after partial startup failure.
- Handle normal termination and repeated start/stop cleanly.
- Avoid fixed test ports and stale NQNs in the formal gate.

### D2. Data And Flush Correctness

- Verify aligned 4 KiB and larger sequential write/read checksums.
- Verify NVMe flush and FUA reach the Seaweed backend sync boundary.
- Reject out-of-range or malformed requests without desynchronizing the bridge.

### D3. Durable Restart And Reconnect

- Write known data through `nvme connect -t rdma`.
- Disconnect and restart `blockvolume` with the same durable root.
- Reconnect and verify the pre-restart checksum.
- Keep capability status honest while the listener is down or restarting.

### D4. Isolation And Bounded Churn

- Run two standalone targets with distinct NQN, port, namespace, and NBD device.
- Prove writes do not cross volume boundaries.
- Run repeated connect/write/read/disconnect cycles and finish with zero residue.

### D5. Refusal And Regression Boundary

- Fail closed for missing modules, configfs, RDMA device, bind address, port
  conflict, or insufficient privilege with stable evidence.
- Keep the existing NVMe/TCP path and tests unchanged.
- Keep RDMA absent from master/CSI publish context in this phase.
- Make no throughput, latency, acceleration, HA, or production SLO claim.

### D6. Close Gate

- Package D1-D5 as one TestOps scenario with independent host, target, backend,
  and cleanup evidence.
- Update the source-gated supported-lab boundary from the same run bundle.

## Required Evidence

```text
phase164_nvme_rdma_standalone_hardening_status=ok
startup_rollback_verified=true
small_and_large_io_verified=true
flush_and_fua_verified=true
durable_restart_reconnect_verified=true
multi_target_isolation_verified=true
bounded_connect_churn_verified=true
negative_preflight_refusal_verified=true
tcp_behavior_unchanged=true
rdma_not_published_to_csi=true
performance_slo_claim_allowed=false
cleanup_status=ok
next_recommendation=phase165_nvme_rdma_kubernetes_publish_attach
```

## Exit Criteria

Phase 164 closes only when the complete standalone hardening gate passes from
one TestOps bundle. Phase 165 may then own Kubernetes publish context,
NodeStage/NodeUnstage, mounted workload I/O, status agreement, and delete
cleanup. Performance comparison comes only after those correctness gates.
