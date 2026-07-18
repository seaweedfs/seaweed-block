# Phase 164 Finished Plan: NVMe/RDMA Standalone Hardening

Status: **closed 2026-07-18, live TestOps gate PASS**.

## Problem

Phase 163 proved one real NVMe/RDMA I/O path, but a single write/read did not
establish a usable standalone lifecycle. Partial startup could leave the
replication and durable stacks open, a target restart had not been checked
against persisted data, and two simultaneous targets had not been isolated.

## Work

- Added a fail-closed RDMA preflight decision before target construction, with
  stable refusal reasons from the existing capability facts.
- Closed replication listener, replication volume, durable provider, status
  server, and host state when RDMA preflight or target startup fails.
- Added one live gate covering partial configfs/NBD rollback, 4 KiB and 1 MiB
  I/O, FUA, flush, durable restart/reconnect, two-target isolation, repeated
  connect/disconnect, capability honesty, refusal, and cleanup.
- Made the multi-target check wait for and verify each namespace's independent
  32 MiB block-device geometry before issuing I/O.
- Kept the path standalone: no master/CSI publication and no performance, HA,
  broad compatibility, or production SLO claim.

## Evidence

TestOps run: `20260718-015204-af29`, 24/24 actions PASS.

```text
phase164_nvme_rdma_standalone_hardening_status=ok
startup_rollback_verified=true
small_and_large_io_verified=true
flush_and_fua_verified=true
durable_restart_reconnect_verified=true
multi_target_isolation_verified=true
bounded_connect_churn_verified=true
negative_preflight_refusal_verified=true
port_conflict_refusal_verified=true
tcp_behavior_unchanged=true
rdma_not_published_to_csi=true
performance_slo_claim_allowed=false
cleanup_status=ok
next_recommendation=phase165_nvme_rdma_kubernetes_publish_attach
```

The two concurrent targets used distinct NQNs, RDMA ports, host namespaces,
and NBD devices. Data written to one volume did not appear in the other. After
the gate, target configfs objects, NBD devices, host controllers, and product
processes returned to baseline.

## Conclusion

The Linux standalone NVMe/RDMA path now has a bounded lifecycle and durable
correctness gate. Phase 165 may integrate this path with Kubernetes, but must
carry the transport explicitly through publication and CSI attach rather than
inferring RDMA from an address or replacing the existing NVMe/TCP default.
