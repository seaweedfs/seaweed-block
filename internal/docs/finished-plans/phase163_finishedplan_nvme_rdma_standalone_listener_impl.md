# Phase 163 Finished Plan: NVMe/RDMA Standalone Listener Implementation

Status: **closed 2026-07-18, live TestOps gate PASS**.

## Problem

Phases 157-162 had capability facts, a transport seam, preflight, and a
disabled start decision, but no Seaweed Block target accepted a standard Linux
`nvme connect -t rdma`. Host RDMA capability alone could not establish a
product data-path claim.

## Design Decision

The implementation uses the standard Linux NVMe/RDMA protocol stack rather
than reproducing NVMe-oF/RDMA queue-pair semantics in Go:

```text
Linux nvme-rdma initiator
-> kernel nvmet-rdma target and namespace
-> /dev/nbdX
-> product-owned NBD request bridge
-> Seaweed Block frontend.Backend
```

The kernel owns NVMe/RDMA protocol compliance. The NBD bridge owns translation
of read, write, flush, FUA, and disconnect into the existing Seaweed backend.
The path is Linux-only and requires root, configfs, `nvmet-rdma`, `nbd`, an
RDMA device, and a non-loopback data-plane address.

## Work

- Added a bounded NBD protocol bridge and Linux NBD device lifecycle.
- Added a Linux NVMe/RDMA target lifecycle over configfs and `nvmet-rdma`.
- Wired explicit `--nvme-transport=rdma` startup into `blockvolume`.
- Kept RDMA targets out of master/CSI publication.
- Made frontend capability status report the actually started RDMA listener.
- Added unit coverage for protocol mapping, bounds, and stream alignment.
- Added a live TestOps gate using the m02 `10.0.0.3` RoCE address and a standard
  m01 Linux NVMe/RDMA initiator.

## Evidence

Run: `20260718-011316-dc5b`, 16/16 actions PASS.

```text
phase163_nvme_rdma_standalone_listener_impl_spike_status=ok
rdma_implementation_path=kernel_nvmet_rdma_nbd_bridge
rdma_bind_ip=10.0.0.3
rdma_device=rocep1s0
rdma_netdev=enp1s0np0
linux_nvme_connect_rdma_succeeded=true
standalone_write_read_verified=true
flush_verified=true
seaweed_backend_write_observed=true
capability_endpoint_reports_rdma_supported=true
rdma_listener_started=true
rdma_not_published_to_csi=true
performance_slo_claim_allowed=false
disconnect_cleanup_status=ok
cleanup_status=ok
phase163_decision=standalone_nvme_rdma_live_io_supported
next_recommendation=phase164_nvme_rdma_standalone_hardening
```

## Conclusion

Seaweed Block now has a real, source-gated standalone NVMe/RDMA data path in
the supported Linux lab. This is not yet a Kubernetes publish/attach,
multi-path/failover, broad compatibility, performance, or production claim.
Phase 164 hardens the standalone lifecycle before Kubernetes integration.
