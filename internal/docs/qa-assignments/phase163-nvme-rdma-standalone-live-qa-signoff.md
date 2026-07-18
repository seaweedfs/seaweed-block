# Phase 163 QA Sign-Off: NVMe/RDMA Standalone Live I/O

Status: **PASS** on 2026-07-18.

TestOps run: `20260718-010533-1ab1`.

Local bundle: `results/20260718-010533-1ab1`.

## Verdict

The supported lab completed a standard Linux `nvme connect -t rdma` from m01
to a Seaweed Block target on m02. A 4 KiB write, NVMe flush, and checksum
readback passed. The blockvolume log independently confirmed a durable write
observer dispatch with a real LSN after the backend write; the test did not
stop at kernel target or raw block-device evidence.

## Gates

- Unit tests for NBD, NVMe/RDMA, NVMe/TCP, and blockvolume: PASS.
- m02 RDMA target bind on `10.0.0.3:11631`: PASS.
- m01 standard Linux NVMe/RDMA connect: PASS.
- Seaweed backend write/read/flush: PASS.
- Frontend capability reports implemented, supported, and started: PASS.
- RDMA remains absent from master/CSI publication: PASS.
- Existing TCP behavior checks: PASS.
- Host controller, configfs target, NBD device, and process cleanup: PASS.

The broader iSCSI process test
`TestT2Process_ISCSI_ReopenAfterMove_ServesNewLineage` remains red because its
projection does not return to Healthy within 30 seconds. The same test fails at
the unchanged `main` commit `be1ef07`; Phase 163 does not touch the iSCSI path.

## Evidence

```text
phase163_nvme_rdma_standalone_listener_impl_spike_status=ok
rdma_implementation_path=kernel_nvmet_rdma_nbd_bridge
rdma_bind_ip=10.0.0.3
rdma_device=rocep1s0
rdma_netdev=enp1s0np0
go_test_nbd_nvmerdma_nvme_blockvolume=ok
tcp_behavior_unchanged=true
rdma_not_published_to_csi=true
capability_endpoint_reports_rdma_supported=true
rdma_listener_started=true
backend_bridge_device=/dev/nbd0
linux_nvme_connect_rdma_succeeded=true
standalone_write_read_verified=true
flush_verified=true
seaweed_backend_write_observed=true
disconnect_cleanup_status=ok
cleanup_status=ok
phase163_decision=standalone_nvme_rdma_live_io_supported
```

## Boundary

This sign-off supports only the source-gated standalone Linux lab path. It does
not validate Kubernetes CSI publication/attach, multiple RDMA paths, failover,
performance improvement, broad hardware/kernel compatibility, or an SLO.
