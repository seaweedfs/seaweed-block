# NVMe/RDMA Capability Boundary

Status: **source-gated standalone and Kubernetes single-path supported-lab
path**. Seaweed Block can serve one explicitly selected NVMe/RDMA target and
mount it through CSI in the supported RoCE lab. Multipath/failover, broad
compatibility, performance improvement, and production SLOs remain non-claims.

## Claim Matrix

| Capability | Current status |
| --- | --- |
| NVMe/TCP Kubernetes CSI path | Source-gated supported lab |
| NVMe/RDMA standalone Linux target | Source-gated supported lab, Phases 163-164 |
| NVMe/RDMA Kubernetes CSI publish/attach | Source-gated supported lab, Phase 165 |
| NVMe/RDMA multipath or failover | Not implemented |
| Broad kernel, NIC, distro, or initiator compatibility | Not claimed |
| RDMA acceleration or performance SLO | Not claimed |

## Implemented Standalone Path

```text
Linux nvme-rdma initiator on m01
-> RoCE data-plane address 10.0.0.3 on m02
-> Linux kernel nvmet-rdma target
-> configfs namespace backed by /dev/nbdX
-> Seaweed Block NBD request bridge
-> frontend.Backend Read/Write/Sync
-> durable volume implementation
```

The kernel owns NVMe-oF/RDMA protocol handling, queue pairs, memory
registration, and namespace presentation. Seaweed Block owns NBD request
translation and the target lifecycle that creates and removes NBD/configfs
state. This avoids the incorrect design of sending NVMe/TCP PDUs over an RDMA
socket and avoids implementing an NVMe/RDMA target protocol stack in Go.

The bridge maps:

- NBD read to `frontend.Backend.Read`;
- NBD write to `frontend.Backend.Write`;
- NBD flush and FUA completion to `frontend.Backend.Sync`;
- disconnect to bounded bridge shutdown.

## Activation And Prerequisites

The path is explicit:

```text
blockvolume \
  --nvme-listen <roce-ip>:<port> \
  --nvme-transport rdma \
  --allow-external-nvme-bind \
  --nvme-subsysnqn <nqn> \
  --nvme-ns <nsid>
```

The current implementation is Linux-only and requires:

- root privileges for NBD ioctls and configfs;
- `nbd` and `nvmet-rdma` kernel modules available;
- `/sys/kernel/config/nvmet` mounted and writable;
- an RDMA device and a non-loopback IP assigned to its data-plane netdev;
- the standard Linux `nvme-rdma` initiator on the client;
- a unique NQN, namespace ID, and target port.

The frontend capability endpoint reports RDMA as supported and started only
after the target has started:

```text
GET /status/frontend-capabilities?volume=<id>
```

The same endpoint carries module, RDMA-device, bind-address, implementation,
start-allowed, and listener-started facts. Missing prerequisites must fail
closed rather than create a false supported/listening state.

## Phase 163 Live Evidence

TestOps run `20260718-011316-dc5b` passed 16/16 actions:

```text
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
disconnect_cleanup_status=ok
cleanup_status=ok
phase163_decision=standalone_nvme_rdma_live_io_supported
```

The initiator wrote 4 KiB, issued `nvme flush`, read the namespace back, and
verified the checksum. Independently, the blockvolume log recorded
`durable: write observer dispatch lba=0 lsn=<value>` after persistence, proving
the request reached the Seaweed backend rather than stopping at a kernel-only
control target.

## Phase 164 Hardening Evidence

TestOps run `20260718-015204-af29` passed 24/24 actions. It added partial-start
rollback, live RDMA-port conflict isolation, aligned 4 KiB and 1 MiB I/O,
FUA/flush evidence, durable restart/reconnect, two simultaneous targets,
bounded connect churn, stable refusal, and zero-residue cleanup.

```text
phase164_nvme_rdma_standalone_hardening_status=ok
startup_rollback_verified=true
port_conflict_refusal_verified=true
small_and_large_io_verified=true
flush_and_fua_verified=true
durable_restart_reconnect_verified=true
multi_target_isolation_verified=true
bounded_connect_churn_verified=true
negative_preflight_refusal_verified=true
cleanup_status=ok
```

## Phase 165 Kubernetes Evidence

TestOps run `20260718-025048-9d6a` passed 14/14 actions with fresh matching
product and CSI images. A dynamic PVC selected `nvmeTransport: rdma`, the target
published `10.0.0.3:4420`, CSI mounted it on m01, and writer/reader verification
passed.

```text
phase165_nvme_rdma_k8s_publish_attach_status=ok
csi_publish_context_transport=rdma
active_host_controller_transport=rdma
active_host_controller_traddr=10.0.0.3
swblockvolume_status_transport=rdma
writer_verified=true
reader_verified=true
tcp_fallback_observed=false
target_configfs_residue_count=0
target_nbd_residue_count=0
app_nvme_controller_residue_count=0
kubernetes_product_residue_count=0
cleanup_status=ok
```

The transport is explicit in lifecycle RPCs, frontend facts, CSI publish
context, CRD status, and the host connect command. Empty transport on older
NVMe records remains TCP for compatibility; RDMA never silently falls back to
TCP. Only RDMA target Pods receive the required privileged host device,
configfs, and module mounts.

Phase 165 proves:

- RDMA addresses in CSI publish context;
- CSI NodeStage/NodeUnstage using `nvme connect -t rdma`;
- mounted application I/O through a dynamic PVC;
- CRD status and live host-controller agreement on RDMA transport;
- Kubernetes delete/uninstall cleanup of the exact RDMA controller, target,
  configfs namespace, NBD device, and product resources.

It does not prove reconnect after target or node movement, RDMA multipath, ANA
or failover behavior, performance improvement, or broad compatibility.

## Next Gates

### Next: Reconnect, Failover, And Performance

RDMA multipath/failover requires its own authority, ANA, reconnect, and mounted
I/O gates. Performance comparison must use the same backend, durability,
request shape, and host topology for TCP and RDMA. Until then, the NBD bridge
must not be described as an acceleration path or used for an SLO claim.

## Related External RDMA Work

`C:\work\rdma\seaweed-mono-rdma-refresh` and `C:\work\rdma\sra-next` provide
useful VFS/object/RustVolume/RDMA/NIXL evidence. They do not implement the
standard Linux NVMe/RDMA target used here and remain separate product surfaces.
