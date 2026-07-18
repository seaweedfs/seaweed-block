# NVMe/RDMA Capability Boundary

Status: **source-gated standalone supported-lab path**. Seaweed Block can serve
one explicitly configured Linux NVMe/RDMA target in the supported RoCE lab.
Kubernetes CSI publication/attach, multipath/failover, broad compatibility,
performance improvement, and production SLOs remain non-claims.

## Claim Matrix

| Capability | Current status |
| --- | --- |
| NVMe/TCP Kubernetes CSI path | Source-gated supported lab |
| NVMe/RDMA standalone Linux target | Source-gated supported lab, Phases 163-164 |
| NVMe/RDMA Kubernetes CSI publish/attach | Not implemented |
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

## Why This Is Not Yet A Kubernetes Claim

The standalone target is deliberately not advertised to blockmaster or CSI.
The existing publish-target path remains NVMe/TCP-only. Therefore Phases
163-164 do not prove:

- RDMA addresses in CSI publish context;
- CSI NodeStage/NodeUnstage using `nvme connect -t rdma`;
- mounted application I/O through a dynamic PVC;
- reconnect after target or node movement;
- RDMA multipath, ANA, or failover behavior;
- CRD/report/dashboard/explain agreement for RDMA target health;
- Kubernetes delete/uninstall cleanup of RDMA host state.

Silently advertising the standalone endpoint as a Kubernetes target before
those ownership and cleanup paths exist would create an unsafe partial claim.

## Next Gates

### Phase 165: Kubernetes Publish/Attach

The next gate carries an explicit RDMA transport through frontend publication,
connects from CSI-node, mounts an application PVC, exposes status, and proves
delete/uninstall cleanup while preserving NVMe/TCP as the default.

### Later: Multipath, Failover, And Performance

RDMA multipath/failover requires its own authority, ANA, reconnect, and mounted
I/O gates. Performance comparison must use the same backend, durability,
request shape, and host topology for TCP and RDMA. Until then, the NBD bridge
must not be described as an acceleration path or used for an SLO claim.

## Related External RDMA Work

`C:\work\rdma\seaweed-mono-rdma-refresh` and `C:\work\rdma\sra-next` provide
useful VFS/object/RustVolume/RDMA/NIXL evidence. They do not implement the
standard Linux NVMe/RDMA target used here and remain separate product surfaces.
