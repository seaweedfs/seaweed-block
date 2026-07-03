# Current Plan: Phase 121 Data-Plane Address Capability

Status: planning and implementation.

Phase 120 proved the default Kubernetes NVMe/TCP path and collected a useful
management-LAN baseline, but it used `kubectl get nodes -o wide` InternalIP
addresses such as `192.168.1.181:4420`. That is correct for functional
cross-node attach, but it is not the right source of truth for a performance
baseline on the 100GbE/RoCE fabric.

Rust volume already uses the better pattern:

```text
[rdma]
enabled = true
ip = "<RoCE/data-plane IP>"
port = 18516

PrepareRdmaRead/Write -> returns ok, rdma_ip, rdma_port, lease/capability
```

Seaweed Block should mirror that model before any more performance or RDMA
claims: the data-plane address must be explicit, queryable, and surfaced as
evidence.

## Why This Phase Exists

There are three distinct network facts that must not be mixed:

```text
Kubernetes node InternalIP: 192.168.1.x
  -> management/LAN path
  -> valid for functional attach gates

100GbE TCP data-plane IP: usually 10.x.x.x in this lab
  -> valid for NVMe/TCP performance baseline
  -> still not RoCE/NVMe-RDMA

RoCE/NVMe-RDMA endpoint
  -> different protocol path
  -> requires explicit RDMA support/capability
```

Phase 121 makes Block express these facts explicitly instead of inferring them
from Kubernetes InternalIP.

## Target Model

Each block node needs separate address fields:

```text
kubernetesNode: m01
managementIP: 192.168.1.181
frontendIP: <data-plane TCP IP for NVMe/TCP, optional>
rdmaIP: <RoCE/RDMA IP, optional>
networkClass: management_lan | 100gbe_tcp | roce
```

The runtime/status surface should expose capability evidence in the same spirit
as Rust volume's `PrepareRdma*` responses:

```text
nvme_tcp_supported=true
nvme_tcp_ip=<ip>
nvme_tcp_port=4420
nvme_tcp_network_class=100gbe_tcp
nvme_rdma_supported=false
roce_ip=<ip-if-configured-or-empty>
rdma_capability_source=config|probe|absent
```

For now, `nvme_rdma_supported=false` remains the correct Block answer.

## Deliverables

1. Add a values/config path that can override the frontend/data-plane IP per
   Kubernetes node without abusing `internalIP`.

2. Add status/report evidence that identifies the selected frontend network:

   ```text
   publish_target=<ip>:4420
   publish_target_network_class=100gbe_tcp
   publish_target_source=configured_data_plane
   management_ip=192.168.1.x
   ```

3. Add a TestOps gate that refuses to call a run a "performance baseline" unless
   the publish target is on the configured data-plane network and route/interface
   evidence agrees.

4. Keep the protocol boundary explicit:

   ```text
   frontend_transport=tcp
   nvme_rdma_claim_allowed=false
   roce_claim_allowed=false
   performance_slo_claim_allowed=false
   ```

## Verification

Local/source checks:

```powershell
go test ./cmd/sw-block ./cmd/blockvolume ./core/launcher
C:\work\swblock.exe validate <phase121-scenario>
```

Live gate should record:

```text
phase121_data_plane_address_capability_status=ok
management_ip=192.168.1.x
publish_target=<100G-IP>:4420
publish_target_network_class=100gbe_tcp
publish_target_source=configured_data_plane
frontend_transport=tcp
nvme_rdma_supported=false
roce_claim_allowed=false
cleanup_status=ok
```

Only after this passes should a new high-speed NVMe/TCP baseline run and replace
the Phase 120 LAN numbers for performance discussion.

## Exit Criteria

Phase 121 can close when:

- values/config can carry a per-node data-plane frontend IP;
- generated/rendered blockvolume frontends use that data-plane IP;
- status/report/QA evidence distinguishes management IP from data-plane IP;
- the gate refuses to label `192.168.1.x` as a performance baseline network;
- cleanup remains zero-residue.

## Non-Claims

Phase 121 does not implement NVMe/RDMA, RoCE I/O, GPU/cuObject, NIXL production
support, performance SLOs, broad host compatibility, or published-image support.
