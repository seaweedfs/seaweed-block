# Phase 159 Design: NVMe/RDMA Standalone Listener Gate

Status: design gate. This is not an implementation claim.

## Problem

Seaweed Block can now report its current frontend capability boundary from the
volume process: NVMe/TCP is supported and NVMe/RDMA is unsupported. The next
risk is implementing something that looks like an RDMA listener but is still
the NVMe/TCP byte-stream protocol underneath.

That would be worse than no RDMA support because Linux `nvme connect -t rdma`
would not exercise the same wire shape as `nvme connect -t tcp`. Phase 159
therefore defines the standalone RDMA listener boundary before code starts.

## Current Code Seams

The current NVMe frontend is TCP-first:

- `core/frontend/nvme/transport.go` defines `TransportTCP`,
  `TransportRDMA`, `ErrTransportUnsupported`, and `ListenerFactory`.
- `core/frontend/nvme/target.go` selects the listener in `Target.Start()`,
  accepts `net.Conn`, then calls `newSession`.
- `core/frontend/nvme/session.go` drives the NVMe session lifecycle.
- `core/frontend/nvme/wire.go` reads and writes NVMe/TCP PDUs.
- `core/frontend/nvme/io.go`, `identify.go`, `fabric.go`, and `admin_log.go`
  contain command handling that should remain reusable.
- `cmd/blockvolume/main.go` wires flags, target creation, frontend target
  publication, and Phase 158 capability status.
- `core/host/volume/status_server.go` exposes
  `/status/frontend-capabilities`.

`ListenerFactory` is a useful transport-selection seam, but it is not enough
for real RDMA. The current session path expects NVMe/TCP stream framing. A real
RDMA implementation needs either a protocol-neutral session core with transport
adapters or a dedicated RDMA session path that reuses the command handlers
without reusing TCP PDU framing.

## Target Scope

The first supported RDMA slice must be standalone, not Kubernetes:

```text
blockvolume RDMA listener
-> Linux host nvme connect -t rdma
-> identify/controller setup succeeds
-> write/read/flush verifies against the same backend
-> disconnect and cleanup leave no NVMe/RDMA residue
```

Kubernetes publish/attach is explicitly deferred until the standalone target
passes. Performance comparison is also deferred until correctness and cleanup
pass.

## Transport Contract

The RDMA transport must provide these product facts before it can replace the
Phase 158 unsupported status:

- `protocol=nvme`
- `transport=rdma`
- `supported=true`
- `listenerImplemented=true`
- `listenerStarted=true`
- `reason=implemented`
- bind address, port, RDMA device or GID evidence, and NQN/NSID identity
- failure reason when listener start or host connect fails

The listener must bind on the RDMA/data network address. A management LAN IP is
not acceptable evidence for RoCE performance or correctness. The standalone
gate must record the selected RDMA bind IP, device, and host-side source path.

## Required Failure Reasons

Unsupported or misconfigured RDMA must fail closed with stable reasons. Minimum
initial reasons:

- `nvme_rdma_transport_unsupported`: implementation not enabled yet.
- `nvme_rdma_module_missing`: host lacks `nvme-rdma`.
- `rdma_device_missing`: no usable `/sys/class/infiniband` device.
- `rdma_bind_address_invalid`: requested address is not an RDMA/data-network
  address for the selected device.
- `rdma_listener_start_failed`: listener could not bind or initialize.
- `nvme_rdma_connect_failed`: Linux `nvme connect -t rdma` failed.

Do not map these to Ready=True. A failed RDMA listener may fall back to the
existing typed unsupported/refusal path, but it must not silently claim TCP
success as RDMA success.

## Standalone Live I/O Gate

The first live gate must prove:

```text
phase160_or_later_nvme_rdma_standalone_live_io_status=ok
rdma_bind_ip=<100Gb/RoCE/data-plane IP>
rdma_device=<device>
rdma_listener_started=true
capability_endpoint_reports_rdma_supported=true
nvme_connect_rdma_succeeded=true
identify_succeeded=true
write_read_verified=true
flush_verified=true
disconnect_cleanup_status=ok
tcp_behavior_unchanged=true
k8s_publish_attach_claim_allowed=false
performance_slo_claim_allowed=false
```

The gate must use Linux `nvme connect -t rdma`, not a custom test client that
can accidentally speak the wrong wire protocol.

## Implementation Checklist

1. Keep `TransportRDMA` and `ErrTransportUnsupported` until live RDMA I/O
   passes.
2. Split the reusable command handlers from the NVMe/TCP PDU stream path.
3. Define an RDMA transport adapter or dedicated RDMA session path that can feed
   admin and I/O commands to the same handlers without pretending RDMA is TCP.
4. Preserve all existing NVMe/TCP unit, component, mounted, reconnect, and
   cleanup gates.
5. Update `/status/frontend-capabilities` only when the RDMA listener is
   actually implemented and started.
6. Add standalone cleanup checks for `nvme list-subsys`, controller devices,
   mounted block devices, and process/listener residue.
7. Only after standalone live I/O passes, design the Kubernetes publish context
   and CSI attach path.

## Out Of Scope

- Kubernetes publish/attach for RDMA.
- Performance or SLO comparison.
- GPU Direct, cuFile, cuObject, NIXL, or RustVolume acceleration claims.
- Treating external RDMA library evidence as a Seaweed Block NVMe/RDMA target
  proof.

## Decision

Phase 159 recommends the next implementation phase as:

```text
phase160_nvme_rdma_transport_adapter_seam
```

That phase should make the TCP wire path and reusable NVMe command/session
handlers explicit before implementing the actual RDMA listener.
