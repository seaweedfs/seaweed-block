# Current Plan: Phase 103 NVMe Multi-Host / RoCE Preflight

Status: active plan.

## Why This Is Next

Phase 100 proved Kubernetes CSI NVMe/TCP multipath attach for the supported lab
path. Phase 101 hardened that path with path-loss status, repeated
stage/unstage cleanup, and bounded writer/reader soak. Phase 102 added a
release-artifact smoke gate, but that gate remains blocked until matching
published images exist.

The next storage-feature question is not performance yet. It is whether the
product can tell an operator, from live host facts, what NVMe transport/topology
is actually supportable:

```text
NVMe/TCP multipath on current lab -> supported-lab path
RoCE / NVMe-RDMA -> explicit preflight only until hardware + live I/O gate exist
multi-host NVMe -> requires non-loopback, host-capability, and topology evidence
```

Without this preflight, RoCE or multi-host NVMe claims would be another
documentation-level promise instead of a product capability.

## Product Goal

Add a read-only transport preflight gate that classifies the current host:

- `nvme` CLI available;
- NVMe subsystem inspection is readable;
- `nvme-fabrics`, `nvme-tcp`, and `nvme-rdma` module loaded/available state;
- RDMA device count from `/sys/class/infiniband`;
- NVMe/TCP host capability;
- RoCE preflight candidacy.

The gate must fail closed:

- missing `nvme` CLI -> blocked, not product failure;
- missing NVMe/TCP capability -> blocked, not product failure;
- no RDMA device or no `nvme-rdma` capability -> not a RoCE candidate;
- RDMA device plus `nvme-rdma` capability -> candidate only, still
  `roce_claim_allowed=false` until a live RoCE I/O gate passes;
- no RoCE live I/O or performance claim may be emitted.

## D1: Gate Scaffold

Status: implemented.

Files:

```text
scripts/run-phase103-nvme-multihost-roce-preflight-gate.sh
testops/scenarios/nvme-multihost-roce-preflight-chain.yaml
```

The script is intentionally read-only. It reads command availability, `/proc`,
`/sys/module`, `/lib/modules`, `/sys/class/infiniband`, and `nvme list-subsys`.
It must not run `modprobe`, `nvme connect`, `nvme disconnect`, or any
Kubernetes mutation.

## D2: Local Contract Validation

Required checks:

```text
bash -n scripts/run-phase103-nvme-multihost-roce-preflight-gate.sh
C:\work\swblock.exe validate testops/scenarios/nvme-multihost-roce-preflight-chain.yaml
go test ./scripts ./internal/testops ./core/frontend/nvme ./core/ops ./cmd/sw-block -count=1
```

Success means the preflight is syntactically valid, scenario-runnable, and
covered by a read-only/claim-bounded script regression.

## D3: Live Host Preflight

Run on the NVMe-capable lab node:

```text
swblock run testops/scenarios/nvme-multihost-roce-preflight-chain.yaml
```

Required PASS evidence:

```text
phase103_nvme_multihost_roce_preflight_status=ok
read_only=true
nvme_cli_present=true
nvme_tcp_preflight_ready=true
roce_live_io_claim=false
performance_claim_allowed=false
```

If the lab lacks RDMA hardware, the expected result is still PASS as long as it
reports:

```text
rdma_device_count=0
roce_preflight_status=blocked_no_rdma_device
roce_preflight_candidate=false
roce_claim_allowed=false
```

If the lab has RDMA hardware and `nvme-rdma`, it may instead report:

```text
roce_preflight_status=candidate_requires_live_roce_gate
roce_preflight_candidate=true
roce_claim_allowed=false
roce_live_io_claim=false
```

That is the correct product behavior: honest candidate status, not a released
RoCE claim.

## D4: Next Decision After Preflight

Only after D3 should the team choose one of:

- RoCE live I/O gate on hardware that has RDMA devices and `nvme-rdma`;
- multi-host NVMe/TCP non-loopback topology gate;
- NVMe performance characterization.

These must stay separate because they prove different things: transport
availability, topology correctness, and performance/SLO.

## Non-Claims

Phase 103 does not claim:

- RoCE or NVMe/RDMA I/O works;
- multi-host failover works;
- performance, latency, throughput, or SLO;
- broad distro/kernel compatibility;
- production HA;
- release-image validation for NVMe.
