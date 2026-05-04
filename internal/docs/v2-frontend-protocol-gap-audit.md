# V2 Frontend Protocol Gap Audit

This audit tracks the gap between the mature V2 block frontend code and the V3
frontend code in `seaweed-block`.

The purpose is not to wholesale-port V2. V3 has a different architecture:

```text
Kubernetes / CSI
  -> blockmaster control plane
  -> frontend protocol adapter
  -> frontend.Backend
  -> walstore / recovery / authority
```

The purpose is to identify protocol-executor behavior that was already learned
in V2 and must not be accidentally lost in V3.

## Current Finding

V3 can pass small K8s alpha writes, but a real OS initiator can still expose
protocol gaps.

The recent iSCSI large-write work changed the failure mode:

```text
Before:
  large WRITE rejected directly
  hostbyte=DID_BAD_TARGET

After first fix:
  large WRITE enters R2T path
  session can still break mid-transfer
  hostbyte=DID_TRANSPORT_DISRUPTED
  server log: expected Data-Out, got SCSI-Command
```

This means the first fix was directionally correct but incomplete. The likely
remaining issue is that the V3 iSCSI session does not yet model the full kernel
initiator state machine, especially command pipelining during Data-Out/R2T.

## iSCSI Gap Table

| Area | V2 Shape | V3 Shape | Product Risk | Priority |
|---|---|---|---|---|
| Data-Out collection | `DataOutCollector` in `iscsi/dataio.go`, reusable and tested. | Inlined inside `Session.collectWriteData`. | Harder to reason about R2T/F-bit/DataSN edge cases. | P0 |
| R2T chunking | Mature Data-Out path with large-write memory tests. | First MaxBurst chunking fix exists, still failing kernel mkfs. | Windows/Linux format can fail. | P0 |
| Pending queue during Data-Out | V2 queues non-Data-Out PDUs while collecting Data-Out. | V3 currently expects Data-Out and errors on SCSI-Command. | Kernel initiator can pipeline next command and break session. | P0 |
| CmdSN window | V2 tracks `expCmdSN` / `maxCmdSN`. | V3 mostly replies with `req.CmdSN()+1`, no full window. | Real initiator pipelining/order behavior under load may fail. | P1 |
| TX loop / StatSN ownership | V2 has `txLoop` assigning StatSN by PDU type. | V3 sends directly from serial loop. | Simpler but brittle as soon as multiple Data-In/R2T/status PDUs interact. | P1 |
| Data-Out timeout | V2 has `DataOutTimeout`. | V3 has no explicit Data-Out deadline. | Broken initiator can pin session goroutine. | P1 |
| Multi Data-In reads | V2 `DataInWriter` splits large reads by max segment. | V3 still sends one Data-In response. | Large reads can exceed negotiated/real initiator limits. | P1 |
| CHAP | V2 has CHAP. | V3 has none. | External iSCSI demo is unauthenticated. | P2 / security |
| ALUA | V2 has ALUA code/tests. | V3 does not expose ALUA. | iSCSI multipath/failover story weak. | P2 |
| QA coverage | V2 has smoke, stability, session storm, memory tests. | V3 has good component tests but fewer OS-initiator tests. | Go test client can miss kernel behavior. | P0 |

## NVMe Gap Table

| Area | V2 Shape | V3 Shape | Product Risk | Priority |
|---|---|---|---|---|
| ANA | V2 has ANA provider, ANA log page, ANA state QA. | V3 intentionally zeros ANA identify fields in current tests. | NVMe multipath/failover cannot be claimed yet. | P1/P2 |
| Write retry | V2 has `writeWithRetry`. | V3 maps backend errors but does not carry same retry policy. | WAL pressure can surface as frontend write failures. | P1 |
| WAL pressure throttle | V2 has throttle helpers/tests. | V3 has flow-control diagnostics, less frontend enforcement. | High write pressure behavior unclear. | P1 |
| Multi IO queue stress | V2 has broad QA around queues. | V3 has useful queue tests but product path not demoed. | NVMe may work in tests but not in kernel/fio path. | P1 |
| K8s CSI path | V2 had lower-level utilities. | V3 CSI product path currently iSCSI-only. | NVMe is not a current product promise. | P2 |

## What Not To Port

Do not port these blindly:

- V2 authority model,
- V2 promote/demote shortcuts,
- heartbeat-as-ready assumptions,
- targetLSN-style completion facts,
- any test helper that mutates authority directly from protocol code.

These conflict with the V3 architecture boundaries:

```text
placement intent != authority
authority moved != data continuity
frontend ready != replica ready
protocol progress != recovery completion
```

## Recommended Work Plan

### Step 1: Freeze Product Code Changes For Protocol Until Gap Is Pinned

Do not keep patching iSCSI directly from log guesses. First add or run a real
OS-initiator reproduction:

```text
Linux iscsiadm login
256 MiB target
mkfs.ext4
write/read small file
logout
no dangling session
```

Windows format can be a second reproduction, but Linux is faster for iteration.

### Step 2: Rebuild iSCSI Executor Semantics From V2, Not V2 Control Plane

Candidate order:

1. Extract a V3 `DataOutCollector` equivalent.
2. Add pending queue for non-Data-Out PDUs during Data-Out collection.
3. Add Data-Out timeout.
4. Add DataInWriter for large reads.
5. Add CmdSN window tests before enabling broader pipelining.

Each step needs a red test that captures an OS initiator behavior, not only a
handwritten happy-path client.

### Step 3: Keep iSCSI As The Product Frontend Until Stable

Do not switch the product story to NVMe to avoid iSCSI bugs. NVMe is important,
but it introduces another OS/kernel path and another multipath story.

Current product order:

```text
1. iSCSI mkfs/format correctness
2. K8s dynamic PVC durability
3. multi-node attach
4. failover + reattach
5. NVMe-oF feature gate
```

### Step 4: Treat V2 NVMe As A Reference, Not A Drop-In

When NVMe becomes active product work, compare V2 and V3 around:

- ANA identify/log page,
- IO queue count,
- R2T/H2C collection,
- write retry and WAL pressure,
- fio/nvme-cli scenarios.

But keep authority and readiness decisions in V3 control-plane code.

## Immediate Open Questions

1. Should the already-pushed iSCSI MaxBurst fix stay on `main`, or be reverted
   and reintroduced through a PR?
2. Should the external iSCSI demo flag in `cmd/blockvolume/main.go` become a
   formal demo-only PR, or be dropped until authentication exists?
3. Do we want a dedicated `frontend-compat` milestone before more K8s feature
   work?

## Decision

Recommended: yes, create a `frontend-compat` milestone.

Rationale: K8s demo success with tiny writes is not enough. A storage product
must survive real OS initiators doing filesystem operations. This is now the
highest-priority product hardening track before broader demos.

