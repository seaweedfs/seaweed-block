# iSCSI / Frontend V2 Coverage Gap Audit

Date: 2026-05-04
Branch: `iscsi-p2/os-initiator-stability`
Scope: compare V2 `weed/storage/blockvol` frontend coverage against V3 `seaweed-block` before adding more iSCSI-P2 product code.

## Goal

Do not port V2 code blindly. Use V2's test suite as a coverage inventory and decide which missing V3 tests would have prevented recent debug loops, especially the iSCSI OS-initiator large I/O issues.

## Source Inventory

V2 paths reviewed:

- `weed/storage/blockvol/iscsi/*_test.go`
- `weed/storage/blockvol/nvme/*_test.go`
- `weed/storage/blockvol/csi/*_test.go`
- `weed/storage/blockvol/test/component/*`
- `weed/storage/blockvol/testrunner/scenarios/*`

V3 paths compared:

- `core/frontend/iscsi/*_test.go`
- `core/frontend/nvme/*_test.go`
- `core/csi/*_test.go`
- `cmd/blockcsi/*_test.go`
- `cmd/blockvolume/*_test.go`
- `internal/docs/iscsi-os-initiator-compat-plan.md`

## What V3 Already Covers Well

### iSCSI protocol basics

V3 has good coverage for:

- PDU framing, AHS, data segment size, truncation.
- Login direct-to-FFP and staged negotiation.
- Discovery session basics and SendTargets.
- Basic SCSI READ/WRITE, stale backend, closed backend, unknown opcode.
- INQUIRY, VPD 0x80/0x83, READ CAPACITY 10/16, MODE SENSE, SYNC CACHE.
- Endpoint version / stale lineage fail-closed.

Representative V3 tests:

- `core/frontend/iscsi/pdu_test.go`
- `core/frontend/iscsi/login_test.go`
- `core/frontend/iscsi/discovery_test.go`
- `core/frontend/iscsi/scsi_test.go`
- `core/frontend/iscsi/scsi_batch10_5_test.go`
- `core/frontend/iscsi/t2_route_iscsi_test.go`

### Recent OS-initiator compatibility fixes

V3 now has focused P1 coverage for the bug class that blocked the demo:

- Large WRITE R2T path.
- Data-Out collector checks DataSN, BufferOffset, TTT, overflow, premature final.
- Bounded pending queue while collecting Data-Out.
- Data-Out timeout.
- Large READ split into bounded Data-In PDUs.
- Residual count plus underflow/overflow direction flags.

Representative V3 tests:

- `core/frontend/iscsi/t2_large_write_test.go`
- `core/frontend/iscsi/dataout_test.go`
- `core/frontend/iscsi/p1_os_initiator_test.go`
- `core/frontend/iscsi/p1_large_read_test.go`
- `core/frontend/iscsi/datain_test.go`

## High-Value Gaps From V2

### Gap 1: iSCSI RX/TX session stress

V2 has a dedicated `qa_rxtx_test.go` suite covering:

- Response-channel backpressure.
- Double session close.
- Tx-loop goroutine leak.
- 50 concurrent sessions.
- SCSI command during Data-Out.
- NOP-Out during Data-Out and response ordering.
- StatSN monotonicity and error-response StatSN.
- Target close while I/O is active.
- New session after target close.

V3 has partial coverage:

- SCSI command during Data-Out is covered in `TestP1_ISCSI_R2TDataOut_AllowsPipelinedCommand`.
- Pending queue bounded is covered.
- Data-Out timeout is covered.

V3 still lacks a compact RX/TX stress pack:

- 50 concurrent sessions.
- target close during active I/O.
- double close no panic.
- StatSN after error response.
- NOP-Out pending ordering.
- goroutine leak budget across many connect/login/logout cycles.

Priority: **P2 high**. These are low-cost in-process tests and can catch correctness regressions before hardware.

### Gap 2: large-write memory pressure and slow backend

V2 has `large_write_mem_test.go`:

- `TestLargeWriteMemory_4MB`: 10 x 4 MiB writes through a real iSCSI session and heap growth check.
- `TestLargeWriteMemory_SlowDevice`: slow `WriteAt` simulates WAL/admission pressure and checks buffers do not accumulate.

V3 has large WRITE protocol tests but not an equivalent memory/slow-backend guard.

Priority: **P2 high**. This directly matches the demo failure class. Add a V3 test with a fake backend that sleeps and tracks heap/goroutines.

### Gap 3: product-backed iSCSI stability

V2 `qa_stability_test.go` covers iSCSI over a real block volume:

- sustained 1000 writes.
- write/read under WAL pressure.
- SYNC CACHE under pressure.
- multiple sessions sharing one volume.
- crash recovery via iSCSI.
- session reconnect.
- rapid open/close target.
- extreme valid config values.

V3 has product-level K8s/CSI smoke and blockvolume L2 tests, but not this exact "frontend over durable backend under pressure" pack.

Priority: **P2/P3 medium-high**. This should be adapted to V3 durable backend semantics, not copied mechanically from V2.

### Gap 4: CHAP

V2 has `auth_test.go` and `qa_chap_test.go`:

- CHAP config validation.
- challenge/response state machine.
- wrong username/password.
- replayed challenge rejected.
- login integration with missing CHAP_N / CHAP_R.

V3 does not implement CHAP.

Priority: **iSCSI-P4**, not P2. Security is product-important but should not block basic single-path protocol correctness.

### Gap 5: ALUA / MPIO foundation

V2 has `alua.go` and `qa_alua_test.go`:

- standby metadata commands allowed.
- unavailable/transitioning reject writes.
- active allows all ops.
- state change mid-stream fences the next write.
- VPD 0x83 / REPORT TARGET PORT GROUPS.
- concurrent state reads and standby reject.

V3 does not implement ALUA.

Priority: **iSCSI-P6**. This belongs after the HA/frontend fact model is stable. Do not mix it into P2.

### Gap 6: CSI node lifecycle hardening

V2 CSI tests are much broader than V3:

- NodeStage/NodeUnstage idempotency.
- login failure cleanup.
- mkfs failure cleanup.
- unstage retry preserves staged entry.
- restart fallback using transport files.
- expand behavior.
- wrong volume at staging path.
- concurrent stage/unstage.
- NVMe vs iSCSI transport stickiness.

V3 has the alpha K8s dynamic PVC path and some controller/node basics, but not the full adversarial node lifecycle matrix.

Priority: **K8s/CSI-P2**, separate from iSCSI-P2. These tests matter before public alpha hardening.

### Gap 7: NVMe QA breadth

V2 NVMe has extensive coverage:

- wire truncation and malformed PDUs.
- connect sequencing.
- admin / IO queue separation.
- large read chunking.
- ANA behavior.
- concurrent IO.
- reconnect/disconnect loops.
- buffer pool safety.
- WAL pressure classification.

V3 NVMe has a solid initial port but less breadth.

Priority: **NVMe-P2+**. Do not block iSCSI-P2, but use the same audit method before claiming NVMe product readiness.

## First V3 Tests To Add Before More Product Code

### Test Slice A: iSCSI RX/TX session stability pack

Add a V3 test file, likely:

- `core/frontend/iscsi/p2_rxtx_stability_test.go`

Initial tests:

- `TestP2_ISCSI_RapidLoginLogout_NoGoroutineLeak`
- `TestP2_ISCSI_ConcurrentSessions50_WriteRead`
- `TestP2_ISCSI_TargetCloseDuringActiveIO_ExitsCleanly`
- `TestP2_ISCSI_NopDuringDataOut_DrainsAfterWrite`
- `TestP2_ISCSI_ErrorResponseAdvancesStatSN`

Rationale: this is cheap, local, and directly protects the new pending-queue/Data-Out/Data-In state machine.

### Test Slice B: large write memory / slow backend

Add:

- `TestP2_ISCSI_LargeWrite4MiB_DoesNotGrowHeapUnbounded`
- `TestP2_ISCSI_LargeWrite_SlowBackend_DoesNotAccumulateBuffers`

Keep thresholds loose enough for CI, but strict enough to catch unbounded buffering.

Rationale: this would likely have exposed the original large WRITE issue before the Windows/Linux demo attempt.

### Test Slice C: real OS initiator harness

Add a harness, not necessarily a normal Go unit test:

```text
Linux iscsiadm -> 256MiB/1GiB target -> mkfs.ext4 -> mount -> fio/dd read/write -> sha256 verify -> logout -> no sessions
```

Rationale: in-process clients do not perfectly model Linux kernel iSCSI behavior. P1 already proved this.

## What Not To Do Now

- Do not port V2 CHAP/ALUA into P2.
- Do not claim MPIO or mounted failover.
- Do not copy V2 blockvol internals into V3 storage/authority layers.
- Do not expand K8s operator cleanup while iSCSI single-path filesystem stability is still under verification.

## Recommended Order

1. Add Test Slice A: RX/TX session stability pack.
2. Add Test Slice B: large-write memory / slow backend.
3. Add Test Slice C: real OS initiator harness and run on M02.
4. Only then raise K8s alpha PVC size from toy/smoke scale to 256MiB/1GiB.
5. Track CHAP as iSCSI-P4, ALUA/MPIO as iSCSI-P6.

## Decision

Proceed with **tests first** on `iscsi-p2/os-initiator-stability`.

The next implementation PR should not be a protocol refactor unless one of the P2 tests turns red and points to a concrete state-machine gap.
