# iSCSI OS Initiator Compatibility Plan

Status: P1-A/B/C/D/E/F implemented on `fix/iscsi-large-read-datain`

Owner track: frontend compatibility

## Goal

Make V3 iSCSI survive real OS initiators, not only in-process Go clients.

The minimum product bar is:

```text
Linux iscsiadm login
256 MiB target
mkfs.ext4 succeeds
mount succeeds
write/read byte-equal file
logout
no dangling iSCSI session
```

Windows iSCSI Initiator format is the second bar, after Linux is green.

## Current Failure

The first V3 large-write fix moved the failure forward but did not close it.

```text
Before MaxBurst chunking:
  kernel large WRITE(10) failed with DID_BAD_TARGET
  target rejected large transfer before real R2T flow

After MaxBurst chunking:
  kernel large WRITE(10) fails with DID_TRANSPORT_DISRUPTED
  target log: expected Data-Out, got SCSI-Command
```

This means V3 now enters R2T/Data-Out flow, but the session state machine is
still too linear for Linux kernel initiator behavior.

## Design Decision

Do not wholesale-port V2.

V2 has useful protocol executor semantics, but it also carries V2 authority,
device lookup, and test assumptions that conflict with V3. The right move is:

```text
Port / rebuild protocol executor semantics.
Do not port V2 control-plane semantics.
```

## V2 Semantics Worth Carrying Forward

### 1. DataOutCollector

V2 has `DataOutCollector` in `weed/storage/blockvol/iscsi/dataio.go`.

Useful semantics:

- one object owns expected transfer length,
- immediate data and solicited Data-Out share one buffer,
- DataSN is monotonic within the transfer,
- BufferOffset must match the assembled cursor,
- overflow is rejected,
- F-bit marks the end of a data sequence.

V3 currently inlines this in `Session.collectWriteData`. That makes the happy
path readable but makes kernel edge behavior harder to reason about.

Recommendation: rebuild a V3-local collector under `core/frontend/iscsi`,
adapted to V3 types and error style.

### 2. Pending Queue During Data-Out

V2 queues non-Data-Out PDUs that arrive while a write is collecting Data-Out:

```text
if got non-Data-Out while collecting:
  append to pending queue
  continue collecting Data-Out

nextPDU drains pending before reading socket again
```

V3 currently errors:

```text
expected Data-Out, got SCSI-Command
```

That matches the observed kernel failure. A Linux initiator can legally pipeline
another command while an earlier command is still in a Data-Out phase. V3 must
not tear down the session just because a SCSI-Command appears during collection.

Recommendation: add bounded pending queue before further large-write patches.

### 3. Data-Out Timeout

V2 has `DataOutTimeout`.

V3 currently blocks in `ReadPDU` while waiting for Data-Out. A broken initiator
can pin a session goroutine.

Recommendation: add a read deadline only while collecting Data-Out and clear it
on exit. Timeout should fail the session explicitly and be test-pinned.

### 4. DataInWriter

V2 splits large READ responses across multiple Data-In PDUs and only the final
PDU carries S-bit/status.

V3 currently sends one Data-In response. That is acceptable for current tiny
K8s smoke writes but not for OS-level filesystem reads under larger volumes.

Recommendation: implement after WRITE/mkfs is green. Do not mix it into the
first WRITE compatibility patch unless a red test proves it is required.

### 5. Single StatSN Owner

V2 has a tx loop assigning StatSN based on PDU type:

- status-bearing responses increment,
- R2T copies current StatSN but does not increment,
- intermediate Data-In does not carry status.

V3 assigns StatSN inline in a serial loop. That is simpler but fragile once
pending commands, multiple R2Ts, and multi-PDU Data-In enter the same session.

Recommendation: do not immediately port V2's full txLoop. First add pending
queue and collector in the existing serial model. If StatSN bugs appear in real
initiator tests, move to a small V3-local response writer.

## What Must Be Rewritten For V3

These should not be copied from V2:

- V2 device lookup and target resolver shape,
- V2 authority/promote/demote assumptions,
- V2 heartbeat/readiness shortcuts,
- any V2 test that asserts authority side effects from protocol code,
- CHAP/ALUA until explicitly pulled into a security or multipath gate.

V3 iSCSI must remain a frontend adapter over:

```text
frontend.Provider
frontend.Backend
V3 authority/recovery/placement layers
```

Protocol code must not decide replica readiness or authority.

## Proposed TDD Slices

### Slice A: Reproduce The Kernel Shape In-Process

Add a failing V3 test where:

1. client sends WRITE requiring R2T,
2. target emits R2T,
3. client sends a new SCSI-Command before completing Data-Out,
4. target must queue that command, not fail the session,
5. client completes Data-Out,
6. target completes first write,
7. target later processes queued command.

This test directly captures `expected Data-Out, got SCSI-Command`.

Implementation status:

```text
TestP1_ISCSI_R2TDataOut_AllowsPipelinedCommand
  old behavior: session failed with "expected Data-Out, got SCSI-Command"
  fixed behavior: queued command is processed after WRITE completes
```

### Slice B: V3 DataOutCollector

Extract collector semantics and tests:

- immediate-only,
- R2T-only,
- immediate + R2T,
- multi-PDU Data-Out,
- wrong DataSN,
- wrong BufferOffset,
- overflow,
- F-bit before expected bytes.

This is mostly portable from V2 tests, but rewritten in V3 naming and error
style.

Implementation status:

```text
dataOutCollector
  immediate + R2T: covered
  multi-PDU Data-Out: covered
  wrong TTT / DataSN / BufferOffset: covered
  overflow / beyond-burst / premature F-bit: covered
```

### Slice C: Bounded Pending Queue

Add queue behavior:

- non-Data-Out during Data-Out is queued,
- queue is drained before reading socket,
- overflow closes/fails the session deterministically.

Use V2's `maxPendingQueue = 64` as a reference, not as a blindly inherited
constant. The exact value is less important than having a bound and a test.

Implementation status:

```text
TestP1_ISCSI_R2TDataOut_PendingQueueBounded
  64 queued non-Data-Out PDUs allowed
  65th fails the session before backend Write
```

### Slice D: Data-Out Timeout

Add timeout behavior:

- target sends R2T,
- initiator does not send Data-Out,
- session exits within configured timeout,
- no goroutine leak.

Implementation status:

```text
TargetConfig.DataOutTimeout
  default: 30s
  test override: 100ms

TestP1_ISCSI_R2TDataOut_TimesOutWhenInitiatorStalls
  target sends R2T
  initiator sends no Data-Out
  session closes within timeout
  backend Write is not called
```

### Slice E: Real OS Initiator Harness

After A-D pass locally, run hardware:

```text
iscsiadm discovery + login
mkfs.ext4 -F
mount
dd 4 KiB random payload
sync
read back and compare
logout
```

This is the acceptance test. Go client tests are not enough.

Implementation status:

```text
Run ID: 20260504T052809Z
Tree: fix/iscsi-os-initiator-compat@1f13f8f
Host: M02 Linux 6.17 / open-iscsi
Artifacts: V:\share\iscsi-p1\runs\20260504T052809Z\

Result:
  iscsiadm discovery/login: PASS
  device materialized: /dev/disk/by-path/...lun-0 -> /dev/sda
  mkfs.ext4 on 256 MiB target: PASS
  mount: PASS
  32 KiB random payload write + sha256 read-back: PASS
  logout: PASS
  no dangling iSCSI session: PASS

Server-side evidence:
  mkfs.ext4 issued 4 MiB WRITE(10) commands
    transferLen=1024, dataOut=4194304
  target logged backend.Write ok for those writes
  no "expected Data-Out" failure in the write path
```

Harness note:

```text
V:\share\iscsi-p1\run-p1-iscsi-os.sh
scripts/run-iscsi-os-smoke.sh
```

The share-driven harness captured the original P1 evidence. The repository
script is the repeatable developer/QA entrypoint. Both start a single-slot
blockmaster/blockvolume pair with a 256 MiB durable target, then drive Linux
`iscsiadm`, `mkfs.ext4`, mount, payload checksum, and cleanup.

### Slice F: Large READ / DataInWriter

Only after WRITE/mkfs is green:

- add multi-PDU Data-In writer,
- add read larger than MaxRecvDataSegmentLength,
- then test filesystem reads under OS initiator.

Implementation status:

```text
dataInWriter
  splits READ data by negotiated MaxRecvDataSegmentLength
  intermediate Data-In PDUs carry no S/F status bits
  final Data-In PDU carries S/F + SCSI status
  residual count is set on underflow/overflow

TestP1_ISCSI_LargeRead_SplitsDataInByMaxRecvSegment
  300 KiB READ
  64 KiB MaxRecvDataSegmentLength
  multiple Data-In PDUs
  byte-equal payload
```

Evidence level:

```text
In-process iSCSI initiator proof: done.
Linux OS-initiator large-read harness: forward-carry unless a later
filesystem/fio scenario exposes a READ-side failure.
```

## Recommended Implementation Order

1. Write Slice A red test. Done.
2. Add V3-local pending queue support in current serial session. Done.
3. Extract V3-local `DataOutCollector`. Done.
4. Add Data-Out timeout. Done.
5. Run Linux OS initiator mkfs harness. Done.
6. Add DataInWriter only if OS test or later fio/read test needs it. Done as a
   conservative protocol-completeness slice after WRITE/mkfs was green.
7. Then decide whether a txLoop is needed.

## Why Not Port txLoop First

V2's txLoop is likely correct protocol architecture long-term, but porting it
first would touch login, response write path, StatSN, close behavior, and tests
in one change. That is too much surface for the current bug.

The current failure points to one narrower missing behavior:

```text
non-Data-Out PDU during Data-Out collection must be queued, not fatal
```

Fix that first, with the collector extraction close behind it.

## Branch / PR Discipline

All work in this track should go through PRs.

Branch:

```text
fix/iscsi-os-initiator-compat
```

Do not push protocol fixes directly to `main`. The already-merged MaxBurst
patch can stay as the base for this branch unless maintainers decide to revert
and replay the whole track through PR.

## Close Criteria

This track can close when all are true:

- in-process pending-command-during-Data-Out test passes,
- collector edge tests pass,
- timeout test passes,
- Linux kernel initiator `mkfs.ext4` succeeds on a 256 MiB target,
- mount + write/read byte-equal succeeds,
- logout leaves no active iSCSI sessions,
- public demo docs no longer need to warn that large OS format is blocked.
