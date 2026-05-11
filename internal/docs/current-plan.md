# Current Plan: iSCSI Session And Backend Pressure Hardening

Status: active. Started after closing
`finished-plans/phase6_finishedplan_iscsi_os_initiator_compatibility.md` on
2026-05-11.

## Goal

Harden the iSCSI frontend behind the now-green Linux and Windows OS initiator
gates by moving remaining risk into fast component tests first.

This plan is not about adding another long m02 integration loop. The rule is:

```text
component first -> product-backed gate only when the component boundary is green
```

## Why This Is Next

The previous plan proved that real Linux and Windows initiators can format,
write, read, and disconnect for the current alpha claim. That closes the
highest-risk protocol credibility gap.

The next remaining iSCSI risk is narrower:

- session lifecycle under pressure,
- backend/WAL pressure,
- large write memory retention,
- target close/restart behavior while sessions are active,
- StatSN/NOP-Out/pending queue edge behavior,
- product-backed iSCSI over durable storage under stress.

This contributes to the roadmap:

- Track B: iSCSI frontend stability.
- Track C: durable state pressure behavior.
- Beta Foundation: keep expensive OS and suite gates as milestone evidence,
  not the main debugging loop.

## Current Coverage Inventory

Fast iSCSI protocol pressure tests already exist and pass.

Command run on 2026-05-11:

```text
go test ./core/frontend/iscsi -run 'TestP2_ISCSI|TestP1_ISCSI|TestDataInWriter|TestDataOut'
```

Result:

```text
ok github.com/seaweedfs/seaweed-block/core/frontend/iscsi 0.480s
```

Covered today:

- `TestP2_ISCSI_ConcurrentSessions50_WriteRead`
- `TestP2_ISCSI_RapidLoginLogout_NoGoroutineLeak`
- `TestP2_ISCSI_TargetCloseWithActiveSessions_ExitsCleanly`
- `TestP2_ISCSI_TargetCloseIsIdempotentWithActiveSessions`
- `TestP2_ISCSI_TargetCloseReleasesListenAddressForRestart`
- `TestP2_ISCSI_NopOutDuringDataOut_DrainsAfterWrite`
- `TestP2_ISCSI_ErrorResponseAdvancesStatSN`
- `TestP2_ISCSI_LargeWrite4MiB_DoesNotGrowHeapUnbounded`
- `TestP2_ISCSI_LargeWrite_SlowBackend_DoesNotAccumulateBuffers`
- P1 Data-Out pending queue, timeout, and large Data-In/Data-Out tests.

Decision:

- Do not spend this plan recreating the P2 protocol executor pack.
- Use it as the fast regression base.
- Move the next work to product-backed durable/backend pressure where V2 had
  more stability coverage and V3 still has less direct evidence.

## Workstream A: Product-Backed Pressure Coverage

Purpose: test the iSCSI frontend over a real V3 durable/backend boundary, not
only `testback.RecordingBackend`.

Candidate coverage:

- sustained writes through iSCSI into a real durable backend,
- write/read under slow or pressured durable writes,
- `SYNCHRONIZE CACHE` under write pressure,
- rapid open/close target with durable backend,
- reconnect after target restart using the same durable root,
- no session/process/goroutine leak after pressure.

Preferred shape:

- fast Go/component or L2 subprocess test first,
- no Kubernetes,
- no OS initiator unless a component test cannot model the failure,
- clear memory/goroutine/session budget.

First target:

```text
product-backed iSCSI restart/reconnect preserves data and releases sessions
```

Reason: this sits between pure protocol tests and full OS initiator/suite
gates. It exercises durable storage plus frontend session lifecycle without
requiring a long lab run.

First delivery:

- Added `TestISCSI_L2DurableRestartReconnect_PreservesData`.
- Layer: subprocess L2, no Kubernetes, no OS initiator.
- Path:
  - start single-slot `blockmaster`,
  - start `blockvolume` with `--durable-root`, `walstore`, and iSCSI,
  - write one 4 KiB block through an iSCSI session,
  - stop `blockvolume`,
  - restart `blockvolume` with the same durable root and same iSCSI address,
  - reconnect over iSCSI and read the same LBA,
  - assert byte equality.
- Targeted run:
  `go test -tags subprocess ./cmd/blockvolume -run TestISCSI_L2DurableRestartReconnect_PreservesData -count=1 -v`
- Result: PASS in `18.15s`.
- Adjacent run:
  `go test -tags subprocess ./cmd/blockvolume -run 'TestISCSI_L2DurableRestartReconnect_PreservesData|TestG8B_L2PrimaryKill_NewPrimaryReadsAcknowledgedISCSIWrite|TestG15a_BlockvolumeReportsFrontendTargetsToMasterStatus' -count=1`
- Result: PASS in `52.344s`.

Second delivery:

- Added `TestISCSI_L2DurableRestartReconnect_RepeatedCycles`.
- Layer: subprocess L2, no Kubernetes, no OS initiator.
- Path:
  - start single-slot product stack,
  - connect over iSCSI,
  - write a distinct 4 KiB LBA,
  - read all prior LBAs,
  - logout,
  - restart `blockvolume` with the same durable root and iSCSI address,
  - repeat for three cycles,
  - final reconnect verifies every written LBA.
- Targeted run:
  `go test -tags subprocess ./cmd/blockvolume -run TestISCSI_L2DurableRestartReconnect_RepeatedCycles -count=1 -v`
- Result: PASS in `29.08s`.
- Restart pack run:
  `go test -tags subprocess ./cmd/blockvolume -run 'TestISCSI_L2DurableRestartReconnect_(PreservesData|RepeatedCycles)' -count=1 -v`
- Result: PASS in `46.552s`.

Third delivery:

- Added `TestISCSI_L2DurableSyncCacheRestart_PreservesSyncedWrites`.
- Layer: subprocess L2, no Kubernetes, no OS initiator.
- Path:
  - start single-slot product stack,
  - write 12 distinct 4 KiB LBAs through iSCSI,
  - issue `SYNCHRONIZE CACHE(10)` every four writes plus a final sync,
  - stop `blockvolume`,
  - restart with the same durable root and iSCSI address,
  - reconnect and verify representative synced LBAs.
- Helper added:
  - `g8IscsiClient.syncCache10`.
- Targeted run:
  `go test -tags subprocess ./cmd/blockvolume -run TestISCSI_L2DurableSyncCacheRestart_PreservesSyncedWrites -count=1 -v`
- Result: PASS in `17.81s`.
- Restart/sync pack run:
  `go test -tags subprocess ./cmd/blockvolume -run 'TestISCSI_L2Durable(RestartReconnect_(PreservesData|RepeatedCycles)|SyncCacheRestart_PreservesSyncedWrites)' -count=1 -v`
- Result: PASS in `64.126s`.

## Workstream B: Existing Gate Hygiene

Purpose: keep the expensive gates useful but not overused.

Milestone gates remain:

- `iscsi-os-initiator-compat-chain`
- `iscsi-p8-compat-soak-chain`
- `beta-hardening-gate`

Use them when:

- a component fix lands,
- a milestone closes,
- QA needs independent evidence,
- a regression is suspected in product-backed behavior.

Do not use them as:

- the first debugging loop,
- proof for a narrow protocol edge that a component test can cover,
- performance evidence.

## Workstream C: V2 Comparison Discipline

Purpose: keep V2 as a reference, not a porting mandate.

Use V2 only when a concrete fast test or product-backed gate fails.

For each V2 comparison, write:

- failing behavior in V3,
- matching or different behavior in V2,
- minimum V3 behavior to add,
- test that proves the change.

Do not port:

- V2 control-plane semantics,
- V2 authority/readiness shortcuts,
- broad txLoop/session architecture unless a focused failure requires it.

## Delivery Gate

This plan is complete when:

1. The current P1/P2 iSCSI component pressure pack is recorded green.
2. At least one product-backed iSCSI pressure/restart/reconnect test is added
   or an existing one is identified as sufficient with evidence.
3. The selected fast gate runs in seconds, not minutes.
4. If a product-backed failure is found, it is fixed with the fast test as the
   regression guard.
5. One milestone gate is rerun only after the fast gate is green.
6. Non-claims are explicit:
   - not performance,
   - not HA,
   - not MPIO,
   - not broad OS matrix,
   - not Kubernetes operator cleanup.

## Dev / QA Split

Developer handles:

- component/L2 test inventory,
- fast test implementation,
- product fixes,
- single-command runner gates when the result is clear.

QA handles:

- independent milestone validation,
- ambiguous lab behavior,
- repeatability claims,
- new scenario design when the expected behavior is not obvious from code.

Default rule:

```text
fast deterministic local/L2 test -> developer
cross-host, cross-OS, repeatability, or ambiguous lab behavior -> QA
```

## Immediate Next Step

Inspect existing product-backed iSCSI L2 tests and pick the smallest missing
restart/reconnect or durable-pressure assertion. Do not start with a long suite.

Candidate files:

- `cmd/blockvolume/g8_iscsi_client_test.go`
- `cmd/blockvolume/g9g_l2_product_loop_test.go`
- `core/frontend/iscsi/t2_l2_iscsi_harness_test.go`
- `core/frontend/durable/integration_iscsi_test.go`

## Non-Claims

- This plan does not change the Linux/Windows OS initiator compatibility claim.
- This plan does not deliver a UI, CLI, or operator.
- This plan does not deliver performance benchmarks.
- This plan does not deliver HA or failover semantics.
- This plan does not replace beta-hardening suite validation.
