# NVMe ANA Technical Note

Status: reference for NVMe-P3/P4 work.

## What ANA Is

- ANA means Asymmetric Namespace Access.
- It is the NVMe equivalent of iSCSI ALUA: the target tells the host which
  path to prefer for a namespace, and which paths are usable only for
  failover or not usable at all.
- Linux NVMe multipath reads ANA information from Identify Controller,
  Identify Namespace, and the ANA log page. If those surfaces disagree, the
  host can make bad path decisions.

## V3 Product Meaning

- V3 authority decides which replica may acknowledge writes.
- NVMe protocol code must only report frontend path facts derived from product
  state. It must not elect authority or decide replica readiness.
- The primary serving path should report optimized access.
- A replica path that is caught up enough for failover probing but not writable
  should report a non-writable ANA state according to the policy we choose.
- A stale or degraded path must never report a state that lets the host send
  successful writes.

## Minimum Wire Surfaces

- Identify Controller:
  - `CMIC` must advertise ANA only when ANA is implemented.
  - `ANACAP`, `ANAGRPMAX`, and `NANAGRPID` must be non-zero only with a real
    ANA log implementation.
- Identify Namespace:
  - `ANAGRPID` must point at the ANA group used by the namespace.
- Get Log Page:
  - log page `0x0c` must return an ANA group descriptor for the namespace.
  - the group must include current ANA state and a change count.
- I/O status mapping:
  - stale primary lineage maps to a path-related status, not success.
  - inaccessible / transition states must fail writes safely.

## Current V3 State

- Without an ANA provider, V3 intentionally zeros ANA Identify fields.
- Existing tests pin that zero state so we do not advertise ANA without a real
  provider, log page, and event source.
- V3 maps stale lineage to NVMe path-related status for I/O errors.
- V3 now has an ANA provider seam and a blockvolume projection provider:
  - optimized means the frontend projection is healthy,
  - non-optimized means the path is present for probing/failover but not the
    optimized write path,
  - ANA change means recovering,
  - inaccessible means degraded, mismatched identity, or missing projection.
- V3 now implements ANA Get Log Page `0x0c` behind the provider seam.
- Identify ANA fields are conditional:
  - no ANA provider => all ANA Identify fields stay zero,
  - ANA provider present => Identify Controller advertises ANA and Identify
    Namespace carries the provider's ANA group.
- Phase 127 added the first async event producer: when an ANA provider is
  wired, Identify Controller advertises OAES ANA Change Notice and a parked AER
  completes when `ANAChangeCount()` advances.
- Phase 128 proved that behavior against a real Linux NVMe/TCP initiator. The
  kernel `nvme:nvme_async_event` tracepoint observed
  `NVME_AEN=0x0c0302` during r1->r2 failover, which decodes to Notice / ANA
  Change / ANA log page.

## V2 Reference Behavior

- V2 has an `ANAProvider` with `ANAState()` and `ANAGroupID()`.
- V2 Identify advertises ANA fields.
- V2 Get Log Page `0x0c` returns a single ANA group.
- V2 gates writes based on ANA state.

V3 should use V2 as the coverage inventory, not as a direct code transplant.
V3 state must come from frontend projection facts and authority lineage, not
from V2 role ownership.

## Design Risks

- Advertising ANA too early is worse than not supporting it: Linux may enable
  multipath policy based on incomplete data.
- A single hard-coded group is acceptable for a first RF=2 path if the state is
  correct and the Identify limits match it. Linux validates ANA group id
  against `ANAGRPMAX` / `NANAGRPID`; for the current single-group target the
  dense group id must be `1`, not a hash-derived value.
- ANA state changes need host-visible evidence. Unit tests alone are not
  enough; P4 must include real `nvme-cli` / Linux multipath validation.
- Reads and writes may have different safety policy. The chosen policy must be
  explicit before code lands.

## Development Order

- P3-A: add provider interface and state mapping tests without advertising ANA.
  - status: done.
- P3-B: implement ANA log page and keep Identify ANA fields off.
  - status: done.
- P3-B2: wire blockvolume projection state into the ANA provider.
  - status: done.
- P3-C: flip Identify Controller / Namespace fields only after the log page
  tests pass.
  - status: done.
- P3-D: add Linux `nvme get-log` / `nvme id-ctrl` / `nvme id-ns` QA assignment.
  - status: done.
- P4: add two-path Linux multipath validation and mounted failover evidence.
  - status: done for standalone host; Kubernetes dynamic reconnect/restage is
    tracked separately.

## Non-Goals For P3

- No NVMe/RDMA claim.
- No Kubernetes CSI protocol switch.
- No performance claim.
- No multi-volume namespace management.
- No Kubernetes dynamic reconnect/restage claim until the CSI/node ownership
  gate passes.
