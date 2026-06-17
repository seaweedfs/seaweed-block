# Phase 40 Finished Plan: Operator Production Hardening

Status: closed on 2026-06-14.

Branch: `phase33-testops-failure-hardening`

## Outcome

Phase 40 closes the status/events-only operator foundation as a release-ready
boundary. The product can now ship a coherent v0.4 beta slice:

```text
Helm + PVC + read-only/status-only operator status + Events + diagnostics +
delete-safety/install-drift visibility. No lifecycle mutation.
```

The main problem addressed by this phase was not a missing user feature; it was
release trust. Phases 35-39 repeatedly found schema/RBAC defects only in live QA.
Phase 40 added release-candidate gates and conformance checks to catch those
classes earlier, then closed the chart/image skew found by QA.

## Delivered

- Status API conformance gate for `SwBlockCluster.status` and
  `SwBlockVolume.status` payload shape:
  - casing drift,
  - enum drift,
  - wrong endpoint usage,
  - RBAC broadening,
  - delete-safety status.
- Status correctness polish:
  - stale `deleteSafety` clearing,
  - single effective node `Ready` condition,
  - bounded/stable Event identity preserved.
- Read-only install drift visibility:
  - chart/app/operator image identity,
  - current/desired/missing/mismatched status,
  - report/dashboard/operator-snapshot/CRD agreement,
  - no upgrade execution.
- Release claim alignment:
  - README, quickstart, release note, roadmap, and QA assignments describe the
    status/events-only operator foundation and explicit non-claims.
- Release candidate gates:
  - local release gate script,
  - TestOps status API conformance scenario,
  - D6 QA assignment and sign-off.
- Chart/image compatibility fix:
  - `--launcher-durable-impl` is gated by
    `compat.launcherDurableImplFlag=false` by default,
  - non-default `blockmaster.durableImpl` fails fast unless the compat flag is
    enabled,
  - local RC gate asserts the default chart render omits the incompatible flag.

## Release Images

Published by GitHub Actions run `27490827782` from commit
`dc2972d0059beffb2447ab8546ceb65e646a7b66`.

```text
ghcr.io/seaweedfs/seaweed-block:sha-dc2972d0059b
ghcr.io/seaweedfs/seaweed-block-csi:sha-dc2972d0059b
```

Published digests:

```text
seaweed-block
  index:       sha256:b8da5ca4e2bbe2f0f630fee0468790c444362615d68807a1be31fd237c84928f
  linux/amd64: sha256:677f6321ea5199b14792345b8691358860bb9ca7376f4e4a2f3a7c0113d5db9b
seaweed-block-csi
  index:       sha256:b5942cd68d28aecdfebec1f1e5ec55a9cafe746169fee3b6c35916c93fffcaa6
  linux/amd64: sha256:1fc636a4e0e63cc8cbee39e6775053c5d1aba7213ca9182a084e1bb6fe71474c
```

## QA Evidence

Primary D6 close:

- `internal/docs/qa-assignments/phase40-d6-release-candidate-qa-signoff.md`

Result:

- Local release gate: PASS.
- G1 first-volume with published image: PASS.
- G2 operator-status CRD/Event/RBAC with fresh release image: PASS.
- G3 negative status: PASS.
- G4 status API conformance: PASS.
- G5 cleanup residue: PASS.

Important QA sequence:

- Initial D6 held because the chart passed `--launcher-durable-impl` to
  published images that did not support that flag.
- The chart compatibility fix resolved the blocker.
- QA reran G1 against the published `sha-6260e46fd3be` image and confirmed
  first-volume still works.
- Fresh images were published from `dc2972d0059b`.
- QA ran G2 against `sha-dc2972d0059b` and confirmed live CRD status, Events,
  and the status/events-only RBAC boundary on the shipped binary.

Supporting evidence:

- `internal/docs/qa-assignments/phase40-d4-status-api-conformance-qa.md`
- `internal/docs/qa-assignments/phase40-d6-release-candidate-qa.md`
- `docs/releases/v0.4-beta-candidate.md`

## Non-Claims

Phase 40 does not claim:

- production readiness,
- mutating Kubernetes operator lifecycle,
- automatic `SwBlockVolume` object creation or ownership,
- finalizer ownership or finalizer add/remove,
- delete execution,
- automatic cleanup,
- automatic support-bundle collection or upload,
- upgrade or rollback execution,
- promote, repair, rebuild, failback, delete, backup, restore, or cleanup
  mutation through UI/API/operator,
- returned-replica rebuild or automated failback,
- NVMe ANA parity expansion,
- production dashboard,
- performance, RTO, RPO, or SLO guarantees.

## Carry-Forwards

1. **Real envtest/live-apiserver status-writer harness.**
   The current conformance gate is schema-aware and useful, but still uses a
   mock server. A real apiserver + real RBAC harness remains the highest-leverage
   way to shift live-only CRD/RBAC defects left.
2. **`swblock -env` flag ordering.**
   Runner flags after the scenario path can be silently ignored. Fix the runner
   or correct all runbooks to put `-env` before the scenario path.
3. **Parameterized support-bundle import nodes.**
   `helm-support-bundle-diagnostics-chain` hardcodes a tp01 import path; make it
   env-overridable so one lab node being down does not block unrelated negative
   status validation.
4. **Restore `tp01`.**
   Lab infra remained `NotReady`/unreachable during the release cycle. Restore
   before RF=3 live multi-node work.
5. **Loopback publish-target documentation.**
   Document loopback publish targets as single-node/local-consumer only.

## Phase 41 Entry Criteria

Phase 41 should start from the v0.4 beta status/events-only baseline and should
not weaken that boundary.

Recommended next scope:

- lifecycle-owner design,
- CR object ownership and finalizer strategy,
- first bounded mutating path with explicit action/precondition/executor/evidence
  contract,
- real envtest/live-apiserver regression before any new mutating RBAC ships.

Do not start with NVMe ANA parity, backup/restore, rebuild/failback, or broad
mutation until the lifecycle-owner boundary is explicit and testable.
