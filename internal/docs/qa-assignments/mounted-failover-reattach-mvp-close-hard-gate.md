# QA Assignment: Basic Mounted Failover And Reattach MVP Close Gate

Status: ready for QA close validation after the D5 primary-failure
safe-refusal gate passed.

Branch: `docs/post-merge-plan`.

## Product Claim Under Test

On the supported alpha Kubernetes iSCSI path, Seaweed Block can:

- run an RF=2 PVC in the two-logical-server development/TestOps topology,
- write and read through the mounted app/PVC path before failure,
- identify the current primary from inventory,
- stop that primary in a scoped way,
- refuse recovery safely when the peer is not promotion-ready,
- publish enough inventory/support-bundle evidence for an operator to
  understand why recovery was not claimed.

This close gate must not expand the claim to automatic RF=2 recovery,
transparent mounted I/O continuation, RF=3, node loss, remote-node loopback
attach, quorum durability, rebuild/reintegration, performance, upgrade safety,
or UI.

## Required Environment

- m02 single-node k3s lab, or equivalent lab that passes
  `scripts/preflight-k8s-alpha.sh --local-k3s`.
- Product checkout at the target commit.
- Current `swblock`/testrunner binary.
- Clean lab before each run: no active Seaweed Block iSCSI sessions, no
  `blockmaster`/`blockvolume`/`blockcsi`/`iscsi-target` processes, and no stale
  `app=sw-blockvolume` Deployments.

## Required Runs

Run the app-path baseline:

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/mounted-failover-rf2-app-baseline `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/mounted-failover-rf2-app-baseline-chain.yaml
```

Run the primary-failure safe-refusal gate:

```powershell
swblock run `
  --results-dir V:/share/g15d-k8s/testops-runs/mounted-failover-rf2-primary-safe-refusal `
  --env product_root=/tmp/seaweed_block `
  --env ssh_key=C:/work/dev_server/testdev_key `
  C:/work/seaweed_block/testops/scenarios/mounted-failover-rf2-primary-failure-safe-refusal-chain.yaml
```

QA may cite already completed runs if they are at the target product commit or
newer and include all required artifacts. Otherwise re-run.

## Hard Gate Clauses

### HG-0: Operations Manual Claim Boundary

Pass:

- `docs/operations-v1.md` documents RF=2 as:
  - default single-logical-server alpha safe refusal,
  - two-logical-server development/TestOps mounted app path,
  - controlled primary failure safe refusal when the peer is not ready.
- The manual explicitly says RF=2 recovery/promotion is not claimed.
- The manual says `data_check_after_failover=not_claimed` for the current
  safe-refusal path.

Fail:

- Any user-facing doc implies RF=2 automatic recovery or transparent failover is
  supported by this plan.

### HG-1: RF=2 Mounted App Baseline

Pass:

- `mounted-failover-rf2-app-baseline-chain` passes.
- Writer verifies `/data/demo.bin`.
- Reader verifies the same `/data/demo.bin` after writer deletion and reader
  attach.
- Generated `blockvolume` manifest contains both `--replica-id=r1` and
  `--replica-id=r2`.
- Inventory after reader verification contains `rf=2 desired=2 observed=2`.
- Nested per-replica `sw-block ops status` bundles are collected.

Fail:

- The run falls back to RF=1, only one replica is generated, or the reader
  checksum is missing.

### HG-2: Pre-Failure Primary And Candidate Evidence

Pass:

- Primary-failure gate before-failure inventory contains exactly one
  `role=primary` replica.
- `before_primary_replica=<rN>` equals `failed_replica=<rN>`.
- Candidate evidence names the other replica and shows it is not promotion
  ready, for example `replication=not_ready`, `role=unknown`, or
  `status=unhealthy`.

Fail:

- The failed replica is hard-coded or cannot be tied to before-failure
  inventory.
- The safe-refusal reason is scripted without matching inventory evidence.

### HG-3: Scoped Primary Failure

Pass:

- Failure class is exactly
  `primary-blockvolume-controlled-stop`.
- Target deployment is a generated `sw-blockvolume` Deployment for the parsed
  primary replica.
- `target_ready_replicas=0`.
- After-failure inventory records the failed primary as degraded, unreachable,
  unavailable, or otherwise not serving.

Fail:

- The failure is an unscoped process kill, a random replica stop, or a broad
  cleanup action.

### HG-4: Safe Refusal Contract

Pass:

The primary-failure bundle contains:

```text
failover_status: refused
ack_profile: best-effort
candidate_ready=false
data_check_after_failover=not_claimed
reason=candidate_not_ready_for_primary
after_issue_evidence=<actionable issue>
```

Fail:

- The bundle claims recovered data without a post-failure reader checksum.
- The refusal reason is missing, generic, or contradicts inventory.

### HG-5: No False Recovery Claim

Pass:

- The primary-failure run does not create a successful reader after the primary
  stop.
- There is no post-failure reader line matching `/data/demo.bin: OK`.
- The report explicitly states that the product stopped at safe refusal.

Fail:

- The run or docs imply recovery because the pre-failure writer succeeded.

### HG-6: Inventory/Support Bundle Self-Explains

Pass:

QA can read the primary-failure artifacts without code context and answer:

- which replica was primary before failure,
- which Deployment was stopped,
- whether the peer was promotion-ready,
- whether recovery was claimed,
- which issue line explains the refusal.

Fail:

- The answer requires reading implementation logs or guessing from missing
  artifacts.

### HG-7: Negative Fixtures And Fast Guards

Pass:

- Fast tests cover unsafe evidence classes:
  - stale primary frontend-ready is unhealthy,
  - primary with non-`none` replication role is unhealthy,
  - non-primary frontend-ready is unhealthy,
  - non-primary with `replication_role=none` is unhealthy.
- The degraded-replica runner gate has passed or is cited with run ID.

Fail:

- A heartbeat-only or not-ready replica can appear healthy/eligible in
  inventory.

### HG-8: Cleanup Hygiene

Pass after all required runs:

- No active iSCSI sessions with `io.seaweedfs`.
- No `blockmaster`, `blockvolume`, `blockcsi`, or `iscsi-target` processes.
- No `kubectl port-forward svc/blockmaster` process.
- No `app=sw-blockvolume` Deployment remains.
- Any run-scoped `/var/lib/sw-block/testops-*` state is removed.

Fail:

- Residue remains without a documented non-claim or cleanup attribution.

### HG-9: Non-Claims Remain Honest

Pass:

`docs/operations-v1.md`, `docs/quickstart-kubernetes.md`, and
`internal/docs/current-plan.md` do not claim:

- RF=2 automatic recovery/promotion,
- RF=3 Kubernetes failover,
- transparent in-place I/O continuation,
- node loss or host-disk failure survival,
- remote-node attach to loopback frontends,
- sync-quorum/sync-all durability,
- rebuild/reintegration,
- performance SLOs,
- upgrade or broad uninstall safety,
- UI/operator-grade remediation.

Fail:

- Any doc expands the claim beyond RF=2 mounted app baseline plus explicit
  primary-failure safe refusal.

## Close Report Template

QA should write:

```text
internal/docs/qa-assignments/mounted-failover-reattach-mvp-close-report.md
```

The report must include:

```text
QA Close - Basic Mounted Failover And Reattach MVP

Verdict: PASS|FAIL
Product commit:
Runner commit:
Host/lab:

HG-0 operations manual claim boundary: PASS|FAIL
HG-1 RF=2 mounted app baseline: PASS|FAIL
HG-2 pre-failure primary/candidate evidence: PASS|FAIL
HG-3 scoped primary failure: PASS|FAIL
HG-4 safe refusal contract: PASS|FAIL
HG-5 no false recovery claim: PASS|FAIL
HG-6 bundle self-explains: PASS|FAIL
HG-7 negative fixtures and fast guards: PASS|FAIL
HG-8 cleanup hygiene: PASS|FAIL
HG-9 non-claims honest: PASS|FAIL

App baseline run id:
Primary safe-refusal run id:
Degraded-replica run id:
Fast-test command/output:
Residue audit:
Blocking findings:
Non-blocking findings:
QA needed next:
```

Any single hard-gate `FAIL` blocks plan close.
