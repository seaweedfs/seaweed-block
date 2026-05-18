# Finished Plan: Basic Mounted Failover And Reattach MVP

Status: closed. QA close report passed 10/10 strict hard-gate clauses in
`qa-assignments/mounted-failover-reattach-mvp-close-report.md`.

Opened after closing
`finished-plans/phase14_finishedplan_multi_node_attach_and_placement_mvp.md`.

Closed after QA-owned runs:

- RF=2 app baseline `20260513-162339-aee3`: PASS, 7/7 phases, 42/42 actions,
- RF=2 primary safe-refusal `20260513-160112-d3f9`: PASS, 7/7 phases,
  49/49 actions,
- RF=2 degraded replica `20260513-151339-56c2`: PASS, 9/9 phases,
  47/47 actions,
- fast guard tests: `go test ./core/ops -count=1` PASS.

The first close attempt failed on one residue issue: the RF=2 app baseline left
`/var/lib/sw-block/testops-<run>-rf2-app`. The baseline chain now removes that
run-scoped hostPath with `sudo` and asserts the exact path is gone. QA reran and
issued strict PASS.

## Product Question

Can an early Kubernetes user recover from a primary `blockvolume` failure while
an app is using an RF=2 PVC, or at least get a safe, self-explaining refusal
instead of a false recovery claim?

## Delivered Claim

This plan does not claim automatic RF=2 recovery.

It delivers a narrower, useful beta-facing availability claim:

```text
On the documented alpha Kubernetes iSCSI path, Seaweed Block can run an RF=2
PVC in the two-logical-server development/TestOps topology, write/read through
the mounted app path before failure, identify the current primary from
inventory, stop that primary in a scoped way, and refuse recovery safely when
the peer replica is not promotion-ready.
```

The safe-refusal bundle says:

```text
failover_status: refused
ack_profile: best-effort
failure_class=primary-blockvolume-controlled-stop
before_primary_replica=r1
failed_replica=r1
candidate_ready=false
candidate_evidence=... r2 ... replication=not_ready ...
data_check_after_failover=not_claimed
reason=candidate_not_ready_for_primary
target_ready_replicas=0
```

## What Changed

- RF=2 lifecycle placement now flows into the authority/inventory evidence
  path without collapsing desired/observed replica counts.
- The alpha two-logical-server TestOps topology can render two generated
  `blockvolume` Deployments for one RF=2 PVC.
- Frontend/status/data/control ports are allocated distinctly for logical
  replicas sharing one physical Kubernetes node.
- Inventory preserves generated replica frontend/status evidence while merging
  nested status health.
- Unsafe failover-looking evidence is marked unhealthy:
  - stale primary frontend-ready,
  - primary with non-`none` replication role,
  - non-primary frontend-ready,
  - non-primary with `replication_role=none`.
- The RF=2 mounted app baseline proves writer and reader checksums before
  failure.
- The controlled primary-failure gate proves safe refusal without creating a
  false post-failure reader success.
- `docs/operations-v1.md` now separates:
  - default RF=2 safe refusal,
  - two-logical-server RF=2 app baseline,
  - controlled primary-failure safe refusal,
  - RF=2 recovery/promotion as a non-claim.

## Evidence

QA close report:

- `qa-assignments/mounted-failover-reattach-mvp-close-report.md`

Hard-gate assignment:

- `qa-assignments/mounted-failover-reattach-mvp-close-hard-gate.md`

Runner scenarios:

- `testops/scenarios/mounted-failover-rf2-safe-refusal-chain.yaml`
- `testops/scenarios/mounted-failover-rf2-placement-chain.yaml`
- `testops/scenarios/mounted-failover-rf2-degraded-replica-chain.yaml`
- `testops/scenarios/mounted-failover-rf2-app-baseline-chain.yaml`
- `testops/scenarios/mounted-failover-rf2-primary-failure-safe-refusal-chain.yaml`

Reference docs:

- `ref/mounted-failover-reattach-audit.md`
- `ref/mounted-failover-reattach-contract.md`
- `ref/product-spec-gate-template.md`

## Non-Claims

This plan explicitly does not claim:

- RF=2 recovery/promotion after primary failure,
- transparent in-place I/O continuation,
- RF=3 Kubernetes lifecycle or failover,
- arbitrary node loss,
- remote-node attach to loopback frontends,
- sync-quorum or sync-all durability,
- rebuild/reintegration completion,
- performance SLOs,
- upgrade or broad uninstall safety,
- UI/operator-grade remediation.

## Next Product Gap

The next plan should target the real missing capability exposed by this plan:

```text
Can the RF=2 peer progress from observed/not_ready to promotion-ready, and then
can the product recover through a controlled primary failure with reattach and
checksum proof?
```

That requires durable frontier/ACK-profile clarity, replica catch-up evidence,
candidate eligibility, stale-primary fencing, and a positive recovery gate. It
should not weaken the safe-refusal semantics just to make a green test.
