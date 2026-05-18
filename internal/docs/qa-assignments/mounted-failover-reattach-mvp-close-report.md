# QA Close — Basic Mounted Failover And Reattach MVP

Formal close report against
`internal/docs/qa-assignments/mounted-failover-reattach-mvp-close-hard-gate.md`.

```text
Verdict:         PASS (strict) — all 10 HG clauses cleared after dev's HG-8 cleanup fix.

Product commit:  shared working tree at HEAD 0606ab1 + dev's RF=2 lifecycle slices + HG-8 cleanup fix
Runner commit:   sw-test-runner-standalone @ 6ec7abd (swblock 15.9 MB Windows)
Host/lab:        m02 (192.168.1.184) — Ubuntu 24.04.3 LTS / k3s v1.34.4+k3s1

App baseline run id:           20260513-162339-aee3  (mounted-failover-rf2-app-baseline-chain, PASS 7/7 phases, 42/42 actions — rerun after HG-8 fix)
Primary safe-refusal run id:   20260513-160112-d3f9  (mounted-failover-rf2-primary-failure-safe-refusal-chain, PASS 7/7 phases, 49/49 actions)
Degraded-replica run id:       20260513-151339-56c2  (mounted-failover-rf2-degraded-replica-chain, PASS 9/9 phases, 47/47 actions)
Fast-test command:             ssh m02 cd /tmp/seaweed_block && go test ./core/ops -count=1   → ok

Prior verdict at the original run set (superseded): FAIL — HG-8 residue. Dev fixed at
testops/scenarios/mounted-failover-rf2-app-baseline-chain.yaml (added sudo rm -rf + final_asserts test ! -e).
This report reflects the recheck after the fix.
```

## HG clause table

```text
HG-0 operations manual claim boundary:    PASS
HG-1 RF=2 mounted app baseline:           PASS
HG-2 pre-failure primary/candidate:       PASS
HG-3 scoped primary failure:              PASS
HG-4 safe refusal contract:               PASS
HG-5 no false recovery claim:             PASS
HG-6 bundle self-explains:                PASS
HG-7 negative fixtures and fast guards:   PASS
HG-8 cleanup hygiene:                     PASS  (recheck after dev's cleanup fix; no testops-* paths leaked)
HG-9 non-claims honest:                   PASS
```

## Evidence

### HG-0 operations manual claim boundary — PASS

`docs/operations-v1.md` §"RF=2 Mounted Failover Status" (line 388+) names the two validated alpha boundaries:

1. Default single-logical-server alpha: RF=2 PVC safely refused.
2. Two-logical-server dev/TestOps: mounted app path works, controlled primary failure is a safe-refusal path when the peer is not promotion-ready.

The manual emits an explicit non-claim:

```text
Not claimed:
- ...
- RF=2 recovery/promotion after primary failure,
- RF=3 Kubernetes lifecycle or mounted failover,
```

And the safe-refusal example block at line 458 carries `data_check_after_failover=not_claimed`.

### HG-1 RF=2 mounted app baseline — PASS

QA-owned run `20260513-153536-af82`:

```text
state=pass  phases=7/7  actions=41/41

writer.log "wrote and verified /data/demo.bin"      : 1
reader.log "verified persisted /data/demo.bin"      : 1
writer "/data/demo.bin: OK"                         : 1
reader "/data/demo.bin: OK"                         : 1
generated-blockvolume.yaml --replica-id=r1          : present
generated-blockvolume.yaml --replica-id=r2          : present
inventory rf=2 desired=2 observed=2                 : present
nested per-replica ops-status bundles                : 2 (r1 + r2)
```

### HG-2 pre-failure primary / candidate evidence — PASS

From `ops-inventory-before-primary-failure/volume-inventory-summary.txt` (run `20260513-160112-d3f9`):

```text
volume: ... rf=2 desired=2 observed=2 primary=r1 status=unhealthy
replica r1: status=ok role=primary healthy=true epoch=1 endpoint_version=1
replica r2: status=unhealthy role=unknown replication=not_ready healthy=false epoch=0
```

Exactly one `role=primary` row (r1). Safe-refusal txt:

```text
before_primary_replica=r1
failed_replica=r1                ← matches before_primary_replica
candidate_evidence=replica: ... replica=r2 ... role=unknown replication=not_ready healthy=false ...
```

`failed_replica == before_primary_replica` proves the primary was dynamically derived from inventory (not hard-coded). Candidate evidence quotes the r2 inventory row showing `replication=not_ready`, `role=unknown`, `healthy=false` — all three not-ready signals.

### HG-3 scoped primary failure — PASS

Safe-refusal txt:

```text
failure_class=primary-blockvolume-controlled-stop
target_deployment=deployment.apps/sw-blockvolume-pvc-9f09abbc-...-r1
target_ready_replicas=0
```

After-failure inventory:

```text
volume: ... primary=unavailable
replica r1: status=unhealthy role=unavailable replication=unavailable healthy=false
            status_addr=127.0.0.1:23260
issues:
- primary_replica_id unavailable
- replica_degraded=r1 status=unhealthy
- replica r1 status_endpoint_unreachable=127.0.0.1:23260
- replica r1 authority_role unavailable
- replica r1 replication_role unavailable
- replica r1 collection_error: ops_status: status port-forward deploy/sw-blockvolume-...-r1 41453:23260 not ready: exit status 1
```

Failure class is exact, target is a scoped `sw-blockvolume` Deployment matching the parsed primary's replica id, target ready replicas is 0, after-failure inventory records r1 as `degraded`, `unreachable`, `unavailable`. All four required signals fire.

### HG-4 safe refusal contract — PASS

Safe-refusal txt contains all six gate-required fields with the exact wording:

```text
failover_status: refused                           ✓
ack_profile: best-effort                           ✓
candidate_ready=false                              ✓
data_check_after_failover=not_claimed              ✓
reason=candidate_not_ready_for_primary             ✓
after_issue_evidence=- volume ... replica_degraded=r1 status=unhealthy  ✓ (actionable)
```

### HG-5 no false recovery claim — PASS

`reader.log` in run `20260513-160112-d3f9`:

```text
error: error from server (NotFound): pods "sw-block-demo-reader" not found in namespace "default"
```

No reader pod was created after the primary stop. There is no `/data/demo.bin: OK` line after the failure. The product explicitly stopped at safe refusal.

The pre-failure writer's `/data/demo.bin: OK` exists in `writer.log` but is the BEFORE-failure verification, not a recovery claim.

### HG-6 bundle self-explains — PASS

Cold-read of the safe-refusal txt alone answers every gate question:

| Question | Answer from bundle |
|---|---|
| Which replica was primary before failure? | `before_primary_replica=r1` |
| Which Deployment was stopped? | `target_deployment=deployment.apps/sw-blockvolume-pvc-9f09abbc-...-r1` (and `target_ready_replicas=0`) |
| Was the peer promotion-ready? | `candidate_ready=false`, plus inline `candidate_evidence=replica r2 ... replication=not_ready healthy=false` |
| Was recovery claimed? | `data_check_after_failover=not_claimed` and `failover_status: refused` |
| Which issue line explains the refusal? | `reason=candidate_not_ready_for_primary` and `after_issue_evidence=- volume ... replica_degraded=r1 status=unhealthy` |

No additional log files needed. Stranger triage works.

### HG-7 negative fixtures and fast guards — PASS

`go test ./core/ops -count=1` → `ok` on m02 (worktree at HEAD shared with dev).

Test functions covering the four unsafe-evidence classes the gate enumerates:

| Gate-required class | Test function |
|---|---|
| stale primary frontend-ready is unhealthy | `TestBuildVolumeInventory_StalePrimaryFrontendReadyBlocksRecoveryClaim` |
| primary with non-`none` replication role is unhealthy | `TestBuildVolumeInventory_PrimaryWithReplicaRoleIsNotEligiblePrimary` |
| non-primary frontend-ready is unhealthy | `TestBuildVolumeInventory_NonPrimaryFrontendReadyBlocksRecoveryClaim` |
| non-primary with `replication_role=none` is unsafe | `TestBuildVolumeInventory_NonPrimaryWithPrimaryReplicationRoleIsUnsafe` |

Plus the degraded-replica runner gate is cited at run id `20260513-151339-56c2` (PASS 9/9 phases / 47/47 actions).

### HG-8 cleanup hygiene — PASS

After the RF=2 app-baseline rerun `20260513-162339-aee3` (which exercises dev's HG-8 fix):

```text
iSCSI sessions:                          No active sessions
iSCSI nodes DB:                          No records found
blockmaster/blockvolume/blockcsi procs:  none
kubectl port-forward svc/blockmaster:    none
app=sw-blockvolume Deployments:          none
run-scoped /var/lib/sw-block/testops-*:  (no testops-* paths)   ← LEAKED PATH NOW REMOVED
```

Dev fixed the chain at `testops/scenarios/mounted-failover-rf2-app-baseline-chain.yaml`:
`collect_and_cleanup` now does `sudo rm -rf -- /var/lib/sw-block/testops-{{ run_id }}-rf2-app`
(plus a glob sweep for any other `testops-{{ run_id }}-*` paths) and `final_asserts`
adds `test ! -e /var/lib/sw-block/testops-{{ run_id }}-rf2-app` to catch future regressions.
The chain now runs 42/42 actions vs the prior 41/41 — one extra action is the new assertion.

(Older `/var/lib/sw-block/pvc-*` directories from prior plan demos exist but are outside
the `testops-*` namespace this gate enforces, and the prior plan's HG-7 disclosed them
as non-claim residue. Not addressed by this fix, not part of this gate.)

### HG-9 non-claims honest — PASS

`docs/operations-v1.md` "Not claimed:" list includes every gate-required item:

```text
- production HA,
- node loss or host-disk failure,
- remote-node attach to a loopback-published blockvolume,
- automatic multi-node scheduling, rescheduling, or rebalancing,
- RF=2 recovery/promotion after primary failure,
- RF=3 Kubernetes lifecycle or mounted failover,
- upgrade or broad uninstall safety,
- repair, rebuild, promote, backup, or restore commands,
- performance SLOs,
- UI or operator-grade reconciliation.
```

`docs/quickstart-kubernetes.md` reinforces: `rf2-rf3-live-kubernetes-operation: live RF=2/RF=3 Kubernetes operation is not claimed unless a runner gate explicitly proves it`, `Failover while a PVC remains mounted is not claimed`, `Remote-node attach to a loopback-published blockvolume is not claimed`.

`internal/docs/current-plan.md`: `alpha_non_claim: transparent in-place I/O continuation is not claimed; pod restart on the same node is the expected path`, `Live RF=2/RF=3 Kubernetes lifecycle is still a non-claim`.

No user-facing doc implies RF=2 automatic recovery, transparent in-place I/O continuation, node loss survival, remote-node attach to loopback frontends, sync-quorum/sync-all durability, rebuild/reintegration, performance SLOs, or upgrade safety.

## Residue audit

```text
iSCSI sessions:                                 No active sessions
iSCSI nodes DB:                                 No records found
NVMe subsystems:                                (not used in this alpha path)
blockmaster/blockvolume/blockcsi/iscsi-target:  none
kubectl port-forward svc/blockmaster:           none
app=sw-blockvolume Deployments:                 none
run-scoped /var/lib/sw-block/testops-*:         leftover testops-20260513-153536-af82-rf2-app
                                                (manually removed by QA after audit; see HG-8 finding)
non-testops /var/lib/sw-block/pvc-*:            pre-existing from prior plan demos (out of scope for this gate per its
                                                run-scoped phrasing)
```

## Blocking findings

None. The prior HG-8 residue finding (RF=2 app-baseline chain leaking
`/var/lib/sw-block/testops-<run_id>-rf2-app`) was closed by dev's edit to
`testops/scenarios/mounted-failover-rf2-app-baseline-chain.yaml` —
`collect_and_cleanup` now runs `sudo rm -rf` over the run-scoped path
and `final_asserts` adds `test ! -e` to catch future regressions. QA
reran the chain at run `20260513-162339-aee3` and verified the path is
gone.

## Non-blocking findings

None.

## Close recommendation

```text
PASS (strict) — the plan is clear to move from current-plan.md to
finished-plans/.
```

The substantive product claim — RF=2 mounted app path works end-to-end,
controlled primary failure produces a self-explaining safe-refusal bundle
when the peer is not promotion-ready, and inventory honestly reports the
post-failure state without claiming recovery — is fully demonstrated.
Cleanup hygiene is now also clean after dev's chain fix.
