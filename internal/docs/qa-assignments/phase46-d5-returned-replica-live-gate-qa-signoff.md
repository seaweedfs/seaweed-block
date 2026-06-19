# Phase 46 D5 Returned-Replica Live Gate QA Sign-off

Status: PASS for host/live returned-replica safety; K8s status-surface gate
remains separate.

Validated source: `f28ecf3`

Lab: m02 via isolated git-bundle clone at
`/tmp/seaweed_block_phase46_f28ecf3_20260619083402`.

## Gates Run

| Gate | Run | Result |
|---|---:|---|
| returned-replica-component-gate.yaml | `20260619-083445-124f` | PASS, 16/16 |
| iscsi-returned-replica-chain.yaml | `20260619-083410-a413` | PASS, 57/57 |

## Live Evidence

The iSCSI returned-replica chain exercised:

```text
r1 primary -> r2 promoted -> r1 returns
```

Verified evidence:

- `r2` remained the only primary:
  - `AuthorityRole=primary`
  - `FrontendPrimaryReady=true`
  - `Healthy=true`
- returned `r1` remained non-primary and frontend-fenced:
  - `authority_non_primary=true`
  - `frontend_primary_ready=false`
  - `healthy=false`
  - `ReplicationRole=replica_ready`
- `r1` was visible to `r2` as a healthy peer after return:
  - `ReplicaID=r1`
  - `State=healthy`
  - `Epoch=2`
  - `ProbeInFlight=false`
  - `Closed=false`
- durable recovery evidence was present:
  - `blockvolume-r1-returned.log`: `durable recovered: recovered LSN=...`
  - `status-durable-r2-primary-after-r1-return.summary`: `operational=true`
- data path survived failover and return:
  - `pre-check-after-failover.log`: `/pre.bin: OK`
  - `post-check.log`: `/post.bin: OK`
- cleanup was clean:
  - no active iSCSI sessions
  - no blockmaster/blockvolume/iscsi-target processes

## Product Fix Proven By The Gate

The first live attempts exposed a real product gap: if an assignment arrived
while durable recovery still blocked local readiness, the host dropped that
assignment and did not replay it after recovery. The returned old primary was
safe (`Healthy=false`, `FrontendPrimaryReady=false`) but not properly projected
as a supporting `replica_ready` replica.

Fix in `c235b86`:

```text
local readiness blocked assignment
-> durable recovery clears block
-> replication volume is installed
-> last blocked assignment is replayed
-> returned replica is admitted as SUPPORTING
-> frontend remains gated
```

The final run confirms the intended log and status:

```text
replaying assignment after local readiness cleared
admitted as SUPPORTING replica r1 ... frontend remains gated
ReplicationRole=replica_ready
FrontendPrimaryReady=false
Healthy=false
```

## Non-Claims

This D5 live gate does not claim automatic failback or automatic rebuild
execution. It proves the returned replica is visible, fenced from frontend use,
and usable as a healthy peer after recovery evidence.

This D5 live gate also does not by itself close the K8s status-surface
requirement. CRD/report/dashboard/explain agreement for returned-replica
projection remains a separate product-surface gate.

## Verdict

D5 host/live returned-replica safety is PASS. The next Phase 46 work should
close the K8s product-surface gate and then the multi-volume close gate.
