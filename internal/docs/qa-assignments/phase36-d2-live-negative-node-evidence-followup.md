# Phase 36 D2 Follow-up - Live Negative Node Evidence

Status: open follow-up.

Discovered by QA during Phase 36 D2 validation on 2026-06-05.

## Finding

`SwBlockCluster.status.nodes[]` and `operator-snapshot.json.cluster.nodes[]`
project node readiness correctly when `NodeEvidence` contains real facts.
However, the live `--master-api` source cannot currently produce unhealthy node
evidence.

`core/host/master/observation_snapshot.go:observationNodes` derives node
evidence from lifecycle registrations and currently hardcodes:

```go
Schedulable: true
Ready:       true
```

It also does not populate `MissingImages`.

Result: live registered nodes always classify as `ready/node_ready`. The D2
negative node reasons are correct and unit-tested but unreachable from the live
blockmaster surface:

- `node_not_ready`
- `node_scheduling_disabled`
- `image_missing_on_node`

## Impact

D2 still passes because:

- G1 1-node live healthy path passed,
- G2 3-node live healthy path passed,
- G3 missing-image node blocker passed via replay,
- G4 read-only boundary passed.

But the live product surface cannot yet prove node blockers without replayed or
crafted evidence.

## Required Fix Shape

Populate live `NodeEvidence` from real sources:

- Kubernetes node Ready condition,
- Kubernetes unschedulable/schedulable state,
- image-presence check for required sw-block images,
- evidence refs for the node/preflight source.

Then add a live negative-node TestOps gate that produces at least one of:

```text
status=unknown reason=node_not_ready
status=blocked reason=node_scheduling_disabled
status=blocked reason=image_missing_on_node
```

## Boundary

The fix must remain read-only. It may observe Kubernetes and image state, but it
must not import images, uncordon nodes, mutate pods, mutate PVCs, or clean host
state.
