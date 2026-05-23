# QA Spike - Phase 27 Multi-Volume HA Independence (D1)

Date: 2026-05-22

This is a QA-side spike for the Phase 27 "Multi-Volume HA Independence" track
that was scoped during v0.3.1 release discussion.

## D1 Result - PASS

| Sub-gate | Scenario | Run | Result |
|---|---|---:|---|
| D1 Multi-volume RF=3 readiness (no fault) | `helm-multi-volume-rf3-readiness-chain.yaml` | `20260522-170901-70c5` | 35/35 PASS |

D1 proves the lowest-bar readiness claim: N PVCs at RF=3 can coexist and serve
writer/reader workloads concurrently on a 3-node lab.

### Evidence

`/v/share/g15d-k8s/20260522-170901-70c5-helm-multi-volume-rf3/multi-volume/multi-volume-summary.txt`:

```text
multi_volume_status=ok
requested_volume_count=3
replication_factor=3
writer_verified_count=3
reader_verified_count=3
managed_volume_count=3
cleanup_status=ok
```

`/v/share/g15d-k8s/20260522-170901-70c5-helm-multi-volume-rf3/multi-volume/status/report/summary.txt`:

```text
volumes=3  nodes=3  events=1024
volume=pvc-...-1 primary=r1@m01 frontend=192.168.1.181:3260 rf=3
volume=pvc-...-2 primary=r2@m02 frontend=192.168.1.184:3259 rf=3
volume=pvc-...-3 primary=r3@tp01 frontend=192.168.1.188:3259 rf=3
managed_volume = ready x 3
```

Key observations:

- 9 blockvolume pods total (RF=3 x 3 volumes), placed 3 per node.
- 3 distinct primaries, one on each node (`r1@m01`, `r2@m02`, `r3@tp01`).
- iSCSI frontend ports allocated per-volume per-node without collision
  (3260 on m01, 3259 on m02, 3259 on tp01).
- Phase 26 placement-port persistence + observation-slot merge changes hold up
  at this load.
- Helm uninstall + launcher async-delete of all 9 generated blockvolume
  Deployments completes cleanly within ~2-3 min.

### Helper change

`scripts/run-multi-volume-example.sh` extended to read
`SW_BLOCK_MULTI_VOLUME_RF` env (default `1`, backward compatible). The
generated StorageClass now uses that value, and the summary emits
`replication_factor=<rf>`.

## What D1 Does NOT Prove

D1 is a **readiness** gate, not an HA gate. Specifically:

- No fault is injected. All 9 blockvolume pods stay healthy throughout.
- No primary failover is exercised.
- Cross-volume non-interference under fault is not measured.

The full Multi-Volume HA Independence bar requires D2/D3/D4 below.

## Proposed D2 - Multi-Volume CSI Reattach Failover

**Goal**: For each of N volumes, kill the current primary blockvolume
Deployment; verify the volume promotes a new primary, the workload pod
re-mounts (pod-recreate is allowed in D2), and the OTHER volumes' workloads
are not interrupted.

**Fault model**: `kubectl scale deploy/<primary-blockvolume> --replicas=0` per
volume in sequence, pod recreate allowed for the affected workload.

**Helper needed**: extend `scripts/run-multi-volume-example.sh` with a new
mode (or new script `scripts/run-multi-volume-failover.sh`) that:

1. Sets up N PVCs at RF=3, writers verify.
2. Records pre-failure `(volume, primary_replica_id, primary_node)` triples.
3. For target volume k:
   - Identify primary blockvolume Deployment: name pattern
     `sw-blockvolume-<pvc-id>-r<role>-<suffix>` where `<role>` is the current
     primary replica id from `sw-block ops volumes -o json`.
   - `kubectl scale --replicas=0` that Deployment.
   - Poll `sw-block ops volumes -o json` until `primary_replica_id` changes
     and `primary_count=1` for volume k.
   - Apply a new reader pod that mounts PVC-k and verifies the writer's
     SHA256.
4. After each per-volume failover: assert that the **other** volumes still
   show the same primary, same frontend, ManagedVolume Ready=True.
5. Emit `multi-volume-failover-summary.txt` with one block per (volume,
   target_volume) pair:
   - `failover_target=<k>`
   - `pre_primary_replica=<rN>` / `pre_primary_node=<node>`
   - `post_primary_replica=<rM>` / `post_primary_node=<node>`
   - `promotion_within=<sec>`
   - `reader_verified=<true/false>`
   - `other_volumes_primary_unchanged=<true/false>`
   - `cross_interference_observed=<false>`

**Assertions in scenario**:

- For each target k: `pre_primary_replica != post_primary_replica`.
- For each target k: `reader_verified=true`.
- For each target k: `other_volumes_primary_unchanged=true`.
- `post_failure_primary_count=1` per volume.
- Final cleanup: no leftover Deployments, no iSCSI sessions, no processes.

**Hard gate**: all of the above; if any non-target volume's primary changes
during failover of target k, that is a cross-volume interference failure.

## Proposed D3 - Multi-Volume Mounted Failover (no pod recreate)

**Goal**: For each volume, kill the primary while the workload pod stays
mounted. The iSCSI multipath stack (with ALUA + dm-multipath) must transparently
fail over to a surviving replica. Other volumes unaffected.

**Fault model**: cordon primary node + scale primary blockvolume Deployment
to zero, BUT workload pod stays running. The reader inside the workload pod
should continue reading from `/data/demo.bin` via the dm-multipath device.

**Helper needed**: extension of the existing single-volume mounted-failover
helper (`run-k8s-demo.sh` mode `cordon-node-scale-primary-to-zero` with
`SW_BLOCK_DEMO_STOP_AFTER=reader-verified`) to a multi-volume harness.
Likely requires stage-2 multipath enabled (`stage2Multipath: true` in helm
values) - that knob exists in the chart already.

**Assertions**:

- Workload pod uptime > pre-failure timestamp (no recreate during fault).
- Reader checksum continues to verify on the same pod.
- Per-volume `post_failure_primary_count=1`.
- Per-volume `multipath_path_state=active|standby` distribution survives.
- Other volumes' workloads also continuous (no read/write gap).

**Hard gate**: same as D2 plus the "no pod recreate during fault" requirement
and the multipath path-state evidence per volume.

## Proposed D4 - Concurrent / Interleaved Multi-Volume Faults

**Goal**: Inject overlapping faults on multiple volumes simultaneously; prove
that the system promotes each volume independently without deadlock or
cross-contamination.

**Fault model**: Kill primary of volume 1 and volume 2 within the same
~5-second window. Volume 3 untouched.

**Helper needed**: same as D2 / D3 but with parallel `kubectl scale` calls
and parallel post-fault verification.

**Assertions**:

- Both volume 1 and volume 2 promote new primaries; both `primary_count=1`
  post-fault.
- Volume 3 primary unchanged.
- No promotion-of-volume-1 stalls promotion-of-volume-2 (each within
  individual SLA, e.g. < 30s).
- No timeline event shows volume 3 as a side-effect target.

**Hard gate**: per-volume promotion isolation + cross-volume isolation.

## Recommendation

1. **Accept D1 result** (this spike) as proof that the v0.3.1 placement /
   port-assignment scheme supports multi-volume RF=3 readiness on the 3-node
   lab. This narrows Phase 27's scope to fault injection.

2. **Phase 27 planning** should sequence D2 -> D3 -> D4. D2 is the lowest
   bar and the closest in shape to existing single-volume failover gates.
   D3 needs stage-2 multipath on the lab. D4 is the most demanding.

3. **Helper script ownership**: dev should own the multi-volume failover
   helper (`scripts/run-multi-volume-failover.sh` or extending the existing
   helper with a fault mode). The scenario YAMLs are then thin wrappers
   that drive the helper and assert on its output, the same pattern as the
   single-volume `run-k8s-demo.sh` + `node-loss-survival-rf3-reattach-chain.yaml`.

4. **Do not bundle into v0.3.1**. Multi-volume HA independence is a separate
   release-grade claim. Phase 27 should be its own plan / close report / PR.

## Artifacts

- New scenario: `testops/scenarios/helm-multi-volume-rf3-readiness-chain.yaml`
- Helper change: `scripts/run-multi-volume-example.sh` adds
  `SW_BLOCK_MULTI_VOLUME_RF` env and emits `replication_factor=<rf>` in
  summary.
- Run result: `/v/share/g15d-k8s/20260522-170901-70c5-helm-multi-volume-rf3/`
