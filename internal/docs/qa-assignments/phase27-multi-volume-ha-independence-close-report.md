# QA Close - Phase 27 Multi-Volume HA Independence

Date: 2026-05-23

Verdict: PASS (strict). All four Phase 27 D1-D4 scenarios pass on a clean
3-node k3s lab with independent QA rerun. Phase 27 is ready to close.

## Scope

Validates the Phase 27 claim:

```text
N PVCs at RF=3 sync-quorum
-> each volume has independent primary / replica set / frontend
-> each volume independently fails over (CSI reattach OR mounted transparent)
-> a fault on one volume does NOT interrupt other volumes
-> primary_count=1 per volume post-failover
-> stale primary fenced; old-primary stale I/O success count = 0
-> cleanup leaves no Helm release, no iSCSI sessions, no leftover processes
```

Not in scope: broad production HA, > 3 volumes, arbitrary node failure,
performance/RTO/SLO, NVMe ANA, operator/CRD lifecycle, mutating admin
workflows.

## Method

- Synced branch (head `f7579af testops: add interleaved multi-volume failover
  gate`) from Windows controller to m02 via tar/scp/extract.
- Lab pre-state confirmed clean: no `sw-block` helm release, no active iSCSI
  sessions, no sw-block pods.
- Ran the four scenarios sequentially via `swblock.exe run` from Windows.
- Each scenario builds its own phase27 images locally
  (`sw-block:phase27-rf3`, `sw-block:phase27-rf3-reattach`,
  `sw-block:phase27-rf3-mounted`, `sw-block:phase27-rf3-interleaved`) and
  imports them to k3s on m01 and tp01.

## Gate Results

| Gate | QA run | Actions | Dev-cited run |
|---|---:|---|---|
| D1 multi-volume RF=3 readiness (no fault) | `20260523-094437-c24c` | 35/35 PASS | n/a (QA-authored spike) |
| D2 multi-volume CSI reattach failover | `20260523-094707-bbf5` | 29/29 PASS | dev's pre-cited |
| D3 multi-volume mounted transparent failover | `20260523-095122-a5c4` | 47/47 PASS | `20260523-090534-24e4` |
| D4 multi-volume interleaved concurrent failover | `20260523-095509-4d02` | 55/55 PASS | `20260523-093348-6b02` |

Total: 166/166 actions across QA reruns.

## Hard-Gate Acceptance Evidence

### D1 - Multi-volume RF=3 readiness (no fault)

`/v/share/g15d-k8s/20260523-094437-c24c-helm-multi-volume-rf3/multi-volume/multi-volume-summary.txt`:

```text
multi_volume_status=ok
requested_volume_count=3
replication_factor=3
writer_verified_count=3
reader_verified_count=3
managed_volume_count=3
cleanup_status=ok
```

3 PVCs at RF=3, 9 blockvolume pods (3 per node), 3 distinct primaries
(r1@m01, r2@m02, r3@tp01), all writers + readers verified, cleanup clean.

### D2 - Multi-volume CSI reattach failover

Per-volume artifacts under
`/v/share/g15d-k8s/20260523-094707-bbf5-helm-multi-volume-rf3-reattach/recovery/failover/volume-{1,2,3}/summary.txt`:

| Vol | before_primary -> promoted | reader_verified | post_failure_primary_count | cross_interference |
|---|---|---|---:|---|
| 1 | r1@m01 -> r2 | true | 1 | false |
| 2 | r2@m02 -> r1 | true | 1 | false |
| 3 | r3@tp01 -> r1 | true | 1 | false |

`recovery/multi-volume-reattach-summary.txt`:

```text
multi_volume_reattach_status=ok
recovered_volume_count=3
cross_interference_observed=false
```

All three target volumes promoted a different replica, reader pod re-mounted
and verified data, non-target volumes did not see primary changes.

### D3 - Multi-volume mounted transparent failover (no pod recreate)

Per-volume artifacts under
`/v/share/g15d-k8s/20260523-095122-a5c4-helm-multi-volume-rf3-mounted-failover/recovery/failover/volume-{1,2,3}/summary.txt`:

| Vol | before_primary -> promoted | writer pod UID before == after | stale_io_success | mounted checksum |
|---|---|---|---:|---|
| 1 | r1@m01 -> r2 | same UID `034c9c31-...` | 0 | passed |
| 2 | r2@m02 -> r1 | same UID `024086dc-...` | 0 | passed |
| 3 | r3@tp01 -> r1 | same UID `f2142e8d-...` | 0 | passed |

`recovery/multi-volume-mounted-failover-summary.txt`:

```text
multi_volume_mounted_failover_status=ok
recovered_volume_count=3
mounted_workload_checksum_passed_count=3
pod_recreate_used=false
cross_interference_observed=false
transparent_failover_claimed=true
```

Every target volume failed over without the workload pod being recreated.
Stale primary I/O success count = 0 per volume (fencing works).

### D4 - Multi-volume interleaved concurrent failover

`/v/share/g15d-k8s/20260523-095509-4d02-helm-multi-volume-rf3-interleaved-failover/recovery/failover/interleaved-summary.txt`:

```text
interleaved_fault_window_seconds=0.472
interleaved_target_volume_count=2
untouched_volume_stable=true
untouched_workload_ok=true
```

Per-volume artifacts confirm both target volumes promoted independently in the
same 0.472s window:

| Vol | interleaved_fault | promoted | pod_recreate_used | stale_io_success | mounted checksum |
|---|---|---|---|---:|---|
| 1 | true | r1 -> r2 | false | 0 | passed |
| 2 | true | r2 -> r1 | false | 0 | passed |
| 3 (untouched) | n/a | unchanged | unchanged | n/a | stable + writable |

Run-level summary `recovery/multi-volume-mounted-failover-summary.txt`:

```text
multi_volume_interleaved_failover_status=ok
recovered_volume_count=2
mounted_workload_checksum_passed_count=2
pod_recreate_used=false
cross_interference_observed=false
interleaved_fault_window_seconds=0.472
untouched_volume_stable=true
untouched_workload_ok=true
transparent_failover_claimed=true
```

### Final residue audit

Required-by-assignment checks (all clean):

```text
helm release sw-block:                  none
iSCSI active sessions:                  none
iSCSI nodes DB (io.seaweedfs):          none
generated app=sw-blockvolume Deployments: none
sw-block / blockvolume pods (default + kube-system): none
per-host product processes (m01 / m02 / tp01): none
```

Per-scenario `cleanup-summary.txt` records all show `cleanup_status=ok`,
`k8s_residue_count=0`, `process_residue_count=0`, `hostpath_residue_count=0`,
`failure_count=0`.

### Non-blocking residue observation

After all four runs, `sudo multipath -ll` on m02 shows two stale
`mpath` maps (`mpathad`, `mpathae`, size=1.0M, matching PVC size). Underlying
iSCSI sessions are gone, so these are orphaned dm-multipath entries left
over by the mounted-failover cleanup path. Not in the assignment's required
residue list and not a Phase 27 hard-gate failure, but a real cleanup-verifier
gap worth filing as a v0.3.1+ follow-up. Suggested fix shape: extend
`scripts/verify-helm-cleanup.sh` to run `multipath -ll` and assert no maps
match the sw-block PVC iSCSI IQN substring.

## Hard-Gate Acceptance Summary

| Requirement | Result |
|---|---|
| D1 multi-volume RF=3 readiness passes | PASS |
| D1 3 distinct primaries on 3 distinct nodes | PASS |
| D1 3 ManagedVolume rows in report at rf=3 | PASS |
| D2 each target volume promotes a different replica | PASS (3/3) |
| D2 post_failure_primary_count=1 per target | PASS (3/3) |
| D2 reader_verified=true per target | PASS (3/3) |
| D2 cross_interference_observed=false per target | PASS (3/3) |
| D3 pod_recreate_used=false per target | PASS (3/3) |
| D3 writer pod UID preserved across fault | PASS (3/3) |
| D3 stale primary I/O success count = 0 per target | PASS (3/3) |
| D3 mounted workload checksum survives | PASS (3/3) |
| D3 transparent_failover_claimed=true | PASS |
| D4 interleaved fault window bounded | PASS (0.472s) |
| D4 both target volumes recover independently | PASS |
| D4 third volume untouched + stable + writable | PASS |
| D4 cross_interference_observed=false | PASS |
| Residue clean (helm/iSCSI/processes/deployments) | PASS |

## Claim Boundary

Phase 27 supports these claims:

- N=3 PVCs at RF=3 sync-quorum can coexist on a 3-node lab.
- Each volume has independent primary / replica / frontend assignment.
- CSI reattach failover works per-volume without cross-interference (D2).
- Mounted transparent failover works per-volume without pod recreate (D3).
- Two concurrent failovers within a sub-second window resolve independently
  while a third volume remains stable and writable (D4).
- Cleanup is hygienic at the K8s / Helm / iSCSI / process layer.

Phase 27 does NOT claim:

- N > 3 PVCs (only 3 tested).
- Arbitrary node failure / Kubernetes node-loss for multi-volume.
- Performance / RTO / SLO numbers for multi-volume failover.
- NVMe ANA parity for multi-volume mounted failover.
- Operator / CRD lifecycle integration.
- Broad production HA without scenario-bounded conditions.

## PM Review Notes

| Claim wording check | Status |
|---|---|
| Multi-volume RF=3 readiness scoped to 3-volume / 3-node lab | OK |
| Mounted transparent failover claim conditioned on iSCSI ALUA + dm-multipath | OK (per D3 evidence) |
| Interleaved failover window bounded (<1s tested) | OK |
| "Multi-volume HA independence" not generalized to "production multi-volume HA" | OK if v0.3.2 release note carries the same non-claim discipline |
| Stale primary fencing per volume named explicitly | OK |

Recommended PM-visible wording for release note:

> Seaweed Block supports multiple PVC-backed volumes at RF=3 with
> independent per-volume failover on the supported 3-node alpha lab. Each
> volume's primary failover (CSI reattach or iSCSI mounted transparent) does
> not interrupt other volumes' workloads, including under concurrent
> sub-second fault windows. Broader production multi-volume HA, multi-node
> failure, performance SLOs, and operator lifecycle are out of scope.

## Non-Claims

Phase 27 does not deliver:

- production-grade multi-volume HA,
- node-loss failover for multi-volume,
- > 3 PVC capacity claim,
- backup/snapshot/restore at multi-volume scope,
- operator/CRD lifecycle,
- mutating admin actions,
- performance/RTO/SLO claims,
- multipath-residue cleanup verifier (carry as follow-up).

## Verdict

PASS for Phase 27 scope.

Recommended close sequence:

1. Mark Phase 27 done; cut v0.3.2 alpha release note using this report as
   gate evidence and the PM-visible wording above.
2. File the multipath-residue cleanup gap as a v0.3.2+ follow-up
   (`scripts/verify-helm-cleanup.sh` extension).
3. Publish phase 27 images to GHCR and pin the SHA in the release note
   (the four scenarios currently build local images per gate; users need a
   published SHA floor before they can consume D2/D3/D4 behavior).
