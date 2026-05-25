# QA Sign-off - Phase 31 D4 RF3 Promotion Restart Persistence

Verdict: **PASS** for product claims; scenario carries two non-blocking
flakes worth carrying as D5+ hardening.

Date: 2026-05-25

Validated source commit: `2bbd7b5 testops: add RF3 promotion restart gate`

## Scope

Independent QA replay of Phase 31 D4. Verifies that after a promotion event
plus a k3s restart on hostPath persistence, the promoted primary, publish
target, and epoch survive without rolling back.

## Run Summary

| Attempt | QA run ID | Result | Notes |
|---|---:|---|---|
| r1 | `20260525-105248-273a` | FAIL at `restart_k3s_and_verify_authority` action 0 | port-forward race after restart; flake N1 below |
| r2 | `20260525-105707-5a93` | Product phases PASS; cleanup wrapper FAIL | overlapping D5 multi-volume run polluted shared lab during r2's cleanup; flake N2 below |

Dev baseline: `20260525-104247-d6da`, 34/34 PASS.

## Product Hard-Claim Compliance (from r2)

Despite r2's wrapper-side failure, the product-claim phases passed and
emitted measured evidence:

| Claim | Value |
|---|---|
| `restart_promotion_status` | `ok` |
| `volume_id` | `pvc-55de4ab6-53bd-4a74-bb2e-1bd67a48ad94` |
| `before_restart_primary` | `r2` |
| `after_restart_primary` | `r2` (preserved) |
| `before_restart_publish_target` | `192.168.1.184:3260` |
| `after_restart_publish_target` | `192.168.1.184:3260` (preserved) |
| `before_restart_epoch` | `2` |
| `after_restart_epoch` | `2` (monotonic, no rollback) |
| `post_restart_primary_count` | `1` (no split brain) |
| `reason` | `authority_persisted` |
| reader-after-restart.log | `/data/demo.bin: OK` (data preserved across restart) |

Phase-level transition for r2:

| Phase | Result |
|---|---|
| pre_clean | PASS |
| build_and_generate_values | PASS |
| helm_install_stack | PASS |
| promote_and_reattach | PASS |
| restart_k3s_and_verify_authority | **PASS** (the claim phase) |
| reader_after_restart | **PASS** (the data check) |
| helm_uninstall_cleanup | FAIL (timed out at 9m - see N2) |

All Phase 31 D4 product claims are independently verified.

## Hard-Gate Acceptance

| Requirement | Result |
|---|---|
| Scenario strict PASS | NOT MET on r1 (port-forward flake) and r2 (cleanup wrapper timeout); product phases PASS |
| `restart_promotion_status=ok` | PASS |
| `before_restart_primary == after_restart_primary` | PASS (r2 -> r2) |
| `after_restart_epoch >= before_restart_epoch` | PASS (2 == 2) |
| `post_restart_primary_count=1` | PASS |
| `reader-after-restart.log` contains `/data/demo.bin: OK` | PASS |
| `cleanup_status=ok` | NOT WRITTEN on r2 (cleanup wrapper timed out before summary was emitted) |

Recommend: re-issue the scenario from a serial lab (no overlapping run), with
the N1 port-forward fix, to confirm the full strict-PASS shape that dev got
on `20260525-104247-d6da`.

## Non-Blocking Findings

### N1: Port-forward race after k3s restart

`restart_k3s_and_verify_authority` action 0 in r1 failed with:

```text
sw-block ops cluster: rpc error: code = Unavailable desc = connection error:
  desc = "transport: Error while dialing: dial tcp 127.0.0.1:51665:
   connect: connection refused"
```

Port-forward log:

```text
error: error upgrading connection: unable to upgrade connection:
  pod does not exist
```

Sequence: k3s restart -> `kubectl rollout status` reports the new
blockmaster Deployment is ready -> the scenario starts
`kubectl port-forward deploy/sw-blockmaster ${port}:9333` -> kubectl
selects ONE pod at command time, but if that selection lands on a
transient pre-rollout pod that has since been deleted, the upgrade fails
immediately.

The same race did NOT trigger on r2 or on the dev's `20260525-104247-d6da`.
1-in-3 observed flake rate this cycle.

Fix shape:

```bash
# pick the freshest Running pod by name, then port-forward to that pod
pod=$(kubectl -n kube-system get pod \
  -l app.kubernetes.io/component=blockmaster \
  --field-selector=status.phase=Running \
  -o jsonpath='{.items[?(@.status.conditions[?(@.type=="Ready" && @.status=="True")])].metadata.name}' \
  | awk '{print $1}')
kubectl -n kube-system port-forward "pod/${pod}" "${port}:9333" &
```

Or retry the whole port-forward + ops cluster sequence with a bounded loop.

### N2: r2 cleanup wrapper hung due to overlapping lab use

r2's `helm_uninstall_cleanup` action 0 timed out at exactly its 9-minute
budget. While my r2 was in progress, another scenario started on the same
lab:

```text
/v/share/g15d-k8s/20260525-105611-2ab3-helm-multi-volume-rf3-restart
```

This is the Phase 31 D5 multi-volume restart smoke. At the time my r2
reached cleanup, that parallel run still had 4 sw-blockvolume pods +
multiple reader pods on the cluster (default ns). My r2's cleanup script
attempted to wait for blockvolume Deployment removal and stalled because
those pods belonged to the parallel run, not to r2.

Lab residue at the end of r2's cycle:

```text
helm release sw-block:     none (uninstalled by r2)
iSCSI sessions:            No active sessions
multipath -ll:             empty
dmsetup ls:                No devices
sw-blockvolume pods:       4 still running (from D5 multi-volume run)
sw-block-multi-reader pods: 2 still Completed (from D5)
per-run hostpath:          (D4's testops-... scoped path absent)
```

Fix shape:

- Serialize Phase 31 D-scenarios on the shared 3-node lab; OR
- Tighten `pre_run_cleanup` to wait for *any* `app=sw-blockvolume` to be
  absent in the target namespace before declaring the lab clean; OR
- Run each scenario in its own lab/namespace partition.

This is a TestOps/lab-orchestration finding, not a Phase 31 D4 product
regression. The actual D4 claim (authority survives restart) was verified
on r2 before the cleanup phase fired.

## Verdict

**Phase 31 D4 product claims PASS** on independent rerun. The promoted
primary, publish target, and epoch survive a k3s restart on hostPath
persistence; post-restart `primary_count=1`; reader data is preserved.

The scenario itself has two flakes worth pulling into D5+ hardening before
nightly/release validation:

- **N1**: port-forward-after-restart pod-selection race (1-in-3 observed).
- **N2**: cleanup wrapper does not tolerate overlapping lab runs.

Strict-PASS recommendation: re-issue the scenario from a serially-owned lab
once N1 is fixed; expect full 34/34 PASS shape matching dev baseline
`20260525-104247-d6da`.

D5 multi-volume restart smoke runs in parallel to my D4 cycle today; that
QA sign-off will be a separate doc once lab access serializes.
