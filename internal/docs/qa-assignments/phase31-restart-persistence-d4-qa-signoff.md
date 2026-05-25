# QA Sign-off - Phase 31 D4 RF3 Promotion Restart Persistence

Verdict: **PASS (strict)** after dev fixes for N1/N2 landed in `75a5660`.

Date: 2026-05-25

Validated source commits:
- `2bbd7b5 testops: add RF3 promotion restart gate` (original cycle)
- `75a5660 testops: harden RF3 restart persistence gate` (N1/N2 fix cycle)

## Scope

Independent QA replay of Phase 31 D4. Verifies that after a promotion event
plus a k3s restart on hostPath persistence, the promoted primary, publish
target, and epoch survive without rolling back.

## Run Summary

| Attempt | Source commit | QA run ID | Result | Notes |
|---|---|---:|---|---|
| r1 | `2bbd7b5` | `20260525-105248-273a` | FAIL at `restart_k3s_and_verify_authority` action 0 | port-forward race after restart; N1 below |
| r2 | `2bbd7b5` | `20260525-105707-5a93` | Product phases PASS; cleanup wrapper FAIL | overlapping D5 multi-volume run polluted shared lab during r2's cleanup; N2 below |
| **r3 (strict)** | `75a5660` | `20260525-122723-f7ed` | **34/34 strict PASS** | dev's N1 + N2 fixes both confirmed; serially-owned lab |

Dev baselines: `20260525-104247-d6da` (original), `20260525-122104-60c3` (post-fix).

## Product Hard-Claim Compliance (from r3 strict run)

| Claim | Value |
|---|---|
| `restart_promotion_status` | `ok` |
| `volume_id` | `pvc-19b2f097-5795-4303-87aa-66d8eb6d2295` |
| `before_restart_primary` | `r2` |
| `after_restart_primary` | `r2` (preserved) |
| `before_restart_publish_target` | `192.168.1.184:3260` |
| `after_restart_publish_target` | `192.168.1.184:3260` (preserved) |
| `before_restart_epoch` | `2` |
| `after_restart_epoch` | `2` (monotonic, no rollback) |
| `post_restart_primary_count` | `1` (no split brain) |
| `reason` | `authority_persisted` |
| reader-after-restart.log | `/data/demo.bin: OK` (data preserved across restart) |

Cleanup-summary (r3): `cleanup_status=ok`, all 5 residue counts 0,
`failure_count=0`.

Lab post-r3 host audit: helm none, iSCSI no sessions, multipath empty,
dmsetup No devices, no sw-block pods.

Same evidence on r2 (product phases only) modulo `volume_id`.

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

| Requirement | Result (r3 strict) |
|---|---|
| Scenario strict PASS | **PASS (34/34)** |
| `restart_promotion_status=ok` | PASS |
| `before_restart_primary == after_restart_primary` | PASS (r2 -> r2) |
| `after_restart_epoch >= before_restart_epoch` | PASS (2 == 2) |
| `post_restart_primary_count=1` | PASS |
| `reader-after-restart.log` contains `/data/demo.bin: OK` | PASS |
| `cleanup_status=ok` | PASS |
| Final residue zero | PASS |

## Non-Blocking Findings — History

Both findings from the original cycle are **RESOLVED** by `75a5660`
("testops: harden RF3 restart persistence gate"). QA-confirmed on r3.

### N1: Port-forward race after k3s restart — RESOLVED

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

**Resolution evidence (r3)**: port-forward log on r3 shows clean
`Forwarding from 127.0.0.1:50967 -> 9333` + `Handling connection for 50967`
with no `pod does not exist` error. `restart_k3s_and_verify_authority`
PASS on r3.

### N2: r2 cleanup wrapper hung due to overlapping lab use — RESOLVED

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

**Resolution evidence (r3)**: pre-clean phase on r3 ran on a serially-owned
lab. Dev's `75a5660` also tightens pre-clean to delete generated
blockvolume Deployments and fail if they remain — preventing dirty
shared-lab starts. r3's full cycle (including helm_uninstall_cleanup)
PASSed in clean time; final cleanup-summary all zeros.

## Verdict

**Phase 31 D4 PASS (strict)** after the N1 + N2 fixes in `75a5660`.

Strict r3 confirms the documented hard claim shape:
- 34/34 actions PASS
- promotion + epoch + publish target preserved across k3s restart
- post-restart primary_count=1 (no resurrection of old primary)
- reader data preserved (`/data/demo.bin: OK`)
- cleanup hygienic (all 5 residue counters = 0)

Phase 31 D4 is QA-cleared. D5 multi-volume restart smoke is a separate
sign-off cycle.
