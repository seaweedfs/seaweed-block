# Phase 34 D3 - Restart Status Convergence Sign-off

Status: PASS on 2026-05-29.

## Scope

D3 separates two restart semantics:

- No-fault restart convergence: after k3s/product restart, the user-facing
  status surface must eventually return to stable `Ready=True`.
- Degraded promotion restart boundary: after a promoted RF3 volume restarts
  with the old failed replica still absent, the surface must remain stable and
  honest. `Unknown/status_endpoint_unreachable` is acceptable; false
  `Ready=True` or `Blocked=True` is not.

This avoids treating a deliberately degraded old-primary state as a failure
while still adding a hard final-convergence gate for the no-fault user path.

## Scenario Changes

Updated:

- `testops/scenarios/helm-single-node-restart-persistence-chain.yaml`
- `testops/scenarios/helm-rf3-promotion-restart-persistence-chain.yaml`

The single-node scenario now includes `post_restart_status_convergence`, which
polls live `sw-block ops report` until the target ManagedVolume is Ready for
three consecutive observations.

The RF3 promotion scenario now includes
`post_restart_degraded_status_boundary`, which requires three consecutive safe
observations. Safe means either:

- `status=ready` with `Ready=True`, or
- `status=unknown reason=status_endpoint_unreachable`

and in both cases:

- no `Blocked=True`
- no false readiness claim
- cleanup remains strict

## D3 Ready-Convergence Run

Scenario:

- `helm-single-node-restart-persistence-chain.yaml`

Run:

- Run ID: `20260529-185123-5078`
- Result: PASS, 45/45 actions
- Runtime: 2m42.348s

Convergence artifact:

```text
restart_status_convergence=ok
volume_id=pvc-ad55d5e0-52e3-4878-a138-c8eff442f3d3
required_consecutive_ready=3
max_consecutive_ready=3
final_reason=first_volume_verified
evidence=observations.txt
```

Observations:

```text
1: status=ready reason=first_volume_verified ready_true=true blocked_true=false consecutive_ready=1
2: status=ready reason=first_volume_verified ready_true=true blocked_true=false consecutive_ready=2
3: status=ready reason=first_volume_verified ready_true=true blocked_true=false consecutive_ready=3
```

This closes the original gap: the test no longer accepts a single transient
Ready or stops at a safe Unknown. It requires stable Ready across three
consecutive live report observations.

## RF3 Degraded Boundary Run

Scenario:

- `helm-rf3-promotion-restart-persistence-chain.yaml`

Run:

- Run ID: `20260529-185413-5c72`
- Result: PASS, 39/39 actions
- Runtime: 2m34.238s

Boundary artifact:

```text
restart_status_boundary=ok
volume_id=pvc-38c1c612-1f22-4631-947b-984042f205c5
required_consecutive_stable=3
max_consecutive_stable=3
final_reason=status_endpoint_unreachable
evidence=observations.txt
```

Observations:

```text
1: status=unknown reason=status_endpoint_unreachable ready_true=false blocked_true=false consecutive_stable=1
2: status=unknown reason=status_endpoint_unreachable ready_true=false blocked_true=false consecutive_stable=2
3: status=unknown reason=status_endpoint_unreachable ready_true=false blocked_true=false consecutive_stable=3
```

The RF3 promoted-authority claim still passes: primary, publish target, epoch,
reader data, and cleanup are verified by the existing scenario. The new boundary
check confirms the degraded status surface does not lie by claiming Ready and
does not misclassify missing evidence as Blocked.

## Verdict

D3 PASS.

Restart convergence is now tested at L3-ish timing realism for the no-fault
path: restart, live report polling, and stable Ready for three consecutive
observations. The promoted-degraded path remains negative-first and does not
over-claim readiness when a failed old replica remains unreachable.
