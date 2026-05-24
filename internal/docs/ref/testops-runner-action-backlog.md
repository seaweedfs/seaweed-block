# TestOps Runner Action Backlog

Date: 2026-05-23

Purpose: convert the runner-native PVC spike and Phase 27/28 cleanup pain into
an executable TestOps backlog. This is not a DSL rewrite plan.

## Decision

Keep the current scenario shape:

```text
phase -> action -> node -> artifact
```

Do not replace mature helper scripts for complex orchestration yet. Helper
scripts still carry valuable retry loops, structured summaries, and
product-specific evidence. Add first-class runner actions only where repeated
shell fragments hide the intent or weaken diagnostics.

## Evidence Inputs

- `internal/docs/qa-assignments/testrunner-product-interface-audit.md`
- `testops/scenarios/experimental-runner-native-pvc-loop.yaml`
- Runner-native PVC spike: `20260523-145417-4f50`, PASS, 22/22 actions
- Phase 28 D2 flake matrix:
  - D3 mounted failover N=5: 5/5 PASS, `flake_rate_percent=0`
  - D4 interleaved failover N=5: 5/5 PASS, `flake_rate_percent=0`

## P0 Actions

| Action | Why | Acceptance Gate |
|---|---|---|
| `assert_no_multipath_maps` | Phase 27 found orphan dm-multipath maps after sessions were gone. This is too important for ad-hoc grep. | Replace one cleanup `exec` grep with the action; verify it fails on `mpath... ##,##` and passes on clean lab. |
| `kubectl_wait_jsonpath` | PVCs, PVs, and many K8s resources do not expose `.status.conditions[]`; current `kubectl_wait_condition` cannot wait for PVC Bound directly. | Runner-native PVC loop waits on `pvc.status.phase=Bound` without shell. |
| `kubectl_wait_completed` | One-shot writer/reader pods become `Succeeded`, not `Ready=True`; current runner-native tests must keep pods sleeping. | Runner-native first-volume loop uses one-shot pods and waits for completion. |
| `helm_install` / `helm_uninstall` | Helm install/uninstall is a stable product path and appears in every Day-1 scenario as raw `exec`. | Helm first-volume scenario installs/uninstalls without raw helm shell wrappers. |

## P1 Actions

| Action | Why | Acceptance Gate |
|---|---|---|
| `assert_alua_aas_transition` | D6 currently parses `sg_rtpg` inside product helper scripts. The claim is important enough to be named. | Given before/after RTPG artifacts, assert old primary `0x00 -> missing/faulty` and promoted path `0x02 -> 0x00`. |
| `iscsi_assert_io_rejected` | D5 stale-primary fencing must stay measured, not hardcoded. | Login/probe stale target and assert read/write failure with captured stderr/sense evidence. |
| `sw_block_ops_cluster` / `sw_block_ops_report` | Product-owned observation is a release surface; scenarios should not hand-roll port-forward + CLI calls every time. | D5 observation gate exports cluster evidence via named runner action. |
| `collect_k8s_snapshot` | Failure bundles need pods, events, deploys, PV/PVC, and CSI logs in a standard shape. | Any failed K8s phase gets the same snapshot directory without bespoke shell. |

## P2 Actions

| Action | Why | Acceptance Gate |
|---|---|---|
| `inject_partition` K8s/node wrapper | Existing chaos tier is unused; partition is the next real HA fault model. | One fail-closed partition scenario with clean rollback. |
| `inject_netem` K8s/node wrapper | Slow replica / delayed ACK behavior needs controlled latency. | RF3 sync-quorum degraded-latency gate produces bounded reason codes. |
| `collect_node_snapshot` via agent | SSH-only collection is enough for now but does not scale to hosted/nightly validation. | Controller/agent spike collects process, disk, network, iSCSI, multipath, kernel, and product logs from all nodes. |

## Non-Goals

- Do not redesign scenario YAML now.
- Do not require agent mode before v0.3.x cleanup/repeatability gates close.
- Do not expose product mutating fault-injection APIs to end users.
- Do not move complex product orchestration out of helper scripts until the
  runner has equivalent primitives and diagnostics.

