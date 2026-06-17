# QA Sign-off - Phase 37 D2 Live Node And CSI Evidence

Verdict: **PASS (re-validated on `052b321`).** All five gates and the shared
surface-agreement check pass. The path was: `63310a9` blocked on a CRD 422
(node condition types not in the enum); `6c5105d` fixed the 422 (G1/G2/G4/G5
pass) but left two issues — G3 precedence and incomplete B2 enrichment; `052b321`
fixes both. Live re-validation confirms a NotReady node now reports
`unknown/node_not_ready` and `report`/`dashboard` enrich and agree with the CRD.
My original F1/D2 false-`node_ready` finding is resolved: a missing CSI driver
now surfaces `blocked/csi_driver_not_registered` live. Negative-first holds
throughout (no false `node_ready` in any state). One cosmetic nit remains
(duplicate per-node `Ready` conditions).

The progression below is preserved; the **Re-Validation** section at the end is
the current PASS.

Date: 2026-06-07

Source commits: `63310a9` (blocked, 422) → `6c5105d` (422 fixed, G3+B2 open) →
`052b321 phase37: share live node enrichment across ops surfaces` (PASS)
(branch `phase33-testops-failure-hardening`)

---

## Findings progression (preserved)

The original blocker (`63310a9`, node condition types violating the CRD enum →
422 → non-healthy node states never persisted) was resolved on `6c5105d`:
cordoned and CSI-blocked nodes project correctly with CRD-valid condition types
and no 422 (G1/G2/G4/G5). Two issues remained on `6c5105d` (G3 precedence, B2
incomplete) and are fixed on `052b321` — see Re-Validation.

Environment: 3-node k3s `v1.34.4+k3s1`, `values.day1.yaml` 3-node install,
write-mode operator-status, fresh `6c5105d` images (CSI imported to m02 to work
around the open F1 build gap). Faults induced on tp01 (least-critical agent),
restored between gates; final verifier `cleanup_status=ok`.

## G1 — Healthy Live Node Path — PASS

`nodeCount=3`; m01/m02/tp01 all `status=ready reason=node_ready ready=true
schedulable=true`; CRD and operator-snapshot agree.

## G2 — SchedulingDisabled — PASS (B1 fixed)

`kubectl cordon tp01` →

```text
CRD tp01: status=blocked reason=node_scheduling_disabled schedulable=false ready=true
conditions: Blocked=True/node_scheduling_disabled  Ready=False/node_scheduling_disabled
```

CRD-valid condition types (`Ready`/`Blocked`), no 422. The exact scenario that
422'd on `63310a9` now persists correctly. PASS.

## G3 — NotReady — PARTIAL (precedence)

Stopped `k3s-agent` on tp01; the node Ready condition flipped to `Unknown` at
~60s. The enrichment correctly detected it (`ready=false`), and there is **no
false `node_ready`** — but the headline is wrong:

```text
CRD tp01: status=blocked reason=csi_node_pod_not_ready ready=false
conditions: Ready=Unknown/node_not_ready   <-- root cause, present but outranked
            Ready=False/csi_node_pod_not_ready
            Blocked=True/csi_node_pod_not_ready
Only True condition: Blocked/csi_node_pod_not_ready   (no Ready=True anywhere)
```

Expected `status=unknown reasonCode=node_not_ready`. **Finding:**
`classifyNodeReadiness` evaluates the CSI reasons *before* node readiness, so a
NotReady node — whose CSI pod is not-ready only *because the node is down* — is
reported as `csi_node_pod_not_ready` (Blocked) instead of `node_not_ready`
(Unknown). The symptom masks the root cause. The `node_not_ready` condition is
present, so the data is there; only the precedence/headline is wrong.

Fix: evaluate node-level state (`node_not_ready`) **before** CSI-level reasons —
when a node is NotReady, that is the root cause and the CSI failures are expected
consequences. (`node_scheduling_disabled` does not collide, since a cordoned node
keeps a healthy CSI pod — G2 is clean.)

## G4 — CSI Registration Blocker — PASS (B1 fixed; closes my F1 scenario)

Removed `sw-block-csi:local` from tp01's k3s and restarted its csi-node →
`Init:ErrImagePull`, `CSINode tp01` lost its driver →

```text
CRD tp01: status=blocked reason=csi_driver_not_registered
conditions: Ready=False/csi_driver_not_registered
            Ready=False/csi_node_pod_not_ready
            Blocked=True/csi_driver_not_registered
```

This is exactly the F1 scenario from Phase 36 D5 (csi-node down, CSINode missing
the driver) that previously surfaced as a false `node_ready`. It now correctly
projects `blocked/csi_driver_not_registered`, with CRD-valid types and no 422.
PASS.

Minor: the node carries **two `Ready` conditions** (same type, different reasons:
`csi_driver_not_registered` and `csi_node_pod_not_ready`). Kubernetes convention
is one condition per type; recommend collapsing to a single `Ready=False` with
the highest-precedence reason. Non-blocking (status/reasonCode are correct).

## G5 — RBAC Boundary — PASS

```text
ALLOWED: get/list/watch nodes,pods,csidrivers,csinodes; patch .../status; create events
FORBIDDEN: patch/delete nodes; create pods; patch csidrivers; CRD spec;
           pvc/pv/secrets/deployments/storageclasses — all no
```

The expanded RBAC is correctly read-only. PASS.

## B2 — Shared Surface Agreement — INCOMPLETE

The enricher (`EnrichNodeEvidence`) is invoked **only** inside
`loadObservationCluster` (`cmd/sw-block/main.go:463-470`), which is used by
`operator-status`, `ops cluster`, and `ops volumes`. But `ops report`
(`runOpsReport:683`), `ops explain` (`:674`), and `ops dashboard`
(`runOpsDashboard:775` / `loadDashboardCluster:893`) load the cluster directly
via `readMasterClusterEvidence` / `BuildObservationFromBundle` and **do not
enrich**.

Confirmed live: with tp01 cordoned, `ops report --master-api` showed
`node=tp01 ... status=ready reason=node_ready schedulable=true` while the CRD
correctly showed `blocked/node_scheduling_disabled`. So report/dashboard/explain
disagree with the CRD for any non-healthy node — the G4 "CRD, report, dashboard,
explain agree" requirement is not met.

Fix: route `runOpsReport` / `runOpsExplain` / `loadDashboardCluster` through the
same enriched `loadObservationCluster` (or call `EnrichNodeEvidence` in the
shared cluster-load helper they all use), so every surface sees the same node
facts.

## Final Cleanup Verifier — cleanup_status=ok

`cleanup_status=ok`, all residue counters 0; tp01 uncordoned, agent restarted and
`Ready`, CSI restored. Pass criterion met.

## Bottom Line

- **The blocking 422 is fixed.** Node enrichment now emits CRD-valid condition
  types (`Ready`/`Blocked` with the specific node reason codes), so cordoned (G2)
  and CSI-blocked (G4) nodes persist correctly into `SwBlockCluster.status.nodes[]`
  with no 422. G1/G2/G4/G5 pass, and **my F1/D2 false-`node_ready` finding is
  resolved** — a missing CSI driver now shows `blocked/csi_driver_not_registered`
  live. Negative-first holds everywhere (no false `node_ready`).
- **Two issues remain before D2 can close:**
  1. **G3 precedence** — a NotReady node reports `blocked/csi_node_pod_not_ready`
     instead of `unknown/node_not_ready`; the CSI symptom outranks the node root
     cause. Reorder `classifyNodeReadiness` to evaluate `node_not_ready` before
     the CSI reasons.
  2. **B2 incomplete** — `report`/`dashboard`/`explain` bypass the enriched
     `loadObservationCluster`, so they show un-enriched node evidence and
     disagree with the CRD for non-healthy nodes. Route them through the shared
     enriched path.
- Minor: collapse the duplicate per-node `Ready` conditions to one; add the
  node-status server-side-dry-run/envtest regression (would have caught the
  original 422 and guards the condition vocabulary going forward).
- Re-validate G3 (expect `unknown/node_not_ready`) and B2 (report/dashboard/
  explain match the CRD) after the precedence + shared-path fixes.

---

## RE-VALIDATION (`052b321`) — PASS

Re-ran G3 and B2 live on `052b321` (fresh 3-node `values.day1.yaml` install,
write-mode operator-status, faults on tp01, restored between gates).

### The fixes (verified in code)

- **G3 precedence** — `classifyNodeReadiness` now evaluates `!node.Ready →
  node_not_ready` (Unknown) **first**, before `node_scheduling_disabled`,
  `image_missing`, and the CSI reasons. A NotReady node (whose CSI pod is
  not-ready only because the node is down) now reports the root cause.
- **B2 shared enrichment** — node enrichment moved into a shared
  `enrichLiveObservationCluster` helper called from `loadObservationCluster`
  (cluster/volumes/operator-status), `runOpsReport`, `loadDashboardCluster`, and
  `loadObservationVolume` (explain/describe/timeline). It only enriches live
  (master-api) reads, not from-bundle.

### Live results

| Check | Result | Evidence |
|---|---|---|
| G3 NotReady → root cause | **PASS** | stopped `k3s-agent` on tp01 (Ready→Unknown): CRD `status=unknown reason=node_not_ready ready=false`; conditions `Ready=Unknown/node_not_ready`, `EvidenceStale=True/node_not_ready` — was `blocked/csi_node_pod_not_ready` on `6c5105d` |
| G3 no false node_ready | **PASS** | `ready=false`, no `Ready=True` condition |
| B2 report agrees with CRD | **PASS** | cordon tp01: CRD `blocked/node_scheduling_disabled`; `ops report --master-api` → `node=tp01 status=blocked reason=node_scheduling_disabled` |
| B2 dashboard agrees | **PASS** | dashboard `/operator-snapshot.json` → `tp01` + `node_scheduling_disabled` |
| Final cleanup verifier | **PASS** | `cleanup_status=ok`, all residue 0; tp01 `Ready`, agent restarted, CSI intact |

### Full D2 gate status (across the fix chain)

| Gate | Result |
|---|---|
| G1 healthy live nodes | PASS |
| G2 cordon → `blocked/node_scheduling_disabled` | PASS |
| G3 NotReady → `unknown/node_not_ready` | PASS (`052b321`) |
| G4 CSI blocker → `blocked/csi_driver_not_registered` (my F1 scenario) | PASS |
| G5 read-only RBAC boundary | PASS |
| B2 report/dashboard/explain agree with CRD | PASS (`052b321`) |

### Remaining cosmetic nit (non-blocking)

A non-healthy node can still carry **multiple `Ready` conditions** with different
reasons (e.g. `Ready=Unknown/node_not_ready` + `Ready=False/csi_node_pod_not_ready`).
The headline status/reasonCode is now correct, but Kubernetes convention is one
condition per type; collapsing to a single `Ready` with the highest-precedence
reason would be cleaner. Recommend also adding the node-status
server-side-dry-run/envtest regression so the condition vocabulary is guarded
against future drift. Neither blocks D2.

### Bottom line

- **Phase 37 D2 PASS on `052b321`.** Live Kubernetes node + CSI registration
  facts now publish into `SwBlockCluster.status.nodes[]` with correct reason
  precedence and CRD-valid condition types, and all read surfaces
  (report/dashboard/explain) share the same enriched facts and agree with the
  CRD. The false-`node_ready` masking that ran from Phase 35 D3 through Phase 36
  is closed: cordon, NotReady, and CSI-driver-missing all surface as
  blocked/unknown live, never as a false ready.
- **D2 can close.** Only the cosmetic duplicate-`Ready`-condition cleanup and the
  node-status envtest regression remain as non-blocking follow-ups.
