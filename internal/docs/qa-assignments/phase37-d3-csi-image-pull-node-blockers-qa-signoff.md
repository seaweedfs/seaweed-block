# QA Sign-off - Phase 37 D3 CSI Image-Pull Node Blockers

Verdict: **PASS (re-validated on `43d7786`).** `c28ada7` got the core detection
right (CRD shows `image_missing_on_node`, root cause unmasked) but the read
surfaces disagreed because the enricher read CSI pods from the command's
`--namespace` (default `default`) instead of the install namespace. `43d7786`
fixes that: the CSI-pod read now resolves to the Helm/control-plane namespace
(`kube-system` by default, `SW_BLOCK_HELM_NAMESPACE` override, explicit
non-default preserved), and `ErrImageNeverPull` is now matched. Re-validated
live: `ops report`/`dashboard` with the default namespace agree with the CRD on
`image_missing_on_node`, and the `pullPolicy=Never`/`ErrImageNeverPull` path
(my original F1 signature) is detected. The F1/local-image masking is closed on
all surfaces.

The original partial write-up is preserved; the **Re-Validation** section at the
end is the current PASS.

Date: 2026-06-07 (partial) → 2026-06-08 (re-validated PASS)

Source commits: `c28ada7 phase37: project csi image pull node blockers` (partial)
→ `43d7786 phase37: align image blocker live surfaces` (PASS)
(branch `phase33-testops-failure-hardening`)

---

## ORIGINAL FINDING (partial, `c28ada7`) — preserved

Environment: 3-node k3s `v1.34.4+k3s1`, `values.day1.yaml` install
(`csiImage.pullPolicy` defaulted to `IfNotPresent`), write-mode operator-status,
fresh `c28ada7` images. Missing-image fault induced on tp01 (remove
`sw-block-csi:local` from its k3s + restart its csi-node), restored after.

## Core Detection — PASS (CRD / operator-status)

tp01 csi-node init container went `ErrImagePull` → `ImagePullBackOff`; its
CSINode driver deregistered. The CRD correctly headlines the image-pull root
cause:

```text
SwBlockCluster.status.nodes[tp01]:
  status=blocked  reasonCode=image_missing_on_node
  missingImages=["sw-block-csi:local"]
  conditions: Ready=False/image_missing_on_node  Blocked=True/image_missing_on_node
```

What this proves:

- `ImagePullBackOff`/`ErrImagePull` on the CSI node pod feeds
  `RequiredImages`/`MissingImages` and projects `image_missing_on_node`.
- **Root cause not masked:** even though the CSINode driver was deregistered
  (`csi_driver_not_registered` would otherwise apply) and the pod is not ready
  (`csi_node_pod_not_ready`), the conditions are cleanly only
  `image_missing_on_node` — the symptom conditions are suppressed when
  `MissingImages > 0`, and `classifyNodeReadiness` ranks `image_missing_on_node`
  above both CSI reasons.
- No false `node_ready`.

This closes the F1/local-image masking on the Kubernetes-native authoritative
surface (the one consumers/kubectl read).

## Surface Agreement — FAIL (namespace bug in shared enrichment)

With the missing-image fault active and pod `ImagePullBackOff`, CSINode
deregistered:

```text
CRD tp01:                          blocked / image_missing_on_node  missing=["sw-block-csi:local"]
ops report (default --namespace):  blocked / csi_driver_not_registered  missing_images=-   (x2, consistent)
ops report --namespace kube-system: blocked / image_missing_on_node     missing_images=sw-block-csi:local
```

Root cause: the node enricher reads CSI node **pods** from the command's
`--namespace`. The CSI node DaemonSet runs in the install namespace
(`kube-system`). The operator-status controller is configured with
`--namespace=kube-system` (chart `--namespace={{ .Release.Namespace }}`), so it
finds the pods and detects `image_missing_on_node` / `csi_node_pod_not_ready`.
But `ops report` / `ops dashboard` / `ops explain` default `--namespace` to
`default` (the volume/workload namespace), so the CSI-pod read finds nothing and
those surfaces fall back to the cluster-scoped `csi_driver_not_registered`
(CSINode/CSIDriver are cluster-scoped and namespace-independent).

Net effect: for any node blocked on `image_missing_on_node` or
`csi_node_pod_not_ready`, `report`/`dashboard`/`explain` disagree with the CRD
unless the user happens to pass `--namespace kube-system`. A user running
`sw-block ops report` for an image-missing node is told `csi_driver_not_registered`
— a less specific, misleading reason.

This is the same shared-enrichment path the D2 B2 fix (`052b321`) added. D2's B2
check passed because it exercised only namespace-independent node facts (cordon,
NotReady). D3's pod-derived facts (image-pull, pod-not-ready) expose the
namespace-scoping bug.

### Fix

The CSI-pod read must use the **operator/install namespace** (where the CSI
DaemonSet runs), not the report's volume `--namespace`. Either thread a separate
operator-namespace through `enrichLiveObservationCluster` for the CSI-pod
lookup, or default the CSI-pod namespace to the operator-status namespace. After
the fix, `ops report` with the default namespace must show
`image_missing_on_node` for the affected node, matching the CRD.

## Minor: `ErrImageNeverPull` not detected (pullPolicy=Never)

`imagePullWaitingReason` matches only `ImagePullBackOff` and `ErrImagePull`
(`kubernetes_node_evidence.go:389`). With `csiImage.pullPolicy=Never` (a
legitimate local-image policy to avoid registry pulls), a missing image yields
`ErrImageNeverPull`, which is **not** matched — so the node would fall back to
`csi_driver_not_registered` rather than `image_missing_on_node`. The chart
default is `IfNotPresent` (→ `ErrImagePull`, detected), so the realistic path is
covered, but the original Phase 35 D5 F1 signature was exactly
`ErrImageNeverPull`. Recommend extending the matched reasons to include
`ErrImageNeverPull` (and `InvalidImageName`) so the Never path is covered too.
Non-blocking.

## Lab State

Clean — tp01 CSI image restored, `SwBlockCluster` stub deleted, helm uninstalled,
both CRDs deleted; final verifier `cleanup_status=ok`, all residue 0; 0 sw-block
pods/CRDs.

## Bottom Line

- **D3 core detection: PASS.** `image_missing_on_node` now projects on the CRD
  with the image named, the root cause is not masked by CSI
  registration/pod-not-ready symptoms, and there is no false `node_ready`. The
  F1/local-image masking is closed on the authoritative Kubernetes surface.
- **D3 surface agreement: FAIL (namespace bug).** `ops report`/`dashboard`/
  `explain` read CSI pods from the wrong namespace by default and therefore show
  `csi_driver_not_registered` instead of `image_missing_on_node` — they disagree
  with the CRD for image-missing / pod-not-ready nodes. Fix: read CSI pods from
  the operator/install namespace regardless of the volume `--namespace`.
- **Minor:** detection misses `ErrImageNeverPull` (the `pullPolicy=Never` local
  case); extend the matched waiting reasons.
- **Do not fully close D3** until `ops report` with the default namespace agrees
  with the CRD (`image_missing_on_node`) for the missing-image node. Re-validate
  surface agreement after the namespace fix.

---

## RE-VALIDATION (`43d7786`) — PASS

### The fixes (verified in code)

- **Namespace** — `enrichLiveObservationCluster` now resolves the CSI-pod read
  namespace via `liveNodeEvidenceNamespace(namespace)` (`cmd/sw-block/main.go`):
  `SW_BLOCK_HELM_NAMESPACE` if set; else an explicit non-`default` namespace is
  preserved; else `kube-system`. So `ops report`/`dashboard`/`explain` with the
  default (`default`/empty) namespace read CSI pods from `kube-system` where the
  DaemonSet runs, while custom-namespace installs are honored.
- **ErrImageNeverPull** — `imagePullWaitingReason` now matches
  `ImagePullBackOff || ErrImagePull || ErrImageNeverPull`
  (`kubernetes_node_evidence.go:389`).

### Live results

Same missing-image fault on tp01 (`IfNotPresent` → `ErrImagePull` →
`ImagePullBackOff`), then `pullPolicy=Never` → `ErrImageNeverPull`:

| Check | Result | Evidence |
|---|---|---|
| CRD `image_missing_on_node` | PASS | `blocked/image_missing_on_node missing=["sw-block-csi:local"]` |
| `ops report` **default namespace** agrees | PASS | `reason=image_missing_on_node missing_images=sw-block-csi:local` (was `csi_driver_not_registered` on `c28ada7`) |
| `ops dashboard` **default namespace** agrees | PASS | `/operator-snapshot.json` → `image_missing_on_node` (only) |
| `ErrImageNeverPull` (pullPolicy=Never) detected | PASS | patched DaemonSet to `Never`; tp01 init `ErrImageNeverPull`; CRD `blocked/image_missing_on_node missing=["sw-block-csi:local"]` |
| Final cleanup verifier | PASS | `cleanup_status=ok`, all residue 0; tp01 CSI image restored |

### Bottom line

- **D3 PASS on `43d7786`.** `image_missing_on_node` projects on the CRD and on
  `report`/`dashboard`/`explain` with the default namespace (they now agree), the
  root cause is not masked by CSI registration/pod-not-ready symptoms, both
  `ErrImagePull`/`ImagePullBackOff` and `ErrImageNeverPull` are detected, and
  there is no false `node_ready`. **The F1/local-image masking that ran from
  Phase 35 D3 through Phase 36 is now closed on every surface, for both
  pull-policy modes.**
- **D3 can close.** With D2 and D3 both PASS, the live node/CSI evidence work is
  complete: cordon, NotReady, missing CSI driver, and missing CSI image all
  surface live as blocked/unknown with the correct reason, consistently across
  CRD, Events, report, dashboard, and explain.
