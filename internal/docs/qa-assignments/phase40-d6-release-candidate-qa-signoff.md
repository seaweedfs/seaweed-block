# QA Sign-off — Phase 40 D6 Release Candidate Gate

Verdict: **Blocker RESOLVED — chart-flag gate verified.** First filed HOLD on
`9a8df78`; re-validated on `5f0566e` (see Re-validation below). The published-image
first-volume gate (G1) now passes. One residual confirmation remains before a full
release PASS (G2 operator-status claim on the actual release image).

## Re-validation — 2026-06-13, commit `5f0566e phase40: gate durable impl chart flag`

The dev gated `--launcher-durable-impl` behind `compat.launcherDurableImplFlag`
(default `false`):

- Default `helm template` now renders blockmaster with **only**
  `--launcher-durable-root` (the flag every published image already has);
  `--launcher-durable-impl` is omitted. `--set compat.launcherDurableImplFlag=true`
  re-adds it. A `fail` guards `blockmaster.durableImpl != walstore` while the
  compat flag is off (no silent drop). Omitting the flag is behavior-neutral for
  the default — the blockmaster binary's own `--launcher-durable-impl` default is
  already `walstore`.

**G1 re-run against the PUBLISHED image** (lab tree synced to `5f0566e`; scenario
default image, no local override):

```text
swblock run testops/scenarios/helm-first-volume-via-sw-block-cli-chain.yaml
day1.yaml image = ghcr.io/seaweedfs/seaweed-block:sha-6260e46fd3be   (published)
=== helm-first-volume-via-sw-block-cli-chain === PASS (52.9s) — 34/34 actions
  helm_install_stack PASS   (blockmaster starts; no CrashLoopBackOff)
  first_volume_user_loop PASS   writer_verified=1  reader_verified=1
  first_volume_asserts PASS   first_volume_ok=1
  helm_uninstall_cleanup PASS   cleanup_ok=1
bundle results/20260613-203738-8794
```

The blocking finding (chart-ahead-of-image flag skew) is **resolved**: the
documented Helm + first-PVC + writer/reader path now works end-to-end with the
currently-published images. Lab left clean (helm 0, pods 0).

**Residual confirmation (not a regression, a scope note).** `sha-6260e46fd3be`
(and `:alpha`) are old published images used here only to prove the chart is
backward-compatible. Their **operator-status binary predates the Phases 35–39
fixes**, so the G2 operator-status live-CRD/Event claim was validated on the
*from-source* build, not on these digests. Before a full release PASS, re-run G2
against the **actual release image** (a fresh publish from the release commit) to
confirm live status/events publication and the RBAC boundary hold on the shipped
binary. The chart-flag gate does not change any binary; it only makes the chart
installable on older images.

## Fresh Release Image For G2

GitHub Actions run `27490827782` published commit `dc2972d0059b` from branch
`phase33-testops-failure-hardening`.

Use these images for the remaining G2 operator-status live CRD/Event/RBAC check:

```text
ghcr.io/seaweedfs/seaweed-block:sha-dc2972d0059b
ghcr.io/seaweedfs/seaweed-block-csi:sha-dc2972d0059b
```

Published digests:

```text
seaweed-block:
  sha256:b8da5ca4e2bbe2f0f630fee0468790c444362615d68807a1be31fd237c84928f
seaweed-block-csi:
  sha256:b5942cd68d28aecdfebec1f1e5ec55a9cafe746169fee3b6c35916c93fffcaa6
```

---

## Original finding (commit `9a8df78`, superseded by the Re-validation above)

Verdict at filing: **HOLD (do not release the published images as-is).**

The product **code** at `9a8df78` is good: with an image built from that source,
the full documented path passes — fresh Helm install, first PVC bind,
writer/reader verify, live read-only/status-only operator-status CRD + Events
publication with a provable status/events-only RBAC boundary, correct
negative-status reporting, and zero-residue cleanup (G1/G2/G3/G4/G5 all pass).

But the release candidate ships a **chart that no published image satisfies**.
The `9a8df78` chart's blockmaster Deployment passes `--launcher-durable-impl`,
and **neither** published image referenced by the release defines that flag:

- `ghcr.io/seaweedfs/seaweed-block:alpha` (the chart's *default* `image.tag` and
  `generate-helm-values`' default `--image`): has `--launcher-durable-root`,
  **missing `--launcher-durable-impl`**.
- `ghcr.io/seaweedfs/seaweed-block:sha-6260e46fd3be` (the pinned digest in the G1
  scenario, the helm-lifecycle scenario, and the status-endpoint scenarios):
  **missing `--launcher-durable-impl`** (older still).

So **any install that uses a published image** — i.e. the default documented user
path — gets `blockmaster: flag provided but not defined: -launcher-durable-impl`
→ blockmaster `CrashLoopBackOff` → `helm install --wait` 10m timeout → no first
volume. This meets the assignment's own blocking criterion ("first-volume
writer/reader fails") for the as-published artifacts.

Date: 2026-06-13

Source commit: `9a8df78 phase40: update beta candidate evidence` (branch
`feature/sw-block` working tree, synced to `/tmp/seaweed_block` on m02)

Image(s):
- **Tested-good:** fresh local build from `9a8df78` → `sw-block:local` /
  `sw-block-csi:local` (has both launcher flags). G1/G2 pass with these.
- **Tested-bad (published):** `ghcr.io/seaweedfs/seaweed-block:alpha` and
  `:sha-6260e46fd3be` — both lack `--launcher-durable-impl`; blockmaster crashes.

Runner: `swblock` from `sw-test-runner-standalone` `d45c60c` (built to
`C:\work\swblock.exe`; runs from Windows, SSHes to m02).

## Lab Node Health

- m01 `Ready`, m02 `Ready`, **tp01 `NotReady`/unreachable** (Connection timed
  out; unchanged since Phase 37). Generated day1 values correctly exclude tp01
  (m01+m02 only), and the csi-node DaemonSet's desired count is 2 (tp01 excluded)
  — so the 2-node install does not wait on tp01. Restore tp01 before any RF=3
  live multi-node gate.
- Two lab-infra issues were hit and resolved during this run (neither is a
  product defect; both are recorded under Non-Blocking):
  1. **m02 disk pressure** (88% > k3s 85% image-GC-high-threshold) caused k3s to
     continuously GC the unused `:local` images, so installs raced GC →
     `ErrImageNeverPull`. Reclaimed 54 GB of dangling docker layers (no project
     data touched) → 63%; GC stopped; installs stable.
  2. **`build-alpha-images.sh` remote import** did not refresh m01's containerd
     tag; pushed the fresh tar to m01 explicitly. (F1-class image-distribution
     friction.)

## Local Gate — PASS

`scripts/run-phase40-release-candidate-local.ps1`:

```text
phase40_release_candidate_local_status=ok
go_test_release_scope=ok
helm_lint=ok
helm_operator_status_template=ok
status_api_conformance_gate=ok
git_diff_check=ok
```

The local gate does not exercise the live Kubernetes path; it passed even though
the published-image install is broken (the skew is only visible when a real
blockmaster pod starts).

## G1 — Minimal New-User Helm Path

- Run (fresh local image, correct flag order):
  `swblock run -env sw_block_image=sw-block:local -env
  sw_block_csi_image=sw-block-csi:local
  testops/scenarios/helm-first-volume-via-sw-block-cli-chain.yaml`
- Result: **PASS — 34/34 actions** (bundle `results/20260613-193258-4963`).
  - `helm_install_stack` PASS, `first_volume_user_loop` PASS,
    `first_volume_ok=1`, `writer_verified=1`, `reader_verified=1`,
    `report_html=1` (index.html/operator-snapshot present),
    `helm_cleanup_ok=1`.
- Result with **published image** (scenario default `sha-6260e46fd3be`):
  **FAIL** — `helm_install_stack` 10m timeout; blockmaster `CrashLoopBackOff`
  (`flag provided but not defined: -launcher-durable-impl`). This is the blocking
  finding below.

## G2 — Operator-Status CRD And Event Path — PASS

Live write-mode install (`operatorStatus.create=true`,
`operatorStatus.dryRun=false`, `interval=15s`, fresh local image) + first volume;
CR stubs created per the assignment (the controller patches existing CRs, it does
not create them):

```text
SwBlockCluster sw-block:          status.readyVolumeCount=1  volumeCount=1  blocked=0
SwBlockVolume  sw-block-example-pvc: status=ready  Ready=True  reason=first_volume_verified
Event: Normal first_volume_verified swblockvolume/sw-block-example-pvc
       "managed volume is ready for the documented path"
```

The operator-status loop reached the blockmaster gRPC
(`blockmaster.kube-system.svc.cluster.local:9333`), observed the ready volume,
and patched live. RBAC boundary (`kubectl auth can-i` as
`system:serviceaccount:kube-system:sw-block-seaweed-block-operator-status`):

```text
patch swblockvolumes --subresource=status   => yes
patch swblockclusters --subresource=status  => yes
create events                               => yes
patch swblockvolumes (main object)          => no
patch pods                                  => no
patch persistentvolumeclaims                => no
update storageclasses                       => no
```

Status/events-only, exactly as claimed; no storage/workload/spec/finalizer
mutation power. Note: the controller keys `SwBlockVolume` by **PVC name**
(`sw-block-example-pvc`), not by `volume_id`.

## G3 — Negative Status — PASS (substance), full-install path blocked by tp01

The negative-status substance was validated on `9a8df78` via the scenario's own
cold synthetic-bundle + `sw-block ops explain`/`report` path (deterministic; no
live cluster needed):

```text
volume pvc-blocked status=blocked  reason=csi_node_image_pull_failed
condition Attach ... pod sw-block-csi-node waiting=ImagePullBackOff on node m02
managed_volume_condition Ready   status=False  reason=csi_node_image_pull_failed
managed_volume_condition Blocked status=True   reason=csi_node_image_pull_failed
safe action: import_csi_image mode=dry_run side_effect=safe_k8s decision=rejected
report: read_only=true   mutation_allowed=false   (summary.txt + operator-snapshot.json)
index.html: Ready=False, Blocked=True   (no false Ready=True anywhere)
```

Pass criteria met: stable blocked reason, no false `Ready=True` on any surface
(explain / report index.html / operator-snapshot), safe actions only
(`read_only`/`dry_run`, `mutation_allowed=false`).

The full `helm-support-bundle-diagnostics-chain` live-install path was **not**
run end-to-end because its `build_and_generate_values` phase hardcodes
`SW_BLOCK_IMPORT_K3S_NODES='192.168.1.181,192.168.1.188'` under `set -euo
pipefail`, and the `.188` (tp01) import aborts the step. Lab-infra block (tp01
down), not a product defect — see Non-Blocking.

## G4 — Status API Conformance TestOps Gate — PASS (with a fidelity caveat)

`swblock run testops/scenarios/operator-status-api-conformance-chain.yaml` →
**PASS, 15/15**:

```text
phase40_status_api_conformance_status=ok
casing_drift_gate=ok  enum_drift_gate=ok  wrong_endpoint_gate=ok
rbac_boundary_gate=ok  delete_safety_status_gate=ok  finalizer_mutation_allowed=false
```

Caveat (so the green is not over-read): `scripts/run-phase40-status-api-conformance.sh`
runs `go test ./core/ops` (5 named tests) + `helm template`, then **derives all
six sub-gate lines from a single `$status`** — they are not independent live
assertions. The underlying test (`kubernetes_status_conformance_test.go`) does
load the **real chart CRD OpenAPI schema** from
`charts/seaweed-block/crds/*.yaml` (so casing/enum drift is genuinely checked
against the real schema — real value), but it validates against an
`httptest.Server` **mock**, not a live apiserver or envtest. The live-only
behaviors that bit Phases 35–39 (subresource 404, RBAC 403 on a finalizers
patch) are simulated, not exercised. This gate is a real improvement for
schema/enum drift; it does **not** close the live-RBAC/subresource gap. The
standing recommendation (a real envtest harness — real apiserver + the operator's
real RBAC for `KubernetesStatusClient`) still stands.

## G5 — Final Cleanup — PASS

`swblock run testops/scenarios/cleanup-residue-chain.yaml` → **PASS, 13/13**:
`host_protocol_residue` (no iSCSI sessions, no NVMe subsys, no product
processes), `kubernetes_residue` (no sw-block/seaweed-block/CSI-driver
resources), `hostpath_residue` (no `/var/lib/sw-block` testops residue),
`collect_and_cleanup` final sweep. Lab left clean.

## Blocking Findings

1. **Chart-ahead-of-image skew — the documented Helm install is broken with every
   published image.** `9a8df78`'s `charts/seaweed-block/templates/blockmaster.yaml`
   passes `--launcher-durable-impl`, but `ghcr.io/seaweedfs/seaweed-block:alpha`
   (chart default + `generate-helm-values` default) and `:sha-6260e46fd3be`
   (pinned in `testops/scenarios/helm-first-volume-via-sw-block-cli-chain.yaml:23-24`,
   `helm-lifecycle-upgrade-rollback-chain.yaml`, `status-endpoint-*-chain.yaml`)
   both lack that flag → blockmaster `CrashLoopBackOff` → install timeout → no
   first volume. **Fix:** publish an image built from the release commit (with
   `--launcher-durable-impl`), retag `:alpha`, and bump every `sha-6260e46fd3be`
   reference (scenarios + any docs/release notes) to it; then re-run G1 **with the
   published image** (not a local override) to confirm. Until then, only
   build-from-source installs work.

## Non-Blocking Findings

1. **Documented `swblock` `-env` override is silently dropped when flags follow
   the scenario path.** The runner (`cli/cli.go`, Go stdlib `flag`) stops parsing
   at the first non-flag arg, so `swblock run <scenario> -env K=V` (the ordering
   shown in this assignment's G1 example) ignores the overrides and the scenario
   falls back to its pinned image. Overrides only take effect as
   `swblock run -env K=V ... <scenario>`. Recommend fixing the runner to accept
   interspersed flags (or correcting every doc/runbook). (Windows note: `/tmp/...`
   values passed via `-env` are MSYS path-mangled to `C:/work/tmp/gotmp/...`;
   leave `product_root` to the scenario default or set `MSYS_NO_PATHCONV=1`.)
2. **G4 conformance is a schema-aware mock, not live/envtest** (see G4 caveat).
   The six sub-gates derive from one status; the envtest harness for
   `KubernetesStatusClient` (real apiserver + real RBAC) remains the gap-closer
   for the live-only RBAC/subresource class.
3. **`helm-support-bundle-diagnostics-chain` hardcodes tp01 in the build import
   list** (`SW_BLOCK_IMPORT_K3S_NODES='192.168.1.181,192.168.1.188'`, fail-fast),
   so it cannot run while tp01 is down. Recommend parameterizing the import node
   list (env-overridable) so single-node-down labs can still run it.
4. **tp01 `NotReady`/unreachable** — lab infra; restore before RF=3 live work.
5. **m02 local-image GC under disk pressure** — `:local` images get pruned when
   m02 disk > 85%; keep m02 below the GC threshold (it was at 88%; 54 GB of
   dangling docker layers reclaimed this run → 63%) or use a registry image so
   `helm install --wait` does not race containerd GC.

## Release Recommendation

**HOLD.** The product code, chart logic, live operator-status read-only/status-only
publication, RBAC boundary, negative-status reporting, and cleanup are all good on
`9a8df78` (proven with a from-source image). The release is gated solely on
release-engineering: **publish a matching image (chart default `:alpha` and any
pinned digest must contain `--launcher-durable-impl`) and bump all references**,
then re-run **G1 against the published image** to flip this to PASS. Do not ship
the chart at `9a8df78` against the current `:alpha`/`sha-6260e46fd3be` images —
the documented first-volume path fails on a clean cluster. The flag-ordering and
envtest items above are follow-ups, not release blockers.

## Developer Follow-Up After HOLD

The selected unblock path is a chart compatibility fix rather than relying only
on immediate image republish:

- `--launcher-durable-impl` is now gated behind
  `compat.launcherDurableImplFlag`.
- The default remains `false`, matching the existing compatibility pattern for
  newer blockmaster launcher flags.
- Default `walstore` behavior is preserved because `walstore` is the
  blockmaster binary default when the flag is omitted.
- Setting `blockmaster.durableImpl` to a non-default value now requires
  `compat.launcherDurableImplFlag=true`, so Helm fails fast instead of silently
  ignoring the requested implementation.

Release is still held until QA reruns the published-image first-volume path and
confirms the chart no longer crashes blockmaster with the current published
images.
