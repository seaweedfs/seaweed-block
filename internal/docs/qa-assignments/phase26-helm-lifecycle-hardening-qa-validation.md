# QA Validation - Phase 26 Helm Lifecycle Hardening

Date: 2026-05-22

Verdict: PASS. All four required gates pass independently on a clean
3-node k3s lab from a fresh tar-synced source tree. Residue audit clean.

This is the QA-side independent replay of the gates the dev close report at
`internal/docs/qa-assignments/phase26-helm-lifecycle-hardening-close-report.md`
already cited. Action counts match the dev-cited counts exactly.

## Method

- Synced branch `phase26-helm-lifecycle-hardening` at `e1fd2b6` from Windows
  controller to m02 via tar/scp/extract per the QA assignment.
- Lab pre-state confirmed clean: no `sw-block` helm release, no active iSCSI
  sessions, no `blockmaster|blockvolume|blockcsi|iscsi-target` processes on
  m01/m02/tp01.
- Ran the four scenarios sequentially through `swblock.exe run` from
  Windows.

## Gate Results

| Gate | QA run | Actions | Dev-cited run |
|---|---:|---|---|
| D1 chart release hygiene | `20260522-155827-ce94` | 15/15 PASS | `20260522-131641-7a61` |
| D2 Helm lifecycle upgrade/rollback | `20260522-155835-2057` | 27/27 PASS | `20260522-131951-a6d4` |
| D3 multi-volume Day-1 | `20260522-155944-27b3` | 29/29 PASS | `20260522-152903-1116` |
| D4 support-bundle diagnostics | `20260522-160203-227d` | 38/38 PASS | `20260522-153929-93a3` |

Total: 109/109 actions across QA reruns. Action counts match dev report 1:1.

## Hard-Gate Acceptance Evidence

### D2 - PV identity preserved across upgrade/rollback

PV `pvc-7aa1dbeb-f5ac-4807-878a-b93d64bda91f`, three confirmation points:

```text
first-volume/first-volume-summary.txt:
  pv=pvc-7aa1dbeb-f5ac-4807-878a-b93d64bda91f
  writer_verified=true reader_verified=true

after-upgrade/existing-pvc-summary.txt:
  pv=pvc-7aa1dbeb-f5ac-4807-878a-b93d64bda91f
  reader_verified=true

after-rollback/existing-pvc-summary.txt:
  pv=pvc-7aa1dbeb-f5ac-4807-878a-b93d64bda91f
  reader_verified=true
```

Artifact root: `/v/share/g15d-k8s/20260522-155835-2057-helm-lifecycle/`.

### D3 - Three-PVC summary fields

`/v/share/g15d-k8s/20260522-155944-27b3-helm-multi-volume/multi-volume/multi-volume-summary.txt`:

```text
multi_volume_status=ok
requested_volume_count=3
writer_verified_count=3
reader_verified_count=3
managed_volume_count=3
cleanup_status=ok
```

All six required fields present and at expected values.

### D4 - Support bundle replay + blocked bundle explain

`/v/share/g15d-k8s/20260522-160203-227d-helm-support-bundle/basic-app/support-bundle/support-bundle-summary.txt`:

```text
support_bundle_status=ok
report_status=ok
explain_status=ok
timeline_status=ok
read_only=true
report=replayed-report/index.html
cluster_evidence=replayed-report/cluster-evidence.json
timeline=replayed-report/timeline.jsonl
explain=explain.txt
```

Blocked-bundle explain at `blocked-bundle/explain.txt` names a stable reason:

```text
volume pvc-blocked status=blocked rf=3 reason=csi_node_image_pull_failed
condition Attach severity=error reason=csi_node_image_pull_failed
managed_volume_condition Ready status=False reason=csi_node_image_pull_failed
managed_volume_condition Blocked status=True reason=csi_node_image_pull_failed
```

Plus a dry-run `safe_k8s.import_csi_image` action with preconditions and
invariant refs - all read-only / dry-run per the discipline.

### Final residue audit

```text
helm release:                  none
iSCSI active sessions:         none
iscsi nodes DB (io.seaweedfs): none
generated app=sw-blockvolume:  none
sw-block / blockvolume pods:   none
m01 / m02 / tp01 product procs none
```

## Image Sourcing Observation

The D3 and D4 scenarios build fresh images locally
(`sw-block:phase26-d3`, `sw-block:phase26-d4`) via
`scripts/build-alpha-images.sh` and import into k3s, because they exercise new
phase 26 product code:

- D3 needs the materialized workload-port persistence change in
  `core/lifecycle/placement_intent.go` and `verified_placement.go`.
- D4 needs the observation-slot per-`(volume, replica)` merge change in
  `core/host/master/launcher_plan.go`.

D1 and D2 run against the published `sha-28a99ce4f644` image (the same SHA the
v0.3 user docs pin). This is correct shape for QA gating - the published image
floor stays unchanged while phase 26 code changes are gated through local
images.

**Implication for v0.3.1 release**: Before users can consume the multi-volume
and support-bundle behavior, the phase 26 SHA needs to be published to GHCR and
the user-facing image pin needs to be updated. See PM Review Note 1 below.

## PM Review Notes

| Check | Status | Notes |
|---|---|---|
| Helm is the supported alpha install path | OK | README, quickstart, v0.3.1 release note all say so. |
| One narrow upgrade/rollback smoke gated | OK | D2 explicitly scoped to single PVC, RF=1, one upgrade and one rollback. |
| Broad production upgrade safety not claimed | OK | "broad upgrade/rollback safety beyond the gated smoke path" listed under non-claims in README + release note. |
| Support bundle / report / dashboard read-only | OK | `read_only=true` evidence confirmed in D4 summary and blocked-bundle actions are `mode=read_only` or `mode=dry_run`. |
| Operator/CRD lifecycle not included | OK | "No operator lifecycle or CRDs" listed in v0.3.1 non-claims. |

Soft / informational findings (not blocking):

1. **Image SHA pin lags Phase 26.** `README.md` and `docs/quickstart-kubernetes.md`
   still say `Current validated v0.3 walkthrough image tag: sha-28a99ce4f644`.
   Phase 26 multi-volume and support-bundle gates required new product code
   built locally. Before v0.3.1 is published as a user release, the GHCR
   publish SHA should be updated and the doc pin advanced. Otherwise users on
   the documented SHA will get v0.3 baseline (Day-1) behavior, not v0.3.1
   multi-volume or observation-slot improvements.

2. **v0.3.1 release note does not disclose D3/D4 image sourcing.** The release
   note's "Validated Gates" table cites all four gates uniformly. An external
   reader might assume `sha-28a99ce4f644` covers all four; in fact D3/D4 ran
   against locally-built images. Worth a one-line note like "D3/D4 require
   phase 26 code; build floor is the v0.3.1 publish SHA" once it is published.

3. **Quickstart still framed as "v0.3 alpha path"** (line 3). Recommend either
   updating to "v0.3 / v0.3.1 alpha path" or noting in v0.3.1 release note that
   the quickstart is unchanged because the Day-1 user loop is identical.

## Blocking Findings

None.

## Verdict

PASS. Phase 26 is ready to flip to release-candidate.

Recommended release sequence:

1. Publish phase 26 images to GHCR with a new SHA.
2. Bump `sha-28a99ce4f644` references in README and quickstart to the new
   phase 26 SHA.
3. Add one line to v0.3.1 release note explicitly naming the consumable SHA
   floor for the new gates.
4. Then mark v0.3.1 PR ready / cut the release tag.
