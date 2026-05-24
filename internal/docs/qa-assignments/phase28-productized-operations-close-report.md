# QA Close - Phase 28 Productized Operations

Verdict: **FAIL**

Date: 2026-05-24

Validated source commit: `22fac60 testops: add phase28 operations close runner`

Close scope: Phase 28 D9-D12. D13 release image publication is intentionally
separate; not all immutable GHCR tags and digests are pinned in this report
yet, so D13 also remains open.

## Run Summary

| Gate | Scenario / Check | Run ID | Result |
|---|---|---:|---|
| G1 | `helm-first-volume-via-sw-block-cli-chain.yaml` | `20260524-093315-c80c` | PASS |
| G2 | `helm-multi-volume-day1-chain.yaml` | `20260524-093404-9c0d` | FAIL (scenario race) |
| G3 | `helm-support-bundle-diagnostics-chain.yaml` | `20260524-093620-088a` | PASS |
| G4 | operator-snapshot report + dashboard artifact check | (from G1) | PARTIAL |
| G5 | `cleanup-residue-chain.yaml` | `20260524-093747-42c2` | PASS |

Total close-helper scenarios: 3 PASS / 1 FAIL (4 total). D11 operator-snapshot
helper reported `failed` due to a path-lookup bug (the file IS present; see
HG-5/HG-7).

Result bundles:

- `C:\work\seaweed_block\results\phase28-productized-ops-close\G1\20260524-093315-c80c\`
- `C:\work\seaweed_block\results\phase28-productized-ops-close\G2\20260524-093404-9c0d\`
- `C:\work\seaweed_block\results\phase28-productized-ops-close\G3\20260524-093620-088a\`
- `C:\work\seaweed_block\results\phase28-productized-ops-close\G5\20260524-093747-42c2\`
- Helper summary:
  `C:\work\seaweed_block\results\phase28-productized-ops-close\phase28-productized-ops-close-summary.txt`

## Hard-Gate Clause Table

| Clause | Result | Evidence |
|---|---|---|
| HG-0 Source contracts present and aligned | PASS | All 5 ref/protocol contracts present; status vocabulary `Ready/Blocked/Recovered` consistent across `managed-volume-operational-model-contract.md`, `operator-crd-condition-event-contract.md`, `read-only-operator-foundation-contract.md`, `multi-volume-ha-support-evidence-contract.md` |
| HG-1 Helm first-volume user loop passes from clean state | PASS | `20260524-093315-c80c`; first-volume-summary fields all green: `first_volume_status=ok writer_verified=true reader_verified=true inventory_status=ok cleanup_status=ok` |
| HG-2 Multi-volume day-1 loop passes and reports 3 ManagedVolumes | PASS (product) / FAIL (scenario assertion) | Product evidence in `multi-volume-summary.txt`: `multi_volume_status=ok requested_volume_count=3 writer_verified_count=3 reader_verified_count=3 managed_volume_count=3 cleanup_status=ok`. Scenario itself failed at brittle 2-min exec race in `multi_volume_asserts` action 8. See B2. |
| HG-3 Healthy support evidence self-explains | PASS | G3 healthy bundle includes `support_bundle_status=ok report_status=ok explain_status=ok timeline_status=ok read_only=true` from prior Phase 27 audit; G3 here PASS again |
| HG-4 Blocked support evidence self-explains with stable reason code | PASS | G3 blocked bundle includes `reason=csi_node_image_pull_failed` and read-only/dry-run action shape |
| HG-5 `sw-block ops report` includes all 5 artifacts including `operator-snapshot.json` | PASS | G1 bundle `/v/share/g15d-k8s/20260524-093315-c80c-helm-cli-first-volume/basic-app/status/report/` contains `index.html`, `cluster-evidence.json`, `timeline.jsonl`, `summary.txt`, `operator-snapshot.json` (all 5) |
| HG-6 Dashboard serves read-only HTML/JSON/JSONL/summary/**operator-snapshot** | **FAIL** | Dashboard served from G1 bundle returns 200 for `/`, `/index.html`, `/summary.txt`, `/cluster-evidence.json`, `/timeline.jsonl`. Returns **404 for `/operator-snapshot.json` and every other reasonable URL**. POST/PUT/PATCH/DELETE correctly 405. See B1. |
| HG-7 Operator snapshot has read-only mutation boundary | PASS | `operator-snapshot.json` carries `"api_version":"block.seaweedfs.com/v1alpha1"`, `"kind":"ReadOnlyOperatorFoundationSnapshot"`, `"read_only":true`, `"mutation":{"mutation_allowed":false,"allowed_modes":["read_only","dry_run"],"non_claims":[...]}`, `crd_contract.group="block.seaweedfs.com"`, one volume entry per ManagedVolume |
| HG-8 ManagedVolume and CRD/Condition contract use same status vocabulary | PASS | `Ready/Blocked/Recovered` Condition types with `reason=<stable_code>` consistent across `operator-crd-condition-event-contract.md`, `read-only-operator-foundation-contract.md`, and live `summary.txt` output |
| HG-9 Cleanup verifier proves zero residue | PASS | G5 PASS; final post-run host audit: `helm list -A` none, `iscsiadm -m session` none, `iscsiadm -m node` no seaweedfs records, `multipath -ll` empty, `dmsetup ls` No devices, `kubectl get deploy -A -l app=sw-blockvolume` No resources, `kubectl get pods | sw-block` none, per-host product procs (m01/m02/tp01) none |
| HG-10 User-facing non-claims remain narrow and visible | PASS | README §Known missing pieces + §What Users Should Not Expect Yet + `docs/quickstart-kubernetes.md` §Current Alpha Limitations + `docs/releases/v0.3.1-alpha.md` §Explicit Non-Claims all consistent: no operator lifecycle, no mutating admin, no backup/restore, no broad SLOs, no NVMe ANA, no transparent node-loss without pod recreate |

## Required Evidence Details

### G1 First Volume

`/v/share/g15d-k8s/20260524-093315-c80c-helm-cli-first-volume/basic-app/first-volume-summary.txt`:

```text
first_volume_status=ok
writer_verified=true
reader_verified=true
inventory_status=ok
cleanup_status=ok
status_report=status/report/index.html
```

(Note: `operator_snapshot=status/report/operator-snapshot.json` line not in
summary today; file IS in the report dir. See N1.)

### G2 Multi-Volume

`/v/share/g15d-k8s/20260524-093404-9c0d-helm-multi-volume/multi-volume/multi-volume-summary.txt`:

```text
multi_volume_status=ok
requested_volume_count=3
replication_factor=1
writer_verified_count=3
reader_verified_count=3
managed_volume_count=3
cleanup_status=ok
```

User-loop fields are green. Scenario failed at separate
`multi_volume_asserts` phase, action 8, on a brittle 2-min `test -z
"$(kubectl get deploy -l app=sw-blockvolume -o name)"` exec - the same
async-cleanup race I fixed in the Phase 27 D1 RF=3 readiness scenario by
adding a 60-iteration poll loop. The helper `scripts/run-multi-volume-example.sh`
already waits asynchronously; the scenario's redundant inline check is the
race source.

### G3 Support Bundle

PASS. Bundle replay artifacts present and self-explaining; same shape as the
Phase 27 close report's G3 evidence.

### G4 Operator Snapshot

`/v/share/g15d-k8s/20260524-093315-c80c-helm-cli-first-volume/basic-app/status/report/operator-snapshot.json`:

```json
{
  "api_version": "block.seaweedfs.com/v1alpha1",
  "kind": "ReadOnlyOperatorFoundationSnapshot",
  "read_only": true,
  "mutation": {
    "mutation_allowed": false,
    "allowed_modes": ["read_only", "dry_run"],
    "non_claims": ["no_promote", "no_repair", "no_rebuild",
                   "no_failback", "no_delete", "no_cleanup_mutation"]
  },
  "crd_contract": {
    "group": "block.seaweedfs.com",
    "version": "v1alpha1",
    "resources": [
      { "kind": "SwBlockCluster", "scope": "Namespaced", ... }
    ]
  }
}
```

JSON contract: PASS. Dashboard route for it: FAIL (HG-6, B1).

### G5 Cleanup

G5 cleanup-residue scenario PASS 13/13. `cleanup-summary.txt`:

```text
cleanup_status=ok
multipath_residue_count=0
dmsetup_residue_count=0
failure_count=0
```

Direct host audit (m02):

```text
helm list -A      -> none
iscsiadm session  -> No active sessions
iscsiadm node     -> no seaweedfs records
multipath -ll     -> (empty)
dmsetup ls        -> No devices found
kubectl pods/deploy | sw-block -> none
per-host procs (m01/m02/tp01)  -> none
```

## PM Review

PM verdict: **CONDITIONAL PASS** (claim boundary acceptable; depends on
HG-6 dashboard fix landing before release).

A user reading G1+G3+G5 artifacts can answer all 7 PM questions:

- What is installed? -> `summary.txt` "sw-block report" header + values
- How many volumes are ready? -> `volumes=N` + `managed_volume = ready` lines
- Which PVC maps to which sw-block volume? -> per-volume line
  `volume=pvc-... pvc=default/<name>`
- Why is a volume blocked? -> G3 blocked bundle explains
  `reason=csi_node_image_pull_failed` + dry-run next-action
- Is this report read-only? -> `read_only=true` + `operator-snapshot.json`
  read_only contract
- What is not claimed yet? -> README + release-note non-claims sections
- What cleanup evidence proves the lab is clean? -> G5 `cleanup-summary.txt`
  + host audit

The claim boundary in user docs is narrow and explicit. The non-claims list
covers operator lifecycle, mutating admin, backup/restore, broad SLOs, NVMe
ANA, transparent node-loss without pod recreate.

## Blocking Findings

### B1: Dashboard does not serve `operator-snapshot.json` (HG-6)

`sw-block ops dashboard --from-bundle <G1>` returns 200 for `/`,
`/index.html`, `/summary.txt`, `/cluster-evidence.json`, `/timeline.jsonl`
but **404 for `/operator-snapshot.json`** and every other reasonable URL
(`/status/report/operator-snapshot.json`, `/report/operator-snapshot.json`,
`/api/operator-snapshot.json` all 404). The file exists in the bundle dir;
the dashboard handler simply has no route to it.

Read-only HTTP boundary is OK (POST/PUT/PATCH/DELETE all return 405).

Fix shape: add a route to the dashboard handler that serves
`<bundle>/status/report/operator-snapshot.json` with the appropriate
`Content-Type: application/json` at `/operator-snapshot.json`.

Reproduce:

```bash
sw-block ops dashboard --from-bundle <G1-bundle> --listen 127.0.0.1:9334 \
  --serve-duration 30s
curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:9334/operator-snapshot.json
# 404
```

### B2: G2 scenario brittle async-cleanup race (HG-2)

`testops/scenarios/helm-multi-volume-day1-chain.yaml` phase
`multi_volume_asserts`, action 8:

```yaml
- action: exec
  cmd: "test -z \"$(kubectl -n default get deploy -l app=sw-blockvolume -o name)\""
  timeout: 2m
```

This fires immediately after the multi-volume helper exits. Because the
launcher async-deletes BlockVolume Deployments triggered by PVC delete events,
the deployments may still be Terminating at this exact moment, especially
at higher RF or under transient kube-api load. The helper itself
(`scripts/run-multi-volume-example.sh`) already polls for ~3 min until
deployments are gone; the inline scenario check is redundant AND racy.

Fix shape: replace the inline `test -z` with a poll loop, e.g.:

```yaml
- action: exec
  cmd: |
    for i in $(seq 1 60); do
      out=$(kubectl -n default get deploy -l app=sw-blockvolume -o name 2>/dev/null);
      [ -z "$out" ] && exit 0;
      sleep 2;
    done;
    kubectl -n default get deploy -l app=sw-blockvolume -o wide >&2;
    exit 1
  timeout: 3m
```

This is the exact pattern I used in
`testops/scenarios/helm-multi-volume-rf3-readiness-chain.yaml`.

### B3: Close-gate helper looks for operator-snapshot in wrong path (D11)

`scripts/run-phase28-productized-ops-close-gate.ps1:112` uses:

```powershell
Get-ChildItem -Path (Join-Path $resolvedResults "G1") -Recurse `
              -Filter "operator-snapshot.json"
```

But the local `results\phase28-productized-ops-close\G1\<run-id>\` bundle
contains only runner metadata (manifest.json, status.json, result.xml,
scenario.yaml). The actual test artifacts including `operator-snapshot.json`
are written to the SMB share at
`/v/share/g15d-k8s/<run-id>-helm-cli-first-volume/basic-app/status/report/`.

Fix shape: read the run_id from G1's `status.json`, then resolve the
artifact path under the SMB share root (which is the scenario's
`{{ run_id }}`-prefixed dir). Or extract from the runner artifacts subdir
that the runner writes on collect.

## Non-Blocking Findings

### N1: G1 `first-volume-summary.txt` does not surface `operator_snapshot=` line

The close-report template includes:

```text
operator_snapshot=status/report/operator-snapshot.json
```

The actual G1 summary today only lists `status_report=status/report/index.html`.
The operator-snapshot file is at the expected path; only the convenience
pointer in the summary is missing. Worth adding to make it discoverable from
the summary without grepping the report dir.

## D13 Release Packaging Status

Immutable image tags: **PENDING - not pinned in this close cycle**

```text
ghcr.io/seaweedfs/seaweed-block:sha-<commit>      [pending publish for phase28]
ghcr.io/seaweedfs/seaweed-block-csi:sha-<commit>  [pending publish for phase28]
```

Digests:

```text
seaweed-block     sha256:<pending>
seaweed-block-csi sha256:<pending>
```

D13 remains explicitly open. Per the assignment this is independent of D12
close; QA notes it here for the release-readiness chain.

## Final Recommendation

**Do not close Phase 28 D12 yet.**

Reason:

The product evidence for first-volume (G1), multi-volume (G2 user-loop
under the failed scenario), support bundle (G3), cleanup (G5), and the
operator-snapshot JSON contract (HG-7) is all green. ManagedVolume / CRD /
Condition / Event vocabulary is aligned. Cleanup is hygienic. User-doc
non-claims are narrow and visible.

But HG-6 has a real product gap: the dashboard does not serve
`/operator-snapshot.json`. The close-gate assignment explicitly requires it,
and shipping with a "Read-Only Operator Foundation" claim while the
operator snapshot is not reachable from the dashboard would mismatch the
product surface. Fix is small (one HTTP route on the dashboard handler) and
unblocks D12.

Recommended sequence:

1. Land B1 fix (dashboard `/operator-snapshot.json` route).
2. Land B2 fix (scenario poll loop for multi-volume cleanup).
3. Land B3 fix (helper script path lookup).
4. Optionally land N1 (add `operator_snapshot=` line to first-volume summary).
5. Rerun the close-gate helper. Expect all 4 scenarios PASS, helper
   `operator_snapshot_status=ok`, and HG-6 dashboard probe returning 200 on
   `/operator-snapshot.json`.
6. Then D12 closes.
7. D13 release packaging proceeds independently when GHCR images are
   published and pinned in README/quickstart/release note.
