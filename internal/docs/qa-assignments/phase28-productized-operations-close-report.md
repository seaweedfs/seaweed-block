# QA Close - Phase 28 Productized Operations

Verdict: **PASS** (conditional on D13 image publication remaining open)

Date: 2026-05-24

Validated source commit: `bf7281b testops: harden phase28 close gate`
(prior cycle FAIL was on `22fac60`; B1/B2/B3 fixes landed in `bf7281b`).

Close scope: Phase 28 D9-D12. D13 release image publication remains
explicitly open until immutable GHCR tags and digests are published and
pinned.

## History

| Cycle | Source commit | Result |
|---|---|---|
| Cycle 1 (original) | `22fac60` | FAIL: HG-6 dashboard route, G2 scenario race, D11 helper path bug |
| Cycle 2 (rerun #1) | `bf7281b` | PARTIAL: B1/B3 fixes confirmed; G2 still flaked once on helper-internal cleanup race |
| Cycle 2 (rerun #2) | `bf7281b` | PASS: 4/4 scenarios PASS, both operator-snapshot checks PASS |

## Run Summary (final cycle, rerun #2)

| Gate | Scenario / Check | Run ID | Result |
|---|---|---:|---|
| G1 | `helm-first-volume-via-sw-block-cli-chain.yaml` | `20260524-103052-beb2` | PASS |
| G2 | `helm-multi-volume-day1-chain.yaml` | `20260524-103143-7c41` | PASS |
| G3 | `helm-support-bundle-diagnostics-chain.yaml` | `20260524-103350-901d` | PASS |
| G4 | operator-snapshot JSON + dashboard route | from G1 + dashboard probe | PASS |
| G5 | `cleanup-residue-chain.yaml` | `20260524-103511-d329` | PASS |

Total: 4/4 scenarios PASS, `operator_snapshot_status=PASS`,
`operator_dashboard_route_status=PASS`, helper
`phase28_productized_ops_close_status=ok`.

Result bundles:

- `C:\work\seaweed_block\results\phase28-productized-ops-close-r2\G{1,2,3,5}\<run-id>\`
- Helper summary:
  `C:\work\seaweed_block\results\phase28-productized-ops-close-r2\phase28-productized-ops-close-summary.txt`

## Hard-Gate Clause Table

| Clause | Result | Evidence |
|---|---|---|
| HG-0 Source contracts present and aligned | PASS | All 5 ref/protocol contracts present; vocabulary `Ready/Blocked/Recovered` consistent across the 4 ref contracts and `read-only-operator-foundation-contract.md` |
| HG-1 Helm first-volume user loop passes from clean state | PASS | G1 run `20260524-103052-beb2`, all summary fields green |
| HG-2 Multi-volume day-1 loop passes and reports 3 ManagedVolumes | PASS | G2 run `20260524-103143-7c41`; `multi_volume_status=ok requested_volume_count=3 writer_verified_count=3 reader_verified_count=3 managed_volume_count=3 cleanup_status=ok` |
| HG-3 Healthy support evidence self-explains | PASS | G3 healthy bundle replay |
| HG-4 Blocked support evidence self-explains with stable reason code | PASS | G3 blocked bundle includes `reason=csi_node_image_pull_failed` |
| HG-5 `sw-block ops report` includes all 5 artifacts incl. `operator-snapshot.json` | PASS | `/v/share/g15d-k8s/20260524-103052-beb2-helm-cli-first-volume/basic-app/status/report/` contains all 5 |
| HG-6 Dashboard serves read-only HTML/JSON/JSONL/summary/**operator-snapshot** | PASS | Helper probe via `operator_dashboard_route_status=PASS`; dashboard-route artifact at `dashboard-route/operator-snapshot.json` verified |
| HG-7 Operator snapshot read-only mutation boundary | PASS | snapshot carries `read_only=true`, `mutation.mutation_allowed=false`, `allowed_modes=[read_only, dry_run]`, `non_claims=[no_promote, no_repair, no_rebuild, no_failback, no_delete, no_cleanup_mutation]`, `crd_contract.group=block.seaweedfs.com` |
| HG-8 ManagedVolume / CRD / Condition contract use same status vocabulary | PASS | `Ready/Blocked/Recovered` Conditions with `reason=<stable_code>` consistent across the 4 contracts and live report |
| HG-9 Cleanup verifier proves zero residue | PASS | G5 PASS; final post-cycle host audit: helm none, iSCSI no sessions/nodes, multipath empty, dmsetup No devices, kubectl no sw-block pods/deploys, per-host product procs (m01/m02/tp01) none |
| HG-10 User-facing non-claims remain narrow and visible | PASS | README + quickstart + v0.3.1 release note all carry consistent narrow non-claims |

## Required Evidence Details

### G1 First Volume

`/v/share/g15d-k8s/20260524-103052-beb2-helm-cli-first-volume/basic-app/first-volume-summary.txt`
shows the required fields including the new B1/N1 fix:

```text
first_volume_status=ok
writer_verified=true
reader_verified=true
inventory_status=ok
cleanup_status=ok
status_report=status/report/index.html
operator_snapshot=status/report/operator-snapshot.json
```

### G2 Multi-Volume

```text
multi_volume_status=ok
requested_volume_count=3
replication_factor=1
writer_verified_count=3
reader_verified_count=3
managed_volume_count=3
cleanup_status=ok
```

### G3 Support Bundle

Healthy + blocked bundles both pass; same shape as Phase 27 close evidence.
Blocked bundle's stable reason code: `csi_node_image_pull_failed`.

### G4 Operator Snapshot

Snapshot path resolved through `-ArtifactShareRoot V:\share\g15d-k8s`:
`V:\share\g15d-k8s\20260524-103052-beb2-helm-cli-first-volume\basic-app\status\report\operator-snapshot.json`

```json
{
  "api_version": "block.seaweedfs.com/v1alpha1",
  "kind": "ReadOnlyOperatorFoundationSnapshot",
  "read_only": true,
  "mutation": {
    "mutation_allowed": false,
    "allowed_modes": ["read_only", "dry_run"],
    "non_claims": ["no_promote","no_repair","no_rebuild",
                   "no_failback","no_delete","no_cleanup_mutation"]
  },
  "crd_contract": { "group": "block.seaweedfs.com", ... }
}
```

Dashboard route probe via the helper:
`dashboard-route/operator-snapshot.json` captured from `sw-block ops
dashboard --from-bundle <bundle>` at `/operator-snapshot.json` returns 200.
Write methods (POST/PUT/PATCH/DELETE) correctly return 405 per the prior
cycle's manual probe (still holds; same handler).

### G5 Cleanup

```text
cleanup_status=ok
multipath_residue_count=0
dmsetup_residue_count=0
failure_count=0
```

Direct host audit after the full close cycle:

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

PM verdict: **PASS**.

A user reading G1+G3+G5 artifacts can answer all 7 PM questions
unambiguously. Claim boundary in README + quickstart + v0.3.1 release note
is narrow and explicit. Operator-snapshot.json carries explicit `read_only`,
`mutation_allowed=false`, and a non_claims array. Vocabulary
`Ready/Blocked/Recovered` with stable `reason=` codes is consistent across
all 4 ref/protocol contracts.

## Blocking Findings

None.

## Non-Blocking Findings

### N2: G2 flaked once across two close-gate cycles (1-in-2 failure)

Cycle 2 rerun #1 (run `20260524-102143-7175`) had G2 fail at
`multi_volume_user_loop` action 0 with `cleanup_status=failed` in the
multi-volume helper. The product user-loop was green
(`writer_verified_count=3 reader_verified_count=3 managed_volume_count=3`);
the failure was the helper's `cleanup_multi_volume()` internal cleanup
deciding rc=1 even though the residue audit showed no resources
("No resources found in default namespace" captured by safe_capture).

Root cause analysis: `scripts/run-multi-volume-example.sh:242-253` has a
TOCTOU race between the wait loop's last `kubectl ... -o name` check and
the post-loop `if kubectl ... -o name | grep -q .` check. If the launcher
finishes async-deletion BETWEEN the wait-loop's last iteration and the
post-loop check (or vice versa), the helper can falsely return rc=1.

Cycle 2 rerun #2 (run `20260524-103143-7c41`) and a third standalone G2
run (`20260524-102808-4873`) both PASS 29/29. So the failure shape is real
but rare.

Fix shape (carry into v0.3.2 hardening or D7 nightly flake matrix):
replace the post-loop `if` check with a flag set by the wait loop:

```bash
local deployments_gone=false
for _ in $(seq 1 90); do
  remaining="$(kubectl ...)"
  if [[ -z "$remaining" ]]; then
    deployments_gone=true
    break
  fi
  sleep 2
done
if [[ "$deployments_gone" != "true" ]]; then
  safe_capture "$ARTIFACT_DIR/logs/blockvolume-deployments.cleanup-timeout.txt" \
    kubectl -n "$NAMESPACE" get deploy -l app=sw-blockvolume -o wide
  rc=1
fi
```

D12 close does not block on N2 because rerun #2 cleared. Worth tracking
into the next flake-matrix cycle so it doesn't regress under load.

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
close.

## Final Recommendation

**Recommended close for Phase 28 D12.**

All HG-0 through HG-10 clauses PASS on rerun #2 with the `bf7281b` fix
commit. The one observed G2 flake on rerun #1 is in the helper's cleanup
path (not in the new scenario poll fix), reproduces only 1-in-3 attempts in
this cycle, and the underlying product evidence is uniformly green when the
helper finishes cleanly. The TOCTOU fix shape is documented as a
non-blocking follow-up for the next hardening cycle.

D13 release packaging remains open as expected; D12 can close ahead of
D13 once the team accepts this report.
