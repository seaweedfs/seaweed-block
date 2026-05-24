# QA Close Template - Phase 28 Productized Operations

Verdict: `PASS|FAIL`

Date: `YYYY-MM-DD`

Validated source commit: `<commit>`

Close scope: Phase 28 D9-D12. D13 release image publication is intentionally
separate unless immutable GHCR tags and digests are included below.

## Run Summary

| Gate | Scenario / Check | Run ID | Result |
|---|---|---:|---|
| G1 | `helm-first-volume-via-sw-block-cli-chain.yaml` | `<run-id>` | `PASS|FAIL` |
| G2 | `helm-multi-volume-day1-chain.yaml` | `<run-id>` | `PASS|FAIL` |
| G3 | `helm-support-bundle-diagnostics-chain.yaml` | `<run-id>` | `PASS|FAIL` |
| G4 | operator snapshot report/dashboard artifact check | `<run-id or bundle>` | `PASS|FAIL` |
| G5 | `cleanup-residue-chain.yaml` | `<run-id>` | `PASS|FAIL` |

Total actions: `<passed>/<total>`

## Hard-Gate Clause Table

| Clause | Result | Evidence |
|---|---|---|
| HG-0 Source contracts present and aligned | `PASS|FAIL` | `<files / notes>` |
| HG-1 Helm first-volume user loop passes from clean state | `PASS|FAIL` | `<run-id, first-volume-summary>` |
| HG-2 Multi-volume day-1 loop passes and reports 3 ManagedVolumes | `PASS|FAIL` | `<run-id, summary fields>` |
| HG-3 Healthy support evidence self-explains | `PASS|FAIL` | `<bundle path>` |
| HG-4 Blocked support evidence self-explains with stable reason code | `PASS|FAIL` | `<reason_code, explain output>` |
| HG-5 `sw-block ops report` includes all five artifacts | `PASS|FAIL` | `index.html, cluster-evidence.json, timeline.jsonl, summary.txt, operator-snapshot.json` |
| HG-6 Dashboard serves read-only HTML/JSON/JSONL/summary/operator snapshot | `PASS|FAIL` | `<dashboard evidence>` |
| HG-7 Operator snapshot has read-only mutation boundary | `PASS|FAIL` | `"read_only": true`, `"mutation_allowed": false` |
| HG-8 ManagedVolume and CRD/Condition contract use same status vocabulary | `PASS|FAIL` | `<contract review>` |
| HG-9 Cleanup verifier proves zero Kubernetes/iSCSI/multipath/dmsetup/process residue | `PASS|FAIL` | `<cleanup-summary, host audit>` |
| HG-10 User-facing non-claims remain narrow and visible | `PASS|FAIL` | `<README/quickstart/release note review>` |

## Required Evidence Details

### G1 First Volume

Required fields:

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

Required fields:

```text
multi_volume_status=ok
requested_volume_count=3
writer_verified_count=3
reader_verified_count=3
managed_volume_count=3
cleanup_status=ok
```

### G3 Support Bundle

Healthy bundle must answer:

- installed stack state,
- volume count,
- ManagedVolume status,
- where evidence is located.

Blocked bundle must answer:

- blocker reason code,
- affected component,
- read-only/dry-run next action,
- non-claims.

### G4 Operator Snapshot

Required JSON evidence:

```json
{
  "api_version": "block.seaweedfs.com/v1alpha1",
  "kind": "ReadOnlyOperatorFoundationSnapshot",
  "read_only": true,
  "mutation": {
    "mutation_allowed": false
  }
}
```

Allowed action modes must be only:

```text
read_only
dry_run
```

### G5 Cleanup

Required fields:

```text
cleanup_status=ok
multipath_residue_count=0
dmsetup_residue_count=0
failure_count=0
```

Direct host audit must show:

```text
iscsiadm -m session -> no active sessions
iscsiadm -m node    -> no sw-block records
multipath -ll       -> no sw-block maps
dmsetup ls          -> no sw-block devices
kubectl             -> no sw-block pods/deployments after uninstall
ps/tasklist         -> no product processes
```

## PM Review

PM verdict: `PASS|FAIL`

PM should confirm a user can understand:

- what was installed,
- what volumes exist,
- which PVC maps to which sw-block volume,
- why a blocked volume is blocked,
- why the operator foundation is read-only,
- what is not claimed yet,
- what cleanup evidence proves the lab is clean.

## Blocking Findings

- `<none or numbered findings>`

## Non-Blocking Findings

- `<none or numbered observations>`

## D13 Release Packaging Status

Immutable image tags:

```text
ghcr.io/seaweedfs/seaweed-block:sha-<commit>
ghcr.io/seaweedfs/seaweed-block-csi:sha-<commit>
```

Digests:

```text
seaweed-block     sha256:<digest or pending>
seaweed-block-csi sha256:<digest or pending>
```

If image tags/digests are pending, D13 remains open even if D12 passes.

## Final Recommendation

`Recommended close|Do not close`

Reason:

```text
<one paragraph>
```
