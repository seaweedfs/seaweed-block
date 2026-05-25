# QA Sign-off - Phase 32 D3 / D4 Status Surface

Verdict: **PASS** for both D3 happy-path and D4 blocked-path surface
agreement. Negative-first rule held: no `Ready=True` anywhere in the
blocked path.

Date: 2026-05-25

Validated source commit: `ece8812 ops: validate phase32 status surfaces`
(builds on `97a8027` D2 EvidenceStale contract).

## Scope

Surface-agreement validation across report `summary.txt`, dashboard
`/operator-snapshot.json`, and operator-snapshot JSON for:
- D3 canonical happy first-volume path,
- D4 canonical blocked CSI-image-pull path.

## Run Summary

| Sub-gate | Scenario / Source | QA run / probe | Result |
|---|---|---:|---|
| Scoped unit tests | `go test ./core/ops ./cmd/sw-block` | local | PASS |
| D3 scenario | `helm-first-volume-via-sw-block-cli-chain.yaml` | `20260525-141234-8a89` | 34/34 PASS |
| D4 scenario | `helm-support-bundle-diagnostics-chain.yaml` | `20260525-141341-9c7d` | 38/38 PASS |
| D3 dashboard `/operator-snapshot.json` | fresh-built binary against G1 bundle | live probe | HTTP 200 |
| D4 dashboard `/operator-snapshot.json` | fresh-built binary against blocked bundle | live probe | HTTP 200 |

## D3 Happy-Path Hard-Gate

`/v/share/g15d-k8s/20260525-141234-8a89-helm-cli-first-volume/basic-app/status/report/summary.txt`:

```text
managed_volume=pvc-03d47a22-300d-4dcc-ac53-e188e4090032 status=ready reason=first_volume_verified
managed_volume_condition=Ready status=True reason=first_volume_verified severity=info
read_only=true
```

`operator-snapshot.json` cluster section:

```json
"read_only": true,
"mutation": {"mutation_allowed": false, "allowed_modes": ["read_only", "dry_run"]},
"cluster": {
  "volume_count": 1,
  "ready_volume_count": 1,
  "blocked_volume_count": 0,
  "stale_volume_count": 0
}
```

Per-volume:

```json
{
  "pvc_name": "sw-block-example-pvc",
  "status": "ready",
  "reason_code": "first_volume_verified",
  "conditions": [{"type": "Ready", "status": "True", ...}]
}
```

Dashboard probe (`sw-block ops dashboard --from-bundle <bundle>`):

```text
200 /
200 /index.html
200 /summary.txt
200 /cluster-evidence.json
200 /timeline.jsonl
200 /operator-snapshot.json   ← carries same ready_volume_count=1 + reason_code=first_volume_verified
POST/PUT/PATCH/DELETE 405
```

| Required | Result |
|---|---|
| Scenario PASS from clean lab | PASS |
| summary `managed_volume status=ready reason=first_volume_verified` | PASS |
| summary `Condition Ready=True reason=first_volume_verified severity=info` | PASS |
| summary `read_only=true` | PASS |
| operator-snapshot `read_only=true` + `mutation_allowed=false` | PASS |
| operator-snapshot `cluster.ready_volume_count=1` | PASS |
| per-volume `status=ready, reason_code=first_volume_verified` | PASS |
| dashboard `/operator-snapshot.json` returns 200 with same reason | PASS |

## D4 Blocked-Path Hard-Gate

Source: `helm-support-bundle-diagnostics-chain.yaml` produces a synthetic
blocked-bundle at
`/v/share/g15d-k8s/20260525-141341-9c7d-helm-support-bundle/blocked-bundle/`
with `explain.txt` + `demo/kube-system-pods-deploys.txt`. Running
`sw-block ops report --from-bundle <blocked-bundle>` against it regenerates
the status surfaces deterministically.

Report summary (regenerated from blocked-bundle):

```text
status=blocked
volume=unknown status=blocked pvc=-/- primary=-@- frontend=- rf=3 ack=-
managed_volume=unknown status=blocked reason=csi_node_image_pull_failed
managed_volume_condition=Ready status=False reason=csi_node_image_pull_failed severity=warning
managed_volume_condition=Blocked status=True reason=csi_node_image_pull_failed severity=warning
managed_volume_action=observe.collect_bundle mode=read_only side_effect=observe executor=ops
managed_volume_action=safe_k8s.import_csi_image mode=dry_run side_effect=safe_k8s executor=installer_or_operator
read_only=true
```

Operator-snapshot cluster + per-volume:

```json
"cluster": {
  "status": "blocked",
  "volume_count": 1,
  "ready_volume_count": 0,
  "blocked_volume_count": 1,
  "stale_volume_count": 0
},
"volumes": [{
  "volume_id": "unknown",
  "status": "blocked",
  "reason_code": "csi_node_image_pull_failed",
  "conditions": [
    {"type": "Ready", "status": "False", "reason": "csi_node_image_pull_failed", ...},
    {"type": "Blocked", "status": "True", "reason": "csi_node_image_pull_failed", ...}
  ]
}]
```

Dashboard probe against blocked-bundle:

```text
HTTP 200 /operator-snapshot.json
  blocked_volume_count: 1
  reason_code: csi_node_image_pull_failed
  mutation_allowed: false (per cluster + per action)
```

| Required | Result |
|---|---|
| Blocked evidence uses `reason=csi_node_image_pull_failed` | PASS |
| summary `managed_volume status=blocked reason=csi_node_image_pull_failed` | PASS |
| summary `Condition Ready=False severity=warning` | PASS |
| summary `Condition Blocked=True severity=warning` | PASS |
| summary action `safe_k8s.import_csi_image mode=dry_run` | PASS |
| operator-snapshot `cluster.blocked_volume_count>=1` | PASS (= 1) |
| operator-snapshot no `Ready=True` for blocked volume | PASS (Ready=False) |
| operator-snapshot per-volume `Blocked=True` | PASS |
| every action has `mutation_allowed=false` | PASS |
| dashboard `/operator-snapshot.json` 200 with same reason | PASS |

## Failure-Rule Check

The PM-visible rule:

> Fail D3/D4 if any product surface reports `Ready=True` for the blocked
> CSI image-pull path.

Audit across all surfaces produced from the blocked-bundle:
- `summary.txt`: `Ready status=False` (1 occurrence). No `Ready=True`.
- `operator-snapshot.json`: per-volume `Ready Status="False"`. Cluster
  `ready_volume_count=0`. No `Ready=True`.
- Dashboard `index.html`: Lifecycle / Managed Volume Conditions table
  renders `Ready=False` for the blocked volume.
- Dashboard `operator-snapshot.json`: same as the report-bundle file.

**No `Ready=True` anywhere in the blocked path.** Rule held.

## Cross-Surface Agreement Matrix

For the blocked CSI image-pull path:

| Surface | `status` | Ready | Blocked | reason_code | action mode |
|---|---|---|---|---|---|
| report `summary.txt` | blocked | False | True | csi_node_image_pull_failed | observe.collect_bundle=read_only, safe_k8s.import_csi_image=dry_run |
| report `operator-snapshot.json` | blocked | False | True | csi_node_image_pull_failed | mutation_allowed=false (cluster + per action) |
| dashboard `/operator-snapshot.json` | blocked | False | True | csi_node_image_pull_failed | mutation_allowed=false |
| dashboard `index.html` lifecycle table | blocked | False | True | csi_node_image_pull_failed | n/a (UI surface) |
| `explain.txt` (cold reader) | blocked | False | True | csi_node_image_pull_failed | dry_run action with preconditions + invariant refs |

All five surfaces agree on every column. Surface-agreement contract from
the D1a Workstream B negative-status review holds in live evidence.

## Final Residue Audit

```text
helm release sw-block:           none
iscsiadm sessions:               No active sessions
multipath -ll:                   empty
dmsetup ls:                      No devices found
sw-block / blockvolume pods:     none
```

## Blocking Findings

**None.**

## Non-Blocking Findings

### N1: Pre-built local CLI on m02 may be stale

My first dashboard probe used `/tmp/sw-block-runner-native` (a binary I
built during a Phase 27 spike) which returned `404 /operator-snapshot.json`
- it predates the Phase 28 B1 dashboard-route fix. The fresh build from
the synced D3/D4 tree returned 200 as expected.

This is a QA-hygiene reminder, not a product issue: when running
dashboard probes against newly built bundles, also build a fresh binary
from the same source commit. Worth a one-liner in the Phase 32 D3/D4
assignment to make this explicit for future QA cycles.

## Verdict

Phase 32 D3 happy and D4 blocked status-surface sign-off **PASS**.

The five user-facing surfaces (report summary, report operator-snapshot,
dashboard HTML, dashboard operator-snapshot endpoint, support-bundle
explain) agree on `status`, `Ready`, `Blocked`, `reason_code`, and action
mutation boundary for both the happy path and the canonical blocked
CSI-image-pull path. Negative-first rule held end to end. Read-only +
non-mutating discipline preserved.

D3 + D4 close the surface-agreement requirement; D5 (restart consistency)
and D6 (multi-volume isolation) at the status-surface level remain as
separate sign-off cycles when dev ships them.
