# Phase 32 D2 CRD / Condition / Event QA Assignment

Date: 2026-05-25

Owner: QA.

Purpose: validate the D2 alpha contract for the read-only Kubernetes status
surface. This is a contract-level QA task, not a full scenario rerun.

## Source Commit Under Test

Use the dev commit that includes:

- `ConditionEvidenceStale`
- `ReasonEvidenceStale`
- CRD contract `read_only=true`
- CRD contract RBAC boundary
- `stale_volume_count` in `operator-snapshot.json`

## Commands

Run from repository root:

```bash
go test ./core/ops ./cmd/sw-block
```

## Contract Checks

Inspect `core/ops/managed_volume_crd_contract.go` or the JSON emitted through
`operator-snapshot.json` and confirm:

- `crd_contract.read_only=true`
- `crd_contract.rbac.mutating_storage_verbs_allowed=false`
- allowed verbs include:
  - `get`
  - `list`
  - `watch`
  - `update_status`
  - `patch_status`
  - `create_event`
- forbidden actions include:
  - `promote`
  - `repair`
  - `rebuild`
  - `failback`
  - `delete_storage`
  - `cleanup_live_state`
- Condition vocabulary includes:
  - `Ready`
  - `Blocked`
  - `Recovering`
  - `Recovered`
  - `CleanupRequired`
  - `EvidenceStale`

## EvidenceStale Checks

Confirm the tests prove this behavior:

```text
ManagedVolumeFacts{EvidenceStale:true}
-> projection.status=unknown
-> reason_code=evidence_stale
-> Ready status=Unknown
-> EvidenceStale status=True severity=warning
-> operator event type=Warning reason=evidence_stale
-> operator snapshot cluster.stale_volume_count=1
```

## Non-Claims

D2 must not add or imply:

- a running Kubernetes controller manager,
- mutating operator actions,
- repair/rebuild/failback,
- cleanup mutation,
- backup/snapshot/restore,
- production SLO.

## Pass Criteria

D2 QA sign-off passes when:

- scoped tests pass,
- all contract checks above are present,
- no mutating action is exposed,
- QA records any wording concerns as non-blocking unless the contract suggests
  mutation or false readiness.

Expected sign-off path:

```text
internal/docs/qa-assignments/phase32-d2-crd-condition-event-qa-signoff.md
```

