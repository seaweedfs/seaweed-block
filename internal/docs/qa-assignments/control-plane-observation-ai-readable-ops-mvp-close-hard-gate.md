# QA Assignment: Control-Plane Observation / AI-Readable Ops MVP Close Hard Gate

Status: draft gate for the active `Control-Plane Observation / AI-Readable Ops
MVP` plan.

This gate is intentionally strict. The plan closes only if recovery evidence is
available from the product observation surface itself, not only from TestOps
reconstructed timelines, raw Kubernetes logs, or SSH inspection.

## Product Contract Under Test

```text
surface=read-only observation
consumers=human CLI, JSON/JSONL automation, support bundle, future dashboard/AI
authority=blockmaster product evidence
mutation=none
validated_flow=RF3 sync-quorum Kubernetes node-loss recovery through CSI/pod recreate
required_artifact=demo/product-observation/cluster-evidence.json
```

The MVP does not ship a hosted dashboard or mutating admin controls. It proves
the evidence model, CLI/API export path, and support-bundle shape that a
dashboard or AI assistant can consume later.

## Required Runner Scenario

Expected scenario:

```text
testops/scenarios/node-loss-survival-rf3-reattach-chain.yaml
```

Expected D5 evidence run:

```text
20260517-011004-4b79
```

If QA uses a newer run, the report must state the replacement run id and why.

Supporting blocked/attach evidence run:

```text
20260516-154813-109a
```

This failed node-loss run is not the recovery proof. It is the preserved
blocked-image artifact used only for the bundle-backed explanation clause.

## Hard-Gate Clauses

Any single `FAIL` blocks close.

### HG-0 Documentation Entry

Pass:

- `docs/operations-v1.md` documents the AI-readable control-plane status path.
- `docs/quickstart-kubernetes.md` shows how to export
  `sw-block ops cluster --master-api <addr> -o json`.
- Docs say the first dashboard/AI path is read-only.

Fail:

- docs still describe `sw-block ops cluster` only as future/intended work,
- docs imply the MVP includes a hosted dashboard,
- docs imply the observation path can mutate repair/promote/rebuild state.

### HG-1 Product-Owned Cluster Evidence Artifact

Pass:

- the D5 bundle contains
  `demo/product-observation/cluster-evidence.json`,
- the artifact is non-empty JSON,
- it is produced by `sw-block ops cluster --master-api`, not by parsing TestOps
  timeline files.

Fail:

- QA must reconstruct the recovery from runner logs,
- the artifact is missing or empty,
- the export path depends on `svc/blockmaster` existing when the scenario only
  preserved the Deployment.

### HG-2 Required Event Types

Pass:

`cluster-evidence.json` contains all required product event types:

- `placement_verified`,
- `promotion_candidate_evaluated`,
- `authority_published`,
- `csi_reattach_observed`.

Fail:

- any required event type is absent,
- `authority_published` appears only in blockmaster logs and not the product
  event stream,
- CSI reattach is inferred only from reader success or Kubernetes pod logs.

### HG-3 Master-Minted Event Identity

Pass:

- product events have stable `event_id`,
- product events have `event_time`,
- `authority_published` has a master-minted event id/time,
- externally reported CSI events are ingested and re-minted by master.

Fail:

- external CSI code can spoof master authority event IDs or event times,
- `event_id` is missing,
- event order cannot be audited.

### HG-4 CSI Reattach Evidence

Pass:

- `csi_reattach_observed` names the node that staged the replacement mount,
- `new_value` or equivalent publish target equals the promoted frontend,
- the frontend differs from the failed primary frontend in the node-loss run,
- epoch and endpoint version are present when known.

Fail:

- CSI reattach is only inferred from reader checksum,
- the event still points to the failed primary frontend,
- promoted frontend and failed frontend cannot be distinguished.

### HG-5 Authority And Recovery Story

Pass:

From `cluster-evidence.json` plus the same run's compact recovery artifacts
(`primary-failure-recovery.txt`, `node-loss-recovery-summary.txt`, and
reader/writer logs), a cold reader can identify:

- failed primary replica and node,
- promoted replica and node,
- exactly one primary after promotion,
- stale primary fenced or unavailable,
- data check result.

Fail:

- stale primary and promoted primary are ambiguous,
- the event stream contradicts the recovery summary,
- the product evidence claims data verification without a writer/reader
  artifact proving it.

### HG-6 Read-Only Boundary

Pass:

- observation CLI/API calls do not mutate Kubernetes, authority, lifecycle,
  placement, iSCSI, or replica state,
- tests or audit notes cover read-only behavior for the master snapshot/API.

Fail:

- any `ops cluster`, `ops describe`, `ops timeline`, `ops explain`, or bundle
  command can promote, repair, rebuild, delete, or cleanup.

### HG-7 Stable Reason Codes And Statuses

Pass:

- recovering or blocked states carry stable reason codes,
- JSON includes schema version or equivalent versioned shape,
- known reason codes include promotion, blocked, stale-primary, and attach/image
  failure vocabulary.

Fail:

- output requires interpreting free-form log text,
- blocked/recovering state lacks a stable reason code.

### HG-8 Bundle-Backed Explanation

Pass:

- bundle-backed `describe`, `timeline`, or `explain` can explain the successful
  node-loss recovery bundle without raw TestOps logs,
- the preserved failed image-pull bundle `20260516-154813-109a` can explain
  `csi_node_image_pull_failed` or equivalent blocked evidence with node, image,
  pod, and next action.

Fail:

- the only diagnosis path is SSH plus `kubectl describe` plus grep,
- missing image or `ImagePullBackOff` cannot be named by the product/support
  output.

### HG-9 Support Evidence Completeness

Pass:

The support path documents or captures:

- cluster evidence JSON,
- inventory summary/JSON,
- per-replica status bundles when reachable,
- Kubernetes pods/events/logs for blockmaster, CSI, and generated
  blockvolumes,
- per-node product image inventory for attach/install failures.

Fail:

- support bundle misses the product timeline,
- support bundle misses Kubernetes runtime evidence for attach/install
  failures.

### HG-10 Watch/Cursor Semantics

Pass:

- `WatchClusterEvents` or equivalent event stream supports reconnect by cursor,
- tests prove reconnect does not miss `authority_published` after the cursor.

Fail:

- event streaming can silently skip retained authority events after reconnect.

### HG-11 User-Facing Non-Claims

Pass:

Docs explicitly say this MVP does not claim:

- hosted dashboard,
- mutating admin repair/promote/rebuild controls,
- Prometheus/alert-manager integration,
- replacement of Kubernetes events,
- application data verification unless a writer/reader check actually ran.

Fail:

- docs imply the observation MVP is a full dashboard or repair system.

### HG-12 Cleanup Hygiene

Pass:

- D5 run cleanup leaves no active iSCSI sessions,
- no `sw-block` processes remain from the run,
- no leaked port-forward remains.

Fail:

- any residue remains unexplained.

## Report Template

```text
QA Close — Control-Plane Observation / AI-Readable Ops MVP

Verdict: PASS|FAIL

HG-0 documentation entry                 PASS|FAIL
HG-1 product-owned cluster evidence      PASS|FAIL
HG-2 required event types                PASS|FAIL
HG-3 master-minted event identity        PASS|FAIL
HG-4 CSI reattach evidence               PASS|FAIL
HG-5 authority and recovery story        PASS|FAIL
HG-6 read-only boundary                  PASS|FAIL
HG-7 stable reason codes/statuses        PASS|FAIL
HG-8 bundle-backed explanation           PASS|FAIL
HG-9 support evidence completeness       PASS|FAIL
HG-10 watch/cursor semantics             PASS|FAIL
HG-11 user-facing non-claims             PASS|FAIL
HG-12 cleanup hygiene                    PASS|FAIL

Run ids:
- D5 product evidence:
- failed image/blocked evidence:
- unit/CLI tests:

Blocking findings:

Non-blocking observations:
```
