# QA Close — Control-Plane Observation / AI-Readable Ops MVP

Formal close report against
`internal/docs/qa-assignments/control-plane-observation-ai-readable-ops-mvp-close-hard-gate.md`
("Control-Plane Observation / AI-Readable Ops MVP", currently at ~95% per
dev's status). This report covers the substantive D5 deliverable (product-owned
event stream export + bundle-backed reader UX) and the supporting
blocked-evidence bundle re-use.

```text
Verdict:        PASS — all 13 hard-gate clauses (HG-0…HG-12) pass

Product commit: shared working tree at HEAD 0606ab1 + dev's observation stack
                  (ObservationService.ReportClusterEvent / WatchClusterEvents,
                   ClusterEvidenceService.GetClusterStatus,
                   sw-block ops cluster|describe|timeline|explain|volumes,
                   bundle-backed describe/explain code path,
                   master-minted event_id + event_time,
                   k3s_renderer external-iSCSI/status,
                   build-alpha-images multi-node import,
                   keep_on_stop honored at reader-verified stop,
                   port-forward via deploy/sw-blockmaster)
Runner commit:  sw-test-runner-standalone @ d45c60c (swblock Windows binary)
Host/lab:       m02 control + m01 + tp01 workers (3-node k3s, LAN TCP/iSCSI)

Run ids:
- D5 product evidence:       20260517-011004-4b79-node-loss-survival
- failed image/blocked:      20260516-154813-109a-node-loss-survival
- unit/CLI tests (dev):      go test ./cmd/sw-block ./core/ops ./core/host/master
                              ./core/csi ./cmd/blockcsi ./core/host/bootstrap
                              ./core/authority ./cmd/blockmaster -count=1 PASS
```

## Hard-gate clause table

| # | Clause | Result |
|---|---|---|
| HG-0 | Documentation entry | **PASS** |
| HG-1 | Product-owned cluster evidence artifact | **PASS** |
| HG-2 | Required event types | **PASS** |
| HG-3 | Master-minted event identity | **PASS** |
| HG-4 | CSI reattach evidence | **PASS** |
| HG-5 | Authority and recovery story | **PASS** |
| HG-6 | Read-only boundary | **PASS** |
| HG-7 | Stable reason codes / statuses | **PASS** |
| HG-8 | Bundle-backed explanation | **PASS** |
| HG-9 | Support evidence completeness | **PASS** |
| HG-10 | Watch / cursor semantics | **PASS** |
| HG-11 | User-facing non-claims | **PASS** |
| HG-12 | Cleanup hygiene | **PASS** |

## Per-clause evidence

### HG-0 — Documentation entry — PASS

`docs/operations-v1.md` §"AI-Readable Control-Plane Status" documents the path:
- exports `sw-block ops cluster --master-api 127.0.0.1:9333 -o json` (line 341),
- enumerates the JSON shape (nodes/volumes/events),
- enumerates the bundle-backed text/JSON CLI surface,
- states explicitly:
  > "The first dashboard and AI assistant path must stay read-only. It should
  > not expose promote, repair, rebuild, backup, restore, or cleanup buttons
  > until those actions have separate strict gates."

`docs/quickstart-kubernetes.md` (line 146) shows the user-facing recipe to
port-forward the Deployment and capture the cluster evidence.

### HG-1 — Product-owned cluster evidence artifact — PASS

D5 run `20260517-011004-4b79` bundle contains:
```text
demo/product-observation/cluster-evidence.json    379498 bytes, non-empty JSON
demo/product-observation/blockmaster-deploy-wait.log
demo/product-observation/blockmaster-port-forward.log
demo/product-observation/blockmaster-port-forward.pid
demo/product-observation/cluster-evidence.stderr.txt
demo/product-observation/kube-system-before-port-forward.txt
```

The JSON was produced by the scenario step running `go run ./cmd/sw-block ops
cluster --master-api 127.0.0.1:${pf_port} --timeout 30s -o json` against the
live master via a Deployment port-forward, not by parsing TestOps timeline
files.

### HG-2 — Required event types — PASS

Catalog of unique `event_type` values present in `cluster-evidence.json`:
```text
placement_verified                ✓ required
promotion_candidate_evaluated     ✓ required
authority_published               ✓ required
csi_reattach_observed             ✓ required
volume_blocked                    (additional, not required, useful for HG-7)
```

All four required event types are present. Total events: 1024 entries across
the run (mostly placement-loop ticks, which is the correct shape for a
sync-quorum control plane that re-verifies placement each tick).

### HG-3 — Master-minted event identity — PASS

Sample `event_id` values in the bundle are sequential `master-NN`:
```text
"event_id": "master-87"   "event_time": "2026-05-17T08:11:23.075213435Z"
"event_id": "master-88"   "event_time": "2026-05-17T08:11:23.080632240Z"
"event_id": "master-89"   "event_time": "2026-05-17T08:11:23.080635191Z"
...
"event_id": "master-274"  "event_time": "2026-05-17T08:11:27.702755866Z"   csi_reattach_observed (r1@m01)
"event_id": "master-276"  "event_time": "2026-05-17T08:11:27.779983313Z"   csi_reattach_observed (r2@m02)
"event_id": "master-516"  "event_time": "2026-05-17T08:11:35.775238755Z"   authority_published r2 epoch=2
"event_id": "master-796"  "event_time": "2026-05-17T08:11:45.075456018Z"
"event_id": "master-797"  "event_time": "2026-05-17T08:11:45.077126184Z"
```

Both `authority_published` AND the CSI-emitted `csi_reattach_observed` carry
the `master-NN` prefix — confirming that externally reported CSI events are
re-minted by master (per `core/host/master/observation_service.go:72`
`ReportClusterEvent` → `s.host.events.append(event)`, which assigns
`event.EventID`). The external CSI client cannot set the master event id; the
validator in `observation_service.go:84` rejects everything except the
sanitized `csi_reattach_observed` shape, blocking authority/lifecycle spoof.

Event order is auditable because the cursor is monotonic.

### HG-4 — CSI reattach evidence — PASS

Two `csi_reattach_observed` events in the bundle, in promotion order:
```json
{
  "event_id": "master-274",
  "event_time": "2026-05-17T08:11:27.702755866Z",
  "volume_id": "pvc-1e435a12-...",
  "replica_id": "r1",
  "node_name": "m01",
  "event_type": "csi_reattach_observed",
  "reason_code": "csi_reattach_observed",
  "new_value": "192.168.1.181:3260",       ← initial stage on r1 frontend (m01)
  "epoch": 1,
  "endpoint_version": 1,
  "evidence_ref": "csi-node"
}
{
  "event_id": "master-276",
  "event_time": "2026-05-17T08:11:27.779983313Z",
  "volume_id": "pvc-1e435a12-...",
  "replica_id": "r2",
  "node_name": "m02",                       ← reattach happened on the survivor node
  "event_type": "csi_reattach_observed",
  "new_value": "192.168.1.184:3260",        ← promoted frontend, NOT the failed primary
  "epoch": 2,                                ← epoch advanced
  "endpoint_version": 1,
  "evidence_ref": "csi-node"
}
```

Both events name the node that staged the mount, the publish target equals
the actually-used frontend, the reattach target differs from the failed
primary, and `epoch`/`endpoint_version` are populated.

### HG-5 — Authority and recovery story — PASS

`sw-block ops explain volume --from-bundle 20260517-011004-4b79 pvc-1e435a12-...`
output (run live on the bundle) answers all five required questions in five
lines without a single raw log fetch:

```text
condition NodeLossRecovery severity=info reason=primary_node_lost
  CSI target changed 192.168.1.181:3260 -> 192.168.1.184:3260;
  reader_verified=true; pod_recreate_used=true
condition StalePrimary severity=info reason=stale_primary_fenced
  old primary stale I/O success count is 0
primary r2 on m02 frontend=192.168.1.184:3260
r1 m01 unavailable        ← failed primary, fenced
r2 m02 primary             ← promoted primary, exactly one
r3 tp01 replica_ready
```

The compact summary `node-loss-recovery-summary.txt` plus `primary-failure-recovery.txt`
plus `reader.log` (already shipped by the upstream Node-Loss Survival plan) all
agree with the event stream. The product event stream does not contradict the
recovery summary; the recovery summary's `data_check_after_node_loss=reader_checksum_passed`
is itself backed by the writer's `OK` line and reader's `/data/demo.bin: OK`.

### HG-6 — Read-only boundary — PASS

`core/rpc/proto/control.proto` RPC catalog:
- `ClusterEvidenceService`: `GetClusterStatus`, `ListVolumes`,
  `GetVolumeStatus`, `GetVolumeTimeline`, `WatchClusterEvents` — **all read**
- `ObservationService`: `ReportHeartbeat`, `ReportClusterEvent` — observation
  ingestion, no authority/lifecycle write surface; `ReportClusterEvent`'s
  validator (`observation_service.go:84`) only accepts `csi_reattach_observed`
  from external clients
- Mutating surface (`CreateVolume`, `DeleteVolume`, etc.) lives on a separate
  service — not invoked by any `sw-block ops` subcommand

Per code review: no `sw-block ops cluster|describe|timeline|explain|volumes`
or bundle-backed command can promote, repair, rebuild, delete, or cleanup.

### HG-7 — Stable reason codes / statuses — PASS

Stable reason codes observed in the bundle:
```text
candidate_covers_required_frontier
candidate_frontier_behind
csi_reattach_observed
no_promotion_ready_candidate
placement_verified
```

Plus the explain-driven condition reason codes:
```text
primary_node_lost           (NodeLossRecovery condition)
stale_primary_fenced        (StalePrimary condition)
csi_node_image_pull_failed  (from the supporting blocked bundle)
```

JSON includes `"schema_version": "1.0"`. The output never requires interpreting
free-form log text; both healthy and blocked states surface a stable
`reason_code`.

### HG-8 — Bundle-backed explanation — PASS

**On the successful node-loss recovery bundle (D5 r3):**
```text
$ go run ./cmd/sw-block ops explain volume \
    --from-bundle /mnt/smb/.../20260517-011004-4b79-node-loss-survival \
    pvc-1e435a12-...

cluster status=degraded volumes=1 nodes=0
volume pvc-1e435a12-... status=ok rf=3 ack=sync-quorum reason=primary_node_lost
pvc default/sw-block-demo-pvc
primary r2 on m02 frontend=192.168.1.184:3260
condition NodeLossRecovery severity=info reason=primary_node_lost
  CSI target changed 192.168.1.181:3260 -> 192.168.1.184:3260;
  reader_verified=true; pod_recreate_used=true
condition StalePrimary severity=info reason=stale_primary_fenced
  old primary stale I/O success count is 0
next action: none
timeline:
- primary_observed              evidence=replica: ... role=primary epoch=1 frontend=192.168.1.181:3260
- candidate_evaluated           reason=promotion_ready ... candidate_frontier_lsn=44 frontier_covered=true
- primary_failure_injected      replica=r1 node=m01 failure_class=primary-kubernetes-node-cordoned-blockvolume-stop
- authority_published           from=r1 to=r2 primary=r2 primary_count=1 epoch=2
- csi_reattach_observed         reader_pod=sw-block-demo-reader method=pod-recreate
- data_check                    reader_verified=true result=reader_checksum_passed
```

**On the preserved failed image-pull bundle (`20260516-154813-109a`):**
```text
$ go run ./cmd/sw-block ops describe volume \
    --from-bundle /mnt/smb/.../20260516-154813-109a-node-loss-survival \
    pvc-edc18a1b-...

cluster status=blocked volumes=1 nodes=0
volume pvc-edc18a1b-... status=blocked rf=3 reason=csi_node_image_pull_failed
condition Attach severity=error reason=csi_node_image_pull_failed
  pod kube-system/sw-block-csi-node waiting=ImagePullBackOff on node m02 image sw-block-csi:local
next action: import sw-block-csi:local to the blocked node or use a registry reachable by all nodes
support bundle: .../demo/kube-system-pods-deploys.txt
```

Both bundles are self-explaining via the product CLI. No SSH + `kubectl describe`
+ grep is required.

### HG-9 — Support evidence completeness — PASS

The D5 bundle includes:
- `demo/product-observation/cluster-evidence.json` — product cluster evidence + events
- `demo/ops-inventory-{before-primary-failure,after-primary-failure,reader-verified}/`
  — inventory summary + JSON + nested per-replica status bundles
- `demo/kube-system-pods-deploys.txt`, `demo/blockvolume-namespace-pods-deploys.txt`
  — kubectl pods/deploys snapshots
- `demo/blockmaster.log`, `demo/blockcsi-controller.log`, `demo/blockcsi-node.log`,
  `demo/blockvolume-generated.log` — kubelet container logs
- `pin_build/alpha-images.env` + multi-node import evidence — per-node product
  image attribution
- The blocked-image bundle additionally surfaces `csi_node_image_pull_failed`
  with the offending node/image/pod (per HG-8)

The Kubernetes runtime evidence for attach/install failures (events, describe,
CSI logs, blockmaster logs, per-node image presence) is present in both the
PASS bundle (preserved as a baseline) and the blocked bundle (the failure
exemplar).

### HG-10 — Watch / cursor semantics — PASS

`core/host/master/observation_service.go:63`:
```go
func (s *services) WatchClusterEvents(req *control.WatchClusterEventsRequest,
    stream control.ClusterEvidenceService_WatchClusterEventsServer) error {
    for _, event := range s.host.events.listAfter("", req.GetSinceEventId()) {
        if err := stream.Send(clusterEventToWire(event)); err != nil { return err }
    }
    return nil
}
```

Reconnect uses `SinceEventId` as a cursor. The dedicated regression test
`TestClusterEvidenceService_WatchClusterEventsCursor` at
`core/host/master/event_ring_test.go:108` exercises:
- initial watch returning the full event ring,
- reconnect with `SinceEventId=<cursor>` returning only newer events,
- specifically validating that `authority_published` after the cursor is not
  skipped on reconnect.

### HG-11 — User-facing non-claims — PASS

`docs/operations-v1.md` AI-Readable section explicitly states:
```text
The first dashboard and AI assistant path must stay read-only. It should not
expose promote, repair, rebuild, backup, restore, or cleanup buttons until
those actions have separate strict gates.
```

By construction:
- The MVP ships read-only CLI commands and a read-only API surface only — no
  hosted dashboard process is shipped or claimed.
- The CLI never invokes mutating RPCs (verified per HG-6).
- The "data check" condition only appears when a writer/reader artifact
  exists in the bundle (the `explain` output's `reader_verified=true` is read
  from `primary-failure-recovery.txt`, which is written only when reader.log
  showed `/data/demo.bin: OK`).
- No Prometheus/alert-manager language anywhere in the observation section.
- No "replaces Kubernetes events" language; both layers are presented as
  complementary support evidence.

### HG-12 — Cleanup hygiene — PASS

D5 run `20260517-011004-4b79`:
```text
83 actions: 83 passed, 0 failed
collect_and_cleanup phase: PASS (3.627s)
  pre_run_cleanup: iSCSI cleanup matched 1 session, logged out, deleted node DB
  assert_no_active_iscsi_sessions: PASS
  assert_no_processes:              PASS
```

Lab state verified now:
```text
iscsiadm:  No active sessions.
blockmaster/blockcsi/blockvolume processes: none
kubectl port-forward ... blockmaster:        none
m01 / m02 / tp01:                            all Ready
```

No leaked port-forward, no leftover `sw-block` processes, no orphan iSCSI
sessions.

## Blocking findings

None.

## Non-blocking observations

1. **The `kube-system-before-port-forward.txt` diagnostic that dev added in
   the port-forward fix is great defensive engineering.** It captures the
   state of the cluster RIGHT before the port-forward attempt, so any future
   failure can be triaged from artifacts alone instead of requiring a live
   re-run. Worth keeping the pattern (capture-then-act) for every subsequent
   product-observation export step.

2. **D5 took three iterations to land** (r1 missing export, r2 svc/blockmaster
   NotFound + keep_on_stop not honored at reader-verified, r3 strict PASS).
   Each iteration surfaced a distinct, real product/scenario gap:
   - r1 — scenario didn't yet export the event stream
   - r2 — `keep_on_stop=1` was honored at blockvolume-ready stop but NOT at
     reader-verified stop (subtle demo-script gap)
   - r2 — `svc/blockmaster` was deleted between snapshot and port-forward
     attempt (Service lifetime didn't track Deployment lifetime under
     keep-on-stop)
   The three iterations each produced a clean dev fix. Worth a brief
   internal note for the team: when extending the demo script's
   stop-point semantics, the `keep_on_stop` honoring needs to cover the new
   stop point explicitly (not assume the existing "blockvolume-ready" path
   covers all stops).

3. **The bundle-backed `explain` text is the most reader-friendly format the
   product has produced so far.** Worth keeping the shape stable so a future
   AI assistant can train against it without prompt re-engineering.

## Close recommendation

```text
PASS — all 13 hard-gate clauses pass on D5 run 20260517-011004-4b79
       plus supporting blocked bundle 20260516-154813-109a.
       Control-Plane Observation / AI-Readable Ops MVP is ready to close.
```

The validated product claim is:

```text
A user, support engineer, automation script, or AI assistant can read the
SeaweedFS Block control plane's authoritative recovery story from one
read-only product surface — either live via the master gRPC API
(sw-block ops cluster --master-api ... -o json) or post-mortem from a
saved support bundle (sw-block ops explain volume --from-bundle <dir>
<volume-id>) — and reconstruct: which PVC, which replica was primary
before failure, which replica was promoted, on which Kubernetes node CSI
re-staged, which frontend was used before and after, whether data was
verified, whether the stale primary was fenced, and what the next operator
action is — without reading raw blockmaster / blockvolume / CSI logs.

The same surface explains attach/install blockers (`csi_node_image_pull_failed`)
with the offending node, image, pod, and next action, using only the
captured support bundle.

Events have stable `event_id`/`event_time` and stable `reason_code` values,
JSON output carries a `schema_version`, the event stream supports cursor-
based reconnect that does not drop authority events, and the entire surface
is read-only — no promote/repair/rebuild path is exposed.
```

Non-claims preserved: no hosted dashboard, no mutating admin controls, no
Prometheus/alert-manager integration, no replacement for Kubernetes events,
no data-verification claim without a corresponding writer/reader artifact.

## QA needed next

Once dev closes this plan, the natural next-gap candidates surfaced by this
work are:

1. **The `volumes` array nesting of replicas is currently verbose**. A
   `--detail=minimal` flag on `sw-block ops cluster -o json` could produce a
   ~10× smaller bundle artifact for the common operator case (PVC + primary +
   recovery condition only). Useful both for support attachments and for AI
   token budgets.

2. **The event ring is currently bounded by master memory** — the bundle's
   1024 events are everything master had at the time of port-forward. For a
   support assistant analyzing a long-running cluster, persisting events to
   a small append-only log on disk (or shipping to the inventory bundle on
   periodic checkpoints) would let support trace failures that started before
   the most recent master restart.

3. **The `next action` line is currently produced by `explain`'s
   condition→advice mapping**. As more conditions are added (rebuild
   failures, NVMe ANA, etc.), keeping this mapping in one place and
   covered by snapshot tests would prevent drift.

None of those are blockers for closing the current observation MVP.
