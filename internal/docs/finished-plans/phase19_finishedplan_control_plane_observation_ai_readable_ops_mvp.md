# Current Plan: Control-Plane Observation / AI-Readable Ops MVP

Status: active, opened after closing
`finished-plans/phase18_finishedplan_node_loss_survival_mvp.md`, 95% complete.

Reference spec:

- `ref/control-plane-observation-api-mvp.md`

Close evidence that motivates this plan:

- Node-Loss Survival close report:
  `qa-assignments/node-loss-survival-mvp-close-report.md`
- D4 close run: `20260516-160306-1e54`
- Prior QA blockers that should have been self-explaining:
  - inventory replica-ID attribution bug,
  - missing `sw-block-csi:local` image on m02,
  - ImagePullBackOff on CSI node pod,
  - reader/reattach waiting on a Kubernetes runtime precondition.

## Product Question

Can a Kubernetes user or support engineer understand Seaweed Block cluster
state from one CLI/API surface, without SSHing into every node, reading raw
Kubernetes events manually, or knowing internal authority implementation
details?

For block storage, a stuck PVC or failover is a serious product moment. The
operator needs a direct answer:

```text
what failed
what the product did
whether data is safe
which target is active
whether recovery is still progressing or blocked
what to collect or do next
```

## Product Position

The last three availability plans moved the data path forward:

```text
Stage 1: RF3 sync-quorum recovery through CSI/pod recreate
Stage 2: RF3 sync-quorum same-node transparent iSCSI ALUA/dm-multipath failover
Stage 3: RF3 sync-quorum Kubernetes-node-loss recovery through CSI/pod recreate
```

The next product gap is operations trust. Mature block products do not require
users to correlate `kubectl describe`, CSI logs, blockmaster logs, support
bundles, and handwritten TestOps timelines. Seaweed Block should expose a
stable observation model that a human, dashboard, CI job, or AI assistant can
read directly.

The product direction should follow the SeaweedFS operations pattern:

```text
shared observation core
-> CLI text for humans, support, QA, and AI
-> JSON/JSONL for automation and bundles
-> master read-only API for dashboard/UI
```

Do not create separate truth sources for CLI, dashboard, TestOps, and AI.

## Target User Experience

Initial CLI:

```bash
sw-block ops cluster
sw-block ops volumes
sw-block ops describe volume <volume-id>
sw-block ops timeline volume <volume-id>
sw-block ops timeline volume <volume-id> -o jsonl
sw-block ops explain volume <volume-id>
sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out /tmp/sw-block-inventory
sw-block ops events --since 1h
```

Default output is human-readable. `-o json` and `-o jsonl` are stable machine
contracts.

Example healthy output:

```text
volume pvc-... status=ok rf=3 ack=sync-quorum
pvc default/mysql-data
primary r1 on m01 frontend=192.168.1.181:3260
replicas desired=3 observed=3
r1 m01 primary ready durable_lsn=44
r2 m02 replica_ready candidate_ready=true durable_lsn=44
r3 tp01 replica_ready candidate_ready=true durable_lsn=44
next action: none
```

Example recovering output:

```text
volume pvc-... status=recovering reason=primary_node_lost
old primary r1 on m01 unavailable
promoted primary r2 on m02 epoch=2 endpoint_version=1
CSI target changed 192.168.1.181:3260 -> 192.168.1.184:3260
reattach method: pod_recreate
next action: wait for app pod readiness, then collect support bundle if stuck
```

Example blocked output:

```text
volume pvc-... status=blocked reason=csi_node_image_pull_failed
node m02 missing image sw-block-csi:local
pod kube-system/sw-block-csi-node-... waiting=ImagePullBackOff
impact: PVC attach cannot proceed on workloads scheduled to m02
next action: import the image to m02 or use a registry reachable by all nodes
```

## Explicit Non-Claims

- No mutating admin controls.
- No promote, repair, rebuild, backup, restore, or cleanup command.
- No hosted dashboard in this MVP.
- No metrics/Prometheus integration in this MVP.
- No alert-manager integration.
- No claim that application-level checksum ran unless TestOps/user actually ran
  it.
- No replacement for Kubernetes events; this plan explains Seaweed Block state
  and includes relevant Kubernetes runtime evidence.

## D1: Observation Evidence Model

Build the shared data model before adding live APIs.

Status: dev pass, QA not requested yet. Implemented the first stable evidence
model and renderers in `core/ops/observation.go`, with golden tests in
`core/ops/observation_test.go`.

Required structs / schema concepts:

```text
ClusterEvidence
NodeEvidence
VolumeEvidence
ReplicaEvidence
ClusterEvent
VolumeTimeline
VolumeExplanation
SupportBundleManifest
```

Required stable statuses:

```text
ok
degraded
recovering
blocked
invalid
unavailable
```

Required reason codes for first slice:

```text
primary_node_lost
candidate_covers_required_frontier
no_promotion_ready_candidate
durable_frontier_missing
candidate_frontier_behind
status_endpoint_unreachable
csi_node_image_pull_failed
image_missing_on_node
generated_deployment_missing
observed_replicas_below_desired
loopback_frontend_rejected
stale_primary_fenced
```

Pass criteria:

- golden examples for healthy, recovering, and blocked evidence,
- text renderer output is readable without internal context,
- JSON output includes schema version and stable reason codes,
- JSONL timeline emits one event per line with `event_type`, `severity`,
  `reason_code`, `volume_id`, `evidence_ref`, and summary text.

Dev evidence:

- `go test ./core/ops -run TestObservation -count=1` PASS.
- `go test ./core/ops -count=1` PASS.
- Healthy text pins `status=ok`, primary frontend, replica durable LSN, and
  `next action: none`.
- Recovering text pins `status=recovering reason=primary_node_lost`, stale
  primary fencing, and CSI target movement.
- Blocked text pins `status=blocked reason=csi_node_image_pull_failed`, node,
  image, and `ImagePullBackOff` pod evidence.
- JSON/JSONL output carries `schema_version=1.0` and stable reason codes.

## D2: CLI From Existing Bundles

Start with existing inventory/support bundles. This avoids waiting for a new
master API and immediately improves support/QA usability.

Status: dev pass, QA not requested yet. Implemented bundle-backed observation
loading in `core/ops/observation_bundle.go` and wired read-only CLI entry points
in `cmd/sw-block/main.go`.

Commands:

```bash
sw-block ops describe volume --from-bundle <dir> <volume-id>
sw-block ops timeline volume --from-bundle <dir> <volume-id>
sw-block ops timeline volume --from-bundle <dir> <volume-id> -o jsonl
sw-block ops explain volume --from-bundle <dir> <volume-id>
```

Inputs:

- `volume-inventory.json`,
- `volume-inventory-summary.txt`,
- `ops-inventory-bundle.json`,
- nested per-replica `ops-status-bundle.json`,
- `primary-failure-recovery.txt`,
- `node-loss-recovery-summary.txt`,
- `control-plane-timeline.txt`,
- Kubernetes runtime evidence when present.

Pass criteria:

- D4 bundle `20260516-160306-1e54` renders the same story as the QA close
  report:
  `r1@m01 -> r2@m02`, CSI target moved, reader checksum passed, stale primary
  fenced, transparent failover not claimed.
- The failed D4 bundle `20260516-154813-109a` renders
  `status=blocked reason=csi_node_image_pull_failed` and names m02 plus
  `sw-block-csi:local`.

Dev evidence:

- Added:
  - `sw-block ops describe volume --from-bundle <dir> <volume-id>`
  - `sw-block ops timeline volume --from-bundle <dir> <volume-id> -o jsonl`
  - `sw-block ops explain volume --from-bundle <dir> <volume-id>`
- `go test ./core/ops -count=1` PASS.
- `go test ./cmd/sw-block -count=1` PASS.
- Real close bundle smoke:
  `go run ./cmd/sw-block ops explain volume --from-bundle V:\share\g15d-k8s\20260516-160306-1e54-node-loss-survival pvc-c606d03a-4136-464d-8716-ef01f92c7b12`
  renders `r2@m02`, target movement `192.168.1.181:3260 ->
  192.168.1.184:3260`, reader verification, stale-primary fence evidence, and
  timeline events.
- Real failed bundle smoke:
  `go run ./cmd/sw-block ops explain volume --from-bundle V:\share\g15d-k8s\20260516-154813-109a-node-loss-survival pvc-c606d03a`
  renders `status=blocked reason=csi_node_image_pull_failed`, node `m02`, and
  image `sw-block-csi:local`.

## D3: Live CLI Over Current Inventory Sources

Use the current live inventory collector and Kubernetes runtime collectors.

Status: dev pass for the first read-only live CLI slice. Implemented live
cluster, volume-list, and per-volume explain/describe/timeline entry points
using the same observation model as D1/D2. This slice reads Kubernetes
inventory and optional per-replica status through the existing collectors; it
does not introduce master write APIs or mutating admin actions.

Commands:

```bash
sw-block ops cluster --namespace default --master 127.0.0.1:9333
sw-block ops volumes --namespace default --master 127.0.0.1:9333
sw-block ops describe volume <volume-id> --namespace default --master 127.0.0.1:9333
sw-block ops explain volume <volume-id> --namespace default --master 127.0.0.1:9333
sw-block ops inventory --namespace default --master 127.0.0.1:9333 --out <dir>
```

Runtime evidence to collect:

- `kubectl get pods -A -o wide`,
- relevant `kubectl describe pod`,
- namespace events,
- blockmaster logs,
- CSI controller logs,
- CSI node logs,
- generated blockvolume pod logs,
- per-node k3s image inventory for selected product images,
- iSCSI sessions and node DB entries when available.

Pass criteria:

- live healthy RF3 node-loss-ready volume explains as `ok`,
- simulated missing image explains as `blocked` with node/image/pod/reason,
- no command mutates Kubernetes, master, iSCSI, or blockvolume state.

Dev evidence:

- Added:
  - `sw-block ops cluster --namespace <ns> [--master <addr>] [-o json]`
  - `sw-block ops volumes --namespace <ns> [--master <addr>] [-o json]`
  - live mode for `sw-block ops describe volume <volume-id> --namespace <ns>
    [--master <addr>] [--out <dir>]`
  - live mode for `sw-block ops explain volume <volume-id> --namespace <ns>
    [--master <addr>] [--out <dir>]`
- `go test ./core/ops -count=1` PASS.
- `go test ./cmd/sw-block -count=1` PASS.
- CLI tests pin live inventory output for:
  - cluster summary,
  - volume list,
  - per-volume describe,
  - bundle timeline JSONL,
  - ImagePullBackOff explanation from preserved failed bundle.

Remaining D3 gap before QA: run the live commands against the actual 3-node lab
while a healthy RF3 node-loss-ready volume is active, then add the simulated
missing-image live fixture if needed.

## D4: Master Read-Only Snapshot API

Add a master read-only API only after the CLI evidence model is stable.

Status: dev pass. Implemented `Host.ObservationSnapshot()` in
`core/host/master/observation_snapshot.go`, added the read-only
`ClusterEvidenceService` to `core/rpc/proto/control.proto`, regenerated
protobuf bindings, registered the service in blockmaster, and wired server
methods in `core/host/master/observation_service.go`.

Provisional API:

```proto
service ClusterEvidenceService {
  rpc GetClusterStatus(GetClusterStatusRequest)
      returns (ClusterStatusResponse);
  rpc ListVolumes(ListVolumesRequest)
      returns (ListVolumesResponse);
  rpc GetVolumeStatus(GetVolumeStatusRequest)
      returns (VolumeEvidence);
  rpc GetVolumeTimeline(GetVolumeTimelineRequest)
      returns (VolumeTimelineResponse);
  rpc WatchClusterEvents(WatchClusterEventsRequest)
      returns (stream ClusterEvent);
}
```

Pass criteria:

- API and CLI share the same evidence model,
- no read API can mutate authority, placement, lifecycle, or replica state,
- one live RF3 volume appears with primary, replicas, frontend, epoch,
  endpoint_version, and reason code.

Dev evidence:

- `Host.ObservationSnapshot()` returns `ops.ClusterEvidence` directly, so the
  master read surface and CLI/bundle surfaces share one evidence schema.
- Snapshot reads only lifecycle volume/node/placement stores, publisher
  authority line, controller evidence, and observation-store heartbeat facts.
- `TestMasterObservationSnapshot_RF3HealthyReadOnly` pins an RF3 volume with
  primary, replicas, frontend, epoch, endpoint_version, PVC identity, and node
  evidence, then asserts authority state is unchanged after the read.
- `TestMasterObservationSnapshot_MissingReplicaIsDegraded` pins
  `observed_replicas_below_desired` and per-replica
  `status_endpoint_unreachable` when one placement slot has no fresh
  observation.
- `TestMasterObservationSnapshot_NoLifecycleStoreReturnsEmptyOK` pins safe
  empty output when lifecycle stores are not configured.
- `go test ./core/host/master -run TestMasterObservationSnapshot -count=1`
  PASS.
- `go test ./core/host/master ./core/ops ./cmd/sw-block -count=1` PASS.

- `TestClusterEvidenceService_GRPCRegistered` calls
  `ClusterEvidenceService.GetVolumeStatus` through a real gRPC client and pins
  that the service is registered on blockmaster.
- `TestClusterEvidenceService_GetClusterStatusSharesObservationSnapshot` pins
  schema/status/volume/node evidence and verifies the read does not mutate
  publisher authority.
- `TestClusterEvidenceService_ListVolumesAndGetVolumeStatus` pins the degraded
  missing-replica wire shape.
- `TestClusterEvidenceService_GetVolumeStatusNotFound` pins `NotFound` for
  unknown volumes.
- `TestClusterEvidenceService_GetVolumeTimelineEmptyButVersioned` keeps the D5
  timeline gap explicit: the API is versioned, but product-owned event content
  lands in the next slice.
- `go test ./core/host/master -run "TestClusterEvidenceService|TestMasterObservationSnapshot" -count=1`
  PASS.
- `go test ./core/host/master ./core/ops ./cmd/sw-block -count=1` PASS.

Remaining D4 gap before QA: validate against the live 3-node lab while an RF3
node-loss-ready volume is active, so the API output is checked against real
Kubernetes node/replica placement and published frontend evidence.

## D5: Product Timeline

Move from TestOps-only timelines to product-owned timeline events.

Status: dev pass for the first product-owned timeline slice. Added a bounded
master event ring in `core/host/master/event_ring.go`, included master-owned
events in `Host.ObservationSnapshot()`, and exposed them through
`ClusterEvidenceService.GetVolumeTimeline`.

Required event types:

```text
volume_created
placement_verified
replica_heartbeat_seen
replica_stale
promotion_candidate_evaluated
authority_published
publish_target_changed
stale_primary_fenced
csi_reattach_observed
pod_image_pull_failed
csi_node_unavailable
volume_recovered
volume_blocked
support_bundle_collected
```

Pass criteria:

- node-loss D4 produces the same six-event story from product evidence, not
  only from TestOps files,
- `WatchClusterEvents` reconnect with cursor does not miss
  `authority_published` or `volume_blocked`.

Dev evidence:

- `RunLifecycleProductTick()` now emits `placement_verified` events when
  placement verification succeeds.
- The promotion gate now emits `promotion_candidate_evaluated` events with
  stable reason codes:
  `candidate_covers_required_frontier`, `candidate_frontier_behind`, or
  `no_promotion_ready_candidate`.
- The promotion gate emits `volume_blocked` when no candidate passes the
  readiness gate.
- Added a publisher post-mint observer hook:
  `authority.WithPublishObserver`.
- `authority_published` is now emitted only after `Publisher.apply()` has
  successfully minted, durably committed when a store exists, updated in-memory
  state, and delivered the fact. It is not emitted from controller intent
  submission.
- CSI/node staging now emits `csi_reattach_observed` through
  `ObservationService.ReportClusterEvent` after a successful `NodeStageVolume`.
  This is an observation-only append to the master event ring; it does not
  mutate lifecycle, authority, placement, Kubernetes, or replica state.
- `cmd/blockcsi` wires a `ControlEventReporter` when `--master` is configured,
  so CSI reattach/stage evidence can show up in the master timeline alongside
  `placement_verified`, `promotion_candidate_evaluated`, and
  `authority_published`.
- `sw-block ops cluster --master-api <addr> -o json` now reads
  `ClusterEvidenceService.GetClusterStatus` directly, so TestOps and support
  bundles can capture the product-owned event ring without scraping
  blockmaster logs or reconstructing the timeline from runner files.
- `node-loss-survival-rf3-reattach-chain.yaml` now captures
  `demo/product-observation/cluster-evidence.json` before cleanup and asserts
  the product event stream contains `placement_verified`,
  `promotion_candidate_evaluated`, `authority_published`, and
  `csi_reattach_observed`.
- `TestPublisher_PublishObserverFiresAfterSuccessfulMintOnly` PASS.
- `TestMasterTimeline_RecordsAuthorityPublishedAfterMint` PASS.
- `TestMasterTimeline_RecordsPlacementVerified` PASS.
- `TestMasterTimeline_RecordsPromotionCandidateEvaluation` PASS.
- `WatchClusterEvents` now streams retained product events after
  `since_event_id`; unknown cursors conservatively replay retained events so
  clients can de-duplicate by event ID instead of silently missing evidence.
- `TestClusterEvidenceService_WatchClusterEventsCursor` PASS and pins that
  reconnect after a cursor does not replay the cursor event and still includes
  `authority_published`.
- `TestObservationService_ReportClusterEventAppearsInVolumeTimeline` PASS and
  pins that a CSI-reported `csi_reattach_observed` event is accepted, assigned
  a master event ID, and appears in `GetVolumeTimeline`.
- `TestNodeStage_ReportsCSIReattachObservedAfterSuccessfulStage` PASS and pins
  CSI staging emits volume/replica/node/epoch/endpoint_version evidence after
  a successful iSCSI stage.
- `TestControlEventReporter_ReportsClusterEvent` PASS and pins the CSI client
  conversion into the observation RPC.
- `TestOpsClusterReadsMasterAPIProductEvents` PASS and pins the CLI can export
  master-owned product events through `--master-api`.
- `go test ./core/host/master -run "TestMasterTimeline|TestClusterEvidenceService|TestMasterObservationSnapshot" -count=1`
  PASS.
- `go test ./core/authority ./core/host/bootstrap ./core/host/master ./cmd/blockmaster ./cmd/blockcsi ./core/csi ./cmd/sw-block ./core/ops -count=1`
  PASS.

QA evidence:

- D5 live rerun `20260517-011004-4b79`: PASS, 8/8 phases, 83/83
  actions.
- `demo/product-observation/cluster-evidence.json` is present in the bundle
  and contains product-owned events exported through
  `sw-block ops cluster --master-api`.
- Required event types found in the product event stream:
  `placement_verified`, `promotion_candidate_evaluated`,
  `authority_published`, and `csi_reattach_observed`.
- `authority_published` has master-minted `event_id` and `event_time` after
  authority publication.
- `csi_reattach_observed` is CSI-emitted and master-ingested with a
  master-minted `event_id`; it records the promoted r2 frontend
  `192.168.1.184:3260`, not the failed r1 frontend.
- The bundle also includes populated node and volume sections: three
  schedulable/ready nodes, one RF=3 volume, primary `r2`, published target
  `192.168.1.184:3260`, epoch `2`, and all three replica rows.

Remaining D5 gap:

- Kubernetes runtime blocker events such as `ImagePullBackOff` are still
  explained by the bundle/inventory path. Promoting them into the master event
  stream is useful, but no longer blocks this MVP because D5's product-owned
  recovery event ingestion is now proven.

## D6: User Tutorial And Hard Gate

Status: dev pass, QA close pending. User-facing docs now describe the
product-owned control-plane export path, and the formal close hard gate exists
under `qa-assignments/control-plane-observation-ai-readable-ops-mvp-close-hard-gate.md`.

Docs must teach:

- how to inspect cluster health,
- how to inspect one volume,
- how to read a timeline,
- how to collect a support bundle,
- how to interpret healthy/recovering/blocked examples,
- what is not claimed.

QA hard gate should fail if:

- any command mutates state,
- a blocked/recovering state lacks a stable reason code,
- missing image / ImagePullBackOff is not explained,
- node-loss recovery evidence is available only by reading raw TestOps logs,
- stale primary and promoted primary cannot be distinguished,
- JSON/JSONL output is unstable or missing schema version,
- support bundle misses Kubernetes pod events/logs for attach/install failures.

Required close artifacts:

- `docs/operations-v1.md` names the new AI-readable observation path and shows
  `sw-block ops cluster --master-api <addr> -o json`.
- `docs/quickstart-kubernetes.md` gives a short "export product evidence"
  path using `kubectl port-forward deploy/sw-blockmaster`.
- `qa-assignments/control-plane-observation-ai-readable-ops-mvp-close-hard-gate.md`
  exists and makes D5 product event evidence a hard close requirement.

Dev evidence:

- `docs/operations-v1.md` now documents the split between product-owned
  `cluster-evidence.json` and inventory/per-replica support bundles.
- `docs/quickstart-kubernetes.md` now shows how to capture
  `/tmp/sw-block-cluster-evidence.json` using
  `sw-block ops cluster --master-api 127.0.0.1:9333 -o json`.
- The close hard gate has HG-0..HG-12 covering docs, product-owned event
  export, required event types, master-minted event identity, CSI reattach
  evidence, read-only boundary, stable reason codes, bundle-backed
  explanation, support evidence completeness, watch/cursor behavior, explicit
  non-claims, and cleanup hygiene.

## Next Step

Ask QA for a formal close review against the observation hard gate. Do not jump
directly to a dashboard. The dashboard should be a consumer of the observation
core, not a new source of truth.
