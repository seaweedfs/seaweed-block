# Current Plan: Phase 64 Blockvolume Runtime Rebuild Endpoint

Status: complete.

## Goal

Phase 60 proved the existing engine/adapter/transport rebuild and catch-up data
path can move bytes. Phase 61 added the authority-executor runtime call-site.
Phase 62 added the HTTP runtime transport. Phase 63 schema-locked the runtime
target facts needed to address a returned replica safely.

Phase 64 wires the first blockvolume-side runtime endpoint against that
contract:

```text
SwBlockReplicaRebuild.spec runtime facts
  -> authority-executor HTTP runtime POST
  -> blockvolume /runtime/rebuild
  -> local assignment/session validation
  -> ReplicationVolume.StartRuntimeRecovery
  -> peer executor StartRebuild/StartCatchUp
```

The key rule remains fail-closed. The endpoint must not infer session, epoch,
endpoint-version, replica identity, or frontend publication from partial
evidence.

## Delivered

### D1: Opt-in Blockvolume Runtime Endpoint

`cmd/blockvolume` now has an explicit flag:

```text
--runtime-rebuild-endpoint
```

When enabled, the blockvolume status server exposes:

```text
POST /runtime/rebuild
```

The endpoint is disabled by default.

### D2: Local Primary And Lineage Validation

The endpoint starts runtime recovery only when all of these hold:

```text
request.volumeID == served volume
status projection says FrontendPrimaryReady
replicaID is present
sessionID > 0
epoch > 0
endpointVersion > 0
frontierHintLsn > 0
runtime recovery source is wired
```

`ReplicationVolume.StartRuntimeRecovery` and `Peer.StartRuntimeRecovery` then
validate the local peer before calling the executor:

```text
replica identity matches
targetDataAddr does not drift
epoch matches
endpointVersion matches
peer is not closed
```

### D3: Runtime Start Without Fake Terminal Evidence

The endpoint returns:

```text
runtimeState=started
rebuildTrafficStarted=true
durableFrontierKnown=false
```

The authority executor preserves `SwBlockReplicaRebuild.status.state=running`
when the runtime reports `started`. It does not mark `caught_up` until a future
terminal-evidence path exists.

### D4: Gate

Gate files:

```text
scripts/run-phase64-blockvolume-runtime-endpoint-gate.sh
testops/scenarios/blockvolume-runtime-endpoint-chain.yaml
```

The gate proves endpoint opt-in, exact-lineage POST behavior, non-primary
rejection, replication lineage rejection, and authority-executor handling of
`runtimeState=started`.

## Non-Claims

Phase 64 does not claim:

```text
terminal durable frontier known
caught_up from runtime start alone
frontend publication
failback
automatic session minting
NVMe/ANA behavior
```

Those belong to later gated phases.

## Verification

Local:

```text
go test ./core/ops ./core/host/volume ./core/replication ./cmd/blockvolume
C:\work\swblock.exe validate testops\scenarios\blockvolume-runtime-endpoint-chain.yaml
```

Live:

```text
20260625-012440-775a blockvolume-runtime-endpoint-chain PASS 18/18
```

Terminal evidence:

```text
phase64_blockvolume_runtime_endpoint_status=ok
runtime_state_started_supported=true
authority_executor_started_result_not_blocked=true
blockvolume_runtime_endpoint_opt_in=true
blockvolume_runtime_endpoint_posts_started=true
blockvolume_runtime_endpoint_requires_primary=true
blockvolume_runtime_endpoint_requires_lineage=true
replication_runtime_rejects_lineage_drift=true
runtime_endpoint_terminal_frontier_claimed=false
frontend_publication_allowed=false
failback_allowed=false
```

## Next

Phase 65 should add terminal runtime evidence before any publication/failback
claim. The next contract should answer:

```text
started -> running -> terminal frontier observed -> caught_up
```

Until that exists, the executor must keep rebuild targets in `running` after a
successful start and must not publish the returned replica to ACK eligibility or
frontend service.
