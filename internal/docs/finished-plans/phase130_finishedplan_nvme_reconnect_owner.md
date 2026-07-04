# Phase 130 Finished Plan: NVMe Reconnect Owner / Trigger Contract

Status: closed 2026-07-04, runner PASS.

## Problem

Phase 129 made mounted `NodeStageVolume` idempotency useful for NVMe: a repeat
stage call can refresh publish context and connect missing NVMe paths without
remounting. It still left a product ownership gap. Kubernetes will not
necessarily call `NodeStageVolume` again when the desired NVMe path set changes,
so a manual test could prove the primitive while the mounted PVC still had no
owner that notices and invokes it.

## Implementation

`core/csi.NodeServer` now owns an opt-in mounted NVMe reconnect loop:

- `ReconcileMountedNVMeVolumes` snapshots mounted staged NVMe volumes;
- validates the mounted staging identity before any host mutation;
- refreshes publish evidence through the existing control-plane lookup;
- invokes the Phase 129 mounted NVMe reconnect primitive;
- records checked/reconnected/failed counts;
- emits the existing `csi_reattach_observed` event only when a new path is
  connected;
- never remounts, reformats, or disconnects all NVMe sessions.

The `blockcsi` binary exposes:

```text
--nvme-reconnect-owner
--nvme-reconnect-interval=<duration>
```

The Helm chart renders those flags only when:

```yaml
csiNode:
  nvmeReconnect:
    enabled: true
```

The default remains disabled.

## Evidence

Runner:

```text
testops/scenarios/nvme-k8s-reconnect-owner-chain.yaml
results/20260704-120159-2e1f
```

Summary:

```text
phase130_nvme_k8s_reconnect_owner_status=ok
scope=csi_node_owner_trigger_contract
live_k8s_failover_claim=false
desired_path_set_changed=true
reconnect_owner=csi-node
reconnect_invoked=true
replacement_path_connected=true
owner_loop_invokes_reconnect=true
default_enabled=false
host_mutation_scope=nvme_connect_missing_paths_only
stale_path_disconnect_claim=false-with-reason=no_stale_path_disconnect_primitive
live_k8s_gate_required_next=true
cleanup_status=ok
```

Local:

```text
go test ./core/csi ./cmd/blockcsi
helm lint charts/seaweed-block
```

## Close Criteria

Phase 130 closes because the product has a real, opt-in owner/trigger that can
call the bounded mounted NVMe reconnect path. It does not claim the full live
Kubernetes mounted failover/reconnect loop.

## Remaining Work

Phase 131 must run the live Kubernetes close gate:

- mounted NVMe PVC starts with two paths;
- one path is removed;
- replacement/desired path evidence is observed;
- CSI-node reconnect owner invokes reconnect;
- replacement path is connected without remount;
- pod UID and mounted I/O remain correct;
- CRD/report/dashboard agree;
- cleanup is clean.
