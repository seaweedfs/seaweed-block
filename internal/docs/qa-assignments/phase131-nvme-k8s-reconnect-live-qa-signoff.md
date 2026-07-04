# Phase 131 NVMe Kubernetes Live Host-Path Reconnect QA Sign-off

Status: PASS.

Phase 131 proves the CSI-node NVMe reconnect owner in a live Kubernetes mounted
PVC path. It intentionally injects host path loss with a scoped
`nvme disconnect -d <controller>` while the desired publish evidence remains
valid. It does not claim control-plane replacement-node/path-set failover; that
remains Phase 132.

## Evidence

Runner:

```text
C:\work\swblock.exe run testops/scenarios/nvme-tcp-k8s-reconnect-live-chain.yaml \
  -output results\phase131-nvme-reconnect-live-run5.json \
  -html results\phase131-nvme-reconnect-live-run5.html

=== nvme-tcp-k8s-reconnect-live-chain === PASS (7m29.375s)
27 actions: 27 passed, 0 failed
run bundle: results\20260704-123729-62e4
```

Summary:

```text
phase131_nvme_k8s_reconnect_live_status=ok
reconnect_owner_enabled=true
reconnect_owner_interval=5s
stage2_multipath_enabled=true
reconnect_owner=csi-node
desired_path_set_changed=false-with-reason=host_path_disconnect_uses_stable_publish_evidence
initial_path_count=2
target_controller=/dev/nvme2
target_addr=192.168.1.184:4420
path_loss_detected=true
after_disconnect_path_count=1
reconnect_invoked=true
replacement_path_connected=true
reconnected_path_count=2
host_mutation_scope=nvme_connect_missing_paths_only
stale_path_disconnect_claim=false-with-reason=gate_disconnects_one_test_path_no_product_stale_disconnect
pod_uid_preserved=true
mounted_io_after_reconnect=ok
crd_status_agrees=true
report_dashboard_agree=true
surface_ready_reason=first_volume_verified
cleanup_status=ok
failure_count=0
```

Owner log:

```text
NodeStageVolume: <volume> reconciled mounted NVMe paths portals=192.168.1.181:4420,192.168.1.184:4420
MountedNVMeReconnectOwner: iteration checked=1 reconnected=1 failed=0
```

Host path evidence:

```text
before: path_count=2
after scoped disconnect: path_count=1
after owner reconnect: path_count=2
```

Local checks:

```text
go test ./core/csi ./cmd/blockcsi
bash -n scripts/run-phase111-nvme-k8s-path-loss-crd-gate.sh scripts/run-phase131-nvme-k8s-reconnect-live-gate.sh
C:\work\swblock.exe validate testops/scenarios/nvme-tcp-k8s-reconnect-live-chain.yaml
```

## Verdict

PASS. The live Kubernetes path now proves:

- mounted RF=2 NVMe/TCP PVC starts with two actual host NVMe paths;
- one path is removed by scoped `nvme disconnect -d`, not `disconnect-all`;
- CSI-node reconnect owner invokes the bounded reconnect path;
- host path count returns to two;
- mounted pod UID is preserved and post-reconnect I/O works;
- CRD/report/dashboard agree on `Ready/first_volume_verified`;
- cleanup returns zero residue.

## Non-blocking Finding

One run observed a transient owner retry log before the successful reconnect:

```text
MountedNVMeReconnectOwner: iteration failed ... missing path <addr>
...
MountedNVMeReconnectOwner: iteration checked=1 reconnected=1 failed=0
```

The final state and gate were correct. The product now treats `nvme connect`
`already connected` as idempotent success and waits longer for post-connect path
visibility, but the owner still may log a retry while the kernel NVMe path is
settling. This is a log-severity/pending-state polish, not a Phase 131 blocker.

## Remaining Work

Phase 132 should prove control-plane desired path-set replacement: after
frontend/failover changes produce a new desired path, the CSI-node owner
connects that path and mounted I/O remains correct.
