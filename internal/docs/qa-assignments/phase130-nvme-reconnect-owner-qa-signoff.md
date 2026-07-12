# Phase 130 NVMe Reconnect Owner / Trigger Gate QA Sign-off

Status: PASS, component/source gate.

Phase 130 closes the ownership gap left by Phase 129: the product now has an
explicit CSI-node owner loop that can trigger mounted NVMe path reconciliation
from refreshed publish evidence. This gate deliberately does not claim a full
live Kubernetes failover close path; Phase 131 owns pod UID, mounted I/O, and
CRD/report/dashboard agreement under a real path-set change.

## Evidence

Local:

```text
bash scripts/run-phase130-nvme-reconnect-owner-gate.sh .
C:\work\swblock.exe validate testops/scenarios/nvme-k8s-reconnect-owner-chain.yaml
go test ./core/csi ./cmd/blockcsi
helm lint charts/seaweed-block
```

Runner:

```text
C:\work\swblock.exe run testops/scenarios/nvme-k8s-reconnect-owner-chain.yaml \
  -output results\phase130-nvme-reconnect-owner\swblock-run.json \
  -html results\phase130-nvme-reconnect-owner\swblock-run.html

=== nvme-k8s-reconnect-owner-chain === PASS (2.255s)
10 actions: 10 passed, 0 failed
run bundle: results\20260704-120159-2e1f
```

Summary:

```text
phase130_nvme_k8s_reconnect_owner_status=ok
scope=csi_node_owner_trigger_contract
live_k8s_failover_claim=false
path_loss_detected=component_missing_path
desired_path_set_changed=true
reconnect_owner=csi-node
reconnect_invoked=true
replacement_path_connected=true
mounted_nodestage_reconnects_missing_path=true
mounted_nodestage_rejects_nqn_mismatch=true
mounted_nodestage_does_not_remount=true
owner_loop_invokes_reconnect=true
default_enabled=false
host_mutation_scope=nvme_connect_missing_paths_only
stale_path_disconnect_claim=false-with-reason=no_stale_path_disconnect_primitive
pod_uid_preserved=not_claimed_component_gate
mounted_io_after_reconnect=not_claimed_component_gate
crd_status_agrees=not_claimed_component_gate
report_dashboard_agree=not_claimed_component_gate
live_k8s_gate_required_next=true
next_phase=phase131_k8s_nvme_reconnect_live_close_gate
cleanup_status=ok
```

## Verdict

PASS for Phase 130. The product owner exists, is opt-in, and invokes the
bounded reconnect primitive without remounting:

- owner: CSI node plugin;
- trigger: opt-in background loop;
- mutation scope: `nvme connect` for missing paths under the same NQN;
- no broad `disconnect-all`;
- no default behavior change.

## Remaining Work

Phase 131 must prove the full live Kubernetes close gate:

```text
mounted NVMe PVC starts with two paths
-> one path is lost
-> desired path evidence changes/restores
-> CSI-node reconnect owner invokes bounded reconnect
-> replacement path is connected
-> pod UID is preserved
-> mounted I/O remains correct
-> CRD/report/dashboard agree
```
