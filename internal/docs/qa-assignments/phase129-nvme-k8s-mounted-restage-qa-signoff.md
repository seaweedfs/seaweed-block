# Phase 129 NVMe Kubernetes Mounted Restage QA Sign-Off

Date: 2026-07-03

Verdict: PASS.

Scope: source/component gate for the CSI mounted restage contract. This phase
does not claim an automatic Kubernetes reconnect trigger.

Runner:

```text
testops/scenarios/nvme-k8s-mounted-restage-chain.yaml
```

Run bundle:

```text
results/20260703-173639-8c2b
```

Result:

```text
=== nvme-k8s-mounted-restage-chain === PASS (2.825s)
8 actions: 8 passed, 0 failed
```

## Terminal Evidence

From `phase129-nvme-k8s-mounted-restage-summary.txt`:

```text
phase129_nvme_k8s_mounted_restage_status=ok
scope=mounted_nodestage_restage_contract
automatic_k8s_reconnect_claim=false
mounted_nodestage_reconnects_missing_path=true
mounted_nodestage_rejects_nqn_mismatch=true
mounted_nodestage_does_not_remount=true
restage_owner=node_stage
host_mutation_scope=nvme_connect_missing_paths_only
stale_path_disconnect_claim=false
automatic_trigger_required_next=true
next_phase=phase130_k8s_nvme_reconnect_owner_trigger_gate
cleanup_status=ok
```

## What Was Validated

- If `NodeStageVolume` is invoked for an already-mounted NVMe staging path, the
  CSI node plugin now refreshes publish context and connects missing NVMe paths.
- The path is bounded to `nvme connect` for missing paths; it does not
  unmount, reformat, remount, or disconnect all sessions.
- A mounted target NQN mismatch fails closed before any connect attempt.
- iSCSI mounted idempotency remains a no-op.

## Non-Claims

This phase does not prove:

- an automatic Kubernetes owner/trigger that calls restage when the desired path
  set changes;
- stale path disconnect/removal as a CSI-owned mutation;
- NVMe/RDMA/RoCE;
- performance/SLO.

Phase 130 owns the live owner/trigger gate.
