# Internal Design Notes

This directory holds internal planning, audits, and decision-control notes.

Use this directory for documents that are useful to maintainers and coding
agents but too detailed or process-heavy for public user documentation.

Public-facing docs belong in `docs/`.

## Operating Rule

- `product-roadmap.md` is the short global product plan.
- `current-plan.md` is the active execution plan. Development should track this
  file.
- When a current plan closes, move it under `finished-plans/` with a
  phase/topic filename such as `phase1_finishedplan_frontend_protocol_readiness.md`,
  then create a new `current-plan.md`.
- Long audits and historical plans live under `ref/`. They are references, not
  the day-to-day driver.
- QA assignments live under `qa-assignments/` and should be linked from
  `current-plan.md` with `#QA`.
- Product spec gates live under `ref/` and should be created or referenced
  before non-trivial P0/P1 implementation. Tests validate the spec; tests are
  not the spec.
- When a plan closes, keep the finished markdown plan in `finished-plans/`.
  Link supporting audits, QA requests, and larger references from that plan
  under `ref/` or `qa-assignments/`.

## Current Index

| File | Purpose |
|---|---|
| `product-roadmap.md` | Short global roadmap, product phases, and priority tracks. |
| `current-plan.md` | Active development plan. Follow this first. |
| `product-management-plan.md` | Older product goals, priorities, evidence links, and decision rules. |
| `qa-assignments/` | Concrete QA tasks with commands, pass criteria, and report templates. |
| `finished-plans/` | Closed phase/topic plans retained for PM history. |
| `ref/product-spec-gate-template.md` | Cold spec-gate template for product semantics, non-negotiables, allowed simplifications, and drift checks. |
| `ref/stage2-transparent-multipath-host-failover-spec.md` | Stage 2 product spec for iSCSI ALUA/dm-multipath Kubernetes host-path failover without pod recreate. |
| `ref/node-loss-survival-mvp-spec.md` | Node-loss survival product spec: RF3 sync-quorum recovery across distinct Kubernetes nodes with non-loopback frontends. |
| `ref/node-loss-lab-setup.md` | Lab setup note for the Node-Loss D3/D4 gates, including 3 Kubernetes nodes on 2 physical machines and physical-host disclosure. |
| `ref/control-plane-observation-api-mvp.md` | Dashboard-grade read-only observation API spec: cluster status, volume timeline, event stream, and support-bundle UX. |
| `finished-plans/phase1_finishedplan_frontend_protocol_readiness.md` | Historical iSCSI/NVMe protocol-readiness plan closed by the protocol release gate. |
| `finished-plans/phase2_finishedplan_beta_hardening_seed.md` | Historical beta hardening seed plan. |
| `finished-plans/phase3_finishedplan_beta_seed_stabilization.md` | Historical beta seed stabilization plan. |
| `finished-plans/phase4_finishedplan_fast_gates_operations_contract_prep.md` | Historical fast-gates and operations contract prep plan. |
| `finished-plans/phase5_finishedplan_read_only_operations_status_report.md` | Historical read-only operations status report plan. |
| `finished-plans/phase6_finishedplan_iscsi_os_initiator_compatibility.md` | Historical iSCSI OS initiator compatibility plan closed by Linux and Windows validation. |
| `finished-plans/phase7_finishedplan_iscsi_session_backend_pressure.md` | Historical iSCSI session/backend pressure plan closed by fast protocol, L2 restart, and Linux OS initiator gates. |
| `finished-plans/phase8_finishedplan_main_merge_pr_readiness.md` | Historical main-merge readiness plan closed by PR #46 squash merge. |
| `finished-plans/phase9_finishedplan_light_use_operations_mvp.md` | Historical light-use operations MVP plan closed by local CLI, bundle, and TestOps control-data validation. |
| `finished-plans/phase10_finishedplan_light_use_install_lifecycle_operations_mvp.md` | Historical light-use install/lifecycle operations MVP plan closed by strict first-volume runbook and scenario validation. |
| `finished-plans/phase11_finishedplan_cluster_ops_inventory_lifecycle_visibility_mvp.md` | Historical cluster operations inventory and lifecycle visibility MVP plan closed by strict multi-volume inventory validation. |
| `finished-plans/phase12_finishedplan_product_owned_blockvolume_lifecycle_mvp.md` | Historical product-owned blockvolume lifecycle MVP plan closed by lifecycle ownership and scoped delete validation. |
| `finished-plans/phase13_finishedplan_durable_volume_restart_reattach_mvp.md` | Historical durable volume restart/reattach MVP plan closed by RF=1 durable hostPath restart gates. |
| `finished-plans/phase14_finishedplan_multi_node_attach_and_placement_mvp.md` | Historical same-node multi-node-capable attach and placement MVP plan. |
| `finished-plans/phase15_finishedplan_basic_mounted_failover_and_reattach_mvp.md` | Historical RF=2 mounted app baseline plus controlled primary-failure safe-refusal plan. |
| `finished-plans/phase16_finishedplan_stage1_mounted_recovery_ack_profile_mvp.md` | Historical Stage 1 mounted recovery ACK profile plan: RF=2 best-effort demo recovery and RF=3 sync-quorum beta recovery through CSI/pod recreate. |
| `finished-plans/phase17_finishedplan_stage2_transparent_multipath_host_failover_mvp.md` | Historical Stage 2 transparent mounted failover plan: RF=3 sync-quorum iSCSI ALUA/dm-multipath recovery without pod recreate. |
| `finished-plans/phase18_finishedplan_node_loss_survival_mvp.md` | Historical Node-Loss Survival plan: RF=3 sync-quorum Kubernetes-node-loss recovery through CSI/pod recreate on a surviving node. |
| `finished-plans/phase19_finishedplan_control_plane_observation_ai_readable_ops_mvp.md` | Historical Control-Plane Observation plan: read-only master evidence API, AI-readable CLI export, product-owned event stream, and support-bundle evidence. |
| `ref/production-readiness-plan.md` | Longer readiness reference and iSCSI P1-P6 detail. |
| `ref/light-use-block-storage-ux-research.md` | Comparison of Longhorn, OpenEBS, Rook/Ceph, Piraeus/LINSTOR, and EBS CSI light-user install/operations UX. |
| `ref/blockvolume-lifecycle-ownership-contract.md` | Product-owned generated blockvolume workload lifecycle ownership contract for the current plan. |
| `ref/iscsi-os-initiator-compat-plan.md` | Detailed iSCSI P1 OS-initiator compatibility plan and evidence. |
| `ref/iscsi-v2-coverage-gap-audit.md` | V2-to-V3 iSCSI coverage gap audit and prioritized test backlog. |
| `ref/v2-frontend-protocol-gap-audit.md` | Broader V2 frontend protocol feature gap audit. |

