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
- When a plan closes, move its finished plan and related references into a
  dedicated subdirectory, for example `finished/iscsi-p2/`.

## Current Index

| File | Purpose |
|---|---|
| `product-roadmap.md` | Short global roadmap, product phases, and priority tracks. |
| `current-plan.md` | Active development plan. Follow this first. |
| `product-management-plan.md` | Older product goals, priorities, evidence links, and decision rules. |
| `qa-assignments/` | Concrete QA tasks with commands, pass criteria, and report templates. |
| `finished-plans/` | Closed phase/topic plans retained for PM history. |
| `finished-plans/phase1_finishedplan_frontend_protocol_readiness.md` | Historical iSCSI/NVMe protocol-readiness plan closed by the protocol release gate. |
| `finished-plans/phase2_finishedplan_beta_hardening_seed.md` | Historical beta hardening seed plan. |
| `finished-plans/phase3_finishedplan_beta_seed_stabilization.md` | Historical beta seed stabilization plan. |
| `finished-plans/phase4_finishedplan_fast_gates_operations_contract_prep.md` | Historical fast-gates and operations contract prep plan. |
| `finished-plans/phase5_finishedplan_read_only_operations_status_report.md` | Historical read-only operations status report plan. |
| `finished-plans/phase6_finishedplan_iscsi_os_initiator_compatibility.md` | Historical iSCSI OS initiator compatibility plan closed by Linux and Windows validation. |
| `finished-plans/phase7_finishedplan_iscsi_session_backend_pressure.md` | Historical iSCSI session/backend pressure plan closed by fast protocol, L2 restart, and Linux OS initiator gates. |
| `ref/production-readiness-plan.md` | Longer readiness reference and iSCSI P1-P6 detail. |
| `ref/iscsi-os-initiator-compat-plan.md` | Detailed iSCSI P1 OS-initiator compatibility plan and evidence. |
| `ref/iscsi-v2-coverage-gap-audit.md` | V2-to-V3 iSCSI coverage gap audit and prioritized test backlog. |
| `ref/v2-frontend-protocol-gap-audit.md` | Broader V2 frontend protocol feature gap audit. |

