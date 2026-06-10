# Phase 37 Finished Plan: Live Node Evidence Hardening

Status: closed on 2026-06-09.

Branch: `phase33-testops-failure-hardening`.

## Goal

Close the live node evidence gap before adding mutating operator behavior. The
phase focused on making Kubernetes node, CSI, image, host-prereq, and loopback
facts visible through the same negative-first status model used by volumes.

## Delivered

- Live Kubernetes node enrichment for Ready and SchedulingDisabled.
- Live CSI evidence for CSIDriver/CSINode registration and CSI node pod health.
- Live image-pull evidence, including `ImagePullBackOff`, `ErrImagePull`, and
  `ErrImageNeverPull`, projected as `image_missing_on_node`.
- Shared live enrichment across operator-status, report, dashboard, and explain.
- Read-only host prerequisite artifact replay through
  `host/host-prereq-summary.txt`, projecting `iscsi_prereq_missing` and
  `multipath_prereq_missing`.
- Loopback cross-node artifact replay through
  `unsupported-cross-node-loopback-attach.txt`, projecting
  `publish_target_loopback_cross_node`.
- Runner-driven D5 E2E gates for same-node loopback success and cross-node
  loopback refusal.
- Chart README clarification: raw Helm defaults are render/development defaults;
  supported alpha installs use generated `values.day1.yaml`.

## QA Evidence

- D2 live node/CSI evidence: PASS on `052b321`.
- D3 CSI image-pull node blockers: PASS on `43d7786`.
- D4 host prereq replay: PASS on `f6a8378`.
- D5 same-node loopback runner E2E: PASS, 47/47 actions.
- D5 cross-node loopback negative runner E2E: PASS, 34/34 actions.
- Final cleanup: zero Kubernetes, iSCSI, multipath, process, and hostPath
  residue in the D4/D5 sign-off.

Sign-off documents:

- `internal/docs/qa-assignments/phase37-d2-live-node-csi-evidence-qa-signoff.md`
- `internal/docs/qa-assignments/phase37-d3-csi-image-pull-node-blockers-qa-signoff.md`
- `internal/docs/qa-assignments/phase37-d4-d5-host-prereq-loopback-qa-signoff.md`

## Non-Claims

- No privileged host probing was added to the operator.
- No host repair, image import, cleanup, finalizer, rebuild, failback, backup,
  restore, or upgrade mutation was added.
- Raw `helm install sw-block charts/seaweed-block` remains a render/development
  default path, not a release-gated user install.
- Host prereq live collection still needs a safe source such as CSI-node facts,
  a future node agent, or explicit support bundle collection.

## Follow-Ups

- Collapse duplicate per-node `Ready` conditions into one Kubernetes-style
  condition per type.
- Add server-side-dry-run/envtest coverage for CRD status payloads so schema
  drift is caught before live QA.
- Document the `swblock` runner build/provisioning path in the QA runbook.
- Begin the lifecycle action contract phase: dry-run actions, rejected-action
  gates, executor authority, and evidence refs before finalizer/delete safety.
